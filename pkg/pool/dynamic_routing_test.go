// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package pool

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newDynamicRoutingHealth(t *testing.T) (*HealthWorker, *PoolHealth, *atomic.Bool) {
	t.Helper()
	s := newTestPoolHealth("A", "B")
	s.sqlHealthState = NewHealthState()
	s.sqlHealthState.Track([]string{"A", "B"})
	s.hybridPolicy = newHybridHealthPolicy(true)
	s.routingRequested = true
	flag := &atomic.Bool{}
	flag.Store(true)
	s.routingEnabledFn = func(context.Context) SQLHealthFeatureFlagEvaluation { return validSQLHealthFeatureFlag(flag.Load()) }
	w := newTestWorker(nil)
	w.applyPatroniHealth(s, map[string]bool{"A": true, "B": false}, nil, []string{"A", "B"}, time.Now())
	return w, s, flag
}

func TestDynamicRoutingRequiresEnabledChecks(t *testing.T) {
	w := newTestWorker(newMockProber(map[string][]string{"db.example.com": {"A", "B"}}, "A"))
	w.newSQLChecker = func(context.Context) *SQLHealthChecker {
		return newTestSQLHealthChecker(t, &fakeSQLHealthProber{
			responses: map[string][]fakeSQLHealthProbeResponse{
				"firstA": {{inRecovery: true}}, "firstB": {{}},
				"secondA": {{inRecovery: true}}, "secondB": {{}},
			},
			calls: make(map[string]int),
		}, 1, SQLHealthCheckHooks{})
	}
	var checks [2]atomic.Bool
	var health *PoolHealth
	for i, credential := range []string{"first", "second"} {
		checks[i].Store(true)
		sub := w.Subscribe("db.example.com", false, "db", nil, &SQLHealthConfig{
			ConnStringForIP: func(ip string) string { return credential + ip },
			ChecksEnabled: func(context.Context) SQLHealthFeatureFlagEvaluation {
				return validSQLHealthFeatureFlag(checks[i].Load())
			},
			RoutingEnabled: func(context.Context) SQLHealthFeatureFlagEvaluation { return validSQLHealthFeatureFlag(true) },
		})
		health = receivePoolHealth(t, sub)
		t.Cleanup(func() { w.Unsubscribe(sub) })
	}
	w.RefreshSQL()
	require.True(t, health.IsHealthy("B"))
	checks[0].Store(false)
	w.RefreshSQL()
	assert.True(t, health.sqlRoutingActive, "another enabled credential can continue routing")
	checks[1].Store(false)
	w.RefreshSQL()
	assert.True(t, health.IsHealthy("A"), "no enabled checks restores Patroni")
	assert.False(t, health.hybridPolicy.isRoutingEnabled())
	checks[0].Store(true)
	w.RefreshSQL()
	assert.True(t, health.IsHealthy("B"), "fresh enabled checks can restore SQL ownership")
}

func TestDynamicRoutingIgnoresOlderChecksDisable(t *testing.T) {
	w, s, _ := newDynamicRoutingHealth(t)
	target := &sqlHealthTarget{poolHealth: s}
	generation, older := s.beginSQLChecksEvaluation(target)
	_, newer := s.beginSQLChecksEvaluation(target)
	_, _, current := s.reserveSQLComparisonPoll(target, time.Now(), generation, true, newer)
	require.True(t, current)
	_, _, current = s.reserveSQLComparisonPoll(target, time.Now(), generation, false, older)
	assert.False(t, current, "an older checks disable cannot invalidate a newer enable")
	w.syncSQLChecksRoutingMode(sqlHealthGroup{poolHealth: s, targets: []*sqlHealthTarget{target}})
	assert.True(t, s.hybridPolicy.isRoutingEnabled())
	assert.Zero(t, s.routingGeneration, "the stale evaluation must not reset routing or timing")
}

func TestDynamicRoutingOrdersConcurrentFlagEvaluations(t *testing.T) {
	for _, tc := range []struct {
		name           string
		initial, older bool
		newer          SQLHealthFeatureFlagEvaluation
		want           bool
	}{
		{"old enable after disable", false, true, validSQLHealthFeatureFlag(false), false},
		{"old disable after enable", false, false, validSQLHealthFeatureFlag(true), true},
		{"new error preserves enabled", true, false, SQLHealthFeatureFlagEvaluation{}, true},
		{"new error preserves disabled", false, true, SQLHealthFeatureFlagEvaluation{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, s, _ := newDynamicRoutingHealth(t)
			s.hybridPolicy = newHybridHealthPolicy(tc.initial)
			s.routingRequested = tc.initial
			started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once sync.Once
			t.Cleanup(func() { once.Do(func() { close(release) }) })
			var calls atomic.Int32
			s.routingEnabledFn = func(context.Context) SQLHealthFeatureFlagEvaluation {
				if calls.Add(1) == 1 {
					close(started)
					<-release
					return validSQLHealthFeatureFlag(tc.older)
				}
				return tc.newer
			}
			go func() { defer close(done); w.syncSQLRoutingMode(s) }()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("flag evaluation did not start")
			}
			w.syncSQLRoutingMode(s)
			once.Do(func() { close(release) })
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("flag evaluation did not finish")
			}
			assert.Equal(t, tc.want, s.hybridPolicy.isRoutingEnabled())
		})
	}
}

func TestDynamicRoutingFiltersCachedAndInFlightEvidence(t *testing.T) {
	w, s, flag := newDynamicRoutingHealth(t)
	old := &sqlHealthTarget{poolHealth: s}
	late := &sqlHealthTarget{poolHealth: s}
	fresh := &sqlHealthTarget{poolHealth: s}
	oldRoles := []SQLHealthCheckResult{{IP: "A", InRecovery: true}, {IP: "B"}}
	at := time.Now()
	s.recordSQLComparisonPoll(false, old, 0, at, at, []string{"A", "B"}, SQLHealthCheckOutcome{Results: oldRoles})
	for range 2 { // Include rapid on/off/on, not only one transition.
		flag.Store(false)
		w.syncSQLRoutingMode(s)
		flag.Store(true)
		w.syncSQLRoutingMode(s)
	}
	require.Equal(t, uint64(4), s.routingGeneration)
	// An old poll can have a later timestamp after a clock regression. Its generation
	// must still be rejected, while genuinely fresh polls can resume immediately.
	w.completeSQLHealthCheck(late, 0, time.Now().Add(time.Hour), []string{"A", "B"}, map[string]struct{}{"A": {}, "B": {}}, SQLHealthCheckOutcome{
		Results: []SQLHealthCheckResult{{IP: "A", Err: &pgconn.PgError{Code: "57P02"}}, {IP: "B"}},
	})
	require.True(t, s.IsHealthy("A"))
	require.Empty(t, s.hybridPolicy.routingBannedIPs, "a previous period cannot create a ban")
	w.completeSQLHealthCheck(fresh, 0, time.Now(), []string{"A", "B"}, map[string]struct{}{"A": {}, "B": {}}, SQLHealthCheckOutcome{
		routingGeneration: s.routingGeneration,
		StartedAt:         time.Now(), Results: []SQLHealthCheckResult{{IP: "A"}, {IP: "B", InRecovery: true}},
	})
	assert.True(t, s.sqlRoutingActive, "old cached B must not conflict with fresh A")
	assert.True(t, s.IsHealthy("A"))
	assert.Len(t, s.sqlComparisonLatest, 3, "raw shadow evidence remains available")

	// An obsolete empty completion cannot restore Patroni over a newer SQL route.
	s.publishRoutingSnapshot(map[string]bool{"B": true}, []string{"B"})
	s.sqlHealthState = NewHealthState()
	w.completeSQLHealthCheck(late, 0, time.Now(), nil, nil, SQLHealthCheckOutcome{})
	assert.True(t, s.IsHealthy("B"))
	assert.True(t, s.sqlRoutingActive)
}

func TestDynamicRoutingClearsHistoryBansAndQueuedTiming(t *testing.T) {
	w, s, flag := newDynamicRoutingHealth(t)
	p := s.hybridPolicy
	at := time.Now()
	p.observeSQL([]string{"A", "B"}, SQLHealthCheckOutcome{PrimaryIPs: []string{"B"}}, at)
	p.observeSQLShutdowns([]string{"A", "B"}, SQLHealthResultsSnapshot{
		ObservedAt: at, Results: []SQLHealthCheckResult{{IP: "A", Err: &pgconn.PgError{Code: "57P02"}}},
	})
	p.recordRoutingLeaderLocked("hybrid", nil, at.Add(time.Second), SQLRoutingResultConclusive)
	require.False(t, p.routingDowntimeStartedAt.IsZero())
	var changes []SQLRoutingBanChange
	var comparisons int
	w.sqlHooks.RoutingBanChanged = func(change SQLRoutingBanChange, _ []string) {
		if assert.True(t, s.routingMu.TryLock(), "mode-change hooks run outside routing locks") {
			s.routingMu.Unlock()
		}
		if assert.True(t, s.sqlComparisonMu.TryLock(), "mode-change hooks run outside comparison locks") {
			s.sqlComparisonMu.Unlock()
		}
		changes = append(changes, change)
	}
	w.sqlHooks.LeaderRoutingCompared = func(LeaderRoutingComparison, []string) { comparisons++ }
	flag.Store(false)
	w.syncSQLRoutingMode(s)
	assert.True(t, s.IsHealthy("A"), "disable restores raw Patroni even when A was banned")
	assert.Empty(t, p.awaitingPatroniLeader)
	assert.Empty(t, p.supersededPatroniLeader)
	assert.Empty(t, p.supersededSQLLeaders)
	assert.False(t, p.patroniRecoveryActive)
	assert.True(t, p.sqlNoPrimaryAt.IsZero())
	assert.True(t, p.patroniRecoveryAt.IsZero())
	assert.True(t, p.routingDowntimeStartedAt.IsZero())
	assert.Empty(t, p.routingTiming.pending)
	require.Equal(t, []SQLRoutingBanChange{{IP: "A", Reason: SQLRoutingUnbanReasonRoutingModeChanged}}, changes)
	// Off-mode ban observations stay diagnostic and are cleared again on enable.
	p.observeSQLShutdowns([]string{"A", "B"}, SQLHealthResultsSnapshot{
		ObservedAt: time.Now(), Results: []SQLHealthCheckResult{{IP: "A", Err: &pgconn.PgError{Code: "57P02"}}},
	})
	flag.Store(true)
	w.syncSQLRoutingMode(s)
	assert.Empty(t, p.routingBannedIPs)
	assert.True(t, s.IsHealthy("A"))
	require.Len(t, changes, 2)
	w.reportHybridDecision(s, hybridRoutingDecision{routingComparison: &LeaderRoutingComparison{HasDowntimeComparison: true}}, false)
	assert.Zero(t, comparisons, "a queued comparison from an old period cannot emit after re-enable")
	w.reportHybridDecision(s, hybridRoutingDecision{routingGeneration: s.routingGeneration, routingComparison: &LeaderRoutingComparison{}}, false)
	assert.Equal(t, 1, comparisons)
}

func TestDynamicRoutingTimingWaitsForFreshAgreement(t *testing.T) {
	p := newHybridHealthPolicy(true)
	at := time.Now()
	ips := []string{"A", "B", "C", "D"}
	observeSQL := func(leader string, offset time.Duration) hybridRoutingDecision {
		outcome := SQLHealthCheckOutcome{StartedAt: at.Add(offset)}
		if leader != "" {
			outcome.PrimaryIPs = []string{leader}
		}
		return p.observeSQL(ips, outcome, at.Add(offset))
	}
	p.observePatroni(map[string]bool{"A": true}, at)
	observeSQL("A", 0)
	observeSQL("", time.Second)
	observeSQL("B", 2*time.Second)
	p.setRoutingEnabled(false)
	p.setRoutingEnabled(true)
	require.Nil(t, p.observePatroni(map[string]bool{"C": true}, at.Add(time.Hour)).routingComparison)
	require.Nil(t, observeSQL("B", time.Hour+time.Second).routingComparison)
	require.Nil(t, observeSQL("C", time.Hour+2*time.Second).routingComparison)
	require.False(t, p.routingTimingNeedsBaseline)
	observeSQL("", time.Hour+3*time.Second)
	observeSQL("D", time.Hour+4*time.Second)
	comparison := p.observePatroni(map[string]bool{"D": true}, at.Add(time.Hour+5*time.Second)).routingComparison
	require.NotNil(t, comparison)
	require.True(t, comparison.HasDowntimeComparison)
	assert.Equal(t, time.Second, comparison.HybridDowntime)
	assert.Equal(t, 2*time.Second, comparison.PatroniDowntime)
	assert.Equal(t, float64(50), comparison.DowntimeReductionPercent)
}
