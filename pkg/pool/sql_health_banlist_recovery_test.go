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
	"github.com/stretchr/testify/require"
)

// Shared fixture for real worker scheduling and availability tests.
func newSQLRecoveryWorker(t *testing.T, ips ...string) (*HealthWorker, *PoolHealth, *credentialSQLHealthProber, HealthUpdates) {
	t.Helper()
	w := newTestWorker(newMockProber(map[string][]string{"db.example.com": ips}, ips[0]))
	probe := &credentialSQLHealthProber{responses: make(map[string]fakeSQLHealthProbeResponse)}
	for i, ip := range ips {
		probe.responses[ip] = fakeSQLHealthProbeResponse{inRecovery: i != 0}
	}
	w.newSQLChecker = func(context.Context) *SQLHealthChecker {
		return newTestSQLHealthChecker(t, probe, 1, SQLHealthCheckHooks{})
	}
	updates := w.Subscribe("db.example.com", false, "db", nil, &SQLHealthConfig{
		ConnStringForIP: func(ip string) string { return ip },
		RoutingEnabled:  func(context.Context) SQLHealthFeatureFlagEvaluation { return validSQLHealthFeatureFlag(true) },
	})
	t.Cleanup(func() { w.Unsubscribe(updates) })
	health := receivePoolHealth(t, updates)
	w.RefreshSQL()
	return w, health, probe, updates
}

func TestSplitSQLPollReportsMultiplePrimariesWithoutDuplicates(t *testing.T) {
	const a, b, c, d = "10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4"
	for _, tt := range []struct {
		name      string
		banned    []string
		primaries []string
		reports   [][]string
	}{
		{"one primary in each half", []string{a}, []string{a, b}, [][]string{{a, b}}},
		{"routing already reported", []string{a}, []string{a, b, c}, [][]string{{b, c}}},
		{"recovery already reported", []string{a, b}, []string{a, b, c}, [][]string{{a, b}}},
		{"both halves already reported", []string{a, b}, []string{a, b, c, d}, [][]string{{a, b}, {c, d}}},
		{"only one primary", []string{a}, []string{b}, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			w, health, probe, _ := newSQLRecoveryWorker(t, a, b, c, d)
			probe.mu.Lock()
			for _, ip := range tt.banned {
				probe.responses[ip] = fakeSQLHealthProbeResponse{err: &pgconn.PgError{Code: "57P01"}}
			}
			probe.mu.Unlock()
			w.RefreshSQL()
			for _, ip := range tt.banned {
				require.Contains(t, health.hybridPolicy.routingBanSnapshot(), ip)
			}

			var mu sync.Mutex
			var reports [][]string
			var reportTags [][]string
			target := health.sqlTargetValues()[0]
			target.checker.hooks.MultiplePrimaries = func(ips, tags []string) {
				mu.Lock()
				defer mu.Unlock()
				reports = append(reports, append([]string(nil), ips...))
				reportTags = append(reportTags, append([]string(nil), tags...))
			}
			probe.mu.Lock()
			for _, ip := range []string{a, b, c, d} {
				probe.responses[ip] = fakeSQLHealthProbeResponse{inRecovery: true}
			}
			for _, ip := range tt.primaries {
				probe.responses[ip] = fakeSQLHealthProbeResponse{}
			}
			probe.mu.Unlock()
			w.RefreshSQL()

			mu.Lock()
			defer mu.Unlock()
			require.ElementsMatch(t, tt.reports, reports)
			for _, tags := range reportTags {
				require.ElementsMatch(t, append(append([]string(nil), target.tags...), "pool_role:primary"), tags)
			}
		})
	}
}

func TestBannedRecoveryDoesNotBlockRoutingOrAvailability(t *testing.T) {
	const a, b, c = "10.0.0.1", "10.0.0.2", "10.0.0.3"
	w, health, probe, updates := newSQLRecoveryWorker(t, a, b, c)
	probe.mu.Lock()
	probe.responses[a] = fakeSQLHealthProbeResponse{err: &pgconn.PgError{Code: "57P01"}}
	probe.mu.Unlock()
	w.RefreshSQL()
	require.Contains(t, health.hybridPolicy.routingBanSnapshot(), a)
	subscriber, err := health.SubscribeSQLHealth(updates)
	require.NoError(t, err)
	t.Cleanup(subscriber.Close)
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(func() {
		unblock()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("recovery did not finish after release")
		}
	})
	probe.mu.Lock()
	probe.responses[a] = fakeSQLHealthProbeResponse{err: context.DeadlineExceeded}
	probe.responses[b] = fakeSQLHealthProbeResponse{}
	probe.started = map[string]chan struct{}{a: started}
	probe.release = map[string]chan struct{}{a: release}
	probe.startOnce = map[string]*sync.Once{a: {}}
	before := probe.calls[a]
	probe.mu.Unlock()
	var raw, missed atomic.Int64
	w.hooks.SQLPatroniCompared = func(SQLPatroniHealthSnapshot, []string) { raw.Add(1) }
	w.hooks.SQLCheckMissed = func([]string) { missed.Add(1) }
	go func() { defer close(done); w.RefreshSQL() }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("recovery did not start")
	}
	baseline := receiveSQLHealthObservation(t, subscriber)
	require.True(t, baseline.Writable)
	target := health.sqlTargetValues()[0]
	require.Eventually(t, func() bool { return health.IsHealthy(b) && !target.sqlRoundRunning() }, time.Second, time.Millisecond)
	require.Zero(t, raw.Load(), "raw comparisons wait for the original complete poll")

	// Registering another subscriber must not queue a writer behind recovery
	// and prevent the following routing tick from taking its read lock.
	subscribed := make(chan error, 1)
	go func() {
		another, err := health.SubscribeSQLHealth(updates)
		if err == nil {
			another.Close()
		}
		subscribed <- err
	}()
	select {
	case err := <-subscribed:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("subscriber waited for banned-IP recovery")
	}

	// A second routing tick must run while the original recovery is blocked.
	probe.mu.Lock()
	probe.responses[b] = fakeSQLHealthProbeResponse{inRecovery: true}
	probe.responses[c] = fakeSQLHealthProbeResponse{}
	probe.mu.Unlock()
	w.runSQLHealthGroup(w.sqlHealthGroups()[0], time.Now(), false)
	require.Eventually(t, func() bool { return health.IsHealthy(c) && !health.IsHealthy(b) && !target.sqlRoundRunning() }, time.Second, time.Millisecond)
	require.True(t, receiveSQLHealthObservation(t, subscriber).Writable)

	// Losing the eligible primary must replace the earlier writable observation.
	probe.mu.Lock()
	probe.responses[c] = fakeSQLHealthProbeResponse{inRecovery: true}
	probe.mu.Unlock()
	w.RefreshSQL()
	unavailable := receiveSQLHealthObservation(t, subscriber)
	require.False(t, unavailable.Writable)
	require.ErrorIs(t, unavailable.Err, ErrSQLHealthCheckMissed)
	require.True(t, unavailable.ScheduledAt.After(baseline.ScheduledAt))
	require.Equal(t, unavailable.ScheduledAt, unavailable.ObservedAt)
	require.Zero(t, raw.Load())
	require.Zero(t, missed.Load(), "eligible probes ran; the scheduler did not miss this tick")

	probe.mu.Lock()
	probe.responses[c] = fakeSQLHealthProbeResponse{}
	probe.mu.Unlock()
	w.RefreshSQL()
	healthy := receiveSQLHealthObservation(t, subscriber)
	require.True(t, healthy.Writable)
	require.True(t, healthy.ScheduledAt.After(unavailable.ScheduledAt))
	select {
	case extra := <-subscriber.Updates():
		t.Fatalf("unexpected observation after writable result: %+v", extra)
	default:
	}
	require.Equal(t, before+1, probe.callCount(a), "do not start duplicate recovery probes")

	// Shutdown still needs exclusive access and must wait for the active probe.
	closed := make(chan struct{})
	go func() {
		target.closeSQL()
		close(closed)
	}()
	require.Eventually(t, func() bool {
		if target.sqlCheckMu.TryRLock() {
			target.sqlCheckMu.RUnlock()
			return false
		}
		return true
	}, time.Second, time.Millisecond, "shutdown did not queue its exclusive lock")
	select {
	case <-closed:
		t.Fatal("checker closed while recovery was still probing")
	default:
	}
	unblock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("old recovery poll did not finish")
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("checker did not close after recovery finished")
	}
	select {
	case <-subscriber.Done():
	default:
		t.Fatal("shutdown left its subscriber open")
	}
	_, err = health.SubscribeSQLHealth(updates)
	require.ErrorIs(t, err, ErrSQLHealthUpdatesUnsupported)
	require.EqualValues(t, 1, raw.Load())
	require.True(t, health.IsHealthy(c), "the older raw completion must not restore B")
	require.Contains(t, health.hybridPolicy.routingBanSnapshot(), a)
	select {
	case extra := <-subscriber.Updates():
		t.Fatalf("old completion overwrote current availability: %+v", extra)
	default:
	}
}

func TestBusyRecoveryDoesNotPublishInvalidatedAvailability(t *testing.T) {
	for _, retired := range []bool{false, true} {
		name := "disabled"
		if retired {
			name = "retired"
		}
		t.Run(name, func(t *testing.T) {
			const a, b = "10.0.0.1", "10.0.0.2"
			w, health, probe, updates := newSQLRecoveryWorker(t, a, b)
			subscriber, err := health.SubscribeSQLHealth(updates)
			require.NoError(t, err)
			t.Cleanup(subscriber.Close)
			target := health.sqlTargetValues()[0]
			health.hybridPolicy.routingBannedIPs[a] = time.Now()
			target.recoveryRunning = true // Model an already active recovery from a prior tick.
			started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			t.Cleanup(func() {
				unblock()
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Error("invalidated poll did not finish after release")
				}
			})
			probe.mu.Lock()
			probe.started = map[string]chan struct{}{b: started}
			probe.release = map[string]chan struct{}{b: release}
			probe.startOnce = map[string]*sync.Once{b: {}}
			probe.mu.Unlock()
			go func() { defer close(done); w.RefreshSQL() }()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("eligible probe did not start")
			}
			if retired {
				health.retireSQLComparisonTarget(target)
			} else {
				health.disableSQLComparisonTarget(target)
			}
			unblock()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("invalidated poll did not finish")
			}
			select {
			case extra := <-subscriber.Updates():
				t.Fatalf("invalidated poll published availability: %+v", extra)
			default:
			}
		})
	}
}

func TestRecoveryUnbanRequiresNewCompleteEligibleCoverage(t *testing.T) {
	const a, b = "10.0.0.1", "10.0.0.2"
	health := newTestPoolHealth(a, b)
	health.sqlHealthState = NewHealthState()
	health.sqlHealthState.Track([]string{a, b})
	health.hybridPolicy = newHybridHealthPolicy(true)
	health.routingEnabledFn = func(context.Context) SQLHealthFeatureFlagEvaluation { return validSQLHealthFeatureFlag(true) }
	w := newTestWorker(nil)
	now := time.Now()
	target := &sqlHealthTarget{poolHealth: health}
	health.hybridPolicy.routingBannedIPs[a] = now.Add(-time.Second)
	w.applyPatroniHealth(health, map[string]bool{a: false, b: true}, nil, []string{a, b}, now)
	retained := map[string]struct{}{a: {}, b: {}}
	outcome := SQLHealthCheckOutcome{StartedAt: now, PrimaryIPs: []string{a}, Results: []SQLHealthCheckResult{{IP: a, StartedAt: now, CompletedAt: now}}}
	w.completeSQLHealthCheckMode(target, 0, now, []string{a, b}, retained, outcome, sqlRecoveryPoll, nil)
	require.NotContains(t, health.hybridPolicy.routingBanSnapshot(), a)
	require.False(t, health.IsHealthy(a), "recovery alone must not publish a newly unbanned primary")
	// This routing poll excluded A before it was unbanned. Cached recovery evidence cannot fill it in.
	w.completeSQLHealthCheckMode(target, 0, now, []string{a, b}, retained, SQLHealthCheckOutcome{StartedAt: now, PrimaryIPs: []string{b}, Results: []SQLHealthCheckResult{{IP: b}}}, sqlRoutingPoll, nil)
	require.False(t, health.IsHealthy(a))
	require.False(t, health.sqlRoutingActive, "an unchecked newly eligible IP prevents SQL publication")
	// A later poll checks both and sees two primaries: it must use Patroni fallback.
	w.completeSQLHealthCheck(target, 0, now.Add(time.Second), []string{a, b}, retained, SQLHealthCheckOutcome{StartedAt: now.Add(time.Second), PrimaryIPs: []string{a, b}, Results: []SQLHealthCheckResult{{IP: a}, {IP: b}}})
	require.False(t, health.sqlRoutingActive)
	require.False(t, health.IsHealthy(a))
	require.True(t, health.IsHealthy(b))
}

func TestRecoveryKeepsOtherCredentialsShutdownEvidence(t *testing.T) {
	const a, b = "10.0.0.1", "10.0.0.2"
	ips := []string{a, b}
	retained := map[string]struct{}{a: {}, b: {}}
	health := newTestPoolHealth(a, b)
	health.sqlHealthState = NewHealthState()
	health.sqlHealthState.Track(ips)
	health.hybridPolicy = newHybridHealthPolicy(true)
	health.routingEnabledFn = func(context.Context) SQLHealthFeatureFlagEvaluation { return validSQLHealthFeatureFlag(true) }
	w := newTestWorker(nil)
	now := time.Now()
	w.applyPatroniHealth(health, map[string]bool{a: false, b: true}, nil, ips, now)
	first, second := &sqlHealthTarget{poolHealth: health}, &sqlHealthTarget{poolHealth: health}
	shutdown := SQLHealthCheckOutcome{StartedAt: now, PrimaryIPs: []string{b}, Results: []SQLHealthCheckResult{{IP: a, Err: &pgconn.PgError{Code: "57P01"}}, {IP: b}}}
	w.completeSQLHealthCheck(first, 0, now, ips, retained, shutdown)
	w.completeSQLHealthCheck(second, 0, now, ips, retained, shutdown)
	recovery := SQLHealthCheckOutcome{StartedAt: now.Add(time.Second), Results: []SQLHealthCheckResult{{IP: a, InRecovery: true}}}
	w.completeSQLHealthCheckMode(first, 0, now.Add(time.Second), ips, retained, recovery, sqlRecoveryPoll, nil)
	require.Contains(t, health.hybridPolicy.routingBanSnapshot(), a, "the second credential still has shutdown evidence")
	w.completeSQLHealthCheckMode(second, 0, now.Add(2*time.Second), ips, retained, SQLHealthCheckOutcome{StartedAt: now.Add(2 * time.Second), Results: []SQLHealthCheckResult{{IP: b}}}, sqlRoutingPoll, nil)
	w.completeSQLHealthCheckMode(first, 0, now.Add(3*time.Second), ips, retained, recovery, sqlRecoveryPoll, nil)
	require.Contains(t, health.hybridPolicy.routingBanSnapshot(), a, "routing polls must preserve cached shutdown evidence on excluded IPs")
	w.completeSQLHealthCheckMode(second, 0, now.Add(4*time.Second), ips, retained, recovery, sqlRecoveryPoll, nil)
	require.NotContains(t, health.hybridPolicy.routingBanSnapshot(), a)
	require.True(t, health.IsHealthy(b))
}

func TestLateRecoveryCannotChangeDisabledTarget(t *testing.T) {
	const ip = "10.0.0.1"
	health := newTestPoolHealth(ip)
	health.sqlHealthState = NewHealthState()
	health.sqlHealthState.Track([]string{ip})
	health.hybridPolicy = newHybridHealthPolicy(true)
	target := &sqlHealthTarget{poolHealth: health}
	w := newTestWorker(nil)
	now := time.Now()
	health.disableSQLComparisonTarget(target)
	outcome := SQLHealthCheckOutcome{StartedAt: now, Results: []SQLHealthCheckResult{{IP: ip, Err: &pgconn.PgError{Code: "57P01"}}}}
	w.completeSQLHealthCheckMode(target, 0, now, []string{ip}, map[string]struct{}{ip: {}}, outcome, sqlRecoveryPoll, nil)
	require.Empty(t, health.hybridPolicy.routingBanSnapshot())
	require.False(t, w.recordSQLCompletePoll(target, 0, now, now, []string{ip}, outcome))
	require.Empty(t, health.sqlComparisonLatest)
	require.Empty(t, health.sqlRoutingLatest)
}

func TestRetentionKeepsPoolsUsedByRecovery(t *testing.T) {
	const ip = "10.0.0.1"
	pools := newTestSQLHealthCheckPools(t)
	_, err := pools.GetOrAdd(ip, testSQLHealthCheckConnString)
	require.NoError(t, err)
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	probe := &credentialSQLHealthProber{
		responses: map[string]fakeSQLHealthProbeResponse{ip: {inRecovery: true}},
		started:   map[string]chan struct{}{ip: started}, release: map[string]chan struct{}{ip: release}, startOnce: map[string]*sync.Once{ip: {}},
	}
	checker := NewSQLHealthChecker(context.Background(), SQLHealthCheckPoolsConfig{}, SQLHealthCheckerConfig{Pools: pools, Prober: probe})
	done := make(chan struct{})
	go func() {
		defer close(done)
		checker.Check(context.Background(), []string{ip}, func(string) string { return ip }, nil, nil, nil)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("recovery probe did not start")
	}
	checker.RetainIPs(nil)
	require.Equal(t, 1, pools.Len(), "DNS eviction must not close a pool while recovery is using it")
	unblock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("recovery probe did not finish")
	}
	checker.RetainIPs(nil)
	require.Zero(t, pools.Len(), "the next retention pass must close the departed IP's idle pool")
}

func TestRecoveryDoesNotEvaluateTheRoutingFlag(t *testing.T) {
	const ip = "10.0.0.1"
	health := newTestPoolHealth(ip)
	health.sqlHealthState = NewHealthState()
	health.sqlHealthState.Track([]string{ip})
	health.hybridPolicy = newHybridHealthPolicy(true)
	health.hybridPolicy.routingBannedIPs[ip] = time.Now().Add(-time.Second)
	health.routingEnabledFn = func(context.Context) SQLHealthFeatureFlagEvaluation {
		t.Error("recovery must not block routing through the flag evaluator")
		return validSQLHealthFeatureFlag(true)
	}
	target := &sqlHealthTarget{poolHealth: health}
	now := time.Now()
	w := newTestWorker(nil)
	w.completeSQLHealthCheckMode(target, 0, now, []string{ip}, map[string]struct{}{ip: {}}, SQLHealthCheckOutcome{StartedAt: now, Results: []SQLHealthCheckResult{{IP: ip, InRecovery: true}}}, sqlRecoveryPoll, nil)
	require.Empty(t, health.hybridPolicy.routingBanSnapshot())
}

func TestCapturedBansDoNotReviveAnotherCredentialsCachedPrimary(t *testing.T) {
	const a, b = "10.0.0.1", "10.0.0.2"
	ips := []string{a, b}
	base := time.Now().Add(-time.Minute)
	at := func(n int) time.Time { return base.Add(time.Duration(n) * time.Second) }
	health := newTestPoolHealth(a, b)
	health.sqlHealthState = NewHealthState()
	health.sqlHealthState.Track(ips)
	first, second := &sqlHealthTarget{poolHealth: health}, &sqlHealthTarget{poolHealth: health}
	result := func(ip string, start int, standby bool, err error) SQLHealthCheckResult {
		return SQLHealthCheckResult{IP: ip, StartedAt: at(start), CompletedAt: at(start + 1), InRecovery: standby, Err: err}
	}
	health.sqlRoutingLatest = map[*sqlHealthTarget]SQLHealthResultsSnapshot{
		first: {StartedAt: at(0), ScheduledAt: at(0), Results: []SQLHealthCheckResult{result(a, 0, false, nil)}},
	}
	// This credential skips banned A, but keeps its raw primary answer for diagnostics.
	health.recordSQLRoutingPollLocked(first, at(20), at(21), SQLHealthCheckOutcome{
		StartedAt: at(20), Results: []SQLHealthCheckResult{result(b, 20, true, nil)},
	}, sqlRoutingPoll, []string{a})
	stored := health.sqlRoutingLatest[first]
	require.Equal(t, []string{a}, stored.BannedIPs)
	require.Len(t, stored.Results, 2)
	// Another credential supplies fresh standby evidence, allowing A's global ban to clear.
	health.recordSQLRoutingPollLocked(second, at(21), at(22), SQLHealthCheckOutcome{
		StartedAt: at(21), Results: []SQLHealthCheckResult{result(a, 21, true, nil), result(b, 21, true, nil)},
	}, sqlCompletePoll, nil)
	// Its next timeout replaces that standby. The first credential's old primary stays excluded.
	health.recordSQLRoutingPollLocked(second, at(23), at(24), SQLHealthCheckOutcome{
		StartedAt: at(23), Results: []SQLHealthCheckResult{result(a, 23, false, context.DeadlineExceeded), result(b, 23, true, nil)},
	}, sqlCompletePoll, nil)
	var all, eligible []SQLHealthCheckOutcome
	for _, snapshot := range health.sqlRoutingLatest {
		all = append(all, SQLHealthCheckOutcome{StartedAt: snapshot.StartedAt, Results: snapshot.Results})
		eligible = append(eligible, snapshot.routingOutcome(ips))
	}
	raw := aggregateSQLHealthCheckOutcomes(all).Outcome
	routing := aggregateSQLHealthCheckOutcomes(eligible).Outcome
	require.Contains(t, raw.PrimaryIPs, a, "raw evidence must remain available")
	require.NotContains(t, routing.PrimaryIPs, a)
	policy := newHybridHealthPolicy(true)
	policy.patroniHealth = map[string]bool{a: false, b: true}
	policy.patroniRecoveryAt = at(10)
	policy.patroniLeaderChangedAt = at(10)
	decision := policy.observeSQLRoundWithRoutingEvidence(ips, raw, &routing, SQLHealthResultsSnapshot{}, at(30))
	require.False(t, decision.health[a])
	require.True(t, decision.health[b], "the timeout must use Patroni rather than revive cached A")
	require.Equal(t, SQLRoutingResultCachedFallback, decision.routingResult)
	// This credential's own next probe replaces the cached role, even when it times out.
	health.recordSQLRoutingPollLocked(first, at(31), at(32), SQLHealthCheckOutcome{
		StartedAt: at(31), Results: []SQLHealthCheckResult{result(a, 31, false, context.DeadlineExceeded), result(b, 31, true, nil)},
	}, sqlCompletePoll, nil)
	require.Empty(t, health.sqlRoutingLatest[first].BannedIPs)
	require.Len(t, health.sqlRoutingLatest[first].routingOutcome(ips).Results, 2)
	// Captured exclusions must be owned copies.
	copy := cloneSQLHealthResultsSnapshot(stored)
	copy.BannedIPs[0] = b
	require.Equal(t, []string{a}, stored.BannedIPs)
}

func TestRoutingRecoveryKeepsItsOwnProbeStart(t *testing.T) {
	const a, b = "10.0.0.1", "10.0.0.2"
	ips := []string{a, b}
	base := time.Now().Add(-time.Minute)
	at := func(n int) time.Time { return base.Add(time.Duration(n) * time.Second) }
	health := newTestPoolHealth(a, b)
	health.sqlHealthState = NewHealthState()
	health.sqlHealthState.Track(ips)
	target := &sqlHealthTarget{poolHealth: health}
	health.recordSQLRoutingPollLocked(target, at(20), at(21), SQLHealthCheckOutcome{
		StartedAt: at(20), Results: []SQLHealthCheckResult{{IP: b, StartedAt: at(20), CompletedAt: at(21), InRecovery: true}},
	}, sqlRoutingPoll, []string{a})
	// Recovery began before Patroni's transition, but finishes after the newer routing poll.
	health.recordSQLRoutingPollLocked(target, at(5), at(25), SQLHealthCheckOutcome{
		StartedAt: at(5), Results: []SQLHealthCheckResult{{IP: a, StartedAt: at(5), CompletedAt: at(25)}},
	}, sqlRecoveryPoll, []string{a})
	stored := health.sqlRoutingLatest[target]
	require.Equal(t, at(20), stored.StartedAt)
	routing := aggregateSQLHealthCheckOutcomes([]SQLHealthCheckOutcome{stored.routingOutcome(ips)}).Outcome
	policy := newHybridHealthPolicy(true)
	policy.patroniRecoveryAt = at(10)
	decision := policy.observeSQLRoundWithRoutingEvidence(ips, routing, &routing, SQLHealthResultsSnapshot{}, at(30))
	require.Equal(t, SQLRoutingResultPatroniRecovery, decision.routingResult)
	require.False(t, decision.publish, "a later completion must not refresh an older probe's start")
}

func TestRoutingOverlappingResultsPreserveFallback(t *testing.T) {
	for _, recoveryArrivesLast := range []bool{false, true} {
		name := "recovery_arrives_first"
		if recoveryArrivesLast {
			name = "recovery_arrives_last"
		}
		t.Run(name, func(t *testing.T) {
			const a, b = "10.0.0.1", "10.0.0.2"
			ips := []string{a, b}
			retained := map[string]struct{}{a: {}, b: {}}
			base := time.Now().Add(-time.Minute)
			at := func(n int) time.Time { return base.Add(time.Duration(n) * time.Second) }
			health := newTestPoolHealth(a, b)
			health.sqlHealthState = NewHealthState()
			health.sqlHealthState.Track(ips)
			health.hybridPolicy = newHybridHealthPolicy(true)
			health.hybridPolicy.patroniHealth = map[string]bool{a: false, b: true}
			target := &sqlHealthTarget{poolHealth: health}
			primary := SQLHealthCheckOutcome{StartedAt: at(1), Results: []SQLHealthCheckResult{
				{IP: a, StartedAt: at(1), CompletedAt: at(5)},
			}}
			standby := SQLHealthCheckOutcome{StartedAt: at(3), Results: []SQLHealthCheckResult{
				{IP: a, StartedAt: at(3), CompletedAt: at(4), InRecovery: true},
			}}
			if recoveryArrivesLast {
				health.recordSQLRoutingPollLocked(target, at(3), at(4), standby, sqlRoutingPoll, nil)
				health.recordSQLRoutingPollLocked(target, at(1), at(5), primary, sqlRecoveryPoll, []string{a})
			} else {
				health.recordSQLRoutingPollLocked(target, at(1), at(5), primary, sqlRecoveryPoll, []string{a})
				health.recordSQLRoutingPollLocked(target, at(3), at(6), standby, sqlRoutingPoll, nil)
			}
			stored := health.sqlRoutingLatest[target]
			_, conflicts := aggregateSQLComparisonRoles(stored.Results)
			require.Equal(t, []string{a}, conflicts)
			require.Len(t, stored.Results, 2, "preserve both overlapping role observations")
			copy := cloneSQLHealthResultsSnapshot(stored)
			copy.Results[0].IP = b
			require.Equal(t, a, stored.Results[0].IP, "snapshot copies must own their results")

			w := newTestWorker(nil)
			var iterations []SQLIteration
			w.sqlHooks.Iteration = func(i SQLIteration, _ []string) { iterations = append(iterations, i) }
			// A timeout replaces the latest error, but cannot resolve overlapping roles.
			w.completeSQLHealthCheck(target, 0, at(6), ips, retained, SQLHealthCheckOutcome{StartedAt: at(6), Results: []SQLHealthCheckResult{
				{IP: a, StartedAt: at(6), CompletedAt: at(7), Err: context.DeadlineExceeded},
				{IP: b, StartedAt: at(6), CompletedAt: at(7), InRecovery: true},
			}})
			_, conflicts = aggregateSQLComparisonRoles(health.sqlRoutingLatest[target].Results)
			require.Equal(t, []string{a}, conflicts)
			require.Len(t, iterations, 1)
			require.Equal(t, SQLHealthFallbackConflictingRoles, iterations[0].FallbackReason)
			require.Equal(t, SQLRoutingResultCachedFallback, iterations[0].RoutingResult)
			require.False(t, health.IsHealthy(a))
			require.True(t, health.IsHealthy(b), "use eligible Patroni routing while SQL remains uncertain")

			// Starting exactly at the boundary still cannot prove a later observation.
			health.recordSQLRoutingPollLocked(target, at(5), at(8), SQLHealthCheckOutcome{StartedAt: at(5), Results: []SQLHealthCheckResult{
				{IP: a, StartedAt: at(5), CompletedAt: at(8), InRecovery: true},
			}}, sqlRecoveryPoll, nil)
			_, conflicts = aggregateSQLComparisonRoles(health.sqlRoutingLatest[target].Results)
			require.Equal(t, []string{a}, conflicts)
			w.completeSQLHealthCheck(target, 0, at(10), ips, retained, SQLHealthCheckOutcome{StartedAt: at(10), Results: []SQLHealthCheckResult{
				{IP: a, StartedAt: at(10), CompletedAt: at(11), InRecovery: true},
				{IP: b, StartedAt: at(10), CompletedAt: at(11)},
			}})
			_, conflicts = aggregateSQLComparisonRoles(health.sqlRoutingLatest[target].Results)
			require.Empty(t, conflicts)
			require.Len(t, iterations, 2)
			require.Empty(t, iterations[1].FallbackReason)
			require.Equal(t, SQLRoutingResultConclusive, iterations[1].RoutingResult)
			require.True(t, health.IsHealthy(b))

			// Membership and target cleanup also discard obsolete role observations.
			cached := health.sqlRoutingLatest[target]
			cached.Results = stored.Results
			health.sqlRoutingLatest[target] = cached
			health.sqlHealthState = NewHealthState()
			health.sqlHealthState.Track([]string{b})
			health.recordSQLRoutingPollLocked(target, at(13), at(14), SQLHealthCheckOutcome{StartedAt: at(13), Results: []SQLHealthCheckResult{
				{IP: b, StartedAt: at(13), CompletedAt: at(14)},
			}}, sqlCompletePoll, nil)
			_, conflicts = aggregateSQLComparisonRoles(health.sqlRoutingLatest[target].Results)
			require.Empty(t, conflicts)
			health.disableSQLComparisonTarget(target)
			require.NotContains(t, health.sqlRoutingLatest, target)
		})
	}
}

func TestRoutingResultMergeBoundsEvidence(t *testing.T) {
	const ip = "10.0.0.1"
	base := time.Now().Add(-time.Hour)
	at := func(n int) time.Time { return base.Add(time.Duration(n) * time.Millisecond) }
	// Keep both useful bounds of a role: its latest start can supersede an
	// opposite role even when another same-role observation finished later.
	merged := mergeSQLRoutingResults(
		[]SQLHealthCheckResult{{IP: ip, StartedAt: at(1), CompletedAt: at(10)}},
		[]SQLHealthCheckResult{{IP: ip, StartedAt: at(7), CompletedAt: at(8)}}, false)
	merged = mergeSQLRoutingResults(merged, []SQLHealthCheckResult{{IP: ip, StartedAt: at(3), CompletedAt: at(4), InRecovery: true}}, false)
	_, conflicts := aggregateSQLComparisonRoles(merged)
	require.Empty(t, conflicts, "the primary probe starting at 7 supersedes the standby ending at 4")

	// Repeated overlaps, unknown intervals, and errors must not accumulate history.
	merged = []SQLHealthCheckResult{{IP: ip, StartedAt: at(1), CompletedAt: at(5000)}}
	for i := 2; i < 1000; i++ {
		for _, result := range []SQLHealthCheckResult{
			{IP: ip, StartedAt: at(i), CompletedAt: at(5000 + i), InRecovery: true},
			{IP: ip}, {IP: ip, InRecovery: true},
			{IP: ip, Err: context.DeadlineExceeded},
		} {
			merged = mergeSQLRoutingResults(merged, []SQLHealthCheckResult{result}, true)
			require.LessOrEqual(t, len(merged), 7, "at most start/end/unknown per role plus the current error")
		}
	}
	reason, _ := sqlHealthFallback(aggregateSQLHealthCheckResults(merged).Outcome)
	require.Equal(t, SQLHealthFallbackConflictingRoles, reason)
}
