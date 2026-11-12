// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package pool

import (
	"context"
	"errors"
	"fmt"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHybridHealthShadowKeepsPatroniRouting(t *testing.T) {
	policy := newHybridHealthPolicy(false)
	patroniAt := time.Now()
	patroni := policy.observePatroni(map[string]bool{"10.0.0.1": true}, patroniAt)
	require.True(t, patroni.publish)
	assert.Equal(t, map[string]bool{"10.0.0.1": true}, patroni.health)

	decision := policy.observeSQL([]string{"10.0.0.1", "10.0.0.2"}, SQLHealthCheckOutcome{
		Results: []SQLHealthCheckResult{
			{IP: "10.0.0.1", InRecovery: true},
			{IP: "10.0.0.2"},
		},
		PrimaryIPs: []string{"10.0.0.2"},
	}, patroniAt.Add(time.Second))

	assert.Equal(t, SQLRoutingResultShadow, decision.routingResult)
	assert.False(t, decision.publish)
	assert.Nil(t, decision.health)
}

func TestHybridHealthPublishesConclusiveSQLRouting(t *testing.T) {
	policy := newHybridHealthPolicy(true)
	policy.observePatroni(map[string]bool{"10.0.0.1": true, "10.0.0.2": false}, time.Now())
	decision := policy.observeSQL([]string{"10.0.0.1", "10.0.0.2"}, SQLHealthCheckOutcome{
		Results: []SQLHealthCheckResult{
			{IP: "10.0.0.1", InRecovery: true},
			{IP: "10.0.0.2"},
		},
		PrimaryIPs: []string{"10.0.0.2"},
	}, time.Now())

	assert.True(t, decision.publish)
	assert.Equal(t, SQLRoutingResultConclusive, decision.routingResult)
	assert.Equal(t, map[string]bool{"10.0.0.1": false, "10.0.0.2": true}, decision.health)
	assert.Empty(t, decision.fallbackReason)
}

func TestHybridHealthTreatsNoPrimaryAsConclusive(t *testing.T) {
	policy := newHybridHealthPolicy(true)
	decision := policy.observeSQL([]string{"10.0.0.1", "10.0.0.2"}, SQLHealthCheckOutcome{
		Results: []SQLHealthCheckResult{
			{IP: "10.0.0.1", InRecovery: true},
			{IP: "10.0.0.2", InRecovery: true},
		},
	}, time.Now())

	assert.True(t, decision.publish)
	assert.Equal(t, SQLRoutingResultConclusive, decision.routingResult)
	assert.Equal(t, map[string]bool{"10.0.0.1": false, "10.0.0.2": false}, decision.health)
}

func policyWithSQLAheadOfPatroni(t *testing.T) (*hybridHealthPolicy, time.Time) {
	t.Helper()
	started := time.Now()
	policy := newHybridHealthPolicy(true)
	policy.observePatroni(map[string]bool{"A": true, "B": false}, started)
	policy.observeSQL([]string{"A", "B"}, SQLHealthCheckOutcome{PrimaryIPs: []string{"A"}}, started)
	selected := policy.observeSQL([]string{"A", "B"}, SQLHealthCheckOutcome{
		Results:    []SQLHealthCheckResult{{IP: "A", InRecovery: true}, {IP: "B"}},
		PrimaryIPs: []string{"B"},
	}, started.Add(time.Second))
	require.Equal(t, map[string]bool{"A": false, "B": true}, selected.health)
	return policy, started
}

func TestHybridHealthRetainsSQLLeaderUntilPatroniAgrees(t *testing.T) {
	for _, err := range []error{
		context.DeadlineExceeded,
		&pgconn.PgError{Code: "28P01"},
		&pgconn.PgError{Code: "58030"},
		&pgconn.PgError{Code: "XX000"},
	} {
		t.Run(err.Error(), func(t *testing.T) {
			policy, started := policyWithSQLAheadOfPatroni(t)
			uncertain := SQLHealthCheckOutcome{Results: []SQLHealthCheckResult{
				{IP: "A", InRecovery: true}, {IP: "B", Err: err},
			}}
			for tick := 2; tick <= 3; tick++ {
				at := started.Add(time.Duration(tick) * time.Second)
				oldPatroni := policy.observePatroni(map[string]bool{"A": true, "B": false}, at)
				assert.False(t, oldPatroni.publish)
				retained := policy.observeSQL([]string{"A", "B"}, uncertain, at.Add(time.Millisecond))
				assert.True(t, retained.publish)
				assert.Equal(t, SQLRoutingResultRetainedSQL, retained.routingResult)
				assert.NotEmpty(t, retained.fallbackReason)
				assert.Equal(t, map[string]bool{"A": false, "B": true}, retained.health)
				assert.Nil(t, retained.routingComparison, "retaining B is not another leader transition")
			}
			agreed := policy.observePatroni(map[string]bool{"A": false, "B": true}, started.Add(4*time.Second))
			require.NotNil(t, agreed.routingComparison)
			assert.Equal(t, 3*time.Second, agreed.routingComparison.Improvement)
			assert.Equal(t, SQLRoutingResultConclusive, agreed.routingComparison.HybridSource)
			fallback := policy.observeSQL([]string{"A", "B"}, uncertain, started.Add(5*time.Second))
			assert.Equal(t, SQLRoutingResultCachedFallback, fallback.routingResult)
			assert.Equal(t, map[string]bool{"A": false, "B": true}, fallback.health)
		})
	}
}

func TestHybridHealthPatroniAdvancesPastRetainedSQLLeader(t *testing.T) {
	for _, newPrimaryKnown := range []bool{false, true} {
		name := "C times out"
		if newPrimaryKnown {
			name = "C reports primary"
		}
		t.Run(name, func(t *testing.T) {
			policy, started := policyWithSQLAheadOfPatroni(t)
			candidates := []string{"A", "B", "C"}
			uncertain := SQLHealthCheckOutcome{
				StartedAt: started.Add(1500 * time.Millisecond),
				Results: []SQLHealthCheckResult{
					{IP: "A", InRecovery: true},
					{IP: "B", Err: context.DeadlineExceeded},
					{IP: "C", Err: context.DeadlineExceeded},
				},
			}
			if newPrimaryKnown {
				uncertain.Results[2] = SQLHealthCheckResult{IP: "C"}
				uncertain.PrimaryIPs = []string{"C"}
			}
			retained := policy.observeSQL(candidates, uncertain, started.Add(2*time.Second))
			require.Equal(t, SQLRoutingResultRetainedSQL, retained.routingResult)
			require.Equal(t, []string{"B"}, healthyIPs(retained.health))

			// This proxy missed Patroni's confirmation of B between A -> B -> C.
			recoveredAt := started.Add(3 * time.Second)
			recoveredHealth := map[string]bool{"A": false, "B": false, "C": true}
			recovered := policy.observePatroni(recoveredHealth, recoveredAt)
			require.True(t, recovered.publish, "a different new leader must replace retained B immediately")
			assert.Equal(t, SQLRoutingResultPatroniRecovery, recovered.routingResult)
			assert.Equal(t, recoveredHealth, recovered.health)
			require.NotNil(t, recovered.routingComparison)
			assert.Equal(t, LeaderRoutingComparison{
				NewLeaderIP: "C", Improvement: 0, HybridSource: SQLRoutingResultPatroniRecovery,
			}, *recovered.routingComparison)

			// Reuse the recovery barrier: a check begun before C's publication
			// cannot finish late and put the old SQL leader B back into routing.
			lateB := SQLHealthCheckOutcome{
				StartedAt: started.Add(2500 * time.Millisecond),
				Results: []SQLHealthCheckResult{
					{IP: "A", InRecovery: true}, {IP: "B"}, {IP: "C", InRecovery: true},
				},
				PrimaryIPs: []string{"B"},
			}
			late := policy.observeSQL(candidates, lateB, started.Add(4*time.Second))
			assert.False(t, late.publish)

			// Fresh uncertainty must use cached Patroni C, not retain B again.
			uncertain.StartedAt = started.Add(4 * time.Second)
			fallback := policy.observeSQL(candidates, uncertain, started.Add(5*time.Second))
			assert.True(t, fallback.publish)
			assert.Equal(t, SQLRoutingResultCachedFallback, fallback.routingResult)
			assert.Equal(t, recoveredHealth, fallback.health)
		})
	}
}

func TestHybridHealthPatroniAdvanceRequiresFreshUniqueNewLeader(t *testing.T) {
	for _, tc := range []struct {
		name   string
		health map[string]bool
		at     time.Duration
	}{
		{"before SQL selection", map[string]bool{"C": true}, 500 * time.Millisecond},
		{"at SQL selection", map[string]bool{"C": true}, time.Second},
		{"superseded A", map[string]bool{"A": true}, 2 * time.Second},
		{"no leader", map[string]bool{"B": false, "C": false}, 2 * time.Second},
		{"multiple new leaders", map[string]bool{"C": true, "D": true}, 2 * time.Second},
		{"old and new leaders", map[string]bool{"A": true, "C": true}, 2 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			policy, started := policyWithSQLAheadOfPatroni(t)
			decision := policy.observePatroni(tc.health, started.Add(tc.at))
			assert.False(t, decision.publish)
			assert.Equal(t, "B", policy.awaitingPatroniLeader)
			assert.Equal(t, "A", policy.supersededPatroniLeader)
			assert.True(t, policy.patroniRecoveryAt.IsZero())
		})
	}
}

func TestHybridHealthDoesNotRetainDisqualifiedSQLLeader(t *testing.T) {
	for _, tc := range []struct {
		name       string
		candidates []string
		results    []SQLHealthCheckResult
		primaries  []string
	}{
		{"standby", []string{"A", "B", "C"}, []SQLHealthCheckResult{{IP: "B", InRecovery: true}}, nil},
		{"shutdown", []string{"A", "B", "C"}, []SQLHealthCheckResult{{IP: "B", Err: &pgconn.PgError{Code: "57P01"}}}, nil},
		{"refused", []string{"A", "B", "C"}, []SQLHealthCheckResult{{IP: "B", Err: syscall.ECONNREFUSED}}, nil},
		{"removed", []string{"A", "C"}, nil, nil},
		{"conflicting roles", []string{"A", "B", "C"}, []SQLHealthCheckResult{{IP: "B"}, {IP: "B", InRecovery: true}}, []string{"B"}},
		{"multiple primaries", []string{"A", "B", "C"}, []SQLHealthCheckResult{{IP: "A"}, {IP: "B"}}, []string{"A", "B"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			policy, started := policyWithSQLAheadOfPatroni(t)
			outcome := SQLHealthCheckOutcome{
				Results:    append(tc.results, SQLHealthCheckResult{IP: "C", Err: context.DeadlineExceeded}),
				PrimaryIPs: tc.primaries,
			}
			unavailable := policy.observeSQL(tc.candidates, outcome, started.Add(2*time.Second))
			assert.True(t, unavailable.publish)
			assert.Empty(t, healthyIPs(unavailable.health), "disqualifying B must not resurrect A")
			oldPatroni := policy.observePatroni(map[string]bool{"A": true}, started.Add(3*time.Second))
			assert.False(t, oldPatroni.publish, "case 2 must not restore the superseded leader")

			// A different Patroni leader can recover immediately through case 2.
			recovered := policy.observePatroni(map[string]bool{"A": false, "B": false, "C": true}, started.Add(4*time.Second))
			assert.True(t, recovered.publish)
			assert.Equal(t, SQLRoutingResultPatroniRecovery, recovered.routingResult)
			assert.Equal(t, []string{"C"}, healthyIPs(recovered.health))
		})
	}
}

func TestHybridHealthConclusiveNoPrimaryDoesNotResurrectSupersededLeader(t *testing.T) {
	policy, started := policyWithSQLAheadOfPatroni(t)
	negative := policy.observeSQL([]string{"A", "B"}, SQLHealthCheckOutcome{
		Results: []SQLHealthCheckResult{{IP: "A", InRecovery: true}, {IP: "B", InRecovery: true}},
	}, started.Add(2*time.Second))
	assert.Empty(t, healthyIPs(negative.health))
	oldPatroni := policy.observePatroni(map[string]bool{"A": true}, started.Add(3*time.Second))
	assert.False(t, oldPatroni.publish)

	// Fresh successful SQL evidence can establish A as primary again.
	returned := policy.observeSQL([]string{"A", "B"}, SQLHealthCheckOutcome{
		Results:    []SQLHealthCheckResult{{IP: "A"}, {IP: "B", InRecovery: true}},
		PrimaryIPs: []string{"A"},
	}, started.Add(4*time.Second))
	assert.Equal(t, []string{"A"}, healthyIPs(returned.health))
	fallback := policy.observeSQL([]string{"A", "B"}, SQLHealthCheckOutcome{
		Results: []SQLHealthCheckResult{{IP: "A", Err: context.DeadlineExceeded}},
	}, started.Add(5*time.Second))
	assert.Equal(t, SQLRoutingResultCachedFallback, fallback.routingResult)
	assert.Equal(t, []string{"A"}, healthyIPs(fallback.health))
}

func TestHybridHealthRetainsLatestConclusiveSQLLeader(t *testing.T) {
	policy, started := policyWithSQLAheadOfPatroni(t)
	policy.observeSQL([]string{"A", "B", "C"}, SQLHealthCheckOutcome{
		Results:    []SQLHealthCheckResult{{IP: "A", InRecovery: true}, {IP: "B", InRecovery: true}, {IP: "C"}},
		PrimaryIPs: []string{"C"},
	}, started.Add(2*time.Second))
	lateB := policy.observePatroni(map[string]bool{"A": false, "B": true, "C": false}, started.Add(2500*time.Millisecond))
	assert.False(t, lateB.publish, "Patroni catching up to superseded SQL leader B must not replace C")
	retained := policy.observeSQL([]string{"A", "B", "C"}, SQLHealthCheckOutcome{
		Results: []SQLHealthCheckResult{{IP: "C", Err: context.DeadlineExceeded}},
	}, started.Add(3*time.Second))
	assert.Equal(t, SQLRoutingResultRetainedSQL, retained.routingResult)
	assert.Equal(t, []string{"C"}, healthyIPs(retained.health))
}

func TestHybridHealthSupersededSQLHistoryTracksCandidates(t *testing.T) {
	policy, started := policyWithSQLAheadOfPatroni(t)
	policy.observeSQL([]string{"A", "B", "C"}, SQLHealthCheckOutcome{
		Results:    []SQLHealthCheckResult{{IP: "A", InRecovery: true}, {IP: "B", InRecovery: true}, {IP: "C"}},
		PrimaryIPs: []string{"C"},
	}, started.Add(2*time.Second))
	require.Contains(t, policy.supersededSQLLeaders, "B")

	policy.observeSQL([]string{"A", "C"}, SQLHealthCheckOutcome{
		Results: []SQLHealthCheckResult{{IP: "A", InRecovery: true}, {IP: "C", Err: context.DeadlineExceeded}},
	}, started.Add(3*time.Second))
	assert.Empty(t, policy.supersededSQLLeaders, "evicted IPs must not accumulate in routing history")
	assert.Equal(t, "A", policy.supersededPatroniLeader, "discovery cleanup must not restore stale A")
}

// Exercise routing publications in observation order, without waiting for real ticks.
func TestHybridHealthPatroniRecoversBeforeNextSQLCompletion(t *testing.T) {
	const oldIP, newIP = "10.0.0.1", "10.0.0.2"
	started := time.Now()
	policy := newHybridHealthPolicy(true)
	policy.observePatroni(map[string]bool{oldIP: true, newIP: false}, started)
	policy.observeSQL([]string{oldIP, newIP}, SQLHealthCheckOutcome{
		PrimaryIPs: []string{oldIP},
	}, started)

	noPrimary := SQLHealthCheckOutcome{Results: []SQLHealthCheckResult{
		{IP: oldIP, Err: &pgconn.PgError{Code: "57P01"}},
		{IP: newIP, InRecovery: true},
	}}
	unavailable := policy.observeSQL([]string{oldIP, newIP}, noPrimary, started.Add(time.Second))
	require.True(t, unavailable.publish)
	require.Equal(t, map[string]bool{oldIP: false, newIP: false}, unavailable.health)

	// A late Patroni observation captured before SQL's no-primary decision must
	// not restore the old leader merely because its callback arrived later.
	olderPatroni := policy.observePatroni(map[string]bool{oldIP: true}, started.Add(500*time.Millisecond))
	assert.False(t, olderPatroni.publish)

	recoveredAt := started.Add(1300 * time.Millisecond)
	recoveredHealth := map[string]bool{oldIP: false, newIP: true}
	recovered := policy.observePatroni(recoveredHealth, recoveredAt)
	require.True(t, recovered.publish, "publish recovery on Patroni's callback, before SQL completes")
	assert.Equal(t, recoveredHealth, recovered.health)
	assert.Equal(t, SQLRoutingResultPatroniRecovery, recovered.routingResult)
	require.NotNil(t, recovered.routingComparison)
	assert.Equal(t, LeaderRoutingComparison{
		NewLeaderIP: newIP, Improvement: 0, HybridSource: SQLRoutingResultPatroniRecovery,
	}, *recovered.routingComparison)

	latePatroni := policy.observePatroni(map[string]bool{oldIP: true}, started.Add(1100*time.Millisecond))
	assert.False(t, latePatroni.publish, "an older Patroni callback must not undo recovery")

	// Neither an old negative snapshot, an old primary, nor missing start-time
	// metadata may undo recovery, even if the snapshot completes afterwards.
	for _, tc := range []struct {
		name    string
		outcome SQLHealthCheckOutcome
	}{
		{"old no primary", SQLHealthCheckOutcome{StartedAt: started.Add(time.Second), Results: noPrimary.Results}},
		{"old primary", SQLHealthCheckOutcome{StartedAt: started.Add(time.Second), PrimaryIPs: []string{oldIP}}},
		{"unknown start", noPrimary},
		{"equal start", SQLHealthCheckOutcome{StartedAt: recoveredAt, Results: noPrimary.Results}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			late := policy.observeSQL([]string{oldIP, newIP}, tc.outcome, started.Add(1500*time.Millisecond))
			assert.False(t, late.publish)
			assert.Nil(t, late.routingComparison)
		})
	}

	// Repeated Patroni polls must not move the barrier and starve a SQL check
	// that started after recovery but before this otherwise unchanged poll.
	policy.observePatroni(recoveredHealth, started.Add(1600*time.Millisecond))
	confirmed := policy.observeSQL([]string{oldIP, newIP}, SQLHealthCheckOutcome{
		StartedAt:  started.Add(1400 * time.Millisecond),
		Results:    []SQLHealthCheckResult{{IP: oldIP, InRecovery: true}, {IP: newIP}},
		PrimaryIPs: []string{newIP},
	}, started.Add(2*time.Second))
	require.True(t, confirmed.publish)
	assert.Equal(t, recoveredHealth, confirmed.health)
	assert.Equal(t, SQLRoutingResultConclusive, confirmed.routingResult)
	assert.Nil(t, confirmed.routingComparison, "confirmation must not emit a second recovery comparison")

	late := policy.observeSQL([]string{oldIP, newIP}, noPrimary, started.Add(3*time.Second))
	assert.False(t, late.publish, "retain the freshness barrier after SQL confirmation")

	// A genuinely newer SQL observation still uses the existing negative policy.
	noPrimary.StartedAt = started.Add(3 * time.Second)
	freshNegative := policy.observeSQL([]string{oldIP, newIP}, noPrimary, started.Add(4*time.Second))
	assert.True(t, freshNegative.publish)
	assert.Equal(t, map[string]bool{oldIP: false, newIP: false}, freshNegative.health)
}

func TestHybridHealthPatroniRecoveryRequiresUniqueLeader(t *testing.T) {
	for _, health := range []map[string]bool{
		{"10.0.0.1": false},
		{"10.0.0.1": true, "10.0.0.2": true},
	} {
		policy := newHybridHealthPolicy(true)
		started := time.Now()
		policy.observeSQL([]string{"10.0.0.1"}, SQLHealthCheckOutcome{}, started)
		decision := policy.observePatroni(health, started.Add(time.Second))
		assert.False(t, decision.publish)
	}
}

func TestHybridHealthPatroniRecoveryRequiresUniqueLeaderBeforeExclusion(t *testing.T) {
	policy, started := policyWithSQLAheadOfPatroni(t)
	noPrimary := policy.observeSQL([]string{"A", "B", "C"}, SQLHealthCheckOutcome{
		Results: []SQLHealthCheckResult{
			{IP: "A", InRecovery: true}, {IP: "B", InRecovery: true}, {IP: "C", InRecovery: true},
		},
	}, started.Add(2*time.Second))
	require.True(t, noPrimary.publish)
	require.Empty(t, healthyIPs(noPrimary.health))

	// Filtering superseded A must not turn an ambiguous A+C snapshot into
	// authority to recover to C and forget A's exclusion on the next poll.
	for _, at := range []time.Duration{3 * time.Second, 4 * time.Second} {
		ambiguous := policy.observePatroni(map[string]bool{"A": true, "C": true}, started.Add(at))
		assert.False(t, ambiguous.publish)
		assert.Equal(t, "A", policy.supersededPatroniLeader)
		assert.False(t, policy.patroniRecoveryActive)
		assert.True(t, policy.patroniRecoveryAt.IsZero())
	}

	// An unambiguous, eligible leader can still restore routing immediately.
	recovered := policy.observePatroni(map[string]bool{"A": false, "C": true}, started.Add(5*time.Second))
	assert.True(t, recovered.publish)
	assert.Equal(t, []string{"C"}, healthyIPs(recovered.health))
	assert.Equal(t, SQLRoutingResultPatroniRecovery, recovered.routingResult)
	assert.Empty(t, policy.supersededPatroniLeader)
}

func TestHybridHealthPatroniRecoveryFollowsLeaderLoss(t *testing.T) {
	policy := newHybridHealthPolicy(true)
	started := time.Now()
	policy.observeSQL([]string{"10.0.0.1"}, SQLHealthCheckOutcome{}, started)
	recovered := policy.observePatroni(map[string]bool{"10.0.0.1": true}, started.Add(time.Second))
	require.True(t, recovered.publish)

	lost := policy.observePatroni(map[string]bool{"10.0.0.1": false}, started.Add(2*time.Second))
	assert.True(t, lost.publish, "do not retain a recovered leader after Patroni removes it")
	assert.Equal(t, map[string]bool{"10.0.0.1": false}, lost.health)
}

func TestHybridHealthFallsBackToCachedPatroniOnTimeout(t *testing.T) {
	policy := newHybridHealthPolicy(true)
	patroniAt := time.Now()
	patroniHealth := map[string]bool{"10.0.0.1": true, "10.0.0.2": false}
	patroniDecision := policy.observePatroni(patroniHealth, patroniAt)
	assert.False(t, patroniDecision.publish)
	patroniHealth["10.0.0.1"] = false

	observedAt := patroniAt.Add(3 * time.Second)
	decision := policy.observeSQL([]string{"10.0.0.1", "10.0.0.2"}, SQLHealthCheckOutcome{
		Results: []SQLHealthCheckResult{
			{IP: "10.0.0.1", Err: context.DeadlineExceeded},
			{IP: "10.0.0.2", InRecovery: true},
		},
	}, observedAt)

	assert.True(t, decision.publish)
	assert.Equal(t, SQLRoutingResultCachedFallback, decision.routingResult)
	assert.Equal(t, SQLHealthFallbackTimeout, decision.fallbackReason)
	assert.Equal(t, SQLHealthTimeoutContext, decision.timeoutType)
	assert.Equal(t, map[string]bool{"10.0.0.1": true, "10.0.0.2": false}, decision.health)
	assert.Equal(t, 3*time.Second, decision.cachedAge)
	assert.True(t, decision.hasCachedAge)
}

func TestHybridHealthUsesSuccessfulRoleWhenAnotherCredentialTimesOutOnSameIP(t *testing.T) {
	policy := newHybridHealthPolicy(true)
	patroniHealth := map[string]bool{"10.0.0.1": true, "10.0.0.2": false}
	policy.observePatroni(patroniHealth, time.Now())
	decision := policy.observeSQL([]string{"10.0.0.1", "10.0.0.2"}, SQLHealthCheckOutcome{
		Results: []SQLHealthCheckResult{
			{IP: "10.0.0.1", InRecovery: true},
			{IP: "10.0.0.2"},
			{IP: "10.0.0.1", InRecovery: true},
			{IP: "10.0.0.2", Err: context.DeadlineExceeded},
		},
		PrimaryIPs: []string{"10.0.0.2"},
	}, time.Now())

	assert.True(t, decision.publish)
	assert.Equal(t, SQLRoutingResultConclusive, decision.routingResult)
	assert.Equal(t, map[string]bool{"10.0.0.1": false, "10.0.0.2": true}, decision.health)
	assert.Empty(t, decision.fallbackReason)
	assert.Empty(t, decision.timeoutType)
}

func TestHybridHealthFallsBackOnConflictingDeterministicRoles(t *testing.T) {
	policy := newHybridHealthPolicy(true)
	patroniHealth := map[string]bool{"10.0.0.1": true, "10.0.0.2": false}
	policy.observePatroni(patroniHealth, time.Now())
	decision := policy.observeSQL([]string{"10.0.0.1", "10.0.0.2"}, SQLHealthCheckOutcome{
		Results: []SQLHealthCheckResult{
			{IP: "10.0.0.1", InRecovery: true},
			{IP: "10.0.0.2"},
			{IP: "10.0.0.1", InRecovery: true},
			{IP: "10.0.0.2", InRecovery: true},
		},
		PrimaryIPs: []string{"10.0.0.2"},
	}, time.Now())

	assert.True(t, decision.publish)
	assert.Equal(t, SQLRoutingResultCachedFallback, decision.routingResult)
	assert.Equal(t, SQLHealthFallbackConflictingRoles, decision.fallbackReason)
	assert.Equal(t, patroniHealth, decision.health)
}
func TestHybridHealthFallsBackOnMultiplePrimaries(t *testing.T) {
	policy := newHybridHealthPolicy(true)
	policy.observePatroni(map[string]bool{"10.0.0.1": true}, time.Now())
	decision := policy.observeSQL([]string{"10.0.0.1", "10.0.0.2"}, SQLHealthCheckOutcome{
		Results:    []SQLHealthCheckResult{{IP: "10.0.0.1"}, {IP: "10.0.0.2"}},
		PrimaryIPs: []string{"10.0.0.1", "10.0.0.2"},
	}, time.Now())

	assert.Equal(t, SQLRoutingResultCachedFallback, decision.routingResult)
	assert.Equal(t, SQLHealthFallbackMultiplePrimaries, decision.fallbackReason)
	assert.Equal(t, map[string]bool{"10.0.0.1": true}, decision.health)
}

func TestHybridHealthFallbackWithoutPatroniSnapshotHasNoAge(t *testing.T) {
	policy := newHybridHealthPolicy(true)
	decision := policy.observeSQL([]string{"10.0.0.1"}, SQLHealthCheckOutcome{
		Results: []SQLHealthCheckResult{{IP: "10.0.0.1", Err: context.DeadlineExceeded}},
	}, time.Now())

	assert.Equal(t, map[string]bool{}, decision.health)
	assert.False(t, decision.hasCachedAge)
	assert.Zero(t, decision.cachedAge)
}

func TestHybridHealthFallsBackOnUnknownProbeError(t *testing.T) {
	policy := newHybridHealthPolicy(true)
	policy.observePatroni(map[string]bool{"10.0.0.1": true}, time.Now())
	decision := policy.observeSQL([]string{"10.0.0.1"}, SQLHealthCheckOutcome{
		Results: []SQLHealthCheckResult{{IP: "10.0.0.1", Err: errors.New("probe failed")}},
	}, time.Now())

	assert.Equal(t, SQLRoutingResultCachedFallback, decision.routingResult)
	assert.Equal(t, map[string]bool{"10.0.0.1": true}, decision.health)
	assert.Equal(t, SQLHealthFallbackIndeterminate, decision.fallbackReason)
	assert.Equal(t, SQLHealthTimeoutNone, decision.timeoutType)
}

func TestSQLHealthTimeoutType(t *testing.T) {
	tests := map[string]struct {
		err  error
		want string
	}{
		"context deadline": {
			err:  context.DeadlineExceeded,
			want: SQLHealthTimeoutContext,
		},
		"postgres statement timeout": {
			err: &pgconn.PgError{
				Code:    "57014",
				Message: "canceling statement due to statement timeout",
			},
			want: SQLHealthTimeoutStatement,
		},
		"postgres cancellation remains indeterminate": {
			err: &pgconn.PgError{
				Code:    "57014",
				Message: "canceling statement due to user request",
			},
			want: SQLHealthTimeoutStatement,
		},
		"ordinary error": {
			err:  errors.New("probe failed"),
			want: SQLHealthTimeoutNone,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, test.want, sqlHealthTimeoutType(test.err))
		})
	}
}

func TestHybridHealthPairsLeaderObservationsWithoutDowntime(t *testing.T) {
	policy := newHybridHealthPolicy(true)
	started := time.Now()
	policy.observePatroni(map[string]bool{"10.0.0.1": true}, started)
	policy.observeSQL(nil, SQLHealthCheckOutcome{PrimaryIPs: []string{"10.0.0.1"}}, started)

	sqlDecision := policy.observeSQL(nil, SQLHealthCheckOutcome{PrimaryIPs: []string{"10.0.0.2"}}, started.Add(time.Second))
	assert.Nil(t, sqlDecision.leaderDetection)
	patroniDecision := policy.observePatroni(map[string]bool{"10.0.0.2": true}, started.Add(4*time.Second))
	require.NotNil(t, patroniDecision.leaderDetection)
	assert.Equal(t, LeaderDetectionComparison{
		NewLeaderIP:       "10.0.0.2",
		SQLRoutingEnabled: true,
	}, *patroniDecision.leaderDetection)
}

func TestHybridHealthIgnoresAmbiguousLeaderDetection(t *testing.T) {
	policy := newHybridHealthPolicy(true)
	started := time.Now()
	policy.observePatroni(map[string]bool{"10.0.0.1": true}, started)
	policy.observeSQL([]string{"10.0.0.1"}, SQLHealthCheckOutcome{
		Results:    []SQLHealthCheckResult{{IP: "10.0.0.1"}},
		PrimaryIPs: []string{"10.0.0.1"},
	}, started)

	ambiguousAt := started.Add(time.Second)
	ambiguous := policy.observeSQL([]string{"10.0.0.1", "10.0.0.2"}, SQLHealthCheckOutcome{
		Results:    []SQLHealthCheckResult{{IP: "10.0.0.1"}, {IP: "10.0.0.2"}},
		PrimaryIPs: []string{"10.0.0.1", "10.0.0.2"},
	}, ambiguousAt)
	assert.Equal(t, SQLRoutingResultCachedFallback, ambiguous.routingResult)
	assert.Nil(t, ambiguous.leaderDetection)

	conclusiveAt := ambiguousAt.Add(time.Second)
	conclusive := policy.observeSQL([]string{"10.0.0.1", "10.0.0.2"}, SQLHealthCheckOutcome{
		Results: []SQLHealthCheckResult{
			{IP: "10.0.0.1", InRecovery: true},
			{IP: "10.0.0.2"},
		},
		PrimaryIPs: []string{"10.0.0.2"},
	}, conclusiveAt)
	assert.Equal(t, SQLRoutingResultConclusive, conclusive.routingResult)
	assert.Nil(t, conclusive.leaderDetection)

	patroniAt := conclusiveAt.Add(time.Second)
	patroni := policy.observePatroni(map[string]bool{"10.0.0.2": true}, patroniAt)
	require.NotNil(t, patroni.leaderDetection)
	assert.Equal(t, LeaderDetectionComparison{
		NewLeaderIP:       "10.0.0.2",
		SQLRoutingEnabled: true,
	}, *patroni.leaderDetection)
}

func TestHybridHealthComparesRoutingPublication(t *testing.T) {
	policy := newHybridHealthPolicy(true)
	started := time.Now()
	policy.observePatroni(map[string]bool{"10.0.0.1": true}, started)
	policy.observeSQL([]string{"10.0.0.1"}, SQLHealthCheckOutcome{PrimaryIPs: []string{"10.0.0.1"}}, started)

	sqlDecision := policy.observeSQL([]string{"10.0.0.2"}, SQLHealthCheckOutcome{PrimaryIPs: []string{"10.0.0.2"}}, started.Add(time.Second))
	assert.Nil(t, sqlDecision.routingComparison)
	patroniDecision := policy.observePatroni(map[string]bool{"10.0.0.2": true}, started.Add(5*time.Second))
	require.NotNil(t, patroniDecision.routingComparison)
	assert.Equal(t, LeaderRoutingComparison{
		NewLeaderIP:  "10.0.0.2",
		Improvement:  4 * time.Second,
		HybridSource: SQLRoutingResultConclusive,
	}, *patroniDecision.routingComparison)
}

func TestHybridHealthResetsRoutingComparisonWhenRoutingFlagChanges(t *testing.T) {
	policy := newHybridHealthPolicy(true)
	started := time.Now()
	policy.observePatroni(map[string]bool{"10.0.0.1": true}, started)
	policy.observeSQL(
		[]string{"10.0.0.1"},
		SQLHealthCheckOutcome{PrimaryIPs: []string{"10.0.0.1"}},
		started,
	)

	// SQL observes the replacement leader, leaving a comparison pending until Patroni catches up.
	sqlDecision := policy.observeSQL(
		[]string{"10.0.0.2"},
		SQLHealthCheckOutcome{PrimaryIPs: []string{"10.0.0.2"}},
		started.Add(time.Second),
	)
	assert.Nil(t, sqlDecision.routingComparison)
	assert.NotEmpty(t, policy.routingTiming.pending)

	// Do not let a Patroni observation after an off/on transition complete the stale comparison
	// with a duration that includes the interval where routing evaluation was disabled.
	policy.setRoutingEnabled(false)
	policy.setRoutingEnabled(true)
	patroniDecision := policy.observePatroni(
		map[string]bool{"10.0.0.2": true},
		started.Add(5*time.Minute),
	)

	assert.Nil(t, patroniDecision.routingComparison)
	assert.Empty(t, policy.routingTiming.pending)
	assert.Contains(t, policy.routingTiming.reference.lastNonempty, "10.0.0.2")
}

func TestHybridHealthRoutingFlagToggleDropsRetainedSQLLeader(t *testing.T) {
	policy, started := policyWithSQLAheadOfPatroni(t)
	uncertain := SQLHealthCheckOutcome{Results: []SQLHealthCheckResult{
		{IP: "A", Err: context.DeadlineExceeded},
		{IP: "B", Err: context.DeadlineExceeded},
	}}
	// Ordinary flag evaluations must preserve retention within the same mode.
	policy.setRoutingEnabled(true)
	retained := policy.observeSQL([]string{"A", "B"}, uncertain, started.Add(2*time.Second))
	require.Equal(t, SQLRoutingResultRetainedSQL, retained.routingResult)
	require.Equal(t, []string{"B"}, healthyIPs(retained.health))

	policy.setRoutingEnabled(false)
	confirmed := policy.observePatroni(map[string]bool{"A": false, "B": true}, started.Add(3*time.Second))
	require.True(t, confirmed.publish)
	current := map[string]bool{"A": true, "B": false}
	returned := policy.observePatroni(current, started.Add(4*time.Second))
	require.True(t, returned.publish)
	require.Equal(t, current, returned.health)

	policy.setRoutingEnabled(true)
	fallback := policy.observeSQL([]string{"A", "B"}, uncertain, started.Add(5*time.Second))
	assert.True(t, fallback.publish)
	assert.Equal(t, SQLRoutingResultCachedFallback, fallback.routingResult)
	assert.Equal(t, current, fallback.health, "do not resurrect SQL B or exclude current Patroni A")
}

func TestHybridHealthRoutingFlagToggleDropsSupersededSQLHistory(t *testing.T) {
	policy, started := policyWithSQLAheadOfPatroni(t)
	candidates := []string{"A", "B", "C", "D"}
	policy.observeSQL(candidates, SQLHealthCheckOutcome{PrimaryIPs: []string{"C"}}, started.Add(2*time.Second))
	policy.setRoutingEnabled(false)
	policy.observePatroni(map[string]bool{"A": true}, started.Add(3*time.Second))
	policy.setRoutingEnabled(true)
	policy.observeSQL(candidates, SQLHealthCheckOutcome{PrimaryIPs: []string{"D"}}, started.Add(4*time.Second))

	// B was superseded in the old mode, but is a valid newer Patroni leader now.
	recovered := policy.observePatroni(map[string]bool{"B": true}, started.Add(5*time.Second))
	assert.True(t, recovered.publish)
	assert.Equal(t, SQLRoutingResultPatroniRecovery, recovered.routingResult)
	assert.Equal(t, []string{"B"}, healthyIPs(recovered.health))
}

func TestHybridHealthRoutingFlagToggleDropsRecoveryState(t *testing.T) {
	for _, recovered := range []bool{false, true} {
		t.Run(fmt.Sprintf("recovered_%t", recovered), func(t *testing.T) {
			policy, started := policyWithSQLAheadOfPatroni(t)
			policy.observeSQL([]string{"A", "B"}, SQLHealthCheckOutcome{}, started.Add(2*time.Second))
			if recovered {
				recovery := policy.observePatroni(map[string]bool{"B": true}, started.Add(3*time.Second))
				require.Equal(t, SQLRoutingResultPatroniRecovery, recovery.routingResult)
			}
			policy.setRoutingEnabled(false)
			policy.observePatroni(map[string]bool{"A": true}, started.Add(4*time.Second))
			policy.setRoutingEnabled(true)

			patroni := policy.observePatroni(map[string]bool{"B": true}, started.Add(5*time.Second))
			assert.False(t, patroni.publish, "old no-primary or active recovery state must not initiate recovery")
			assert.Equal(t, SQLRoutingResultShadow, patroni.routingResult)
			// With no recovery in the new mode, its first SQL observation needs no
			// timestamp comparison against a recovery from the previous mode.
			sql := policy.observeSQL([]string{"A", "B"}, SQLHealthCheckOutcome{PrimaryIPs: []string{"B"}}, started.Add(6*time.Second))
			assert.True(t, sql.publish)
			assert.Equal(t, SQLRoutingResultConclusive, sql.routingResult)
			assert.Equal(t, []string{"B"}, healthyIPs(sql.health))
		})
	}
}

func TestHybridHealthPreservesNewerPendingRoutingComparison(t *testing.T) {
	policy := newHybridHealthPolicy(true)
	started := time.Now()
	// Keep all candidates while Patroni catches up through SQL's earlier leaders.
	candidates := []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}
	policy.observePatroni(map[string]bool{"10.0.0.1": true}, started)
	policy.observeSQL(candidates, SQLHealthCheckOutcome{PrimaryIPs: []string{"10.0.0.1"}}, started)

	firstHybridAt := started.Add(time.Second)
	firstHybrid := policy.observeSQL(
		candidates,
		SQLHealthCheckOutcome{PrimaryIPs: []string{"10.0.0.2"}},
		firstHybridAt,
	)
	assert.Nil(t, firstHybrid.routingComparison)

	secondHybridAt := started.Add(2 * time.Second)
	secondHybrid := policy.observeSQL(
		candidates,
		SQLHealthCheckOutcome{PrimaryIPs: []string{"10.0.0.3"}},
		secondHybridAt,
	)
	assert.Nil(t, secondHybrid.routingComparison)

	firstPatroniAt := started.Add(4 * time.Second)
	firstPatroni := policy.observePatroni(map[string]bool{"10.0.0.2": true}, firstPatroniAt)
	assert.False(t, firstPatroni.publish, "late confirmation of the first SQL leader must not roll routing back")
	// Hybrid has already left this leader, so its old timestamp is no longer comparable.
	assert.Nil(t, firstPatroni.routingComparison)
	assert.Contains(t, policy.routingTiming.pending, "10.0.0.3")

	secondPatroniAt := started.Add(6 * time.Second)
	secondPatroni := policy.observePatroni(map[string]bool{"10.0.0.3": true}, secondPatroniAt)
	require.NotNil(t, secondPatroni.routingComparison)
	assert.Equal(t, LeaderRoutingComparison{
		NewLeaderIP:  "10.0.0.3",
		Improvement:  secondPatroniAt.Sub(secondHybridAt),
		HybridSource: SQLRoutingResultConclusive,
	}, *secondPatroni.routingComparison)
	assert.Empty(t, policy.routingTiming.pending)
}

func TestHybridHealthDiscardsSupersededRoutingObservations(t *testing.T) {
	for _, firstSource := range []string{"hybrid", "patroni"} {
		for _, nextLeader := range []string{"C", ""} {
			name := "new_leader"
			if nextLeader == "" {
				name = "no_leader"
			}
			t.Run(firstSource+"/"+name, func(t *testing.T) {
				policy := newHybridHealthPolicy(true)
				started := time.Now()
				observe := func(source, leader string, elapsed time.Duration) *LeaderRoutingComparison {
					health := map[string]bool{"A": false, "B": false, "C": false}
					if leader != "" {
						health[leader] = true
					}
					return policy.recordRoutingLeaderLocked(source, health, started.Add(elapsed), SQLRoutingResultConclusive)
				}
				otherSource := "patroni"
				if firstSource == "patroni" {
					otherSource = "hybrid"
				}
				observe(firstSource, "A", 0)
				observe(otherSource, "A", 0)
				require.Nil(t, observe(firstSource, "B", 10*time.Second))

				// The lagging source still seeing A must not discard the leading source's B.
				require.Nil(t, observe(otherSource, "A", 12*time.Second))
				require.Contains(t, policy.routingTiming.pending, "B")
				require.Nil(t, observe(firstSource, nextLeader, 20*time.Second))
				require.NotContains(t, policy.routingTiming.pending, "B")
				if nextLeader != "" {
					require.NotNil(t, observe(otherSource, nextLeader, 21*time.Second))
				}

				// A later B observation must not complete the previous promotion's pair.
				require.Nil(t, observe(otherSource, "B", time.Minute))
				if nextLeader != "" {
					comparison := observe(firstSource, "B", time.Minute+time.Second)
					require.NotNil(t, comparison)
					improvement := -time.Second
					if firstSource == "patroni" {
						improvement = time.Second
					}
					assert.Equal(t, LeaderRoutingComparison{
						NewLeaderIP:  "B",
						Improvement:  improvement,
						HybridSource: SQLRoutingResultConclusive,
					}, *comparison)
				}
			})
		}
	}
}

func TestHybridHealthRearmsInterruptedLeaderComparisons(t *testing.T) {
	for _, interruptedSource := range []string{"sql", "patroni"} {
		for _, recoveryOrder := range []string{"before_other_source", "after_other_source"} {
			t.Run(interruptedSource+"/"+recoveryOrder, func(t *testing.T) {
				policy := newHybridHealthPolicy(true)
				started := time.Now()
				observe := func(source, leader string, elapsed time.Duration) hybridRoutingDecision {
					health := map[string]bool{"A": false, "B": false}
					var primaries []string
					if leader != "" {
						health[leader] = true
						primaries = []string{leader}
					}
					if source == "patroni" {
						return policy.observePatroni(health, started.Add(elapsed))
					}
					return policy.observeSQL([]string{"A", "B"}, SQLHealthCheckOutcome{
						StartedAt:  started.Add(elapsed),
						PrimaryIPs: primaries,
					}, started.Add(elapsed))
				}
				otherSource := "patroni"
				if interruptedSource == "patroni" {
					otherSource = "sql"
				}
				observe("sql", "A", 0)
				observe("patroni", "A", time.Nanosecond)
				observe(interruptedSource, "B", 10*time.Second)
				observe(interruptedSource, "", 20*time.Second)
				observe(interruptedSource, "", 25*time.Second)

				first, second := interruptedSource, otherSource
				if recoveryOrder == "after_other_source" {
					first, second = second, first
				}
				pending := observe(first, "B", 30*time.Second)
				require.Nil(t, pending.leaderDetection)
				recoveredViaPatroni := interruptedSource == "sql" && first == "patroni"
				if recoveredViaPatroni {
					require.NotNil(t, pending.routingComparison)
					assert.Equal(t, SQLRoutingResultPatroniRecovery, pending.routingComparison.HybridSource)
					assert.Zero(t, pending.routingComparison.Improvement)
				} else {
					require.Nil(t, pending.routingComparison)
				}
				completed := observe(second, "B", 35*time.Second)
				require.NotNil(t, completed.leaderDetection)
				if recoveredViaPatroni {
					require.Nil(t, completed.routingComparison)
				} else {
					require.NotNil(t, completed.routingComparison)
					wantSpeedup := 5 * time.Second
					if first == "patroni" {
						wantSpeedup = -wantSpeedup
					}
					assert.Equal(t, wantSpeedup, completed.routingComparison.Improvement)
					assert.Equal(t, "B", completed.routingComparison.NewLeaderIP)
				}
				assert.Equal(t, "B", completed.leaderDetection.NewLeaderIP)
				// B recovered after its own outage; this is not replacement-leader downtime.
				assert.False(t, completed.leaderDetection.HasDowntimeComparison)
				assert.Empty(t, policy.detectionTiming.pending)
				assert.Empty(t, policy.routingTiming.pending)
			})
		}
	}
}

func TestHybridHealthComparesDowntimeReductionWhenSQLRecoversFirst(t *testing.T) {
	policy := newHybridHealthPolicy(true)
	started := time.Now()
	policy.observePatroni(map[string]bool{"10.0.0.1": true}, started)
	policy.observeSQL([]string{"10.0.0.1"}, SQLHealthCheckOutcome{PrimaryIPs: []string{"10.0.0.1"}}, started)

	policy.observeSQL([]string{"10.0.0.1", "10.0.0.2"}, SQLHealthCheckOutcome{}, started.Add(time.Second))
	sqlDecision := policy.observeSQL(
		[]string{"10.0.0.1", "10.0.0.2"},
		SQLHealthCheckOutcome{PrimaryIPs: []string{"10.0.0.2"}},
		started.Add(2*time.Second),
	)
	assert.Nil(t, sqlDecision.leaderDetection)
	patroniDecision := policy.observePatroni(map[string]bool{"10.0.0.2": true}, started.Add(5*time.Second))
	require.NotNil(t, patroniDecision.leaderDetection)
	assert.Equal(t, LeaderDetectionComparison{
		NewLeaderIP:              "10.0.0.2",
		SQLRoutingEnabled:        true,
		SQLDowntime:              time.Second,
		PatroniDowntime:          4 * time.Second,
		DowntimeReductionPercent: 75,
		HasDowntimeComparison:    true,
	}, *patroniDecision.leaderDetection)
}

func TestHybridHealthComparesNegativeDowntimeReductionWhenPatroniRecoversFirst(t *testing.T) {
	policy := newHybridHealthPolicy(true)
	started := time.Now()
	policy.observePatroni(map[string]bool{"10.0.0.1": true}, started)
	policy.observeSQL([]string{"10.0.0.1"}, SQLHealthCheckOutcome{PrimaryIPs: []string{"10.0.0.1"}}, started)
	policy.observeSQL([]string{"10.0.0.1", "10.0.0.2"}, SQLHealthCheckOutcome{}, started.Add(time.Second))
	patroniDecision := policy.observePatroni(map[string]bool{"10.0.0.2": true}, started.Add(2*time.Second))
	assert.Nil(t, patroniDecision.leaderDetection)
	sqlDecision := policy.observeSQL(
		[]string{"10.0.0.1", "10.0.0.2"},
		SQLHealthCheckOutcome{PrimaryIPs: []string{"10.0.0.2"}},
		started.Add(4*time.Second),
	)
	require.NotNil(t, sqlDecision.leaderDetection)
	assert.Equal(t, LeaderDetectionComparison{
		NewLeaderIP:              "10.0.0.2",
		SQLRoutingEnabled:        true,
		SQLDowntime:              3 * time.Second,
		PatroniDowntime:          time.Second,
		DowntimeReductionPercent: -200,
		HasDowntimeComparison:    true,
	}, *sqlDecision.leaderDetection)
}

func TestHybridHealthDoesNotCompareDowntimeFromEarlierPromotionOfSameLeader(t *testing.T) {
	policy := newHybridHealthPolicy(true)
	started := time.Now()
	policy.observePatroni(map[string]bool{"10.0.0.1": true}, started)
	policy.observeSQL(nil, SQLHealthCheckOutcome{PrimaryIPs: []string{"10.0.0.1"}}, started)

	policy.observeSQL(nil, SQLHealthCheckOutcome{}, started.Add(time.Second))
	policy.observeSQL(nil, SQLHealthCheckOutcome{PrimaryIPs: []string{"10.0.0.2"}}, started.Add(2*time.Second))
	policy.observeSQL(nil, SQLHealthCheckOutcome{PrimaryIPs: []string{"10.0.0.3"}}, started.Add(3*time.Second))
	policy.observePatroni(map[string]bool{"10.0.0.3": true}, started.Add(4*time.Second))
	policy.observeSQL(nil, SQLHealthCheckOutcome{PrimaryIPs: []string{"10.0.0.2"}}, started.Add(5*time.Second))

	patroniDecision := policy.observePatroni(map[string]bool{"10.0.0.2": true}, started.Add(6*time.Second))
	require.NotNil(t, patroniDecision.leaderDetection)
	assert.Equal(t, LeaderDetectionComparison{
		NewLeaderIP:       "10.0.0.2",
		SQLRoutingEnabled: true,
	}, *patroniDecision.leaderDetection)
	assert.NotContains(t, policy.detectionTiming.pending, "10.0.0.2")
}

func TestHybridHealthDiscardsSupersededLeaderObservations(t *testing.T) {
	for _, firstSource := range []string{"sql", "patroni"} {
		t.Run(firstSource, func(t *testing.T) {
			policy := newHybridHealthPolicy(true)
			started := time.Now()
			observe := func(source, ip string, elapsed time.Duration) hybridRoutingDecision {
				if source == "patroni" {
					return policy.observePatroni(map[string]bool{ip: true}, started.Add(elapsed))
				}
				return policy.observeSQL(nil, SQLHealthCheckOutcome{PrimaryIPs: []string{ip}}, started.Add(elapsed))
			}
			otherSource := "patroni"
			if firstSource == "patroni" {
				otherSource = "sql"
			}
			observe("sql", "A", 0)
			observe("patroni", "A", 0)
			policy.observeSQL(nil, SQLHealthCheckOutcome{}, started.Add(time.Second))
			observe(firstSource, "B", 5*time.Second)
			// The other observer misses B entirely; both subsequently agree on C.
			observe(firstSource, "C", 15*time.Second)
			comparison := observe(otherSource, "C", 20*time.Second)
			require.NotNil(t, comparison.leaderDetection)
			assert.NotContains(t, policy.detectionTiming.pending, "B")

			// A later promotion of B must not complete the earlier B comparison.
			later := observe(otherSource, "B", time.Minute)
			assert.Nil(t, later.leaderDetection)
			fresh := observe(firstSource, "B", time.Minute+5*time.Second)
			require.NotNil(t, fresh.leaderDetection)
			assert.Equal(t, "B", fresh.leaderDetection.NewLeaderIP)
			assert.Empty(t, policy.detectionTiming.pending)
			assert.False(t, fresh.leaderDetection.HasDowntimeComparison)
		})
	}
}

func TestHybridHealthDoesNotCompareDowntimeWhenSameSQLLeaderRecovers(t *testing.T) {
	policy := newHybridHealthPolicy(true)
	started := time.Now()
	policy.observePatroni(map[string]bool{"10.0.0.1": true}, started)
	policy.observeSQL([]string{"10.0.0.1"}, SQLHealthCheckOutcome{PrimaryIPs: []string{"10.0.0.1"}}, started)
	policy.observeSQL([]string{"10.0.0.1"}, SQLHealthCheckOutcome{}, started.Add(time.Second))
	recovered := policy.observeSQL(
		[]string{"10.0.0.1"},
		SQLHealthCheckOutcome{PrimaryIPs: []string{"10.0.0.1"}},
		started.Add(2*time.Second),
	)

	assert.Nil(t, recovered.leaderDetection)
	assert.Nil(t, recovered.routingComparison)
	assert.Empty(t, policy.detectionTiming.pending)
}

func TestConclusiveSQLUnroutableErrorAllowlist(t *testing.T) {
	tests := map[string]struct {
		err  error
		want bool
	}{
		"connection refused": {
			err:  syscall.ECONNREFUSED,
			want: true,
		},
		"admin shutdown": {
			err:  &pgconn.PgError{Code: "57P01"},
			want: true,
		},
		"crash shutdown": {
			err:  &pgconn.PgError{Code: "57P02"},
			want: true,
		},
		"starting or shutting down": {
			err:  &pgconn.PgError{Code: "57P03"},
			want: true,
		},
		"disk full": {
			err:  &pgconn.PgError{Code: "53100"},
			want: true,
		},
		"out of memory": {
			err:  &pgconn.PgError{Code: "53200"},
			want: true,
		},
		"system IO error": {
			err:  &pgconn.PgError{Code: "58030"},
			want: false,
		},
		"internal error": {
			err:  &pgconn.PgError{Code: "XX000"},
			want: false,
		},
		"connection reset": {
			err:  syscall.ECONNRESET,
			want: false,
		},
		"invalid password": {
			err:  &pgconn.PgError{Code: "28P01"},
			want: false,
		},
		"too many connections": {
			err:  &pgconn.PgError{Code: "53300"},
			want: false,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, test.want, isConclusiveSQLUnroutableError(test.err))
		})
	}
}

func TestHybridHealthTreatsSystemAndInternalErrorsAsIndeterminate(t *testing.T) {
	for _, code := range []string{"58000", "58030", "58P01", "58P02", "58P03", "XX000", "XX001", "XX002"} {
		t.Run(code, func(t *testing.T) {
			policy := newHybridHealthPolicy(true)
			patroniHealth := map[string]bool{"10.0.0.1": true}
			policy.observePatroni(patroniHealth, time.Now())
			decision := policy.observeSQL([]string{"10.0.0.1"}, SQLHealthCheckOutcome{
				Results: []SQLHealthCheckResult{{IP: "10.0.0.1", Err: &pgconn.PgError{Code: code}}},
			}, time.Now())

			assert.True(t, decision.publish)
			assert.Equal(t, SQLRoutingResultCachedFallback, decision.routingResult)
			assert.Equal(t, patroniHealth, decision.health)
			assert.Equal(t, SQLHealthFallbackIndeterminate, decision.fallbackReason)
			assert.Equal(t, SQLHealthTimeoutNone, decision.timeoutType)
		})
	}
}

func TestHybridHealthFallsBackOnInvalidPassword(t *testing.T) {
	policy := newHybridHealthPolicy(true)
	patroniHealth := map[string]bool{"10.0.0.1": true}
	policy.observePatroni(patroniHealth, time.Now())
	decision := policy.observeSQL([]string{"10.0.0.1"}, SQLHealthCheckOutcome{
		Results: []SQLHealthCheckResult{{IP: "10.0.0.1", Err: &pgconn.PgError{Code: "28P01"}}},
	}, time.Now())

	assert.Equal(t, SQLRoutingResultCachedFallback, decision.routingResult)
	assert.Equal(t, patroniHealth, decision.health)
	assert.Equal(t, SQLHealthFallbackIndeterminate, decision.fallbackReason)
}

func TestHybridHealthUsesSuccessfulRoleWhenOtherCredentialHasInvalidPasswordOnSameIP(t *testing.T) {
	policy := newHybridHealthPolicy(true)
	policy.observePatroni(map[string]bool{"10.0.0.1": false}, time.Now())
	decision := policy.observeSQL([]string{"10.0.0.1"}, SQLHealthCheckOutcome{
		Results: []SQLHealthCheckResult{
			{IP: "10.0.0.1"},
			{IP: "10.0.0.1", Err: &pgconn.PgError{Code: "28P01"}},
		},
		PrimaryIPs: []string{"10.0.0.1"},
	}, time.Now())

	assert.Equal(t, SQLRoutingResultConclusive, decision.routingResult)
	assert.Equal(t, map[string]bool{"10.0.0.1": true}, decision.health)
	assert.Empty(t, decision.fallbackReason)
}

func TestHybridHealthPrimaryTakesPrecedenceOverAdminShutdownOnSameIP(t *testing.T) {
	const primaryIP, otherIP = "10.0.0.1", "10.0.0.2"
	primary := SQLHealthCheckResult{IP: primaryIP}
	terminated := SQLHealthCheckResult{IP: primaryIP, Err: &pgconn.PgError{Code: "57P01"}}
	for _, tc := range []struct {
		name    string
		results []SQLHealthCheckResult
	}{
		{name: "primary then admin shutdown", results: []SQLHealthCheckResult{primary, terminated}},
		{name: "admin shutdown then primary", results: []SQLHealthCheckResult{terminated, primary}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			policy := newHybridHealthPolicy(true)
			observedAt := time.Now()
			// Patroni selects a different IP, so falling back cannot satisfy the SQL routing assertion.
			policy.observePatroni(map[string]bool{primaryIP: false, otherIP: true}, observedAt)
			outcome := SQLHealthCheckOutcome{
				Results:    append(tc.results, SQLHealthCheckResult{IP: otherIP, InRecovery: true}),
				PrimaryIPs: []string{primaryIP},
			}
			decision := policy.observeSQL([]string{primaryIP, otherIP}, outcome, observedAt.Add(time.Second))
			assert.True(t, decision.publish)
			assert.Equal(t, SQLRoutingResultConclusive, decision.routingResult)
			assert.Empty(t, decision.fallbackReason)
			assert.Empty(t, decision.timeoutType)
			assert.Equal(t, map[string]bool{primaryIP: true, otherIP: false}, decision.health)
		})
	}
}

func TestHybridHealthFallsBackWhenInvalidPasswordAndTimeoutOnSameIP(t *testing.T) {
	policy := newHybridHealthPolicy(true)
	patroniHealth := map[string]bool{"10.0.0.1": true}
	policy.observePatroni(patroniHealth, time.Now())
	decision := policy.observeSQL([]string{"10.0.0.1"}, SQLHealthCheckOutcome{
		Results: []SQLHealthCheckResult{
			{IP: "10.0.0.1", Err: &pgconn.PgError{Code: "28P01"}},
			{IP: "10.0.0.1", Err: context.DeadlineExceeded},
		},
	}, time.Now())

	assert.Equal(t, SQLRoutingResultCachedFallback, decision.routingResult)
	assert.Equal(t, patroniHealth, decision.health)
	assert.Equal(t, SQLHealthFallbackTimeout, decision.fallbackReason)
	assert.Equal(t, SQLHealthTimeoutContext, decision.timeoutType)
}

func TestHybridHealthFallsBackWhenOtherIPHasInvalidPassword(t *testing.T) {
	policy := newHybridHealthPolicy(true)
	patroniHealth := map[string]bool{"10.0.0.1": false, "10.0.0.2": true}
	policy.observePatroni(patroniHealth, time.Now())
	decision := policy.observeSQL([]string{"10.0.0.1", "10.0.0.2"}, SQLHealthCheckOutcome{
		Results: []SQLHealthCheckResult{
			{IP: "10.0.0.1"},
			{IP: "10.0.0.2", Err: &pgconn.PgError{Code: "28P01"}},
		},
		PrimaryIPs: []string{"10.0.0.1"},
	}, time.Now())

	assert.Equal(t, SQLRoutingResultCachedFallback, decision.routingResult)
	assert.Equal(t, patroniHealth, decision.health)
	assert.Equal(t, SQLHealthFallbackIndeterminate, decision.fallbackReason)
}

func TestHybridHealthRoutesPrimaryWhenOtherIPRefusesConnections(t *testing.T) {
	policy := newHybridHealthPolicy(true)
	policy.observePatroni(map[string]bool{"10.0.0.1": false, "10.0.0.2": true}, time.Now())
	decision := policy.observeSQL([]string{"10.0.0.1", "10.0.0.2"}, SQLHealthCheckOutcome{
		Results: []SQLHealthCheckResult{
			{IP: "10.0.0.1"},
			{IP: "10.0.0.2", Err: syscall.ECONNREFUSED},
		},
		PrimaryIPs: []string{"10.0.0.1"},
	}, time.Now())

	assert.Equal(t, SQLRoutingResultConclusive, decision.routingResult)
	assert.Equal(t, map[string]bool{"10.0.0.1": true, "10.0.0.2": false}, decision.health)
	assert.Empty(t, decision.fallbackReason)
}

func TestHybridHealthFallsBackWhenOtherIPResetsConnection(t *testing.T) {
	policy := newHybridHealthPolicy(true)
	patroniHealth := map[string]bool{"10.0.0.1": false, "10.0.0.2": true}
	policy.observePatroni(patroniHealth, time.Now())
	decision := policy.observeSQL([]string{"10.0.0.1", "10.0.0.2"}, SQLHealthCheckOutcome{
		Results: []SQLHealthCheckResult{
			{IP: "10.0.0.1"},
			{IP: "10.0.0.2", Err: syscall.ECONNRESET},
		},
		PrimaryIPs: []string{"10.0.0.1"},
	}, time.Now())

	assert.Equal(t, SQLRoutingResultCachedFallback, decision.routingResult)
	assert.Equal(t, patroniHealth, decision.health)
	assert.Equal(t, SQLHealthFallbackIndeterminate, decision.fallbackReason)
}

func TestHybridHealthAgreementDiscardsUnmatchedExcursions(t *testing.T) {
	for _, sqlFirst := range []bool{true, false} {
		t.Run(leaderTimingCase(sqlFirst, "A"), func(t *testing.T) {
			p := newHybridHealthPolicy(true)
			observe := func(sql bool, ip string, second int) hybridRoutingDecision {
				if sql {
					return p.observeSQL([]string{ip}, SQLHealthCheckOutcome{PrimaryIPs: []string{ip}}, leaderTimingAt(second))
				}
				return p.observePatroni(leaderTimingIPs(ip), leaderTimingAt(second))
			}
			observe(true, "A", 0)
			observe(false, "A", 0)
			observe(sqlFirst, "B", 1)
			observe(sqlFirst, "A", 2)
			require.Empty(t, p.detectionTiming.pending)
			require.Empty(t, p.routingTiming.pending)
			observe(!sqlFirst, "B", 3600)
			got := observe(!sqlFirst, "A", 3601)
			assert.Nil(t, got.leaderDetection, "independent excursions must not emit an hour-long speedup")
			assert.Nil(t, got.routingComparison)
		})
	}
}
