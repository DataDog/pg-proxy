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
	for _, err := range []error{context.DeadlineExceeded, &pgconn.PgError{Code: "XX000"}} {
		t.Run(err.Error(), func(t *testing.T) {
			policy, started := policyWithSQLAheadOfPatroni(t)
			uncertain := SQLHealthCheckOutcome{Results: []SQLHealthCheckResult{
				{IP: "A", InRecovery: true}, {IP: "B", Err: err},
			}}
			for tick := 2; tick <= 3; tick++ {
				at := started.Add(time.Duration(tick) * time.Second)
				policy.setRoutingEnabled(true) // An unchanged flag must preserve retention.
				oldPatroni := policy.observeAndPublishPatroni(map[string]bool{"A": true, "B": false}, at)
				assert.False(t, oldPatroni.publish)
				retained := policy.observeSQL([]string{"A", "B"}, uncertain, at.Add(time.Millisecond))
				assert.True(t, retained.publish)
				assert.Equal(t, SQLRoutingResultRetainedSQL, retained.routingResult)
				assert.NotEmpty(t, retained.fallbackReason)
				assert.Equal(t, map[string]bool{"A": false, "B": true}, retained.health)
				assert.Nil(t, retained.routingComparison, "retaining B is not another leader transition")
			}
			agreed := policy.observeAndPublishPatroni(map[string]bool{"A": false, "B": true}, started.Add(4*time.Second))
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
			recovered := policy.observeAndPublishPatroni(recoveredHealth, recoveredAt)
			require.True(t, recovered.publish, "a different new leader must replace retained B immediately")
			assert.Equal(t, SQLRoutingResultPatroniRecovery, recovered.routingResult)
			assert.Equal(t, recoveredHealth, recovered.health)
			require.NotNil(t, recovered.routingComparison)
			assert.Equal(t, LeaderRoutingComparison{
				NewLeaderIP: "C", Improvement: 0, HybridSource: SQLRoutingResultPatroniRecovery,
			}, *recovered.routingComparison)

			// Fresh uncertainty must use cached Patroni C, not retain B again.
			uncertain.StartedAt = started.Add(4 * time.Second)
			fallback := policy.observeSQL(candidates, uncertain, started.Add(5*time.Second))
			assert.True(t, fallback.publish)
			assert.Equal(t, SQLRoutingResultCachedFallback, fallback.routingResult)
			assert.Equal(t, recoveredHealth, fallback.health)
		})
	}
}

func TestHybridHealthSQLSupersedesRecoveredPatroniLeader(t *testing.T) {
	policy, started := policyWithSQLAheadOfPatroni(t)
	candidates := []string{"A", "B", "C", "D"}
	recovered := policy.observeAndPublishPatroni(map[string]bool{"C": true}, started.Add(2*time.Second))
	require.True(t, recovered.publish)
	require.Equal(t, []string{"C"}, healthyIPs(recovered.health))

	// This complete SQL round starts after recovery and establishes D as the
	// new primary, while Patroni's current snapshot still reports C.
	selected := policy.observeSQL(candidates, SQLHealthCheckOutcome{
		StartedAt:  started.Add(2500 * time.Millisecond),
		PrimaryIPs: []string{"D"},
		Results: []SQLHealthCheckResult{
			{IP: "A", InRecovery: true}, {IP: "B", InRecovery: true},
			{IP: "C", InRecovery: true}, {IP: "D"},
		},
	}, started.Add(3*time.Second))
	require.True(t, selected.publish)
	require.Equal(t, SQLRoutingResultConclusive, selected.routingResult)
	require.Equal(t, []string{"D"}, healthyIPs(selected.health))

	// Unchanged Patroni C must not look like a new leader after D. Moving
	// C into the current exclusion must also preserve older exclusions A/B.
	for tick, leader := range []string{"C", "C", "A", "B"} {
		delayed := policy.observeAndPublishPatroni(map[string]bool{leader: true}, started.Add(time.Duration(4+tick)*time.Second))
		assert.False(t, delayed.publish, "superseded Patroni %s must not replace SQL D", leader)
		assert.Nil(t, delayed.routingComparison)
	}

	retained := policy.observeSQL(candidates, SQLHealthCheckOutcome{
		StartedAt: started.Add(7500 * time.Millisecond),
		Results: []SQLHealthCheckResult{
			{IP: "A", InRecovery: true}, {IP: "B", InRecovery: true},
			{IP: "C", InRecovery: true}, {IP: "D", Err: context.DeadlineExceeded},
		},
	}, started.Add(8*time.Second))
	assert.True(t, retained.publish)
	assert.Equal(t, SQLRoutingResultRetainedSQL, retained.routingResult)
	assert.Equal(t, []string{"D"}, healthyIPs(retained.health))
}

func TestHybridHealthPatroniRecoveryEligibility(t *testing.T) {
	for _, mode := range []string{"awaiting confirmation", "no primary"} {
		for _, tc := range []struct {
			name   string
			health map[string]bool
			delay  time.Duration
		}{
			{"before boundary", map[string]bool{"C": true}, -time.Millisecond},
			{"at boundary", map[string]bool{"C": true}, 0},
			{"superseded A", map[string]bool{"A": true}, time.Second},
			{"no leader", map[string]bool{"B": false, "C": false}, time.Second},
			{"multiple new leaders", map[string]bool{"C": true, "D": true}, time.Second},
			{"ambiguous before exclusions", map[string]bool{"A": true, "C": true}, time.Second},
		} {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				policy, started := policyWithSQLAheadOfPatroni(t)
				boundary, awaiting := started.Add(time.Second), "B"
				if mode == "no primary" {
					boundary, awaiting = started.Add(2*time.Second), ""
					policy.observeSQL([]string{"A", "B", "C"}, SQLHealthCheckOutcome{}, boundary)
				}
				for range 2 {
					decision := policy.observePatroni(tc.health, boundary.Add(tc.delay))
					assert.False(t, decision.publish)
					assert.Equal(t, awaiting, policy.awaitingPatroniLeader)
					assert.Equal(t, "A", policy.supersededPatroniLeader)
					assert.True(t, policy.patroniRecoveryAt.IsZero())
				}
				// Rejected observations must not prevent later unambiguous recovery.
				recovered := policy.observePatroni(map[string]bool{"C": true}, boundary.Add(2*time.Second))
				require.True(t, recovered.publish)
				assert.Equal(t, SQLRoutingResultPatroniRecovery, recovered.routingResult)
				assert.Equal(t, []string{"C"}, healthyIPs(recovered.health))
				assert.Equal(t, "A", policy.supersededPatroniLeader)
			})
		}
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

func TestHybridHealthSupersededSQLLeadersSurviveLeaderLoss(t *testing.T) {
	for _, conclusive := range []bool{false, true} {
		t.Run(fmt.Sprintf("conclusive_%t", conclusive), func(t *testing.T) {
			policy, started := policyWithSQLAheadOfPatroni(t)
			candidates := []string{"A", "B", "C"}
			policy.observeSQL(candidates, SQLHealthCheckOutcome{PrimaryIPs: []string{"C"}}, started.Add(2*time.Second))
			patroniB := map[string]bool{"A": false, "B": true, "C": false}
			require.False(t, policy.observePatroni(patroniB, started.Add(3*time.Second)).publish)
			retained := policy.observeSQL(candidates, SQLHealthCheckOutcome{
				Results: []SQLHealthCheckResult{{IP: "C", Err: context.DeadlineExceeded}},
			}, started.Add(3500*time.Millisecond))
			require.Equal(t, SQLRoutingResultRetainedSQL, retained.routingResult)
			require.Equal(t, []string{"C"}, healthyIPs(retained.health))

			b := SQLHealthCheckResult{IP: "B", Err: context.DeadlineExceeded}
			if conclusive {
				b = SQLHealthCheckResult{IP: "B", InRecovery: true}
			}
			lost := policy.observeSQL(candidates, SQLHealthCheckOutcome{
				Results: []SQLHealthCheckResult{
					{IP: "A", InRecovery: true}, b, {IP: "C", Err: &pgconn.PgError{Code: "57P01"}},
				},
			}, started.Add(4*time.Second))
			assert.Empty(t, healthyIPs(lost.health), "losing C must not restore cached Patroni B")
			for _, tick := range []int{5, 6} {
				delayed := policy.observePatroni(patroniB, started.Add(time.Duration(tick)*time.Second))
				assert.False(t, delayed.publish, "repeated stale B observations must not recover routing")
			}

			// Fresh conclusive SQL can establish B again. Agreement with cached
			// Patroni B then ends the catch-up phase and permits normal fallback.
			returned := policy.observeSQL(candidates, SQLHealthCheckOutcome{
				StartedAt:  started.Add(6 * time.Second),
				PrimaryIPs: []string{"B"},
				Results:    []SQLHealthCheckResult{{IP: "A", InRecovery: true}, {IP: "B"}, {IP: "C", InRecovery: true}},
			}, started.Add(7*time.Second))
			assert.Equal(t, []string{"B"}, healthyIPs(returned.health))
			fallback := policy.observeSQL(candidates, SQLHealthCheckOutcome{
				Results: []SQLHealthCheckResult{{IP: "B", Err: context.DeadlineExceeded}},
			}, started.Add(8*time.Second))
			assert.Equal(t, SQLRoutingResultCachedFallback, fallback.routingResult)
			assert.Equal(t, []string{"B"}, healthyIPs(fallback.health))
		})
	}
}

func TestHybridHealthSupersededSQLHistorySurvivesNoPrimaryGap(t *testing.T) {
	for _, fallback := range []bool{false, true} {
		t.Run(fmt.Sprintf("fallback_%t", fallback), func(t *testing.T) {
			policy, started := policyWithSQLAheadOfPatroni(t)
			candidates := []string{"A", "B", "C"}
			c := SQLHealthCheckResult{IP: "C", InRecovery: true}
			if fallback {
				c = SQLHealthCheckResult{IP: "C", Err: context.DeadlineExceeded}
			}
			gap := policy.observeSQL(candidates, SQLHealthCheckOutcome{
				Results: []SQLHealthCheckResult{{IP: "A", InRecovery: true}, {IP: "B", InRecovery: true}, c},
			}, started.Add(2*time.Second))
			require.Empty(t, healthyIPs(gap.health))
			selected := policy.observeSQL(candidates, SQLHealthCheckOutcome{
				PrimaryIPs: []string{"C"},
				Results:    []SQLHealthCheckResult{{IP: "A", InRecovery: true}, {IP: "B", InRecovery: true}, {IP: "C"}},
			}, started.Add(3*time.Second))
			require.Equal(t, []string{"C"}, healthyIPs(selected.health))
			delayed := policy.observeAndPublishPatroni(map[string]bool{"A": false, "B": true, "C": false}, started.Add(4*time.Second))
			assert.False(t, delayed.publish, "B before the gap must not replace the newer SQL leader C")
			assert.Nil(t, delayed.routingComparison)
		})
	}
}

func TestHybridHealthPatroniRecoveryPreservesSupersededExclusions(t *testing.T) {
	policy, started := policyWithSQLAheadOfPatroni(t)
	candidates := []string{"A", "B", "C", "D"}
	policy.observeSQL(candidates, SQLHealthCheckOutcome{PrimaryIPs: []string{"C"}}, started.Add(2*time.Second))
	policy.observeSQL(candidates, SQLHealthCheckOutcome{
		Results: []SQLHealthCheckResult{
			{IP: "A", InRecovery: true}, {IP: "B", InRecovery: true},
			{IP: "C", Err: &pgconn.PgError{Code: "57P01"}}, {IP: "D", InRecovery: true},
		},
	}, started.Add(3*time.Second))
	recovered := policy.observePatroni(map[string]bool{"D": true}, started.Add(4*time.Second))
	require.True(t, recovered.publish)
	require.Equal(t, []string{"D"}, healthyIPs(recovered.health))

	// Recovering to a different Patroni leader must not make delayed old
	// leaders eligible again while SQL has not confirmed the new topology.
	for tick, leader := range []string{"A", "B", "C"} {
		delayed := policy.observePatroni(map[string]bool{leader: true}, started.Add(time.Duration(5+tick)*time.Second))
		assert.Empty(t, healthyIPs(delayed.health), "superseded %s must stay excluded during recovery", leader)
	}
}

func TestHybridHealthConclusiveSQLReestablishesSupersededLeader(t *testing.T) {
	for _, leader := range []string{"A", "B"} {
		t.Run(leader, func(t *testing.T) {
			policy, started := policyWithSQLAheadOfPatroni(t)
			candidates := []string{"A", "B", "C"}
			if leader == "B" {
				policy.observeSQL(candidates, SQLHealthCheckOutcome{PrimaryIPs: []string{"C"}}, started.Add(1500*time.Millisecond))
			}
			negative := policy.observeSQL(candidates, SQLHealthCheckOutcome{}, started.Add(2*time.Second))
			require.Empty(t, healthyIPs(negative.health))
			assert.False(t, policy.observePatroni(map[string]bool{"A": true}, started.Add(3*time.Second)).publish)
			returned := policy.observeSQL(candidates, SQLHealthCheckOutcome{PrimaryIPs: []string{leader}}, started.Add(4*time.Second))
			require.True(t, returned.publish)
			require.Equal(t, SQLRoutingResultConclusive, returned.routingResult)
			require.Equal(t, []string{leader}, healthyIPs(returned.health))
			uncertain := SQLHealthCheckOutcome{Results: []SQLHealthCheckResult{{IP: leader, Err: context.DeadlineExceeded}}}
			decision := policy.observeSQL(candidates, uncertain, started.Add(5*time.Second))
			want := SQLRoutingResultCachedFallback // Patroni already agrees on A.
			if leader == "B" {
				want = SQLRoutingResultRetainedSQL
			}
			assert.Equal(t, want, decision.routingResult)
			assert.Equal(t, []string{leader}, healthyIPs(decision.health))
			policy.observePatroni(map[string]bool{leader: true}, started.Add(6*time.Second))
			policy.observePatroni(map[string]bool{"C": true}, started.Add(7*time.Second))
			fallback := policy.observeSQL(candidates, uncertain, started.Add(8*time.Second))
			assert.Equal(t, SQLRoutingResultCachedFallback, fallback.routingResult)
			assert.Equal(t, []string{"C"}, healthyIPs(fallback.health))
		})
	}
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
	policy.observeAndPublishPatroni(map[string]bool{oldIP: true, newIP: false}, started)
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
	olderPatroni := policy.observeAndPublishPatroni(map[string]bool{oldIP: true}, started.Add(500*time.Millisecond))
	assert.False(t, olderPatroni.publish)

	recoveredAt := started.Add(1300 * time.Millisecond)
	recoveredHealth := map[string]bool{oldIP: false, newIP: true}
	recovered := policy.observeAndPublishPatroni(recoveredHealth, recoveredAt)
	require.True(t, recovered.publish, "publish recovery on Patroni's callback, before SQL completes")
	assert.Equal(t, recoveredHealth, recovered.health)
	assert.Equal(t, SQLRoutingResultPatroniRecovery, recovered.routingResult)
	require.NotNil(t, recovered.routingComparison)
	assert.Equal(t, LeaderRoutingComparison{
		NewLeaderIP: newIP, Improvement: 0, HybridSource: SQLRoutingResultPatroniRecovery,
		HybridDowntime: 300 * time.Millisecond, PatroniDowntime: 300 * time.Millisecond,
		HasDowntimeComparison: true,
	}, *recovered.routingComparison)

	latePatroni := policy.observeAndPublishPatroni(map[string]bool{oldIP: true}, started.Add(1100*time.Millisecond))
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
	policy.observeAndPublishPatroni(recoveredHealth, started.Add(1600*time.Millisecond))
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

func TestHybridHealthSQLFreshnessAfterPatroniLeaderChange(t *testing.T) {
	for _, gap := range []map[string]bool{nil, {"A": true, "B": true}} {
		for _, tc := range []struct {
			name, primary string
			offset        time.Duration
			want          string
		}{
			{"old", "A", -time.Millisecond, SQLRoutingResultCachedFallback},
			{"equal", "A", 0, SQLRoutingResultCachedFallback},
			{"fresh", "A", time.Millisecond, SQLRoutingResultConclusive},
			{"agreement", "B", -time.Millisecond, SQLRoutingResultConclusive},
		} {
			t.Run(fmt.Sprintf("%s/ambiguous_gap_%t", tc.name, gap != nil), func(t *testing.T) {
				policy := newHybridHealthPolicy(true)
				before := time.Now()
				policy.observePatroni(map[string]bool{"A": true}, before)
				policy.observeSQL([]string{"A", "B"}, SQLHealthCheckOutcome{PrimaryIPs: []string{"A"}}, before)
				policy.observePatroni(gap, before.Add(500*time.Millisecond))
				changedAt := before.Add(time.Second)
				policy.observePatroni(map[string]bool{"B": true}, changedAt)
				policy.observePatroni(map[string]bool{"B": true}, before.Add(3*time.Second))
				decision := policy.observeSQL([]string{"A", "B"}, SQLHealthCheckOutcome{
					StartedAt: changedAt.Add(tc.offset), PrimaryIPs: []string{tc.primary},
				}, before.Add(4*time.Second))
				require.True(t, decision.publish)
				assert.Equal(t, tc.want, decision.routingResult)
				leader := tc.primary
				if tc.want == SQLRoutingResultCachedFallback {
					leader = "B"
					assert.Equal(t, SQLHealthFallbackStaleEvidence, decision.fallbackReason)
					assert.Empty(t, policy.supersededPatroniLeader)
				}
				assert.Equal(t, []string{leader}, healthyIPs(decision.health))
			})
		}
	}
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

func TestHybridHealthIgnoresAmbiguousLeaderForRoutingTiming(t *testing.T) {
	policy := newHybridHealthPolicy(true)
	started := time.Now()
	policy.observeAndPublishPatroni(map[string]bool{"10.0.0.1": true}, started)
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
	assert.Nil(t, ambiguous.routingComparison)

	conclusiveAt := ambiguousAt.Add(time.Second)
	conclusive := policy.observeSQL([]string{"10.0.0.1", "10.0.0.2"}, SQLHealthCheckOutcome{
		Results: []SQLHealthCheckResult{
			{IP: "10.0.0.1", InRecovery: true},
			{IP: "10.0.0.2"},
		},
		PrimaryIPs: []string{"10.0.0.2"},
	}, conclusiveAt)
	assert.Equal(t, SQLRoutingResultConclusive, conclusive.routingResult)
	assert.Nil(t, conclusive.routingComparison)

	patroniAt := conclusiveAt.Add(time.Second)
	patroni := policy.observeAndPublishPatroni(map[string]bool{"10.0.0.2": true}, patroniAt)
	require.NotNil(t, patroni.routingComparison)
	assert.Equal(t, LeaderRoutingComparison{
		NewLeaderIP: "10.0.0.2", Improvement: time.Second, HybridSource: SQLRoutingResultConclusive,
	}, *patroni.routingComparison)
}

func TestHybridHealthComparesRoutingPublication(t *testing.T) {
	policy := newHybridHealthPolicy(true)
	started := time.Now()
	policy.observeAndPublishPatroni(map[string]bool{"10.0.0.1": true}, started)
	policy.observeSQL([]string{"10.0.0.1"}, SQLHealthCheckOutcome{PrimaryIPs: []string{"10.0.0.1"}}, started)

	sqlDecision := policy.observeSQL([]string{"10.0.0.2"}, SQLHealthCheckOutcome{PrimaryIPs: []string{"10.0.0.2"}}, started.Add(time.Second))
	assert.Nil(t, sqlDecision.routingComparison)
	patroniDecision := policy.observeAndPublishPatroni(map[string]bool{"10.0.0.2": true}, started.Add(5*time.Second))
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
	policy.observeAndPublishPatroni(map[string]bool{"10.0.0.1": true}, started)
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
	patroniDecision := policy.observeAndPublishPatroni(
		map[string]bool{"10.0.0.2": true},
		started.Add(5*time.Minute),
	)

	assert.Nil(t, patroniDecision.routingComparison)
	assert.Empty(t, policy.routingTiming.pending)
	assert.Contains(t, policy.routingTiming.reference.lastNonempty, "10.0.0.2")
}

func TestHybridHealthRoutingFlagToggleDropsLeadershipState(t *testing.T) {
	for _, state := range []string{"retained SQL", "superseded SQL", "no primary", "Patroni recovery"} {
		t.Run(state, func(t *testing.T) {
			policy, started := policyWithSQLAheadOfPatroni(t)
			candidates := []string{"A", "B", "C", "D"}
			switch state {
			case "superseded SQL":
				policy.observeSQL(candidates, SQLHealthCheckOutcome{PrimaryIPs: []string{"C"}}, started.Add(2*time.Second))
			case "no primary", "Patroni recovery":
				policy.observeSQL(candidates, SQLHealthCheckOutcome{}, started.Add(2*time.Second))
				if state == "Patroni recovery" {
					require.True(t, policy.observePatroni(map[string]bool{"C": true}, started.Add(3*time.Second)).publish)
				}
			}
			policy.setRoutingEnabled(false)
			for tick, leader := range []string{"B", "A"} {
				current := map[string]bool{leader: true}
				decision := policy.observePatroni(current, started.Add(time.Duration(4+tick)*time.Second))
				require.True(t, decision.publish)
				require.Equal(t, current, decision.health)
			}
			policy.setRoutingEnabled(true)
			assert.True(t, policy.patroniLeaderChangedAt.IsZero())
			// No previous-mode recovery barrier may reject the first new-mode SQL result.
			initial := policy.observeSQL(candidates, SQLHealthCheckOutcome{PrimaryIPs: []string{"A"}}, started.Add(5500*time.Millisecond))
			require.True(t, initial.publish)
			require.Equal(t, SQLRoutingResultConclusive, initial.routingResult)
			// Old no-primary/recovery/retention must not initiate a new recovery.
			assert.False(t, policy.observePatroni(map[string]bool{"E": true}, started.Add(6*time.Second)).publish)
			policy.observePatroni(map[string]bool{"A": true}, started.Add(7*time.Second))
			uncertain := SQLHealthCheckOutcome{Results: []SQLHealthCheckResult{{IP: "B", Err: context.DeadlineExceeded}}}
			fallback := policy.observeSQL(candidates, uncertain, started.Add(8*time.Second))
			require.True(t, fallback.publish)
			assert.Equal(t, SQLRoutingResultCachedFallback, fallback.routingResult)
			assert.Equal(t, []string{"A"}, healthyIPs(fallback.health))
			selected := policy.observeSQL(candidates, SQLHealthCheckOutcome{
				StartedAt: started.Add(8500 * time.Millisecond), PrimaryIPs: []string{"D"},
			}, started.Add(9*time.Second))
			require.True(t, selected.publish)
			assert.Equal(t, SQLRoutingResultConclusive, selected.routingResult)
			assert.Equal(t, []string{"D"}, healthyIPs(selected.health))
			// B's superseded history belonged to the old mode.
			recovered := policy.observePatroni(map[string]bool{"B": true}, started.Add(10*time.Second))
			require.True(t, recovered.publish)
			assert.Equal(t, SQLRoutingResultPatroniRecovery, recovered.routingResult)
			assert.Equal(t, []string{"B"}, healthyIPs(recovered.health))
		})
	}
}

func TestHybridHealthPreservesNewerPendingRoutingComparison(t *testing.T) {
	policy := newHybridHealthPolicy(true)
	started := time.Now()
	// Keep all candidates while Patroni catches up through SQL's earlier leaders.
	candidates := []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}
	policy.observeAndPublishPatroni(map[string]bool{"10.0.0.1": true}, started)
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
	firstPatroni := policy.observeAndPublishPatroni(map[string]bool{"10.0.0.2": true}, firstPatroniAt)
	assert.False(t, firstPatroni.publish, "late confirmation of the first SQL leader must not roll routing back")
	// Hybrid has already left this leader, so its old timestamp is no longer comparable.
	assert.Nil(t, firstPatroni.routingComparison)
	assert.Contains(t, policy.routingTiming.pending, "10.0.0.3")

	secondPatroniAt := started.Add(6 * time.Second)
	secondPatroni := policy.observeAndPublishPatroni(map[string]bool{"10.0.0.3": true}, secondPatroniAt)
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
						return policy.observeAndPublishPatroni(health, started.Add(elapsed))
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
				require.Nil(t, pending.routingComparison)
				if interruptedSource == "sql" && first == "patroni" {
					assert.False(t, pending.publish, "Patroni must not recover to SQL B after SQL withdrew B")
				}
				completed := observe(second, "B", 35*time.Second)
				require.NotNil(t, completed.routingComparison)
				wantSpeedup := 5 * time.Second
				if first == "patroni" {
					wantSpeedup = -wantSpeedup
				}
				assert.Equal(t, wantSpeedup, completed.routingComparison.Improvement)
				assert.Equal(t, "B", completed.routingComparison.NewLeaderIP)
				// B recovered after its own outage; this is not replacement-leader downtime.
				assert.False(t, completed.routingComparison.HasDowntimeComparison)
				assert.Empty(t, policy.routingTiming.pending)
			})
		}
	}
}

func TestHybridHealthDoesNotCompareDowntimeWhenSameHybridLeaderRecovers(t *testing.T) {
	policy := newHybridHealthPolicy(true)
	started := time.Now()
	policy.observeAndPublishPatroni(map[string]bool{"10.0.0.1": true}, started)
	policy.observeSQL([]string{"10.0.0.1"}, SQLHealthCheckOutcome{PrimaryIPs: []string{"10.0.0.1"}}, started)
	policy.observeSQL([]string{"10.0.0.1"}, SQLHealthCheckOutcome{}, started.Add(time.Second))
	recovered := policy.observeSQL(
		[]string{"10.0.0.1"},
		SQLHealthCheckOutcome{PrimaryIPs: []string{"10.0.0.1"}},
		started.Add(2*time.Second),
	)

	assert.Nil(t, recovered.routingComparison)
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

func TestHybridHealthPrimaryTakesPrecedenceOverStartupOnSameIP(t *testing.T) {
	const primaryIP, otherIP = "10.0.0.1", "10.0.0.2"
	primary := SQLHealthCheckResult{IP: primaryIP}
	startup := SQLHealthCheckResult{IP: primaryIP, Err: &pgconn.PgError{Code: "57P03", Message: "the database system is starting up"}}
	for _, tc := range []struct {
		name    string
		results []SQLHealthCheckResult
	}{
		{name: "primary then startup", results: []SQLHealthCheckResult{primary, startup}},
		{name: "startup then primary", results: []SQLHealthCheckResult{startup, primary}},
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
				return p.observeAndPublishPatroni(leaderTimingIPs(ip), leaderTimingAt(second))
			}
			observe(true, "A", 0)
			// Establish agreement after SQL selected A before testing independent excursions.
			p.observeAndPublishPatroni(leaderTimingIPs("A"), leaderTimingAt(0).Add(time.Nanosecond))
			observe(sqlFirst, "B", 1)
			observe(sqlFirst, "A", 2)
			require.Empty(t, p.routingTiming.pending)
			observe(!sqlFirst, "B", 3600)
			got := observe(!sqlFirst, "A", 3601)
			assert.Nil(t, got.routingComparison)
		})
	}
}

func TestHybridHealthRoutingBanlistPersistsUntilConclusiveResult(t *testing.T) {
	const (
		oldPrimary = "10.0.0.1"
		standby    = "10.0.0.2"
	)
	policy := newHybridHealthPolicy(true)
	policy.observePatroni(map[string]bool{oldPrimary: true, standby: false}, time.Now())

	shutdown := policy.observeSQL([]string{oldPrimary, standby}, SQLHealthCheckOutcome{
		Results: []SQLHealthCheckResult{
			{IP: oldPrimary, Err: &pgconn.PgError{Code: sqlHealthCheckCrashShutdown}},
			{IP: standby, InRecovery: true},
		},
	}, time.Now())
	require.Equal(t, []SQLRoutingBanChange{{
		IP:     oldPrimary,
		Banned: true,
		Reason: SQLRoutingBanReasonPostgresShutdown,
	}}, shutdown.routingBanChanges)

	replacementPrimary := policy.observeSQL([]string{oldPrimary, standby}, SQLHealthCheckOutcome{
		Results: []SQLHealthCheckResult{
			{IP: oldPrimary, Err: context.DeadlineExceeded},
			{IP: standby},
		},
		PrimaryIPs: []string{standby},
	}, time.Now())
	assert.Equal(t, SQLRoutingResultConclusive, replacementPrimary.routingResult)
	assert.Empty(t, replacementPrimary.fallbackReason)
	assert.Empty(t, replacementPrimary.timeoutType)
	assert.Equal(t, map[string]bool{oldPrimary: false, standby: true}, replacementPrimary.health)
	assert.Empty(t, replacementPrimary.routingBanChanges, "repeated failures must not emit duplicate ban events")

	eligibleTimeout := policy.observeSQL([]string{oldPrimary, standby}, SQLHealthCheckOutcome{
		Results: []SQLHealthCheckResult{
			{IP: oldPrimary, Err: context.DeadlineExceeded},
			{IP: standby, Err: context.DeadlineExceeded},
		},
	}, time.Now())
	assert.Equal(t, SQLRoutingResultRetainedSQL, eligibleTimeout.routingResult)
	assert.Equal(t, SQLHealthFallbackTimeout, eligibleTimeout.fallbackReason)
	assert.Equal(t, SQLHealthTimeoutContext, eligibleTimeout.timeoutType)
	assert.Equal(t, map[string]bool{oldPrimary: false, standby: true}, eligibleTimeout.health, "retain the unbanned SQL leader while Patroni still reports the banned old leader")
	assert.Empty(t, eligibleTimeout.routingBanChanges)

	recovered := policy.observeSQL([]string{oldPrimary, standby}, SQLHealthCheckOutcome{
		Results: []SQLHealthCheckResult{
			{IP: oldPrimary, InRecovery: true},
			{IP: standby},
		},
		PrimaryIPs: []string{standby},
	}, time.Now())
	assert.Equal(t, SQLRoutingResultConclusive, recovered.routingResult)
	assert.Equal(t, map[string]bool{oldPrimary: false, standby: true}, recovered.health)
	assert.Equal(t, []SQLRoutingBanChange{{
		IP:     oldPrimary,
		Reason: SQLRoutingUnbanReasonConclusiveResult,
	}}, recovered.routingBanChanges)
}

func TestHybridHealthRoutingBanlistShutdownWinsOverSameRoundSuccess(t *testing.T) {
	admin := &pgconn.PgError{Code: "57P01"}
	crash := &pgconn.PgError{Code: sqlHealthCheckCrashShutdown}
	for name, shutdown := range map[string]error{
		"plain":              crash,
		"admin termination":  admin,
		"admin before crash": errors.Join(admin, crash),
		"crash before admin": errors.Join(crash, admin),
		"wrapped joined":     fmt.Errorf("connect: %w", errors.Join(admin, crash)),
	} {
		t.Run(name, func(t *testing.T) {
			const primary = "10.0.0.1"
			policy := newHybridHealthPolicy(true)
			policy.observePatroni(map[string]bool{primary: true}, time.Now())

			mixed := policy.observeSQL([]string{primary}, SQLHealthCheckOutcome{
				Results: []SQLHealthCheckResult{
					{IP: primary, Err: shutdown},
					{IP: primary},
				},
				PrimaryIPs: []string{primary},
			}, time.Now())
			require.Equal(t, []SQLRoutingBanChange{{
				IP:     primary,
				Banned: true,
				Reason: SQLRoutingBanReasonPostgresShutdown,
			}}, mixed.routingBanChanges)
			assert.Equal(t, map[string]bool{primary: false}, mixed.health)

			recovered := policy.observeSQL([]string{primary}, SQLHealthCheckOutcome{
				Results:    []SQLHealthCheckResult{{IP: primary}},
				PrimaryIPs: []string{primary},
			}, time.Now())
			assert.Equal(t, []SQLRoutingBanChange{{
				IP:     primary,
				Reason: SQLRoutingUnbanReasonConclusiveResult,
			}}, recovered.routingBanChanges)
			assert.Equal(t, map[string]bool{primary: true}, recovered.health)
		})
	}
}

func TestHybridHealthRoutingBanPreservesPatroniShadowAndRawEvidence(t *testing.T) {
	const ip = "10.0.0.1"
	policy := newHybridHealthPolicy(false)
	patroni := map[string]bool{ip: true}
	policy.observePatroni(patroni, time.Now())
	outcome := SQLHealthCheckOutcome{Results: []SQLHealthCheckResult{
		{IP: ip, Err: &pgconn.PgError{Code: sqlHealthCheckCrashShutdown}},
		{IP: ip},
	}, PrimaryIPs: []string{ip}}
	decision := policy.observeSQL([]string{ip}, outcome, time.Now())
	require.Len(t, decision.routingBanChanges, 1)
	assert.False(t, decision.publish)
	assert.Nil(t, decision.health)
	assert.Equal(t, []string{ip}, outcome.PrimaryIPs)
	assert.Len(t, outcome.Results, 2)
	assert.Equal(t, patroni, policy.patroniSnapshot())
	refresh := policy.observePatroni(patroni, time.Now())
	assert.True(t, refresh.publish)
	assert.Equal(t, patroni, refresh.health)
}

func TestHybridHealthRoutingBanPreventsRetainingShutdownLeader(t *testing.T) {
	policy, started := policyWithSQLAheadOfPatroni(t)
	decision := policy.observeSQL([]string{"A", "B", "C"}, SQLHealthCheckOutcome{
		StartedAt: started.Add(2 * time.Second),
		Results: []SQLHealthCheckResult{
			{IP: "A", InRecovery: true},
			{IP: "B", Err: &pgconn.PgError{Code: sqlHealthCheckCrashShutdown}},
			{IP: "C", Err: context.DeadlineExceeded},
		},
	}, started.Add(3*time.Second))

	require.Equal(t, SQLRoutingResultCachedFallback, decision.routingResult)
	assert.Contains(t, decision.routingBannedIPs, "B")
	assert.Empty(t, healthyIPs(decision.health), "neither banned B nor superseded Patroni A may be selected")
	assert.Empty(t, policy.awaitingPatroniLeader)
}

func TestHybridHealthPatroniRecoveryRespectsRoutingBan(t *testing.T) {
	policy := newHybridHealthPolicy(true)
	started := time.Now()
	policy.observePatroni(map[string]bool{"A": true, "B": false}, started)
	noPrimary := policy.observeSQL([]string{"A", "B"}, SQLHealthCheckOutcome{
		StartedAt: started.Add(time.Second),
		Results: []SQLHealthCheckResult{
			{IP: "A", Err: &pgconn.PgError{Code: sqlHealthCheckCrashShutdown}},
			{IP: "B", InRecovery: true},
		},
	}, started.Add(2*time.Second))
	require.Equal(t, SQLRoutingResultConclusive, noPrimary.routingResult)
	require.Contains(t, noPrimary.routingBannedIPs, "A")
	banned := policy.observePatroni(map[string]bool{"A": true, "B": false}, started.Add(3*time.Second))
	assert.False(t, banned.publish, "a banned Patroni leader must not initiate recovery")
	assert.Empty(t, healthyIPs(banned.health))

	recovered := policy.observePatroni(map[string]bool{"A": false, "B": true}, started.Add(4*time.Second))
	require.True(t, recovered.publish)
	assert.Equal(t, SQLRoutingResultPatroniRecovery, recovered.routingResult)
	assert.Equal(t, []string{"B"}, healthyIPs(recovered.health))
	bannedAgain := policy.observePatroni(map[string]bool{"A": true, "B": false}, started.Add(5*time.Second))
	assert.True(t, bannedAgain.publish, "active recovery still publishes loss of its allowed leader")
	assert.Empty(t, healthyIPs(bannedAgain.health), "active recovery must not route back to banned A")
	assert.Contains(t, policy.routingBannedIPs, "A")
}

func TestHybridHealthBannedPatroniLeaderDoesNotInvalidateSQLLeader(t *testing.T) {
	policy, started := policyWithSQLAheadOfPatroni(t)
	candidates := []string{"A", "B", "C"}
	policy.observeSQL(candidates, SQLHealthCheckOutcome{
		StartedAt: started.Add(2 * time.Second),
		Results: []SQLHealthCheckResult{
			{IP: "A", InRecovery: true},
			{IP: "B"},
			{IP: "C", Err: &pgconn.PgError{Code: sqlHealthCheckCrashShutdown}},
		},
		PrimaryIPs: []string{"B"},
	}, started.Add(3*time.Second))

	patroni := policy.observePatroni(map[string]bool{"C": true}, started.Add(4*time.Second))
	require.False(t, patroni.publish, "banned C cannot replace SQL leader B")

	// This SQL poll began before Patroni changed to C, but C is not an eligible
	// alternative. Its newer Patroni timestamp must not force fallback to no route.
	decision := policy.observeSQL(candidates, SQLHealthCheckOutcome{
		StartedAt: started.Add(3 * time.Second),
		Results: []SQLHealthCheckResult{
			{IP: "A", InRecovery: true},
			{IP: "B"},
			{IP: "C", Err: context.DeadlineExceeded},
		},
		PrimaryIPs: []string{"B"},
	}, started.Add(5*time.Second))
	assert.Equal(t, SQLRoutingResultConclusive, decision.routingResult)
	assert.Equal(t, []string{"B"}, healthyIPs(decision.health))
	assert.Contains(t, decision.routingBannedIPs, "C")
}

// observeSQL models a policy decision immediately published at observedAt.
// Production uses observeSQLRound and records the actual worker publication separately.
func (p *hybridHealthPolicy) observeSQL(candidateIPs []string, outcome SQLHealthCheckOutcome, observedAt time.Time) hybridRoutingDecision {
	decision := p.observeSQLRound(candidateIPs, outcome, SQLHealthResultsSnapshot{
		ScheduledAt: observedAt, ObservedAt: observedAt, Results: outcome.Results,
	})
	if decision.publish {
		decision.routingComparison = p.observePublishedRouting(decision.health, observedAt, decision.routingResult)
	}
	return decision
}

// observeAndPublishPatroni models immediate publication when the policy requests it.
func (p *hybridHealthPolicy) observeAndPublishPatroni(health map[string]bool, observedAt time.Time) hybridRoutingDecision {
	decision := p.observePatroni(health, observedAt)
	if decision.publish {
		if comparison := p.observePublishedRouting(decision.health, observedAt, decision.routingResult); comparison != nil {
			decision.routingComparison = comparison
		}
	}
	return decision
}
