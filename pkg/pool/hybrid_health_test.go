package pool

import (
	"context"
	"errors"
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

func TestHybridHealthFallsBackWhenAnyCredentialTimesOut(t *testing.T) {
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
	assert.Equal(t, SQLRoutingResultCachedFallback, decision.routingResult)
	assert.Equal(t, patroniHealth, decision.health)
	assert.Equal(t, SQLHealthFallbackTimeout, decision.fallbackReason)
	assert.Equal(t, SQLHealthTimeoutContext, decision.timeoutType)
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

func TestHybridHealthTreatsNonTimeoutProbeErrorAsConclusiveUnhealthy(t *testing.T) {
	policy := newHybridHealthPolicy(true)
	policy.observePatroni(map[string]bool{"10.0.0.1": true}, time.Now())
	decision := policy.observeSQL([]string{"10.0.0.1"}, SQLHealthCheckOutcome{
		Results: []SQLHealthCheckResult{{IP: "10.0.0.1", Err: errors.New("probe failed")}},
	}, time.Now())

	assert.Equal(t, SQLRoutingResultConclusive, decision.routingResult)
	assert.Equal(t, map[string]bool{"10.0.0.1": false}, decision.health)
	assert.Empty(t, decision.fallbackReason)
	assert.Empty(t, decision.timeoutType)
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

func TestHybridHealthComparesLeaderDetection(t *testing.T) {
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
		Speedup:           3 * time.Second,
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
		Speedup:           time.Second,
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
	assert.NotEmpty(t, policy.pendingRouting)

	// Do not let a Patroni observation after an off/on transition complete the stale comparison
	// with a duration that includes the interval where routing evaluation was disabled.
	policy.setRoutingEnabled(false)
	policy.setRoutingEnabled(true)
	patroniDecision := policy.observePatroni(
		map[string]bool{"10.0.0.2": true},
		started.Add(5*time.Minute),
	)

	assert.Nil(t, patroniDecision.routingComparison)
	assert.Empty(t, policy.pendingRouting)
	assert.Contains(t, policy.patroniRoutingObservation.primaryIPs, "10.0.0.2")
}

func TestHybridHealthPreservesNewerPendingRoutingComparison(t *testing.T) {
	policy := newHybridHealthPolicy(true)
	started := time.Now()
	policy.observePatroni(map[string]bool{"10.0.0.1": true}, started)
	policy.observeSQL([]string{"10.0.0.1"}, SQLHealthCheckOutcome{PrimaryIPs: []string{"10.0.0.1"}}, started)

	firstHybridAt := started.Add(time.Second)
	firstHybrid := policy.observeSQL(
		[]string{"10.0.0.2"},
		SQLHealthCheckOutcome{PrimaryIPs: []string{"10.0.0.2"}},
		firstHybridAt,
	)
	assert.Nil(t, firstHybrid.routingComparison)

	secondHybridAt := started.Add(2 * time.Second)
	secondHybrid := policy.observeSQL(
		[]string{"10.0.0.3"},
		SQLHealthCheckOutcome{PrimaryIPs: []string{"10.0.0.3"}},
		secondHybridAt,
	)
	assert.Nil(t, secondHybrid.routingComparison)

	firstPatroniAt := started.Add(4 * time.Second)
	firstPatroni := policy.observePatroni(map[string]bool{"10.0.0.2": true}, firstPatroniAt)
	// Hybrid has already left this leader, so its old timestamp is no longer comparable.
	assert.Nil(t, firstPatroni.routingComparison)
	assert.Contains(t, policy.pendingRouting, "10.0.0.3")

	secondPatroniAt := started.Add(6 * time.Second)
	secondPatroni := policy.observePatroni(map[string]bool{"10.0.0.3": true}, secondPatroniAt)
	require.NotNil(t, secondPatroni.routingComparison)
	assert.Equal(t, LeaderRoutingComparison{
		NewLeaderIP:  "10.0.0.3",
		Improvement:  secondPatroniAt.Sub(secondHybridAt),
		HybridSource: SQLRoutingResultConclusive,
	}, *secondPatroni.routingComparison)
	assert.Empty(t, policy.pendingRouting)
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
					var primaries []string
					if leader != "" {
						health[leader] = true
						primaries = []string{leader}
					}
					if source == "patroni" {
						return policy.observePatroni(health, started.Add(elapsed)).routingComparison
					}
					return policy.observeSQL([]string{"A", "B", "C"}, SQLHealthCheckOutcome{
						PrimaryIPs: primaries,
					}, started.Add(elapsed)).routingComparison
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
				require.Contains(t, policy.pendingRouting, "B")
				require.Nil(t, observe(firstSource, nextLeader, 20*time.Second))
				require.NotContains(t, policy.pendingRouting, "B")
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
						PrimaryIPs: primaries,
					}, started.Add(elapsed))
				}
				otherSource := "patroni"
				if interruptedSource == "patroni" {
					otherSource = "sql"
				}
				observe("sql", "A", 0)
				observe("patroni", "A", 0)
				observe(interruptedSource, "B", 10*time.Second)
				observe(interruptedSource, "", 20*time.Second)
				observe(interruptedSource, "", 25*time.Second)

				first, second := interruptedSource, otherSource
				if recoveryOrder == "after_other_source" {
					first, second = second, first
				}
				pending := observe(first, "B", 30*time.Second)
				require.Nil(t, pending.leaderDetection)
				require.Nil(t, pending.routingComparison)
				completed := observe(second, "B", 35*time.Second)
				require.NotNil(t, completed.leaderDetection)
				require.NotNil(t, completed.routingComparison)
				wantSpeedup := 5 * time.Second
				if first == "patroni" {
					wantSpeedup = -wantSpeedup
				}
				assert.Equal(t, wantSpeedup, completed.leaderDetection.Speedup)
				assert.Equal(t, wantSpeedup, completed.routingComparison.Improvement)
				assert.Equal(t, "B", completed.leaderDetection.NewLeaderIP)
				assert.Equal(t, "B", completed.routingComparison.NewLeaderIP)
				// B recovered after its own outage; this is not replacement-leader downtime.
				assert.False(t, completed.leaderDetection.HasDowntimeComparison)
				assert.Empty(t, policy.sqlDowntimesAwaitingPatroni)
				assert.Empty(t, policy.pendingLeaderDetections)
				assert.Empty(t, policy.pendingRouting)
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
		Speedup:                  3 * time.Second,
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
		Speedup:                  -2 * time.Second,
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
		Speedup:           time.Second,
		SQLRoutingEnabled: true,
	}, *patroniDecision.leaderDetection)
	assert.NotContains(t, policy.sqlDowntimesAwaitingPatroni, "10.0.0.2")
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
			assert.NotContains(t, policy.pendingLeaderDetections, "B")
			assert.NotContains(t, policy.sqlDowntimesAwaitingPatroni, "B")

			// A later promotion of B must not complete the earlier B comparison.
			later := observe(otherSource, "B", time.Minute)
			assert.Nil(t, later.leaderDetection)
			fresh := observe(firstSource, "B", time.Minute+5*time.Second)
			require.NotNil(t, fresh.leaderDetection)
			wantSpeedup := -5 * time.Second
			if firstSource == "patroni" {
				wantSpeedup = 5 * time.Second
			}
			assert.Equal(t, wantSpeedup, fresh.leaderDetection.Speedup)
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
	assert.Empty(t, policy.sqlDowntimesAwaitingPatroni)
}
