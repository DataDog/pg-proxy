// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package pool

import (
	"context"
	"errors"
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
	require.NotNil(t, firstPatroni.routingComparison)
	assert.Equal(t, LeaderRoutingComparison{
		NewLeaderIP:  "10.0.0.2",
		Improvement:  firstPatroniAt.Sub(firstHybridAt),
		HybridSource: SQLRoutingResultConclusive,
	}, *firstPatroni.routingComparison)

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
			want: true,
		},
		"internal error": {
			err:  &pgconn.PgError{Code: "XX000"},
			want: true,
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

func TestHybridHealthTreatsConclusiveSystemErrorAsUnroutable(t *testing.T) {
	policy := newHybridHealthPolicy(true)
	policy.observePatroni(map[string]bool{"10.0.0.1": true}, time.Now())
	decision := policy.observeSQL([]string{"10.0.0.1"}, SQLHealthCheckOutcome{
		Results: []SQLHealthCheckResult{{IP: "10.0.0.1", Err: &pgconn.PgError{Code: "58030"}}},
	}, time.Now())

	assert.Equal(t, SQLRoutingResultConclusive, decision.routingResult)
	assert.Equal(t, map[string]bool{"10.0.0.1": false}, decision.health)
	assert.Empty(t, decision.fallbackReason)
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
