// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package pool

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeSQLHealthProbeResponse struct {
	inRecovery bool
	err        error
}

type fakeSQLHealthProber struct {
	mu        sync.Mutex
	responses map[string][]fakeSQLHealthProbeResponse
	calls     map[string]int
}

func (p *fakeSQLHealthProber) Probe(_ context.Context, _ string, connString string, _ []string) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	call := p.calls[connString]
	p.calls[connString] = call + 1
	responses := p.responses[connString]
	if len(responses) == 0 {
		return false, errors.New("no fake SQL health-check response")
	}
	if call >= len(responses) {
		call = len(responses) - 1
	}
	return responses[call].inRecovery, responses[call].err
}

func (p *fakeSQLHealthProber) callCount(connString string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls[connString]
}

func retrySQLHealthChecks(maxAttempts int) SQLHealthCheckRunner {
	return func(ctx context.Context, _ []string, attempt func() error) error {
		var err error
		for range maxAttempts {
			err = attempt()
			if err == nil || !IsSQLHealthCheckRetriable(err) {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
		return err
	}
}

func newTestSQLHealthChecker(t *testing.T, prober SQLHealthProber, maxAttempts int, hooks SQLHealthCheckHooks) *SQLHealthChecker {
	t.Helper()
	checker := NewSQLHealthChecker(context.Background(), SQLHealthCheckPoolsConfig{}, SQLHealthCheckerConfig{
		Prober:       prober,
		RunWithRetry: retrySQLHealthChecks(maxAttempts),
		Hooks:        hooks,
	})
	t.Cleanup(checker.Close)
	return checker
}

func TestSQLHealthCheckerReportsSnapshotAndComparison(t *testing.T) {
	prober := &fakeSQLHealthProber{
		responses: map[string][]fakeSQLHealthProbeResponse{
			"conn-10.0.0.1": {{inRecovery: false}},
			"conn-10.0.0.2": {{inRecovery: true}},
		},
		calls: make(map[string]int),
	}
	type observation struct {
		result         SQLHealthCheckResult
		patroniHealthy bool
		tags           []string
	}
	var mu sync.Mutex
	var observations []observation
	var completed []string
	var firstCompletionAt time.Time
	checker := newTestSQLHealthChecker(t, prober, 1, SQLHealthCheckHooks{
		ProbeFinished: func(result SQLHealthCheckResult, patroniHealthy bool, _ time.Duration, tags []string) {
			mu.Lock()
			defer mu.Unlock()
			observations = append(observations, observation{result, patroniHealthy, tags})
		},
	})

	beforeCheck := time.Now()
	outcome := checker.Check(
		context.Background(),
		[]string{"10.0.0.1", "10.0.0.2"},
		func(ip string) string { return "conn-" + ip },
		map[string]bool{"10.0.0.1": true, "10.0.0.2": false},
		[]string{"database:test"},
		func(result SQLHealthCheckResult) {
			mu.Lock()
			defer mu.Unlock()
			if firstCompletionAt.IsZero() {
				firstCompletionAt = time.Now()
			}
			completed = append(completed, result.IP)
		},
	)

	assert.False(t, outcome.StartedAt.Before(beforeCheck))
	assert.False(t, outcome.StartedAt.After(firstCompletionAt), "start time must precede probe completion")
	require.Len(t, outcome.Results, 2)
	assert.True(t, outcome.Results[0].HealthyPrimary())
	assert.False(t, outcome.Results[1].HealthyPrimary())
	assert.Equal(t, []string{"10.0.0.1"}, outcome.PrimaryIPs)
	require.Len(t, observations, 2)
	byIP := make(map[string]observation, len(observations))
	for _, observed := range observations {
		byIP[observed.result.IP] = observed
	}
	assert.True(t, byIP["10.0.0.1"].patroniHealthy)
	assert.False(t, byIP["10.0.0.2"].patroniHealthy)
	assert.ElementsMatch(t, []string{"database:test", "pool_role:primary", "ip:10.0.0.1"}, byIP["10.0.0.1"].tags)
	assert.ElementsMatch(t, []string{"10.0.0.1", "10.0.0.2"}, completed)
}

func TestSQLHealthCheckerRetriesAndEventuallySucceeds(t *testing.T) {
	prober := &fakeSQLHealthProber{
		responses: map[string][]fakeSQLHealthProbeResponse{
			"conn-10.0.0.1": {
				{err: errors.New("temporary")},
				{inRecovery: false},
			},
		},
		calls: make(map[string]int),
	}
	checker := newTestSQLHealthChecker(t, prober, 3, SQLHealthCheckHooks{})
	outcome := checker.Check(
		context.Background(),
		[]string{"10.0.0.1"},
		func(ip string) string { return "conn-" + ip },
		nil,
		nil,
		nil,
	)

	require.Len(t, outcome.Results, 1)
	assert.NoError(t, outcome.Results[0].Err)
	assert.True(t, outcome.Results[0].HealthyPrimary())
	assert.Equal(t, 2, prober.callCount("conn-10.0.0.1"))
}

func TestSQLHealthCheckerStopsAfterConclusivePostgresError(t *testing.T) {
	prober := &fakeSQLHealthProber{
		responses: map[string][]fakeSQLHealthProbeResponse{
			"conn-10.0.0.1": {{err: &pgconn.PgError{Code: sqlHealthCheckCrashShutdown}}},
		},
		calls: make(map[string]int),
	}
	checker := newTestSQLHealthChecker(t, prober, 3, SQLHealthCheckHooks{})
	outcome := checker.Check(
		context.Background(),
		[]string{"10.0.0.1"},
		func(ip string) string { return "conn-" + ip },
		nil,
		nil,
		nil,
	)

	require.Len(t, outcome.Results, 1)
	assert.Error(t, outcome.Results[0].Err)
	assert.Equal(t, 1, prober.callCount("conn-10.0.0.1"))
}

func TestSQLHealthCheckerReportsMultiplePrimaries(t *testing.T) {
	prober := &fakeSQLHealthProber{
		responses: map[string][]fakeSQLHealthProbeResponse{
			"conn-10.0.0.2": {{inRecovery: false}},
			"conn-10.0.0.1": {{inRecovery: false}},
		},
		calls: make(map[string]int),
	}
	var reported []string
	checker := newTestSQLHealthChecker(t, prober, 1, SQLHealthCheckHooks{
		MultiplePrimaries: func(primaryIPs []string, _ []string) {
			reported = append([]string(nil), primaryIPs...)
		},
	})
	outcome := checker.Check(
		context.Background(),
		[]string{"10.0.0.2", "10.0.0.1"},
		func(ip string) string { return "conn-" + ip },
		nil,
		nil,
		nil,
	)

	assert.Equal(t, []string{"10.0.0.1", "10.0.0.2"}, outcome.PrimaryIPs)
	assert.Equal(t, outcome.PrimaryIPs, reported)
}

func TestRegistrySQLHealthProberReportsPoolLookupFailure(t *testing.T) {
	var stage SQLHealthCheckStage
	var reportedErr error
	checker := NewSQLHealthChecker(context.Background(), SQLHealthCheckPoolsConfig{}, SQLHealthCheckerConfig{
		Hooks: SQLHealthCheckHooks{
			StageFinished: func(observedStage SQLHealthCheckStage, _ time.Duration, _ []string, err error) {
				stage = observedStage
				reportedErr = err
			},
		},
	})
	t.Cleanup(checker.Close)

	_, err := checker.prober.Probe(context.Background(), "10.0.0.1", "host='unterminated", nil)
	require.Error(t, err)
	assert.Equal(t, SQLHealthCheckPoolLookupStage, stage)
	assert.Equal(t, SQLHealthCheckPoolLookupStage, sqlHealthCheckResultStage(err))
	assert.Error(t, reportedErr)
}

func TestIsSQLHealthCheckRetriable(t *testing.T) {
	assert.True(t, IsSQLHealthCheckRetriable(errors.New("temporary")))
	assert.False(t, IsSQLHealthCheckRetriable(&pgconn.PgError{Code: sqlHealthCheckCrashShutdown}))
	assert.False(t, IsSQLHealthCheckRetriable(&pgconn.PgError{Code: sqlHealthCheckCannotConnectNow}))
	assert.False(t, IsSQLHealthCheckRetriable(&pgconn.PgError{Code: sqlHealthCheckInvalidPassword}))
}

func TestIsSQLHealthCheckPasswordAuthenticationError(t *testing.T) {
	passwordErr := &pgconn.PgError{Code: sqlHealthCheckInvalidPassword}
	assert.True(t, IsSQLHealthCheckPasswordAuthenticationError(passwordErr))
	assert.True(t, IsSQLHealthCheckPasswordAuthenticationError(&sqlHealthProbeError{
		stage: SQLHealthCheckAcquireStage,
		err:   passwordErr,
	}))
	assert.False(t, IsSQLHealthCheckPasswordAuthenticationError(errors.New("temporary")))
}

func TestAggregateSQLHealthCheckOutcomesOptimisticallyAcrossCredentials(t *testing.T) {
	timeoutErr := context.DeadlineExceeded
	aggregation := aggregateSQLHealthCheckOutcomes([]SQLHealthCheckOutcome{
		{
			Results: []SQLHealthCheckResult{
				{IP: "10.0.0.1", InRecovery: false},
				{IP: "10.0.0.2", InRecovery: true},
				{IP: "10.0.0.3", Err: timeoutErr},
			},
		},
		{
			Results: []SQLHealthCheckResult{
				{IP: "10.0.0.1", InRecovery: true},
				{IP: "10.0.0.2", Err: timeoutErr},
				{IP: "10.0.0.3", InRecovery: false},
			},
		},
	})

	assert.Equal(t, map[string]bool{
		"10.0.0.1": true,
		"10.0.0.2": false,
		"10.0.0.3": true,
	}, aggregation.Health)
	assert.Equal(t, []string{"10.0.0.1", "10.0.0.3"}, aggregation.Outcome.PrimaryIPs)
	assert.Len(t, aggregation.Outcome.Results, 6, "raw results remain available to conservative routing policies")
}

func TestAggregateSQLHealthCheckOutcomesPreservesEarliestStart(t *testing.T) {
	first := time.Now()
	later := first.Add(time.Second)
	for _, tc := range []struct {
		name   string
		starts []time.Time
		want   time.Time
	}{
		{"oldest first", []time.Time{first, later}, first},
		{"oldest last", []time.Time{later, first}, first},
		{"unknown first", []time.Time{{}, later}, time.Time{}},
		{"unknown last", []time.Time{later, {}}, time.Time{}},
		{"empty", nil, time.Time{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var outcomes []SQLHealthCheckOutcome
			for _, startedAt := range tc.starts {
				outcomes = append(outcomes, SQLHealthCheckOutcome{StartedAt: startedAt})
			}
			assert.Equal(t, tc.want, aggregateSQLHealthCheckOutcomes(outcomes).Outcome.StartedAt)
		})
	}
}

func TestAggregateSQLHealthCheckOutcomesLeavesIndeterminateIPUnknown(t *testing.T) {
	for _, err := range []error{context.DeadlineExceeded, errors.New("unknown"), &pgconn.PgError{Code: "28P01"}} {
		t.Run(err.Error(), func(t *testing.T) {
			aggregation := aggregateSQLHealthCheckOutcomes([]SQLHealthCheckOutcome{{
				Results: []SQLHealthCheckResult{{IP: "10.0.0.1", Err: err}},
			}})
			assert.NotContains(t, aggregation.Health, "10.0.0.1")
			assert.Len(t, aggregation.Outcome.Results, 1, "raw evidence remains available to routing policy")
		})
	}
}

// Different credentials can complete on opposite sides of a role change. Only
// non-overlapping successful probes establish which observation is newer.
func TestAggregateSQLHealthRolesUsesProbeOrder(t *testing.T) {
	const ip = "10.0.0.1"
	base := time.Now()
	role := func(standby bool, start, end time.Duration) SQLHealthCheckResult {
		return SQLHealthCheckResult{IP: ip, InRecovery: standby, StartedAt: base.Add(start), CompletedAt: base.Add(end)}
	}
	oldStandby, oldPrimary := role(true, 0, time.Millisecond), role(false, 0, time.Millisecond)
	newStandby, newPrimary := role(true, 2*time.Millisecond, 3*time.Millisecond), role(false, 2*time.Millisecond, 3*time.Millisecond)
	timeout := newPrimary
	timeout.Err = context.DeadlineExceeded
	shutdown := newPrimary
	shutdown.Err = &pgconn.PgError{Code: "57P02"}
	otherIP := newPrimary
	otherIP.IP = "10.0.0.2"
	for _, tc := range []struct {
		name    string
		results []SQLHealthCheckResult
		reason  string
		primary bool
		kept    int
	}{
		{"promotion", []SQLHealthCheckResult{oldStandby, newPrimary}, "", true, 1},
		{"demotion", []SQLHealthCheckResult{oldPrimary, newStandby}, "", false, 1},
		{"overlapping", []SQLHealthCheckResult{role(true, 0, 3*time.Millisecond), newPrimary}, SQLHealthFallbackConflictingRoles, false, 2},
		{"touching", []SQLHealthCheckResult{role(true, 0, 2*time.Millisecond), newPrimary}, SQLHealthFallbackConflictingRoles, false, 2},
		{"new observation resolves overlapping conflict", []SQLHealthCheckResult{role(true, 0, 3*time.Millisecond), newPrimary, role(false, 4*time.Millisecond, 5*time.Millisecond)}, "", true, 1},
		{"missing timing", []SQLHealthCheckResult{{IP: ip, InRecovery: true}, newPrimary}, SQLHealthFallbackConflictingRoles, false, 2},
		{"invalid timing", []SQLHealthCheckResult{oldStandby, role(false, 3*time.Millisecond, 2*time.Millisecond)}, SQLHealthFallbackConflictingRoles, false, 2},
		{"new timeout preserves successful role", []SQLHealthCheckResult{oldPrimary, timeout}, "", true, 2},
		{"new shutdown preserves successful role and error", []SQLHealthCheckResult{oldPrimary, shutdown}, "", true, 2},
		{"new result cannot replace another IP", []SQLHealthCheckResult{oldPrimary, otherIP}, SQLHealthFallbackMultiplePrimaries, true, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, reverse := range []bool{false, true} {
				results := slices.Clone(tc.results)
				if reverse {
					slices.Reverse(results)
				}
				original := slices.Clone(results)
				var outcomes []SQLHealthCheckOutcome
				for _, result := range results {
					outcomes = append(outcomes, SQLHealthCheckOutcome{StartedAt: result.StartedAt, Results: []SQLHealthCheckResult{result}})
				}
				aggregation := aggregateSQLHealthCheckOutcomes(outcomes)
				reason, _ := sqlHealthFallback(aggregation.Outcome)
				assert.Equal(t, tc.reason, reason)
				assert.Len(t, aggregation.Outcome.Results, tc.kept)
				roles, conflicting := aggregateSQLComparisonRoles(results)
				if tc.reason == SQLHealthFallbackConflictingRoles {
					assert.Equal(t, []string{ip}, conflicting)
					assert.NotContains(t, roles, ip)
				} else {
					assert.Empty(t, conflicting)
					assert.Equal(t, tc.primary, roles[ip])
					assert.Equal(t, tc.primary, aggregation.Health[ip])
				}
				assert.Equal(t, original, results, "cached diagnostic evidence must not be changed")
			}
		})
	}
}

func TestSQLHealthCheckerRecordsEachIPBeforeTheRoundCompletes(t *testing.T) {
	prober := &partiallyBlockingSQLHealthProber{blockingIP: "slow", blockingRecovery: true, blocked: make(chan struct{}), release: make(chan struct{})}
	var once sync.Once
	release := func() { once.Do(func() { close(prober.release) }) }
	defer release()
	checker := newTestSQLHealthChecker(t, prober, 1, SQLHealthCheckHooks{})
	quick := make(chan SQLHealthCheckResult, 1)
	done := make(chan SQLHealthCheckOutcome, 1)
	go func() {
		done <- checker.Check(context.Background(), []string{"quick", "slow"}, func(ip string) string { return ip }, nil, nil, func(result SQLHealthCheckResult) {
			if result.IP == "quick" {
				quick <- result
			}
		})
	}()
	select {
	case <-prober.blocked:
	case <-time.After(time.Second):
		t.Fatal("slow probe did not start")
	}
	var result SQLHealthCheckResult
	select {
	case result = <-quick:
	case <-time.After(time.Second):
		t.Fatal("quick probe did not finish independently")
	}
	require.False(t, result.StartedAt.IsZero())
	assert.False(t, result.CompletedAt.Before(result.StartedAt))
	unblockedAt := time.Now()
	release()
	select {
	case outcome := <-done:
		assert.False(t, result.StartedAt.Before(outcome.StartedAt))
		assert.False(t, result.CompletedAt.After(unblockedAt), "per-IP completion must not wait for the slow sibling")
		assert.False(t, outcome.Results[1].CompletedAt.Before(unblockedAt))
	case <-time.After(time.Second):
		t.Fatal("round did not complete")
	}
}
