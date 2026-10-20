package pool

import (
	"context"
	"errors"
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
	checker := newTestSQLHealthChecker(t, prober, 1, SQLHealthCheckHooks{
		ProbeFinished: func(result SQLHealthCheckResult, patroniHealthy bool, _ time.Duration, tags []string) {
			mu.Lock()
			defer mu.Unlock()
			observations = append(observations, observation{result, patroniHealthy, tags})
		},
	})

	outcome := checker.Check(
		context.Background(),
		[]string{"10.0.0.1", "10.0.0.2"},
		func(ip string) string { return "conn-" + ip },
		map[string]bool{"10.0.0.1": true, "10.0.0.2": false},
		[]string{"database:test"},
	)

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
}
