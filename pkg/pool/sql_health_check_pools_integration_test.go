// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

//go:build integration

package pool

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/puddle/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/DataDog/pg-proxy/internal/itest"
)

type integrationHealthProber struct {
	mu  sync.Mutex
	ips []string
}

func (p *integrationHealthProber) Lookup(string, []string) []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.ips...)
}

func (*integrationHealthProber) Probe(string, string, string, bool, []string) (bool, string, error) {
	return true, "", nil
}

func (p *integrationHealthProber) setIPs(ips ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ips = append([]string(nil), ips...)
}

type delayedIntegrationSQLHealthProber struct {
	inner SQLHealthProber

	mu           sync.Mutex
	blockNextIP  string
	blockStarted chan struct{}
	releaseBlock chan struct{}
}

func (p *delayedIntegrationSQLHealthProber) Probe(
	ctx context.Context,
	ip string,
	connString string,
	tags []string,
) (bool, error) {
	inRecovery, err := p.inner.Probe(ctx, ip, connString, tags)

	p.mu.Lock()
	shouldBlock := p.blockNextIP == ip
	if shouldBlock {
		p.blockNextIP = ""
	}
	p.mu.Unlock()
	if shouldBlock {
		close(p.blockStarted)
		<-p.releaseBlock
	}
	return inRecovery, err
}

func (p *delayedIntegrationSQLHealthProber) blockNext(ip string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.blockNextIP = ip
}

func startSQLHealthCheckIntegrationPostgres(t *testing.T) (context.Context, string, *pgx.ConnConfig) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	postgresContainer, err := postgres.Run(ctx, itest.PostgresImage(),
		postgres.WithDatabase("sql_health_check_integration"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(time.Minute),
		),
	)
	require.NoError(t, err, "start Postgres container")
	t.Cleanup(func() { require.NoError(t, postgresContainer.Terminate(context.Background())) })

	connString, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	connConfig, err := pgx.ParseConfig(connString)
	require.NoError(t, err)
	return ctx, connString, connConfig
}

func TestSQLHealthCheckPoolAgainstPostgres(t *testing.T) {
	ctx, connString, connConfig := startSQLHealthCheckIntegrationPostgres(t)

	healthCheckPools := NewSQLHealthCheckPools(context.Background(), SQLHealthCheckPoolsConfig{})
	t.Cleanup(healthCheckPools.Close)

	pool, err := healthCheckPools.GetOrAdd(connConfig.Host, connString)
	require.NoError(t, err)
	samePool, err := healthCheckPools.GetOrAdd(connConfig.Host, connString)
	require.NoError(t, err)
	assert.Same(t, pool, samePool)
	assert.Equal(t, 1, healthCheckPools.Len())

	conn, err := pool.Acquire(ctx)
	require.NoError(t, err)

	var inRecovery bool
	require.NoError(t, conn.QueryRow(ctx, SQLHealthCheckQuery).Scan(&inRecovery))
	assert.False(t, inRecovery)
	conn.Release()

	prober := registrySQLHealthProber{pools: healthCheckPools}
	for attempt := 1; attempt <= 2; attempt++ {
		inRecovery, err = prober.Probe(ctx, connConfig.Host, connString, nil)
		require.NoError(t, err, "SQL health probe attempt %d", attempt)
		assert.False(t, inRecovery, "Postgres should report itself as primary")
		assert.EqualValues(t, 0, pool.Stat().AcquiredConns(), "probe attempt %d should release its connection", attempt)
	}
	assert.EqualValues(t, 1, pool.Stat().TotalConns(), "both probes should reuse the single health-check connection")

	admin, err := pgx.Connect(ctx, connString)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, admin.Close(context.Background())) })

	var healthCheckConnections int
	err = admin.QueryRow(ctx, `
		SELECT count(*)
		FROM pg_stat_activity
		WHERE datname = current_database()
		  AND application_name = $1
	`, DefaultSQLHealthCheckApplicationName).Scan(&healthCheckConnections)
	require.NoError(t, err)
	assert.Equal(t, 1, healthCheckConnections)
}

func TestSQLHealthWorkerHostWideRoundPreventsStaleCredentialOverlap(t *testing.T) {
	ctx, connString, connConfig := startSQLHealthCheckIntegrationPostgres(t)
	oldIP := connConfig.Host
	replacementIP := "127.0.0.1"
	if oldIP == replacementIP {
		replacementIP = "localhost"
	}
	if oldIP != "localhost" && oldIP != "127.0.0.1" {
		t.Skipf("integration test requires a local Docker endpoint, got %q", oldIP)
	}

	// Prove both independently tracked address strings reach the real PostgreSQL server before
	// relying on them for the simulated DNS replacement.
	replacementConfig := connConfig.Copy()
	replacementConfig.Host = replacementIP
	replacementConn, err := pgx.ConnectConfig(ctx, replacementConfig)
	require.NoError(t, err, "connect to replacement address")
	require.NoError(t, replacementConn.Close(ctx))

	patroniProber := &integrationHealthProber{ips: []string{oldIP}}
	slowPools := NewSQLHealthCheckPools(context.Background(), SQLHealthCheckPoolsConfig{})
	fastPools := NewSQLHealthCheckPools(context.Background(), SQLHealthCheckPoolsConfig{})
	t.Cleanup(slowPools.Close)
	t.Cleanup(fastPools.Close)
	slowProber := &delayedIntegrationSQLHealthProber{
		inner:        registrySQLHealthProber{pools: slowPools},
		blockStarted: make(chan struct{}),
		releaseBlock: make(chan struct{}),
	}
	fastProber := registrySQLHealthProber{pools: fastPools}
	checkers := []*SQLHealthChecker{
		NewSQLHealthChecker(context.Background(), SQLHealthCheckPoolsConfig{}, SQLHealthCheckerConfig{
			Pools: slowPools, Prober: slowProber,
		}),
		NewSQLHealthChecker(context.Background(), SQLHealthCheckPoolsConfig{}, SQLHealthCheckerConfig{
			Pools: fastPools, Prober: fastProber,
		}),
	}
	nextChecker := 0
	w := NewHealthWorker(HealthWorkerConfig{
		Interval:         time.Second,
		AbsenceThreshold: 1,
		Prober:           patroniProber,
		NewSQLChecker: func(context.Context) *SQLHealthChecker {
			checker := checkers[nextChecker]
			nextChecker++
			return checker
		},
	})

	// Different application_name values make these separate credential targets while preserving
	// the same real PostgreSQL credentials and database.
	slowConnString := connString + "&application_name=slow-credential"
	fastConnString := connString + "&application_name=fast-credential"
	slowUpdates := w.Subscribe("primary.local", false, "slow", nil, &SQLHealthConfig{
		ConnStringForIP: func(string) string { return slowConnString },
	})
	health := <-slowUpdates
	fastUpdates := w.Subscribe("primary.local", false, "fast", nil, &SQLHealthConfig{
		ConnStringForIP: func(string) string { return fastConnString },
	})
	<-fastUpdates
	t.Cleanup(func() {
		w.Unsubscribe(slowUpdates)
		w.Unsubscribe(fastUpdates)
	})

	// Establish the old address with real pg_is_in_recovery() queries and capture both warm pools.
	w.RefreshSQL()
	require.Equal(t, 1, slowPools.Len())
	require.Equal(t, 1, fastPools.Len())
	slowPools.mu.Lock()
	slowOldPool := slowPools.pools[oldIP].pool
	slowPools.mu.Unlock()
	fastPools.mu.Lock()
	fastOldPool := fastPools.pools[oldIP].pool
	fastPools.mu.Unlock()
	require.EqualValues(t, 1, slowOldPool.Stat().TotalConns())
	require.EqualValues(t, 1, fastOldPool.Stat().TotalConns())

	// The slow credential completes a real writable query against the old address, then delays
	// returning that result to the worker.
	slowProber.blockNext(oldIP)
	oldRoundDone := make(chan struct{})
	go func() {
		w.RefreshSQL()
		close(oldRoundDone)
	}()
	var releaseOnce sync.Once
	releaseSlow := func() { releaseOnce.Do(func() { close(slowProber.releaseBlock) }) }
	t.Cleanup(releaseSlow)
	select {
	case <-slowProber.blockStarted:
	case <-time.After(time.Second):
		t.Fatal("slow credential did not finish and block its old-address query")
	}
	health.sqlMu.RLock()
	fastTarget := health.subscriptionTargets[fastUpdates]
	health.sqlMu.RUnlock()
	require.Eventually(t, func() bool {
		return !fastTarget.sqlRoundRunning()
	}, time.Second, time.Millisecond, "fast credential did not finish the old round")

	// DNS changes while the slow credential is still finishing its older round. The next refresh
	// independently restarts the fast credential and records the replacement in the new snapshot.
	patroniProber.setIPs(replacementIP)
	w.RefreshSQL()
	fastPools.mu.Lock()
	_, fastReplacementCreated := fastPools.pools[replacementIP]
	fastPools.mu.Unlock()
	assert.True(t, fastReplacementCreated,
		"an idle credential should probe the replacement without waiting for another credential")

	releaseSlow()
	select {
	case <-oldRoundDone:
	case <-time.After(time.Second):
		t.Fatal("old SQL round did not finish after releasing the slow credential")
	}
	for _, target := range health.sqlTargetValues() {
		assert.False(t, target.sqlRoundRunning())
	}

	// The next round lets both credentials confirm the replacement. The following round applies DNS
	// absence cleanup and closes both old probe pools.
	w.RefreshSQL()
	w.RefreshSQL()
	assert.Equal(t, []string{replacementIP}, health.sqlHealthState.Resolved())
	assert.Equal(t, 1, slowPools.Len())
	assert.Equal(t, 1, fastPools.Len())
	for _, oldPool := range []*pgxpool.Pool{slowOldPool, fastOldPool} {
		conn, acquireErr := oldPool.Acquire(context.Background())
		if acquireErr == nil {
			conn.Release()
		}
		assert.ErrorIs(t, acquireErr, puddle.ErrClosedPool)
	}

	for _, pools := range []*SQLHealthCheckPools{slowPools, fastPools} {
		pools.mu.Lock()
		replacementPool, ok := pools.pools[replacementIP]
		pools.mu.Unlock()
		require.True(t, ok, "replacement pool was not retained")
		conn, acquireErr := replacementPool.pool.Acquire(ctx)
		require.NoError(t, acquireErr)
		var inRecovery bool
		require.NoError(t, conn.QueryRow(ctx, SQLHealthCheckQuery).Scan(&inRecovery))
		assert.False(t, inRecovery)
		conn.Release()
	}
}
