//go:build integration

package pool

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/DataDog/pg-proxy/internal/itest"
)

func TestSQLHealthCheckPoolAgainstPostgres(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	itest.UseImageRegistry()
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
