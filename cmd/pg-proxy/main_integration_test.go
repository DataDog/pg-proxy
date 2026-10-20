//go:build integration

package main

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestProxyAgainstPostgres(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	pg, err := postgres.Run(ctx, "postgres:14-bookworm@sha256:5a05f61a534e50e679d27fb45953afec7821f2ffc2aa6c738b94515ca0891ac2",
		postgres.WithDatabase("postgres"), postgres.WithUsername("postgres"), postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2)),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pg.Terminate(context.Background())) })
	adminURL, err := pg.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	admin, err := pgx.Connect(ctx, adminURL)
	require.NoError(t, err)
	defer admin.Close(context.Background())

	// Minimum connections must open during startup, before the periodic health check.
	for _, minimum := range []string{"pool_min_conns=3", "pool_min_idle_conns=3"} {
		t.Run(minimum, func(t *testing.T) {
			name := "pg-proxy-" + minimum
			url, err := pg.ConnectionString(ctx, "sslmode=disable", "pool_max_conns=3",
				minimum, "pool_health_check_period=1h", "application_name="+name)
			require.NoError(t, err)
			proxy, err := newProxy(ctx, config{
				listenAddress: "127.0.0.1:0", database: "local_db", backendURL: url, maxSessions: 10,
			})
			require.NoError(t, err)
			defer proxy.backend.Close()
			defer proxy.server.Close()
			require.Eventually(t, func() bool {
				var connections int
				err := admin.QueryRow(ctx, "select count(*) from pg_stat_activity where application_name = $1 and state = 'idle'", name).Scan(&connections)
				return err == nil && connections == 3
			}, 5*time.Second, 10*time.Millisecond)
		})
	}
	url, err := pg.ConnectionString(ctx, "sslmode=disable", "pool_max_conns=1")
	require.NoError(t, err)
	proxyCtx, stop := context.WithCancel(ctx)
	proxy, err := newProxy(proxyCtx, config{
		listenAddress: "127.0.0.1:0", database: "local_db", backendURL: url, maxSessions: 10,
	})
	require.NoError(t, err)
	done := make(chan struct{})
	var serveErr error
	go func() {
		serveErr = serve(proxyCtx, stop, proxy.server, proxy.sessions, proxy.backend.Close, 5*time.Second)
		close(done)
	}()
	t.Cleanup(func() {
		stop()
		select {
		case <-done:
			require.NoError(t, serveErr)
		case <-time.After(10 * time.Second):
			t.Error("proxy did not stop")
		}
	})
	proxyURL := "postgres://postgres@" + proxy.server.Addr().String() + "/local_db?sslmode=disable"
	client, err := pgx.Connect(ctx, proxyURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	other, err := pgx.Connect(ctx, proxyURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = other.Close(context.Background()) })

	// Two frontend sessions share one backend connection across transactions.
	var firstPID, secondPID int
	require.NoError(t, client.QueryRow(ctx, "select pg_backend_pid()", pgx.QueryExecModeSimpleProtocol).Scan(&firstPID))
	require.NoError(t, other.QueryRow(ctx, "select pg_backend_pid()", pgx.QueryExecModeSimpleProtocol).Scan(&secondPID))
	require.Equal(t, firstPID, secondPID)
	var value int
	for _, connection := range []*pgx.Conn{client, other, client} {
		require.NoError(t, connection.QueryRow(ctx, "select $1::int", 42).Scan(&value))
		require.Equal(t, 42, value)
	}
	tx, err := client.Begin(ctx)
	require.NoError(t, err)
	require.NoError(t, tx.QueryRow(ctx, "select 7").Scan(&value))
	require.NoError(t, tx.Commit(ctx))

	// Reject unmapped databases, mismatched identities, and unsupported role switching.
	for _, connectionString := range []string{
		"postgres://postgres@" + proxy.server.Addr().String() + "/unknown?sslmode=disable",
		"postgres://unknown@" + proxy.server.Addr().String() + "/local_db?sslmode=disable",
		proxyURL + "&options=-c%20pg_proxy.passthrough_user%3Dtrue",
	} {
		connection, err := pgx.Connect(ctx, connectionString)
		if connection != nil {
			_ = connection.Close(ctx)
		}
		require.Error(t, err)
	}

	// A separate admin connection observes the running query before cancellation.
	queryDone := make(chan error, 1)
	go func() {
		_, err := client.Exec(ctx, "select pg_sleep(30)", pgx.QueryExecModeSimpleProtocol)
		queryDone <- err
	}()
	require.Eventually(t, func() bool {
		var running bool
		err := admin.QueryRow(ctx, "select exists(select 1 from pg_stat_activity where pid = $1 and state = 'active' and query = 'select pg_sleep(30)')", firstPID).Scan(&running)
		return err == nil && running
	}, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, client.PgConn().CancelRequest(ctx))
	select {
	case err := <-queryDone:
		require.ErrorContains(t, err, "canceling statement")
	case <-time.After(5 * time.Second):
		t.Fatal("query cancellation did not reach PostgreSQL")
	}
	require.NoError(t, other.QueryRow(ctx, "select 1").Scan(&value))
	// Shutdown with idle frontend connections still open must finish promptly.
	stop()
	select {
	case <-done:
		require.NoError(t, serveErr)
	case <-time.After(10 * time.Second):
		t.Fatal("proxy did not drain idle clients")
	}
}
