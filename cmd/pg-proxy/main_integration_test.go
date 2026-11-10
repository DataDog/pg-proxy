// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

//go:build integration

package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestProxyAgainstPostgres(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	connectionString := postgresTestConnectionString(t, ctx)
	adminURL, err := connectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	admin, err := pgx.Connect(ctx, adminURL)
	require.NoError(t, err)
	defer admin.Close(context.Background())
	t.Run("exports traces metrics and logs", func(t *testing.T) { verifyProxyOTLP(t, adminURL) })
	t.Run("client TLS", func(t *testing.T) { verifyProxyTLS(t, adminURL, admin) })
	t.Run("client authentication", func(t *testing.T) { verifyProxyAuthentication(t, adminURL, admin) })

	// Minimum connections must open during startup, before the periodic health check.
	for _, minimum := range []string{"pool_min_conns=3", "pool_min_idle_conns=3"} {
		t.Run(minimum, func(t *testing.T) {
			name := "pg-proxy-" + minimum
			url, err := connectionString(ctx, "sslmode=disable", "pool_max_conns=3",
				minimum, "pool_health_check_period=1h", "application_name="+name)
			require.NoError(t, err)
			proxy, err := newProxy(ctx, backendConfig(t, url))
			require.NoError(t, err)
			defer proxy.inventory.Close()
			defer proxy.server.Close()
			require.Eventually(t, func() bool {
				var connections int
				err := admin.QueryRow(ctx, "select count(*) from pg_stat_activity where application_name = $1 and state = 'idle'", name).Scan(&connections)
				return err == nil && connections == 3
			}, 5*time.Second, 10*time.Millisecond)
		})
	}
	t.Run("startup failure closes earlier pools", func(t *testing.T) {
		url, err := connectionString(ctx, "sslmode=disable", "application_name=pg-proxy-partial-startup")
		require.NoError(t, err)
		cfg := backendConfig(t, url)
		unreachable := cfg.databases["primary"].Copy()
		unreachable.ConnConfig.Port = 1
		unreachable.ConnConfig.ConnectTimeout = time.Second
		cfg.databases["unreachable"] = unreachable
		cfg.clusters["zz-failure"] = "unreachable"
		_, err = newProxy(ctx, cfg)
		require.Error(t, err)
		require.Eventually(t, func() bool {
			var connections int
			err := admin.QueryRow(ctx, "select count(*) from pg_stat_activity where application_name = 'pg-proxy-partial-startup'").Scan(&connections)
			return err == nil && connections == 0
		}, 5*time.Second, 10*time.Millisecond)
	})
	_, err = admin.Exec(ctx, "CREATE ROLE reports_user LOGIN PASSWORD 'reports-password'")
	require.NoError(t, err)
	_, err = admin.Exec(ctx, "CREATE DATABASE reports OWNER reports_user")
	require.NoError(t, err)
	url, err := connectionString(ctx, "sslmode=disable", "pool_max_conns=1")
	require.NoError(t, err)
	proxyCtx, stop := context.WithCancel(ctx)
	connection, err := pgx.ParseConfig(url)
	require.NoError(t, err)
	path := writeConfig(t, "yaml", fmt.Sprintf(`databases:
- name: primary
  host: %s
  port: %d
  dbname: postgres
  username: postgres
  password: postgres
  sslmode: disable
  max_conns: 1
- name: reports-primary
  host: %s
  port: %d
  dbname: reports
  username: reports_user
  password: reports-password
  sslmode: disable
  max_conns: 1
clusters:
  orders: {primary: primary}
  reports: {primary: reports-primary}
logical_databases:
  local_db: {cluster: orders}
  reports_db: {cluster: reports}
proxy:
  listener_address: '127.0.0.1:0'
  postgres_msg_buffer_size: 4096
`, connection.Host, connection.Port, connection.Host, connection.Port))
	cfg, err := parseConfig([]string{"-config", path})
	require.NoError(t, err)
	proxy, err := newProxy(proxyCtx, cfg)
	require.NoError(t, err)
	done := make(chan struct{})
	var serveErr error
	go func() {
		serveErr = serve(proxyCtx, stop, proxy.server, proxy.sessions, proxy.inventory.Close, 5*time.Second)
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

	reports, err := pgx.Connect(ctx, "postgres://reports_user@"+proxy.server.Addr().String()+"/reports_db?sslmode=disable")
	require.NoError(t, err)
	t.Cleanup(func() { _ = reports.Close(context.Background()) })
	for connection, expected := range map[*pgx.Conn][]string{
		client:  {"postgres", "postgres"},
		reports: {"reports", "reports_user"},
	} {
		var database, user string
		require.NoError(t, connection.QueryRow(ctx, "select current_database(), current_user", pgx.QueryExecModeSimpleProtocol).Scan(&database, &user))
		require.Equal(t, expected, []string{database, user})
	}

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
		"postgres://postgres@" + proxy.server.Addr().String() + "/reports_db?sslmode=disable",
		"postgres://reports_user@" + proxy.server.Addr().String() + "/local_db?sslmode=disable",
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
