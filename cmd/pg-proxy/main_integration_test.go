// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

//go:build integration

package main

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	pgproxy "github.com/DataDog/pg-proxy"
	pgconfig "github.com/DataDog/pg-proxy/pkg/config"
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
	t.Run("pool statistics", func(t *testing.T) { verifyProxyPoolStats(t, adminURL) })

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
	// A database that refuses connections must stop startup and close every
	// pool that opened.
	for _, tc := range []struct {
		name, application, wantErr string
		wire                       func(*config)
	}{
		{
			name: "unreachable primary closes other backends", application: "pg-proxy-partial-startup",
			wantErr: `connect to backend "zz-failure" primary`,
			wire:    func(cfg *config) { cfg.clusters["zz-failure"] = pgconfig.Cluster{Primary: "unreachable"} },
		},
		{
			name: "unreachable replica closes its cluster", application: "pg-proxy-replica-startup",
			wantErr: `connect to backend "local" replica: failed to open test conn to postgres for unreachable`,
			wire: func(cfg *config) {
				cfg.databases["reachable"] = cfg.databases["primary"].Copy()
				cfg.clusters["local"] = pgconfig.Cluster{Primary: "primary", Replicas: []string{"reachable", "unreachable"}}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			url, err := connectionString(ctx, "sslmode=disable", "application_name="+tc.application)
			require.NoError(t, err)
			cfg := backendConfig(t, url)
			unreachable := cfg.databases["primary"].Copy()
			unreachable.ConnConfig.Port = 1
			unreachable.ConnConfig.ConnectTimeout = time.Second
			cfg.databases["unreachable"] = unreachable
			tc.wire(&cfg)
			_, err = newProxy(ctx, cfg)
			require.ErrorContains(t, err, tc.wantErr)
			require.Eventually(t, func() bool {
				var connections int
				err := admin.QueryRow(ctx, "select count(*) from pg_stat_activity where application_name = $1", tc.application).Scan(&connections)
				return err == nil && connections == 0
			}, 5*time.Second, 10*time.Millisecond)
		})
	}
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
	t.Run("ping_protocol", func(t *testing.T) {
		testPingProtocolAgainstPostgres(t, adminURL, proxyURL)
	})
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

type gaugeRecorder struct {
	pgproxy.NoopMetrics
	mu     sync.Mutex
	gauges map[string][][]string
}

func (r *gaugeRecorder) Gauge(name string, _ float64, tags []string, _ float64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gauges[name] = append(r.gauges[name], slices.Clone(tags))
	return nil
}

func (r *gaugeRecorder) snapshot() map[string][][]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	snapshot := make(map[string][][]string, len(r.gauges))
	for name, calls := range r.gauges {
		snapshot[name] = slices.Clone(calls)
	}
	return snapshot
}

// Stub collectors stand in for DogStatsD and OTel; the backend selection decides
// whether newProxy starts collection.
func verifyProxyPoolStats(t *testing.T, backendURL string) {
	for _, tc := range []struct {
		name      string
		metrics   metricBackend
		namespace string
	}{
		{"metrics disabled", metricNone, ""},
		{"dogstatsd", metricDogStatsD, ""},
		{"otel with custom namespace", metricOTel, "custom."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recorder := &gaugeRecorder{gauges: make(map[string][][]string)}
			t.Cleanup(pgproxy.Bootstrap(pgproxy.Dependencies{Metrics: recorder, MetricNamespace: tc.namespace}))
			cfg := backendConfig(t, backendURL)
			cfg.telemetry.Metrics = tc.metrics
			proxy, err := newProxy(t.Context(), cfg)
			require.NoError(t, err)
			defer proxy.server.Close()

			// Ticks fire at two and four seconds; close between them.
			time.Sleep(3 * time.Second)
			proxy.inventory.Close()
			got := recorder.snapshot()
			if tc.metrics == metricNone {
				require.Empty(t, got)
				return
			}
			tags := []string{"database:" + cfg.databases["primary"].ConnConfig.Database, "database_cluster:local"}
			want := map[string][][]string{pgproxy.MetricName("pg_proxy.sessions.waiting"): {tags}}
			poolTags := append(slices.Clone(tags), "direct_pool:false")
			for _, name := range []string{"pool_total", "pool_acquired", "pool_acquired_total", "pool_idle", "pool_constructing", "pool_max"} {
				want[pgproxy.MetricName("pg_proxy.connections."+name)] = [][]string{poolTags}
			}
			require.Equal(t, want, got)

			time.Sleep(2 * time.Second)
			require.Equal(t, want, recorder.snapshot(), "collection continued after Close")
		})
	}
}
