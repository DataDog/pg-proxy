// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

//go:build integration

package main

import (
	"context"
	"errors"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"

	"github.com/DataDog/pg-proxy/internal/itest"
)

// TestBackendStatementsPerClientStatement counts, with pg_stat_statements, what
// Postgres executes for one-shot extended statements sent through the proxy.
// pkg/session's TestProtocolCycleRoundTripBudgets pins the same costs in
// process; this checks them against a real server.
func TestBackendStatementsPerClientStatement(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	_, external := os.LookupEnv(testDatabaseURLVariable)
	connectionString := postgresTestConnectionString(t, ctx, itest.WithContainerOptions(
		testcontainers.WithCmdArgs("-c", "shared_preload_libraries=pg_stat_statements")))
	adminURL, err := connectionString(ctx)
	require.NoError(t, err)
	admin, err := pgx.Connect(ctx, adminURL)
	require.NoError(t, err)
	// Cleanups run in reverse order: the proxy stops, the test database and any
	// extension this test created are dropped, and then admin closes.
	t.Cleanup(func() { _ = admin.Close(context.Background()) })
	requireStatementStatistics(t, ctx, admin, external)
	const database = "statement_budget"
	_, err = admin.Exec(ctx, "DROP DATABASE IF EXISTS "+database)
	require.NoError(t, err)
	_, err = admin.Exec(ctx, "CREATE DATABASE "+database)
	require.NoError(t, err)
	t.Cleanup(func() { cleanupExec(t, admin, "DROP DATABASE "+database) })

	backendURL, err := url.Parse(adminURL)
	require.NoError(t, err)
	backendURL.Path = "/" + database
	query := backendURL.Query()
	query.Set("pool_max_conns", "1")
	query.Set("pool_health_check_period", "1h") // no background statements during the count
	backendURL.RawQuery = query.Encode()
	proxyCtx, stop := context.WithCancel(ctx)
	defer stop()
	proxy, err := newProxy(proxyCtx, backendConfig(t, backendURL.String()))
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() {
		done <- serve(proxyCtx, stop, proxy.server, proxy.sessions, proxy.inventory.Close, 5*time.Second)
	}()
	t.Cleanup(func() {
		stop()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Error("proxy did not stop")
		}
	})
	client, err := pgx.Connect(ctx, "postgres://postgres@"+proxy.server.Addr().String()+"/local_db?sslmode=disable")
	require.NoError(t, err)
	defer client.Close(context.Background())

	// The proxy's startup checks ran in the same database, so count from here.
	// Resetting only this database leaves the server's other statistics alone.
	_, err = admin.Exec(ctx, "SELECT pg_stat_statements_reset(dbid => oid) FROM pg_database WHERE datname = $1", database)
	require.NoError(t, err)
	const clientStatements = 1000
	for i := range clientStatements {
		// Parse, Bind, Describe, Execute, and Sync in one round trip.
		_, err := client.Exec(ctx, "SELECT $1::int", pgx.QueryExecModeExec, i)
		require.NoError(t, err)
	}

	rows, err := admin.Query(ctx, `SELECT query, calls FROM pg_stat_statements
		WHERE dbid = (SELECT oid FROM pg_database WHERE datname = $1)`, database)
	require.NoError(t, err)
	calls, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (statementCalls, error) {
		var call statementCalls
		return call, row.Scan(&call.query, &call.calls)
	})
	require.NoError(t, err)
	byQuery := make(map[string]int64, len(calls))
	var backendStatements int64
	for _, call := range calls {
		byQuery[call.query] = call.calls
		backendStatements += call.calls
	}
	t.Logf("calls per statement for %d client statements: %v", clientStatements, byQuery)
	require.EqualValues(t, clientStatements, byQuery["SELECT $1::int"])

	// Each statement is its own transaction, so the proxy releases the backend
	// after each one, and the release runs one reset query. Without role
	// management, that one simple query holds three statements: RESET SESSION
	// AUTHORIZATION, RESET ROLE, and RESET ALL, so RESET ALL counts reset
	// queries. Today a client statement costs 2 round trips and 4 statements.
	// Lower these budgets when the proxy elides or merges the reset.
	resetsPerClientStatement := float64(byQuery["RESET ALL"]) / clientStatements
	require.LessOrEqual(t, resetsPerClientStatement, 1.0, "more than one reset query per client statement")
	roundTripsPerClientStatement := float64(byQuery["SELECT $1::int"]+byQuery["RESET ALL"]) / clientStatements
	require.LessOrEqual(t, roundTripsPerClientStatement, 2.0, "more than 2 backend round trips per client statement")
	statementsPerClientStatement := float64(backendStatements) / clientStatements
	require.LessOrEqual(t, statementsPerClientStatement, 4.0, "Postgres runs a statement the proxy did not run before")
}

type statementCalls struct {
	query string
	calls int64
}

// requireStatementStatistics creates pg_stat_statements if it is missing. It
// then checks that the server preloads the library and counts utility
// statements; otherwise RESET statements count 0 and every budget passes. An
// external database that does not qualify skips the test.
func requireStatementStatistics(t *testing.T, ctx context.Context, admin *pgx.Conn, external bool) {
	t.Helper()
	var installed bool
	require.NoError(t, admin.QueryRow(ctx,
		"SELECT EXISTS (SELECT FROM pg_extension WHERE extname = 'pg_stat_statements')").Scan(&installed))
	if !installed {
		_, err := admin.Exec(ctx, "CREATE EXTENSION pg_stat_statements")
		require.NoError(t, err)
		t.Cleanup(func() { cleanupExec(t, admin, "DROP EXTENSION pg_stat_statements") })
	}
	// The setting exists only when the server preloads the library.
	var trackUtility string
	err := admin.QueryRow(ctx, "SELECT current_setting('pg_stat_statements.track_utility')").Scan(&trackUtility)
	if err == nil && trackUtility != "on" {
		err = errors.New("pg_stat_statements.track_utility is off")
	}
	if err != nil {
		if external {
			t.Skipf("%s must preload pg_stat_statements and track utility statements: %v", testDatabaseURLVariable, err)
		}
		require.NoError(t, err)
	}
}

// cleanupExec runs a cleanup statement, which outlives the test's context.
func cleanupExec(t *testing.T, conn *pgx.Conn, sql string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := conn.Exec(ctx, sql)
	require.NoError(t, err)
}
