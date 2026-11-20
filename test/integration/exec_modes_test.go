// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/DataDog/pg-proxy/internal/itest"
)

// execModes are the pgx query exec modes, as default_query_exec_mode values,
// that the driver-level tests run in. cache_statement, pgx's default,
// prepares a named statement for each query and reuses it. exec sends each
// query as a one-shot unnamed Parse, Bind, Describe, Execute and Sync, which
// is what most clients of a transaction-pooling proxy send.
var execModes = []string{"cache_statement", "exec"}

// forEachExecMode runs test once for each of execModes, as a subtest named
// after the mode.
func forEachExecMode(t *testing.T, test func(t *testing.T, mode string)) {
	t.Helper()
	for _, mode := range execModes {
		t.Run(mode, func(t *testing.T) { test(t, mode) })
	}
}

// withExecMode returns connStr with pgx's default query exec mode set to mode.
func withExecMode(t *testing.T, connStr, mode string) string {
	t.Helper()
	u, err := url.Parse(connStr)
	require.NoError(t, err)
	query := u.Query()
	query.Set("default_query_exec_mode", mode)
	u.RawQuery = query.Encode()
	return u.String()
}

// TestInterleavedClients verifies that two clients taking turns on dbPooled's
// single backend connection each get their own results, for parameters and
// results of several types. In exec mode each query replaces the other
// client's unnamed statement on the backend.
func TestInterleavedClients(t *testing.T) {
	forEachExecMode(t, func(t *testing.T, mode string) {
		connStr := withExecMode(t, startProxy(t).URL(dbPooled, itest.PostgresUser), mode)
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		clients := make([]*pgx.Conn, 2)
		for i := range clients {
			conn, err := pgx.Connect(ctx, connStr)
			require.NoError(t, err)
			defer conn.Close(context.Background())
			clients[i] = conn
		}

		created := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
		for i := range 10 {
			var doubled int
			require.NoError(t, clients[0].QueryRow(ctx, "SELECT $1::int * 2", i).Scan(&doubled))
			require.Equal(t, 2*i, doubled)
			var name string
			require.NoError(t, clients[1].QueryRow(ctx, "SELECT upper($1::text)", fmt.Sprint("user ", i)).Scan(&name))
			require.Equal(t, fmt.Sprint("USER ", i), name)
			var payload []byte
			require.NoError(t, clients[0].QueryRow(ctx, "SELECT decode($1, 'hex')", "00ff").Scan(&payload))
			require.Equal(t, []byte{0x00, 0xff}, payload)
			var missing *int
			require.NoError(t, clients[1].QueryRow(ctx, "SELECT nullif($1::int, $1::int)", i).Scan(&missing))
			require.Nil(t, missing)
			var later time.Time
			require.NoError(t, clients[0].QueryRow(ctx, "SELECT $1::timestamptz + interval '1 day'", created).Scan(&later))
			require.True(t, created.AddDate(0, 0, 1).Equal(later), "got %v", later)
		}

		var pids [2]uint32
		for i, conn := range clients {
			require.NoError(t, conn.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pids[i]))
		}
		require.Equal(t, pids[0], pids[1], "both clients should share the single pooled backend connection")
	})
}

// TestBatch verifies that a pipelined batch, many queries followed by one
// Sync, returns the same results through the proxy as from PostgreSQL
// directly: in order, up to an error that ends the batch, and inside a
// transaction.
func TestBatch(t *testing.T) {
	require.NoError(t, backend.Exec(t.Context(),
		"DROP TABLE IF EXISTS batch_users", "CREATE TABLE batch_users (id int PRIMARY KEY)"))
	// run sends queries as one batch, optionally inside a transaction, and
	// returns each query's first value or SQLSTATE, then the transaction
	// status after the batch and the row count.
	run := func(t *testing.T, conn *pgx.Conn, inTransaction bool, queries ...string) []string {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		_, err := conn.Exec(ctx, "TRUNCATE batch_users")
		require.NoError(t, err)
		batch := &pgx.Batch{}
		if inTransaction {
			batch.Queue("BEGIN")
		}
		for _, query := range queries {
			batch.Queue(query)
		}
		if inTransaction {
			batch.Queue("COMMIT")
		}
		results := conn.SendBatch(ctx, batch)
		var outcomes []string
		for range batch.Len() {
			var value any
			err := results.QueryRow().Scan(&value)
			var pgErr *pgconn.PgError
			switch {
			case errors.As(err, &pgErr):
				outcomes = append(outcomes, "error "+pgErr.Code)
			case errors.Is(err, pgx.ErrNoRows):
				outcomes = append(outcomes, "no rows")
			default:
				require.NoError(t, err)
				outcomes = append(outcomes, fmt.Sprint(value))
			}
		}
		_ = results.Close()
		outcomes = append(outcomes, fmt.Sprintf("status %c", conn.PgConn().TxStatus()))
		if conn.PgConn().TxStatus() != 'I' {
			_, err := conn.Exec(ctx, "ROLLBACK")
			require.NoError(t, err)
		}
		var count int
		require.NoError(t, conn.QueryRow(ctx, "SELECT count(*) FROM batch_users").Scan(&count))
		return append(outcomes, fmt.Sprint("rows ", count))
	}
	many := make([]string, 200)
	for i := range many {
		many[i] = fmt.Sprintf("INSERT INTO batch_users VALUES (%d) RETURNING id", i)
	}
	failing := []string{
		"INSERT INTO batch_users VALUES (1) RETURNING id",
		"SELECT 1 / 0",
		"INSERT INTO batch_users VALUES (2) RETURNING id",
	}

	forEachExecMode(t, func(t *testing.T, mode string) {
		targets := map[string]string{
			"direct": backend.URL("postgres"),
			"proxy":  startProxy(t).URL(dbPooled, itest.PostgresUser),
		}
		outcomes := map[string][][]string{}
		for target, connStr := range targets {
			conn, err := pgx.Connect(t.Context(), withExecMode(t, connStr, mode))
			require.NoError(t, err)
			defer conn.Close(context.Background())
			outcomes[target] = [][]string{
				run(t, conn, false, many...),
				run(t, conn, false, failing...),
				run(t, conn, true, failing...),
			}
		}
		require.Equal(t, []string{"status I", "rows 200"}, outcomes["direct"][0][200:])
		require.Equal(t, []string{"1", "error 22012"}, outcomes["direct"][1][:2])
		require.Equal(t, "rows 0", outcomes["direct"][2][len(outcomes["direct"][2])-1])
		require.Equal(t, outcomes["direct"], outcomes["proxy"])
	})
}

// TestSavepoints verifies that a transaction keeps its backend connection
// across a savepoint and a rollback to it, which pgx issues for a nested
// transaction.
func TestSavepoints(t *testing.T) {
	require.NoError(t, backend.Exec(t.Context(),
		"DROP TABLE IF EXISTS savepoint_users", "CREATE TABLE savepoint_users (id int PRIMARY KEY)"))
	forEachExecMode(t, func(t *testing.T, mode string) {
		connStr := withExecMode(t, startProxy(t).URL(dbPooled, itest.PostgresUser), mode)
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		conn, err := pgx.Connect(ctx, connStr)
		require.NoError(t, err)
		defer conn.Close(context.Background())
		_, err = conn.Exec(ctx, "TRUNCATE savepoint_users")
		require.NoError(t, err)

		tx, err := conn.Begin(ctx)
		require.NoError(t, err)
		_, err = tx.Exec(ctx, "INSERT INTO savepoint_users VALUES ($1)", 1)
		require.NoError(t, err)
		nested, err := tx.Begin(ctx)
		require.NoError(t, err)
		_, err = nested.Exec(ctx, "INSERT INTO savepoint_users VALUES ($1)", 1)
		var pgErr *pgconn.PgError
		require.ErrorAs(t, err, &pgErr)
		require.Equal(t, "23505", pgErr.Code)
		require.NoError(t, nested.Rollback(ctx))
		_, err = tx.Exec(ctx, "INSERT INTO savepoint_users VALUES ($1)", 2)
		require.NoError(t, err, "the transaction should continue after the rollback to the savepoint")
		require.NoError(t, tx.Commit(ctx))

		rows, err := conn.Query(ctx, "SELECT id FROM savepoint_users ORDER BY id")
		require.NoError(t, err)
		ids, err := pgx.CollectRows(rows, pgx.RowTo[int32])
		require.NoError(t, err)
		require.Equal(t, []int32{1, 2}, ids)
	})
}
