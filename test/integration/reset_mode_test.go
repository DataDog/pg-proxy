// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

//go:build integration

package integration

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"

	"github.com/DataDog/pg-proxy/internal/itest"
	pgconfig "github.com/DataDog/pg-proxy/pkg/config"
)

// startResetModeProxy starts startProxy's configuration with dbPooled's single
// backend connection reset in mode, so consecutive clients share one backend.
// Audit checks every release, so a test sees each check it causes.
func startResetModeProxy(t *testing.T, mode pgconfig.ResetMode) *itest.Proxy {
	t.Helper()
	cfg := proxyConfig()
	for i := range cfg.Databases {
		if cfg.Databases[i].Name == pooledPrimary {
			cfg.Databases[i].ResetMode = mode
			cfg.Databases[i].ResetAuditRate = 1
		}
	}
	return itest.MustStartProxy(t, cfg)
}

// resettingModes are the modes that must keep every in-scope change from reaching
// the next client.
var resettingModes = []pgconfig.ResetMode{pgconfig.ResetAlways, pgconfig.ResetAudit, pgconfig.ResetDynamic}

// asClient runs fn on a fresh client session, the way the next client of the
// pooled backend would see it.
func asClient(t *testing.T, ctx context.Context, proxy *itest.Proxy, fn func(*pgx.Conn)) {
	t.Helper()
	conn, err := proxy.Connect(ctx, dbPooled, itest.PostgresUser)
	require.NoError(t, err)
	defer conn.Close(context.Background())
	fn(conn)
}

func showSetting(t *testing.T, ctx context.Context, conn *pgx.Conn, name string) string {
	t.Helper()
	var value string
	require.NoError(t, conn.QueryRow(ctx, "SELECT pg_catalog.current_setting($1)", name).Scan(&value))
	return value
}

// TestResetModesIsolateSettings checks that each way a client's SQL can change a
// setting is undone before the next client of the same backend, in every mode
// that resets.
func TestResetModesIsolateSettings(t *testing.T) {
	require.NoError(t, backend.Exec(t.Context(),
		`CREATE OR REPLACE FUNCTION itest_reset_timezone() RETURNS void LANGUAGE plpgsql AS
		 $$ BEGIN PERFORM pg_catalog.set_config('TimeZone', 'Pacific/Apia', false); END $$`,
		`CREATE OR REPLACE PROCEDURE itest_reset_search_path() LANGUAGE plpgsql AS
		 $$ BEGIN PERFORM pg_catalog.set_config('search_path', 'from_procedure', false); END $$`,
	))
	t.Cleanup(func() {
		_ = backend.Exec(context.Background(),
			"DROP FUNCTION IF EXISTS itest_reset_timezone()",
			"DROP PROCEDURE IF EXISTS itest_reset_search_path()",
		)
	})
	cases := []struct {
		name string
		run  func(context.Context, *pgx.Conn) error
	}{
		{"autocommit SET", execSQL("SET search_path = leaked")},
		{"SET in a committed transaction", func(ctx context.Context, conn *pgx.Conn) error {
			return pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
				_, err := tx.Exec(ctx, "SET search_path = leaked")
				return err
			})
		}},
		{"set_config", execSQL("SELECT pg_catalog.set_config('search_path', 'leaked', false)")},
		{"UPDATE pg_settings", execSQL("UPDATE pg_settings SET setting = 'leaked' WHERE name = 'search_path'")},
		{"SET after the first statement", execSQL("SELECT 1; SET search_path = leaked")},
		{"named statement parsed in one checkout and run in another", func(ctx context.Context, conn *pgx.Conn) error {
			if _, err := conn.Prepare(ctx, "itest_set_search_path", "SET search_path = leaked"); err != nil {
				return err
			}
			_, err := conn.Exec(ctx, "itest_set_search_path")
			return err
		}},
		{"function changing a reported setting", execSQL("SELECT itest_reset_timezone()")},
		{"CALL", execSQL("CALL itest_reset_search_path()")},
		{"EXECUTE", execSQL("PREPARE itest_cfg AS SELECT pg_catalog.set_config('search_path', 'leaked', false); EXECUTE itest_cfg; DEALLOCATE itest_cfg")},
	}
	for _, mode := range resettingModes {
		t.Run(string(mode), func(t *testing.T) {
			proxy := startResetModeProxy(t, mode)
			ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
			defer cancel()
			var searchPath, timeZone string
			asClient(t, ctx, proxy, func(conn *pgx.Conn) {
				searchPath, timeZone = showSetting(t, ctx, conn, "search_path"), showSetting(t, ctx, conn, "TimeZone")
			})
			for _, c := range cases {
				asClient(t, ctx, proxy, func(conn *pgx.Conn) { require.NoError(t, c.run(ctx, conn), c.name) })
				asClient(t, ctx, proxy, func(conn *pgx.Conn) {
					require.Equal(t, searchPath, showSetting(t, ctx, conn, "search_path"), c.name)
					require.Equal(t, timeZone, showSetting(t, ctx, conn, "TimeZone"), c.name)
				})
			}
		})
	}
}

func execSQL(sql string) func(context.Context, *pgx.Conn) error {
	return func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, sql)
		return err
	}
}

// lastBackendQuery returns the last query the pooled backend ran, read directly
// from Postgres, which shows whether the release sent a reset.
func lastBackendQuery(t *testing.T, ctx context.Context) string {
	t.Helper()
	conn, err := backend.Connect(ctx, "postgres")
	require.NoError(t, err)
	defer conn.Close(context.Background())
	var query string
	require.NoError(t, conn.QueryRow(ctx,
		"SELECT query FROM pg_stat_activity WHERE application_name = $1", pooledApplicationName).Scan(&query))
	return query
}

// TestResetModeReleaseQuery checks what each mode sends when a clean checkout
// releases its backend: dynamic and off skip the round trip entirely, and audit
// adds its check to the same reset that always sends.
func TestResetModeReleaseQuery(t *testing.T) {
	const clean = "SELECT 1 AS itest_clean_checkout"
	const reset = "RESET SESSION AUTHORIZATION; RESET ROLE; RESET ALL"
	for mode, want := range map[pgconfig.ResetMode]func(string) bool{
		pgconfig.ResetAlways: func(query string) bool { return strings.HasPrefix(query, reset) },
		pgconfig.ResetAudit: func(query string) bool {
			// pg_stat_activity cuts the query at track_activity_query_size (1 kB),
			// before the reset, so only the check that starts it shows. A sampled
			// release starts with the full check instead.
			return strings.HasPrefix(query, "SELECT pg_catalog.current_setting('search_path', true)") ||
				strings.HasPrefix(query, "SELECT pg_catalog.string_agg(name")
		},
		pgconfig.ResetDynamic: func(query string) bool { return strings.HasPrefix(query, clean) },
		pgconfig.ResetOff:     func(query string) bool { return strings.HasPrefix(query, clean) },
	} {
		t.Run(string(mode), func(t *testing.T) {
			proxy := startResetModeProxy(t, mode)
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			asClient(t, ctx, proxy, func(conn *pgx.Conn) {
				_, err := conn.Exec(ctx, clean)
				require.NoError(t, err)
			})
			query := lastBackendQuery(t, ctx)
			require.True(t, want(query), "last query: %s", query)
			require.NotContains(t, proxy.Logs(), "reset_would_leak", "a clean checkout leaks nothing")
		})
	}
}

// TestResetModeServerSideGap pins the documented gap of dynamic: settings that a
// function changes without naming them in the client's SQL, and that Postgres does
// not report, survive a skipped reset. Audit reports the same release.
func TestResetModeServerSideGap(t *testing.T) {
	require.NoError(t, backend.Exec(t.Context(),
		`CREATE OR REPLACE FUNCTION itest_reset_timeout() RETURNS void LANGUAGE plpgsql AS
		 $$ BEGIN
		   PERFORM pg_catalog.set_config('statement_timeout', '4321', false);
		   PERFORM pg_catalog.set_config('synchronous_commit', 'off', false);
		 END $$`,
	))
	t.Cleanup(func() { _ = backend.Exec(context.Background(), "DROP FUNCTION IF EXISTS itest_reset_timeout()") })
	for mode, wantLeak := range map[pgconfig.ResetMode]bool{pgconfig.ResetAudit: false, pgconfig.ResetDynamic: true} {
		t.Run(string(mode), func(t *testing.T) {
			proxy := startResetModeProxy(t, mode)
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			asClient(t, ctx, proxy, func(conn *pgx.Conn) {
				_, err := conn.Exec(ctx, "SELECT itest_reset_timeout()")
				require.NoError(t, err)
			})
			asClient(t, ctx, proxy, func(conn *pgx.Conn) {
				require.Equal(t, wantLeak, showSetting(t, ctx, conn, "statement_timeout") == "4321ms")
				require.Equal(t, wantLeak, showSetting(t, ctx, conn, "synchronous_commit") == "off")
			})
			if mode == pgconfig.ResetAudit {
				require.Eventually(t, func() bool {
					logs := proxy.Logs()
					return strings.Contains(logs, "reset_would_leak") &&
						strings.Contains(logs, "statement_timeout") && strings.Contains(logs, "synchronous_commit")
				}, 5*time.Second, 50*time.Millisecond, "audit must report the settings dynamic would leak")
			}
		})
	}
}

// TestResetModesLeaveCursorsAndSequencesAsAlways checks that audit and dynamic
// treat held cursors and sequence state exactly as always does: none of them
// closes or discards them yet. Fixing that (OSQQUERY-823, -826) changes every mode
// at once, and this test with it.
func TestResetModesLeaveCursorsAndSequencesAsAlways(t *testing.T) {
	require.NoError(t, backend.Exec(t.Context(), "CREATE SEQUENCE IF NOT EXISTS itest_reset_seq"))
	t.Cleanup(func() { _ = backend.Exec(context.Background(), "DROP SEQUENCE IF EXISTS itest_reset_seq") })
	for _, mode := range resettingModes {
		t.Run(string(mode), func(t *testing.T) {
			proxy := startResetModeProxy(t, mode)
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()

			asClient(t, ctx, proxy, func(conn *pgx.Conn) {
				_, err := conn.Exec(ctx, "BEGIN; DECLARE itest_held CURSOR WITH HOLD FOR SELECT 'row of client 1'; COMMIT")
				require.NoError(t, err)
			})
			asClient(t, ctx, proxy, func(conn *pgx.Conn) {
				var row string
				require.NoError(t, conn.QueryRow(ctx, "FETCH NEXT FROM itest_held").Scan(&row))
				require.Equal(t, "row of client 1", row)
				_, err := conn.Exec(ctx, "CLOSE itest_held")
				require.NoError(t, err)
			})

			var issued int64
			asClient(t, ctx, proxy, func(conn *pgx.Conn) {
				require.NoError(t, conn.QueryRow(ctx, "SELECT nextval('itest_reset_seq')").Scan(&issued))
			})
			asClient(t, ctx, proxy, func(conn *pgx.Conn) {
				var last int64
				require.NoError(t, conn.QueryRow(ctx, "SELECT lastval()").Scan(&last))
				require.Equal(t, issued, last)
			})
		})
	}
}

// TestUnnamedStatementGuardRollsBack checks that the guard's 26000 aborts the
// transaction it ends, as on a connection that reset: an INSERT earlier in the
// same pipeline is not committed.
func TestUnnamedStatementGuardRollsBack(t *testing.T) {
	require.NoError(t, backend.Exec(t.Context(), "CREATE TABLE itest_guard_rows (id int)"))
	t.Cleanup(func() { _ = backend.Exec(context.Background(), "DROP TABLE IF EXISTS itest_guard_rows") })
	const clientA, clientB = 0, 1
	const insert = "INSERT INTO itest_guard_rows VALUES (1)"

	for _, mode := range append(resettingModes, pgconfig.ResetOff) {
		t.Run(string(mode), func(t *testing.T) {
			require.NoError(t, backend.Exec(t.Context(), "TRUNCATE itest_guard_rows"))
			proxy := startResetModeProxy(t, mode)
			replies := exchange(t, proxy.URL(dbPooled, itest.PostgresUser), []step{
				onClient(clientA, ready(&pgproto3.Parse{Query: "SELECT 'from client A'"}, &pgproto3.Sync{})),
				onClient(clientB, ready(
					&pgproto3.Parse{Name: "insert", Query: insert}, &pgproto3.Bind{PreparedStatement: "insert"}, &pgproto3.Execute{},
					&pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{},
				)),
			}, 0)
			require.Equal(t, []string{"ParseComplete", "BindComplete", "CommandComplete INSERT 0 1", "ErrorResponse 26000", "ReadyForQuery I"},
				summarize(replies[len(replies)-1]))

			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			asClient(t, ctx, proxy, func(conn *pgx.Conn) {
				var rows int
				require.NoError(t, conn.QueryRow(ctx, "SELECT count(*) FROM itest_guard_rows").Scan(&rows))
				require.Zero(t, rows, "the failed pipeline's INSERT was committed")
			})
		})
	}
}

// TestUnnamedStatementGuardAfterSkippedParse checks the guard when an earlier
// failure makes Postgres skip the Parse that would replace the previous client's
// unnamed statement. The client keeps its checkout through the failure with a
// savepoint, never sending a simple Query or unnamed Parse that would drop the
// statement, then binds the unnamed statement: it must get 26000 when it never
// had one, and its own statement when it did.
func TestUnnamedStatementGuardAfterSkippedParse(t *testing.T) {
	const clientA, clientB = 0, 1
	// viaNamed runs each statement as client B through a named statement, which
	// leaves the unnamed one in place.
	viaNamed := func(sqls ...string) step {
		var msgs []pgproto3.FrontendMessage
		for _, sql := range sqls {
			msgs = append(msgs, &pgproto3.Parse{Name: sql, Query: sql}, &pgproto3.Bind{PreparedStatement: sql}, &pgproto3.Execute{})
		}
		return onClient(clientB, ready(append(msgs, &pgproto3.Sync{})...))
	}
	leaveUnnamed := func(client int, sql string) step {
		return onClient(client, ready(&pgproto3.Parse{Query: sql}, &pgproto3.Sync{}))
	}
	begin := viaNamed("BEGIN", "SAVEPOINT sp")
	rollback := viaNamed("ROLLBACK TO SAVEPOINT sp")
	bindUnnamed := onClient(clientB, ready(&pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{}))
	badParse := &pgproto3.Parse{Name: "bad", Query: "SELEC 1"}

	for _, mode := range append(resettingModes, pgconfig.ResetOff) {
		t.Run(string(mode), func(t *testing.T) {
			proxy := startResetModeProxy(t, mode)
			url := proxy.URL(dbPooled, itest.PostgresUser)

			replies := exchange(t, url, []step{
				leaveUnnamed(clientA, "SELECT 'from client A'"),
				begin,
				onClient(clientB, ready(badParse, &pgproto3.Parse{Query: "SELECT 'from client B'"}, &pgproto3.Sync{})),
				rollback,
				bindUnnamed,
			}, 0)
			// Client B's own Parse was skipped, so it has no unnamed statement.
			require.Equal(t, []string{"ErrorResponse 26000", "ReadyForQuery E"}, summarize(replies[len(replies)-1]))

			replies = exchange(t, url, []step{
				leaveUnnamed(clientB, "SELECT 'from client B'"),
				leaveUnnamed(clientA, "SELECT 'from client A'"),
				begin,
				// The proxy re-creates client B's statement before the Bind, and
				// Postgres skips both.
				onClient(clientB, ready(badParse, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{})),
				rollback,
				bindUnnamed,
			}, 0)
			require.Equal(t, []string{"BindComplete", "DataRow from client B", "CommandComplete SELECT 1", "ReadyForQuery T"},
				summarize(replies[len(replies)-1]))
		})
	}
}
