// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

//go:build integration

package integration

import (
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

// Known bugs in the proxy's record of the prepared statements on a backend.
// Each case that hits one still runs against PostgreSQL directly; set
// PG_PROXY_ITEST_RUN_KNOWN_BUGS=1 to reproduce it through the proxy.
const (
	staleAfterResultTypeChange = "TODO: after a result-type change, proxy answers a re-Parse of the same SQL " +
		"with a local ParseComplete for the backend's stale statement, so 0A000 repeats on every retry " +
		"and for a new client on the same backend"
	statementsDiscarded = "TODO: proxy does not notice that %s dropped the backend's statements, " +
		"so it answers a later client's Parse of the same SQL locally and Bind gets 26000"
	sameSQLInPipeline = "TODO: when two statement names in one pipeline have the same SQL, " +
		"proxy does not record the second name, so Bind of it gets 26000"
	sqlDeallocate = "TODO: proxy passes SQL DEALLOCATE of a client statement name to PostgreSQL, " +
		"which returns 26000, and keeps the name: a re-Parse gets 42P05 and a later Bind runs the old SQL"
)

// TestPreparedStatementLifecycle verifies that named statements behave as in
// PostgreSQL when something other than Parse and Close changes them: DDL, SQL
// that drops statements, two names for the same SQL, and errors inside a
// transaction. The proxy maps client statement names to its own names on the
// backend and reuses a backend statement for every client that parses the
// same SQL, so each of these must also keep that mapping right. Each case
// runs against PostgreSQL directly and through the proxy.
func TestPreparedStatementLifecycle(t *testing.T) {
	userByID := "SELECT * FROM lifecycle_users WHERE id = $1"
	parseUserByID := func(name string) *pgproto3.Parse {
		return &pgproto3.Parse{Name: name, Query: userByID, ParameterOIDs: []uint32{23}}
	}
	bindExecute := func(name string, params ...string) []pgproto3.FrontendMessage {
		var values [][]byte
		for _, param := range params {
			values = append(values, []byte(param))
		}
		return []pgproto3.FrontendMessage{&pgproto3.Bind{PreparedStatement: name, Parameters: values}, &pgproto3.Execute{}, &pgproto3.Sync{}}
	}
	parseBindExecute := func(parse *pgproto3.Parse, params ...string) step {
		return ready(append([]pgproto3.FrontendMessage{parse}, bindExecute(parse.Name, params...)...)...)
	}
	selected := func(row string) []string {
		return []string{"BindComplete", "DataRow " + row, "CommandComplete SELECT 1", "ReadyForQuery I"}
	}
	parsedAndSelected := func(row string) []string { return append([]string{"ParseComplete"}, selected(row)...) }
	plusOne := "SELECT $1::int + 1 AS n"
	discarded := func(name, statement string) protocolCase {
		return protocolCase{
			name:      name,
			proxySkip: fmt.Sprintf(statementsDiscarded, statement),
			steps: []step{
				parseBindExecute(&pgproto3.Parse{Name: "a", Query: plusOne}, "1"),
				simpleQuery(statement),
				onClient(1, parseBindExecute(&pgproto3.Parse{Name: "b", Query: plusOne}, "2")),
			},
			check: func(t *testing.T, replies [][]pgproto3.BackendMessage) {
				requireReplies(t, replies,
					parsedAndSelected("2"),
					[]string{"CommandComplete " + statement, "ReadyForQuery I"},
					parsedAndSelected("3"),
				)
			},
		}
	}

	runProtocolCases(t, startProxy(t), []protocolCase{{
		// The DDL runs on another session between client 0's executions.
		name: "result type changes after prepare",
		setup: []string{
			"DROP TABLE IF EXISTS lifecycle_users",
			"CREATE TABLE lifecycle_users (id int, name text)",
			"INSERT INTO lifecycle_users VALUES (1, 'alice')",
		},
		proxySkip: staleAfterResultTypeChange,
		steps: []step{
			parseBindExecute(parseUserByID("user_by_id"), "1"),
			{send: bindExecute("user_by_id", "1"), until: untilReady, before: func(t *testing.T) {
				require.NoError(t, backend.Exec(t.Context(), "ALTER TABLE lifecycle_users ADD COLUMN email text"))
			}},
			ready(append([]pgproto3.FrontendMessage{&pgproto3.Close{ObjectType: 'S', Name: "user_by_id"}, parseUserByID("user_by_id")},
				bindExecute("user_by_id", "1")...)...),
			onClient(1, parseBindExecute(parseUserByID("user_by_id_2"), "1")),
		},
		check: func(t *testing.T, replies [][]pgproto3.BackendMessage) {
			requireReplies(t, replies,
				parsedAndSelected("1|alice"),
				[]string{"ErrorResponse 0A000", "ReadyForQuery I"},
				append([]string{"CloseComplete"}, parsedAndSelected("1|alice|NULL")...),
				parsedAndSelected("1|alice|NULL"),
			)
		},
	},
		discarded("a later client after DISCARD ALL", "DISCARD ALL"),
		discarded("a later client after DEALLOCATE ALL", "DEALLOCATE ALL"),
		{
			name:      "two names for the same SQL in one pipeline",
			proxySkip: sameSQLInPipeline,
			steps: []step{ready(append([]pgproto3.FrontendMessage{
				&pgproto3.Parse{Name: "first", Query: "SELECT 'one pipeline' AS label"},
				&pgproto3.Parse{Name: "second", Query: "SELECT 'one pipeline' AS label"},
			}, bindExecute("second")...)...)},
			check: func(t *testing.T, replies [][]pgproto3.BackendMessage) {
				requireReplies(t, replies, append([]string{"ParseComplete"}, parsedAndSelected("one pipeline")...))
			},
		}, {
			name: "two names for the same SQL across syncs",
			steps: []step{
				ready(&pgproto3.Parse{Name: "first", Query: "SELECT 'two segments' AS label"}, &pgproto3.Sync{}),
				ready(&pgproto3.Parse{Name: "second", Query: "SELECT 'two segments' AS label"}, &pgproto3.Sync{}),
				ready(bindExecute("second")...),
				ready(bindExecute("first")...),
			},
			check: func(t *testing.T, replies [][]pgproto3.BackendMessage) {
				requireReplies(t, replies,
					[]string{"ParseComplete", "ReadyForQuery I"},
					[]string{"ParseComplete", "ReadyForQuery I"},
					selected("two segments"),
					selected("two segments"),
				)
			},
		}, {
			// PostgreSQL aborts the transaction, so COMMIT rolls back both inserts.
			name:  "duplicate Parse in a transaction",
			setup: []string{"DROP TABLE IF EXISTS lifecycle_events", "CREATE TABLE lifecycle_events (label text)"},
			steps: []step{
				simpleQuery("BEGIN"),
				ready(&pgproto3.Parse{Name: "duplicate", Query: "SELECT 'first' AS label"}, &pgproto3.Sync{}),
				simpleQuery("INSERT INTO lifecycle_events VALUES ('before')"),
				ready(&pgproto3.Parse{Name: "duplicate", Query: "SELECT 'second' AS label"}, &pgproto3.Sync{}),
				simpleQuery("INSERT INTO lifecycle_events VALUES ('after')"),
				simpleQuery("COMMIT"),
				simpleQuery("SELECT count(*) FROM lifecycle_events"),
			},
			check: func(t *testing.T, replies [][]pgproto3.BackendMessage) {
				requireReplies(t, replies,
					[]string{"CommandComplete BEGIN", "ReadyForQuery T"},
					[]string{"ParseComplete", "ReadyForQuery T"},
					[]string{"CommandComplete INSERT 0 1", "ReadyForQuery T"},
					[]string{"ErrorResponse 42P05", "ReadyForQuery E"},
					[]string{"ErrorResponse 25P02", "ReadyForQuery E"},
					[]string{"CommandComplete ROLLBACK", "ReadyForQuery I"},
					[]string{"RowDescription", "DataRow 0", "CommandComplete SELECT 1", "ReadyForQuery I"},
				)
			},
		}, {
			name:      "SQL DEALLOCATE of a statement name",
			proxySkip: sqlDeallocate,
			steps: []step{
				ready(&pgproto3.Parse{Name: "deallocated", Query: "SELECT 7"}, &pgproto3.Sync{}),
				simpleQuery("DEALLOCATE deallocated"),
				ready(&pgproto3.Parse{Name: "deallocated", Query: "SELECT 8"}, &pgproto3.Sync{}),
				ready(bindExecute("deallocated")...),
			},
			check: func(t *testing.T, replies [][]pgproto3.BackendMessage) {
				requireReplies(t, replies,
					[]string{"ParseComplete", "ReadyForQuery I"},
					[]string{"CommandComplete DEALLOCATE", "ReadyForQuery I"},
					[]string{"ParseComplete", "ReadyForQuery I"},
					selected("8"),
				)
			},
		}, {
			// The protocol counterpart of the SQL DEALLOCATE case.
			name: "Close of a statement name",
			steps: []step{
				ready(&pgproto3.Parse{Name: "closed", Query: "SELECT 9"}, &pgproto3.Sync{}),
				ready(&pgproto3.Close{ObjectType: 'S', Name: "closed"}, &pgproto3.Sync{}),
				ready(&pgproto3.Parse{Name: "closed", Query: "SELECT 10"}, &pgproto3.Sync{}),
				ready(bindExecute("closed")...),
			},
			check: func(t *testing.T, replies [][]pgproto3.BackendMessage) {
				requireReplies(t, replies,
					[]string{"ParseComplete", "ReadyForQuery I"},
					[]string{"CloseComplete", "ReadyForQuery I"},
					[]string{"ParseComplete", "ReadyForQuery I"},
					selected("10"),
				)
			},
		}})
}
