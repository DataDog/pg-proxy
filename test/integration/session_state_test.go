// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

//go:build integration

package integration

import (
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

// Known bugs: session state that the proxy leaves on a backend it releases.
// Each case that hits one still runs against PostgreSQL directly; set
// PG_PROXY_ITEST_RUN_KNOWN_BUGS=1 to reproduce it through the proxy.
const (
	leakedListen = "TODO: proxy does not UNLISTEN when it releases a backend, and it closes the session " +
		"with FATAL 08006 when that backend sends a NotificationResponse: client 1's COMMIT commits, " +
		"but client 1 gets the error"
	leakedAdvisoryLock = "TODO: proxy does not release session advisory locks when it releases a backend, " +
		"so client 1's backend still holds client 0's lock"
	leakedTempTable = "TODO: proxy does not drop temporary tables when it releases a backend, " +
		"so client 1's backend still has client 0's table"
	leakedCursor = "TODO: proxy does not close WITH HOLD cursors when it releases a backend, " +
		"so client 1's backend still has client 0's cursor"
)

// TestSessionStateLeaks verifies that session state one client creates does
// not reach another client that shares its backend connection. Client 0
// creates the state, and client 1, on the same backend through the proxy but
// in its own session directly, looks for it. Each case runs against
// PostgreSQL directly and through the proxy.
func TestSessionStateLeaks(t *testing.T) {
	query := func(sql string) step { return ready(&pgproto3.Query{String: sql}) }
	onlyZero := func(t *testing.T, replies [][]pgproto3.BackendMessage) {
		for _, reply := range replies[:len(replies)-1] {
			requireNoError(t, reply)
		}
		require.Equal(t, "0", dataRowValue(t, replies[len(replies)-1]))
	}
	runProtocolCases(t, startProxy(t), []protocolCase{{
		// A NOTIFY from another session reaches a backend that LISTENs, once
		// its transaction ends: PostgreSQL sends it before ReadyForQuery.
		name:      "notification after a leaked LISTEN",
		setup:     []string{"DROP TABLE IF EXISTS listen_users", "CREATE TABLE listen_users (id int)"},
		proxySkip: leakedListen,
		steps: []step{
			query("LISTEN users_changed"),
			onClient(1, query("BEGIN")),
			onClient(1, query("INSERT INTO listen_users VALUES (1)")),
			{send: []pgproto3.FrontendMessage{&pgproto3.Query{String: "COMMIT"}}, until: untilReady, client: 1,
				before: func(t *testing.T) {
					require.NoError(t, backend.Exec(t.Context(), "NOTIFY users_changed, 'user 1'"))
				}},
			onClient(2, query("SELECT count(*) FROM listen_users")),
		},
		check: func(t *testing.T, replies [][]pgproto3.BackendMessage) {
			requireReplies(t, replies,
				[]string{"CommandComplete LISTEN", "ReadyForQuery I"},
				[]string{"CommandComplete BEGIN", "ReadyForQuery T"},
				[]string{"CommandComplete INSERT 0 1", "ReadyForQuery T"},
				[]string{"CommandComplete COMMIT", "ReadyForQuery I"},
				[]string{"RowDescription", "DataRow 1", "CommandComplete SELECT 1", "ReadyForQuery I"},
			)
		},
	}, {
		name:      "session advisory lock",
		proxySkip: leakedAdvisoryLock,
		steps: []step{
			query("SELECT pg_advisory_lock(4242)"),
			onClient(1, query(advisoryLocksQuery)),
		},
		check: onlyZero,
	}, {
		name:      "temporary table",
		proxySkip: leakedTempTable,
		steps: []step{
			query("CREATE TEMP TABLE session_users (id int)"),
			onClient(1, query(tempRelationsQuery)),
		},
		check: onlyZero,
	}, {
		name:      "cursor WITH HOLD",
		proxySkip: leakedCursor,
		steps: []step{
			query("BEGIN"),
			query("DECLARE users_cursor CURSOR WITH HOLD FOR SELECT 1"),
			query("COMMIT"),
			onClient(1, query(cursorsQuery)),
		},
		check: onlyZero,
	}})
}
