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
// The LISTEN case's proxy run must fail, and so must the others' hygiene check
// after it; set PG_PROXY_ITEST_RUN_KNOWN_BUGS=1 to see how they fail.
const (
	leakedListen = "TODO: proxy does not UNLISTEN when it releases a backend, and it closes the session " +
		"with FATAL 08006 when that backend sends a NotificationResponse: client 1's COMMIT commits, " +
		"but client 1 gets the error"
	leakedAdvisoryLock = "TODO: proxy does not release session advisory locks when it releases a backend, " +
		"so the next client's backend still holds the lock"
	leakedTempTable = "TODO: proxy does not drop temporary tables when it releases a backend, " +
		"so the next client's backend still has the table"
	leakedCursor = "TODO: proxy does not close WITH HOLD cursors when it releases a backend, " +
		"so the next client's backend still has the cursor"
)

// TestSessionStateLeaks verifies that session state one client creates does
// not reach another client that shares its backend connection. Client 0
// creates the state. Through the proxy, the hygiene check after the run looks
// for it on the backend; the LISTEN case also has later clients on that
// backend. Each case runs against PostgreSQL directly and through the proxy.
func TestSessionStateLeaks(t *testing.T) {
	noErrors := func(t testing.TB, replies [][]pgproto3.BackendMessage) {
		for _, reply := range replies {
			requireNoError(t, reply)
		}
	}
	runProtocolCases(t, startProxy(t), []protocolCase{{
		// A NOTIFY from another session reaches a backend that LISTENs, once
		// its transaction ends: PostgreSQL sends it before ReadyForQuery.
		name:      "notification after a leaked LISTEN",
		setup:     []string{"DROP TABLE IF EXISTS listen_users", "CREATE TABLE listen_users (id int)"},
		proxySkip: leakedListen,
		steps: []step{
			simpleQuery("LISTEN users_changed"),
			onClient(1, simpleQuery("BEGIN")),
			onClient(1, simpleQuery("INSERT INTO listen_users VALUES (1)")),
			{send: []pgproto3.FrontendMessage{&pgproto3.Query{String: "COMMIT"}}, until: untilReady, client: 1,
				before: func(t testing.TB) {
					require.NoError(t, backend.Exec(t.Context(), "NOTIFY users_changed, 'user 1'"))
				}},
			onClient(2, simpleQuery("SELECT count(*) FROM listen_users")),
		},
		check: func(t testing.TB, replies [][]pgproto3.BackendMessage) {
			requireReplies(t, replies,
				[]string{"CommandComplete LISTEN", "ReadyForQuery I"},
				[]string{"CommandComplete BEGIN", "ReadyForQuery T"},
				[]string{"CommandComplete INSERT 0 1", "ReadyForQuery T"},
				[]string{"CommandComplete COMMIT", "ReadyForQuery I"},
				[]string{"RowDescription", "DataRow 1", "CommandComplete SELECT 1", "ReadyForQuery I"},
			)
		},
	}, {
		name:        "session advisory lock",
		hygieneSkip: leakedAdvisoryLock,
		steps:       []step{simpleQuery("SELECT pg_advisory_lock(4242)")},
		check:       noErrors,
	}, {
		name:        "temporary table",
		hygieneSkip: leakedTempTable,
		steps:       []step{simpleQuery("CREATE TEMP TABLE session_users (id int)")},
		check:       noErrors,
	}, {
		name:        "cursor WITH HOLD",
		hygieneSkip: leakedCursor,
		steps: []step{
			simpleQuery("BEGIN"),
			simpleQuery("DECLARE users_cursor CURSOR WITH HOLD FOR SELECT 1"),
			simpleQuery("COMMIT"),
		},
		check: noErrors,
	}})
}
