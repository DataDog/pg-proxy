// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

//go:build integration

package integration

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

// TestPreparedIsolation verifies that two clients sharing dbPooled's single
// backend connection each see only their own statement named "shared", across
// interleaved Sync-terminated segments, a Close and re-Parse by one client,
// and a replaced backend connection. Each case runs against PostgreSQL directly
// and through the proxy.
func TestPreparedIsolation(t *testing.T) {
	for _, size := range bufferSizes {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			runProtocolCases(t, startProxyWithBuffer(t, size), []protocolCase{
				preparedIsolation("different statements", false, false),
				preparedIsolation("backend replaced", true, false),
				preparedIsolation("same statement", false, true),
			})
		})
	}
}

// preparedIsolation builds a two-client case. Each client parses "shared" and
// runs it in four segments through the portal "same_portal", taking turns
// segment by segment; client 0 closes and re-parses it with other SQL before
// its third segment. With reassign, the backend connection is terminated
// before client 0's second segment. With sameSQL, both clients parse the same
// SQL at first.
func preparedIsolation(name string, reassign, sameSQL bool) protocolCase {
	type client struct {
		sql    string
		oid    uint32
		params []string
		values []string
	}
	clients := []client{
		{"SELECT $1::int4 + 1 AS value", 23, []string{"7", "10", "7", "9"}, []string{"8", "11", "14", "18"}},
		{"SELECT upper($1::text) AS value", 25, []string{"one", "two", "three", "four"}, []string{"ONE", "TWO", "THREE", "FOUR"}},
	}
	if sameSQL {
		clients[1] = client{clients[0].sql, 23, []string{"2", "4", "6", "8"}, []string{"3", "5", "7", "9"}}
	}
	var steps []step
	var want [][]string
	for segment := range 4 {
		for i, c := range clients {
			var send []pgproto3.FrontendMessage
			var reply []string
			if i == 0 && segment == 2 {
				send = append(send, &pgproto3.Close{ObjectType: 'S', Name: "shared"})
				reply = append(reply, "CloseComplete")
				c.sql = "SELECT $1::int4 * 2 AS value"
			}
			if segment == 0 || i == 0 && segment == 2 {
				send = append(send, &pgproto3.Parse{Name: "shared", Query: c.sql, ParameterOIDs: []uint32{c.oid}})
				reply = append(reply, "ParseComplete")
			}
			send = append(send,
				&pgproto3.Bind{PreparedStatement: "shared", DestinationPortal: "same_portal", Parameters: [][]byte{[]byte(c.params[segment])}},
				&pgproto3.Execute{Portal: "same_portal"},
				&pgproto3.Sync{},
			)
			s := ready(send...)
			s.client = i
			if reassign && i == 0 && segment == 1 {
				s.before = terminatePooledBackend
			}
			steps = append(steps, s)
			want = append(want, append(reply, "BindComplete", "DataRow "+c.values[segment], "CommandComplete SELECT 1", "ReadyForQuery I"))
		}
	}
	for i := range clients {
		s := ready(&pgproto3.Query{String: "SELECT 1 AS recovery"})
		s.client = i
		steps = append(steps, s)
		want = append(want, []string{"RowDescription", "DataRow 1", "CommandComplete SELECT 1", "ReadyForQuery I"})
	}
	return protocolCase{
		name:     name,
		database: dbPooled,
		steps:    steps,
		check: func(t *testing.T, replies [][]pgproto3.BackendMessage) {
			requireReplies(t, replies, want...)
		},
	}
}

// terminatePooledBackend terminates the proxy's idle backend connection for
// dbPooled, if it has one, and waits for it to exit, so that the proxy's next
// checkout opens a new connection that has none of the clients' statements.
//
// pgxpool pings a connection before handing it out only once it has been idle
// for over a second, so terminatePooledBackend then waits that long. Without
// the wait the proxy hands out the dead connection and the client's session fails.
func terminatePooledBackend(t *testing.T) {
	t.Helper()
	ctx := t.Context()
	conn, err := backend.Connect(ctx, "postgres")
	require.NoError(t, err)
	defer conn.Close(context.Background())
	rows, err := conn.Query(ctx,
		"SELECT pg_terminate_backend(pid, 5000) FROM pg_stat_activity WHERE application_name = $1", pooledApplicationName)
	require.NoError(t, err)
	for rows.Next() {
		var terminated bool
		require.NoError(t, rows.Scan(&terminated))
		require.True(t, terminated, "pooled backend did not exit within 5s")
	}
	require.NoError(t, rows.Err())
	time.Sleep(1100 * time.Millisecond)
}
