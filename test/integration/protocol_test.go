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
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"

	"github.com/DataDog/pg-proxy/internal/itest"
)

// TestUnnamedStatementAcrossSync verifies that the unnamed prepared statement
// lives until the client replaces it, as in PostgreSQL, although the proxy
// releases the backend connection at each Sync outside a transaction. psql's
// \gdesc, for example, sends Parse+Sync and then Describe+Sync. Each case runs
// against PostgreSQL directly and through the proxy.
func TestUnnamedStatementAcrossSync(t *testing.T) {
	parse := &pgproto3.Parse{Query: "SELECT $1::int AS n"}
	bindExecute := []pgproto3.FrontendMessage{
		&pgproto3.Bind{Parameters: [][]byte{[]byte("7")}}, &pgproto3.Execute{}, &pgproto3.Sync{},
	}
	runProtocolCases(t, startProxy(t), []protocolCase{{
		name: "describe after sync",
		steps: []step{
			ready(parse, &pgproto3.Sync{}),
			ready(&pgproto3.Describe{ObjectType: 'S'}, &pgproto3.Sync{}),
		},
		check: func(t *testing.T, replies [][]pgproto3.BackendMessage) {
			requireNoError(t, replies[1])
			require.IsType(t, &pgproto3.ParameterDescription{}, replies[1][0])
			require.IsType(t, &pgproto3.RowDescription{}, replies[1][1])
		},
	}, {
		name: "bind after sync",
		steps: []step{
			ready(parse, &pgproto3.Sync{}),
			ready(bindExecute...),
		},
		check: func(t *testing.T, replies [][]pgproto3.BackendMessage) {
			requireNoError(t, replies[1])
			require.Equal(t, "7", dataRowValue(t, replies[1]))
		},
	}, {
		name: "bind twice across syncs",
		steps: []step{
			ready(parse, &pgproto3.Sync{}),
			ready(bindExecute...),
			ready(bindExecute...),
		},
		check: func(t *testing.T, replies [][]pgproto3.BackendMessage) {
			require.Equal(t, "7", dataRowValue(t, replies[1]))
			require.Equal(t, "7", dataRowValue(t, replies[2]))
		},
	}, {
		name: "simple query drops it",
		steps: []step{
			ready(parse, &pgproto3.Sync{}),
			ready(&pgproto3.Query{String: "SELECT 1"}),
			ready(bindExecute...),
		},
		check: func(t *testing.T, replies [][]pgproto3.BackendMessage) {
			requireErrorCode(t, replies[2], "26000")
		},
	}, {
		// pgx sends "-- ping" as a health check; the proxy answers it locally
		// when idle, but it is still a simple Query.
		name: "local ping drops it",
		steps: []step{
			ready(parse, &pgproto3.Sync{}),
			ready(&pgproto3.Query{String: "-- ping"}),
			ready(bindExecute...),
		},
		check: func(t *testing.T, replies [][]pgproto3.BackendMessage) {
			requireErrorCode(t, replies[2], "26000")
		},
	}, {
		name: "close drops it",
		steps: []step{
			ready(parse, &pgproto3.Sync{}),
			ready(&pgproto3.Close{ObjectType: 'S'}, &pgproto3.Sync{}),
			ready(bindExecute...),
		},
		check: func(t *testing.T, replies [][]pgproto3.BackendMessage) {
			requireNoError(t, replies[1])
			requireErrorCode(t, replies[2], "26000")
		},
	}, {
		// The ParseComplete arrives after the Close was sent; it must not
		// restore the closed statement.
		name: "close in the same batch drops it",
		steps: []step{
			ready(parse, &pgproto3.Close{ObjectType: 'S'}, &pgproto3.Sync{}),
			ready(bindExecute...),
		},
		check: func(t *testing.T, replies [][]pgproto3.BackendMessage) {
			requireNoError(t, replies[0])
			requireErrorCode(t, replies[1], "26000")
		},
	}, {
		// PostgreSQL drops the first statement before parsing the second, which
		// fails. The first ParseComplete must not restore the dropped statement.
		name: "failed parse in the same batch drops the previous one",
		steps: []step{
			ready(parse, &pgproto3.Parse{Query: "SELEC 1"}, &pgproto3.Sync{}),
			ready(bindExecute...),
		},
		check: func(t *testing.T, replies [][]pgproto3.BackendMessage) {
			requireErrorCode(t, replies[0], "42601")
			requireErrorCode(t, replies[1], "26000")
		},
	}, {
		name: "failed parse drops the previous one",
		steps: []step{
			ready(parse, &pgproto3.Sync{}),
			ready(&pgproto3.Parse{Query: "SELEC 1"}, &pgproto3.Sync{}),
			ready(bindExecute...),
		},
		check: func(t *testing.T, replies [][]pgproto3.BackendMessage) {
			requireErrorCode(t, replies[1], "42601")
			requireErrorCode(t, replies[2], "26000")
		},
	}, {
		name: "parse replaces it",
		steps: []step{
			ready(parse, &pgproto3.Sync{}),
			ready(&pgproto3.Parse{Query: "SELECT 'replaced' WHERE $1::int IS NOT NULL"}, &pgproto3.Sync{}),
			ready(bindExecute...),
		},
		check: func(t *testing.T, replies [][]pgproto3.BackendMessage) {
			require.Equal(t, "replaced", dataRowValue(t, replies[2]))
		},
	}, {
		name: "inside a transaction",
		steps: []step{
			ready(&pgproto3.Query{String: "BEGIN"}),
			ready(parse, &pgproto3.Sync{}),
			ready(bindExecute...),
			ready(&pgproto3.Query{String: "COMMIT"}),
		},
		check: func(t *testing.T, replies [][]pgproto3.BackendMessage) {
			require.Equal(t, "7", dataRowValue(t, replies[2]))
		},
	}})
}

// TestAbortedPipeline verifies that after an error in a pipeline (messages
// sent without Sync), the proxy, like PostgreSQL, ignores every message until
// the next Sync: it sends nothing back and remembers nothing they did. psql's
// \startpipeline, \parse, \bind_named and \close_prepared exercise this.
func TestAbortedPipeline(t *testing.T) {
	badBind := failing(
		&pgproto3.Parse{Query: "SELECT $1"}, &pgproto3.Bind{}, &pgproto3.Describe{ObjectType: 'P'}, &pgproto3.Execute{},
		&pgproto3.Flush{},
	)
	useNamed := []pgproto3.FrontendMessage{
		&pgproto3.Bind{PreparedStatement: "a", Parameters: [][]byte{[]byte("1")}}, &pgproto3.Execute{},
	}
	parseNamed := &pgproto3.Parse{Name: "a", Query: "SELECT $1::int"}
	runProtocolCases(t, startProxy(t), []protocolCase{{
		name: "messages after the error get no reply",
		steps: []step{
			badBind,
			quiet(append([]pgproto3.FrontendMessage{parseNamed}, append(useNamed,
				&pgproto3.Close{ObjectType: 'S', Name: "a"}, &pgproto3.Flush{})...)...),
			ready(&pgproto3.Sync{}),
		},
		check: func(t *testing.T, replies [][]pgproto3.BackendMessage) {
			requireErrorCode(t, replies[0], "08P01")
			require.Empty(t, replies[1])
			require.Len(t, replies[2], 1)
		},
	}, {
		name: "a statement parsed after the error does not exist",
		steps: []step{
			badBind,
			quiet(parseNamed, &pgproto3.Flush{}),
			ready(&pgproto3.Sync{}),
			ready(append(useNamed, &pgproto3.Sync{})...),
		},
		check: func(t *testing.T, replies [][]pgproto3.BackendMessage) {
			requireErrorCode(t, replies[3], "26000")
		},
	}, {
		name: "a statement parsed after the error can be parsed again",
		steps: []step{
			badBind,
			quiet(append(append([]pgproto3.FrontendMessage{parseNamed}, useNamed...),
				&pgproto3.Close{ObjectType: 'S', Name: "a"}, &pgproto3.Flush{})...),
			ready(&pgproto3.Sync{}),
			ready(append(append([]pgproto3.FrontendMessage{parseNamed}, useNamed...),
				&pgproto3.Close{ObjectType: 'S', Name: "a"}, &pgproto3.Sync{})...),
		},
		check: func(t *testing.T, replies [][]pgproto3.BackendMessage) {
			require.Equal(t, "1", dataRowValue(t, replies[3]))
		},
	}, {
		// psql_pipeline's "After an error, pipeline is aborted" block.
		name: "named statement in the failing batch and after recovery",
		steps: []step{
			failing(append(append([]pgproto3.FrontendMessage{
				&pgproto3.Parse{Query: "SELECT $1"}, &pgproto3.Bind{}, &pgproto3.Describe{ObjectType: 'P'}, &pgproto3.Execute{},
				parseNamed,
			}, useNamed...), &pgproto3.Close{ObjectType: 'S', Name: "a"}, &pgproto3.Flush{})...),
			quiet(append(append([]pgproto3.FrontendMessage{parseNamed}, useNamed...),
				&pgproto3.Close{ObjectType: 'S', Name: "a"}, &pgproto3.Flush{})...),
			ready(&pgproto3.Sync{}),
			quiet(append(append([]pgproto3.FrontendMessage{parseNamed}, useNamed...),
				&pgproto3.Close{ObjectType: 'S', Name: "a"}, &pgproto3.Flush{})...),
			ready(&pgproto3.Sync{}),
		},
		check: func(t *testing.T, replies [][]pgproto3.BackendMessage) {
			require.Equal(t, "1", dataRowValue(t, replies[3]))
			require.IsType(t, &pgproto3.CloseComplete{}, replies[3][len(replies[3])-1])
		},
	}})
}

// TestCommitErrorAtSync verifies that an error committing the implicit
// transaction at Sync, here a deferred unique constraint, reaches the client
// followed by ReadyForQuery, and the session stays usable.
func TestCommitErrorAtSync(t *testing.T) {
	require.NoError(t, backend.Exec(t.Context(),
		"CREATE TABLE IF NOT EXISTS deferred_unique (a int UNIQUE DEFERRABLE INITIALLY DEFERRED)"))
	insertTwice := []pgproto3.FrontendMessage{
		&pgproto3.Parse{Query: "INSERT INTO deferred_unique VALUES ($1::int), ($1::int)"},
		&pgproto3.Bind{Parameters: [][]byte{[]byte("1")}}, &pgproto3.Execute{},
	}
	runProtocolCases(t, startProxy(t), []protocolCase{{
		name: "extended query",
		steps: []step{
			ready(append(insertTwice, &pgproto3.Sync{})...),
			ready(&pgproto3.Query{String: "SELECT count(*) FROM deferred_unique"}),
		},
		check: func(t *testing.T, replies [][]pgproto3.BackendMessage) {
			requireErrorCode(t, replies[0], "23505")
			require.Equal(t, &pgproto3.ReadyForQuery{TxStatus: 'I'}, replies[0][len(replies[0])-1])
			require.Equal(t, "0", dataRowValue(t, replies[1]))
		},
	}, {
		name: "pipeline",
		steps: []step{
			ready(append(append(insertTwice, &pgproto3.Sync{}),
				&pgproto3.Parse{Query: "SELECT 'after sync'"}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{})...),
			ready(),
		},
		check: func(t *testing.T, replies [][]pgproto3.BackendMessage) {
			requireErrorCode(t, replies[0], "23505")
			require.Equal(t, "after sync", dataRowValue(t, replies[1]))
		},
	}})
}

// protocolCase is a message exchange checked against PostgreSQL directly and
// through the proxy.
type protocolCase struct {
	name  string
	steps []step
	check func(t *testing.T, replies [][]pgproto3.BackendMessage)
	// database is the logical database the proxy run connects to; empty means dbDefault.
	database string
	// setup runs on the backend before each run, for example to recreate a table.
	setup []string
	// proxySkip, if set, skips the proxy run: it names a known proxy bug.
	proxySkip string
}

func runProtocolCases(t *testing.T, proxy *itest.Proxy, cases []protocolCase) {
	t.Helper()
	for _, tc := range cases {
		database := tc.database
		if database == "" {
			database = dbDefault
		}
		targets := map[string]string{
			"direct": backend.URL("postgres"),
			"proxy":  proxy.URL(database, itest.PostgresUser),
		}
		for target, url := range targets {
			t.Run(tc.name+"/"+target, func(t *testing.T) {
				if target == "proxy" && tc.proxySkip != "" {
					t.Skip(tc.proxySkip)
				}
				require.NoError(t, backend.Exec(t.Context(), tc.setup...))
				tc.check(t, exchange(t, url, tc.steps))
			})
		}
	}
}

// replyEnd says how far to read a step's replies.
type replyEnd int

const (
	untilReady  replyEnd = iota // up to and including ReadyForQuery
	untilError                  // up to and including an ErrorResponse
	untilQuiet                  // whatever arrives before the server goes quiet
	untilCopyIn                 // up to and including CopyInResponse or an ErrorResponse
)

// step is a batch of messages sent together and how far to read the replies.
type step struct {
	send  []pgproto3.FrontendMessage
	until replyEnd
	// client is the connection that sends the step, for cases with several.
	client int
	// before, if set, runs before the step is sent.
	before func(t *testing.T)
}

func ready(msgs ...pgproto3.FrontendMessage) step   { return step{send: msgs, until: untilReady} }
func failing(msgs ...pgproto3.FrontendMessage) step { return step{send: msgs, until: untilError} }
func quiet(msgs ...pgproto3.FrontendMessage) step   { return step{send: msgs, until: untilQuiet} }
func copyIn(msgs ...pgproto3.FrontendMessage) step  { return step{send: msgs, until: untilCopyIn} }

// exchange opens one connection to url per client the steps name, sends each
// step's messages on its client's connection and collects its replies.
func exchange(t *testing.T, url string, steps []step) [][]pgproto3.BackendMessage {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	clients := 1
	for _, step := range steps {
		clients = max(clients, step.client+1)
	}
	pgConns := make([]*pgconn.PgConn, clients)
	for i := range pgConns {
		conn, err := pgx.Connect(ctx, url)
		require.NoError(t, err)
		defer conn.Close(context.Background())
		pgConns[i] = conn.PgConn()
		require.NoError(t, pgConns[i].Conn().SetDeadline(time.Now().Add(20*time.Second)))
	}

	var replies [][]pgproto3.BackendMessage
	for _, step := range steps {
		if step.before != nil {
			step.before(t)
		}
		pgConn := pgConns[step.client]
		frontend := pgConn.Frontend()
		for _, msg := range step.send {
			frontend.Send(msg)
		}
		require.NoError(t, frontend.Flush())
		var reply []pgproto3.BackendMessage
		if step.until == untilQuiet {
			require.NoError(t, pgConn.Conn().SetReadDeadline(time.Now().Add(500*time.Millisecond)))
		}
		for {
			msg, err := frontend.Receive()
			var netErr net.Error
			if step.until == untilQuiet && errors.As(err, &netErr) && netErr.Timeout() {
				require.NoError(t, pgConn.Conn().SetDeadline(time.Now().Add(20*time.Second)))
				break
			}
			require.NoError(t, err)
			reply = append(reply, copyBackendMessage(t, msg))
			_, isReady := msg.(*pgproto3.ReadyForQuery)
			_, isError := msg.(*pgproto3.ErrorResponse)
			_, isCopyIn := msg.(*pgproto3.CopyInResponse)
			if (step.until == untilReady && isReady) || (step.until == untilError && isError) ||
				(step.until == untilCopyIn && (isCopyIn || isError)) {
				break
			}
		}
		replies = append(replies, reply)
	}
	return replies
}

// copyBackendMessage detaches msg from the frontend's reused messages and
// buffers, such as the single ReadyForQuery it decodes every one into.
func copyBackendMessage(t *testing.T, msg pgproto3.BackendMessage) pgproto3.BackendMessage {
	t.Helper()
	encoded, err := msg.Encode(nil)
	require.NoError(t, err)
	copied := reflect.New(reflect.TypeOf(msg).Elem()).Interface().(pgproto3.BackendMessage)
	require.NoError(t, copied.Decode(encoded[5:]))
	return copied
}

// summarize renders each message of reply as its type and the fields that
// identify it, for example "CommandComplete SELECT 1" or "DataRow 1|NULL".
func summarize(reply []pgproto3.BackendMessage) []string {
	lines := make([]string, 0, len(reply))
	for _, msg := range reply {
		line := reflect.TypeOf(msg).Elem().Name()
		switch msg := msg.(type) {
		case *pgproto3.CommandComplete:
			line += " " + string(msg.CommandTag)
		case *pgproto3.ReadyForQuery:
			line += " " + string(msg.TxStatus)
		case *pgproto3.ErrorResponse:
			line += " " + msg.Code
		case *pgproto3.DataRow:
			values := make([]string, len(msg.Values))
			for i, value := range msg.Values {
				values[i] = string(value)
				if value == nil {
					values[i] = "NULL"
				}
			}
			line += " " + strings.Join(values, "|")
		case *pgproto3.CopyInResponse:
			line += fmt.Sprintf(" %d %v", msg.OverallFormat, msg.ColumnFormatCodes)
		case *pgproto3.CopyOutResponse:
			line += fmt.Sprintf(" %d %v", msg.OverallFormat, msg.ColumnFormatCodes)
		case *pgproto3.CopyData:
			line += fmt.Sprintf(" %q", msg.Data)
		}
		lines = append(lines, line)
	}
	return lines
}

// requireReplies checks the summary of each reply in order.
func requireReplies(t *testing.T, replies [][]pgproto3.BackendMessage, want ...[]string) {
	t.Helper()
	require.Len(t, replies, len(want))
	for i := range want {
		require.Equal(t, want[i], summarize(replies[i]), "reply %d", i)
	}
}

func requireNoError(t *testing.T, reply []pgproto3.BackendMessage) {
	t.Helper()
	for _, msg := range reply {
		if errorResponse, ok := msg.(*pgproto3.ErrorResponse); ok {
			t.Fatalf("unexpected error %s: %s", errorResponse.Code, errorResponse.Message)
		}
	}
}

func requireErrorCode(t *testing.T, reply []pgproto3.BackendMessage, code string) {
	t.Helper()
	for _, msg := range reply {
		if errorResponse, ok := msg.(*pgproto3.ErrorResponse); ok {
			require.Equal(t, code, errorResponse.Code, errorResponse.Message)
			return
		}
	}
	t.Fatalf("expected error %s, got %#v", code, reply)
}

func dataRowValue(t *testing.T, reply []pgproto3.BackendMessage) string {
	t.Helper()
	requireNoError(t, reply)
	for _, msg := range reply {
		if row, ok := msg.(*pgproto3.DataRow); ok {
			require.Len(t, row.Values, 1)
			return string(row.Values[0])
		}
	}
	t.Fatalf("no data row in %#v", reply)
	return ""
}
