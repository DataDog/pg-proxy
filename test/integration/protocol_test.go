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
	"maps"
	"net"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
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
		check: func(t testing.TB, replies [][]pgproto3.BackendMessage) {
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
		check: func(t testing.TB, replies [][]pgproto3.BackendMessage) {
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
		check: func(t testing.TB, replies [][]pgproto3.BackendMessage) {
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
		check: func(t testing.TB, replies [][]pgproto3.BackendMessage) {
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
		check: func(t testing.TB, replies [][]pgproto3.BackendMessage) {
			requireErrorCode(t, replies[2], "26000")
		},
	}, {
		name: "close drops it",
		steps: []step{
			ready(parse, &pgproto3.Sync{}),
			ready(&pgproto3.Close{ObjectType: 'S'}, &pgproto3.Sync{}),
			ready(bindExecute...),
		},
		check: func(t testing.TB, replies [][]pgproto3.BackendMessage) {
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
		check: func(t testing.TB, replies [][]pgproto3.BackendMessage) {
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
		check: func(t testing.TB, replies [][]pgproto3.BackendMessage) {
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
		check: func(t testing.TB, replies [][]pgproto3.BackendMessage) {
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
		check: func(t testing.TB, replies [][]pgproto3.BackendMessage) {
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
		check: func(t testing.TB, replies [][]pgproto3.BackendMessage) {
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
		check: func(t testing.TB, replies [][]pgproto3.BackendMessage) {
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
		check: func(t testing.TB, replies [][]pgproto3.BackendMessage) {
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
		check: func(t testing.TB, replies [][]pgproto3.BackendMessage) {
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
		check: func(t testing.TB, replies [][]pgproto3.BackendMessage) {
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
		check: func(t testing.TB, replies [][]pgproto3.BackendMessage) {
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
		check: func(t testing.TB, replies [][]pgproto3.BackendMessage) {
			requireErrorCode(t, replies[0], "23505")
			require.Equal(t, "after sync", dataRowValue(t, replies[1]))
		},
	}})
}

// protocolCase is a message exchange checked against PostgreSQL directly and
// through the proxy's dbPooled. After the proxy run, a new client checks that
// the case left the backend clean (see requireCleanBackend).
type protocolCase struct {
	name  string
	steps []step
	check func(t testing.TB, replies [][]pgproto3.BackendMessage)
	// setup runs on the backend before each run, for example to recreate a table.
	setup []string
	// proxySkip, if set, names a known proxy bug. The proxy run must fail, and
	// is then skipped (see requireKnownBug).
	proxySkip string
	// hygieneSkip, if set, names a known proxy bug that leaves state on the
	// backend. The hygiene check after the proxy run must fail, and is then
	// skipped.
	hygieneSkip string
	// stepTimeout bounds the wait for each step's replies; zero means 20 seconds.
	stepTimeout time.Duration
}

// runProtocolCases runs each case against PostgreSQL directly and through
// proxy's dbPooled, whose single backend connection every client of the proxy
// run and its hygiene check share.
func runProtocolCases(t *testing.T, proxy *itest.Proxy, cases []protocolCase) {
	t.Helper()
	parsed := clientStatements{sql: map[string]bool{}, names: map[string]bool{}}
	for _, tc := range cases {
		for _, step := range tc.steps {
			for _, msg := range step.send {
				if parse, ok := msg.(*pgproto3.Parse); ok && parse.Name != "" {
					parsed.sql[parse.Query] = true
					parsed.names[parse.Name] = true
				}
			}
		}
		t.Run(tc.name+"/direct", func(t *testing.T) { tc.run(t, backend.URL("postgres")) })
		t.Run(tc.name+"/proxy", func(t *testing.T) {
			defer func() {
				// A known bug or a failure may leave state on the backend; keep it from later cases.
				if t.Failed() || tc.proxySkip != "" || tc.hygieneSkip != "" {
					terminatePooledBackend(t)
				}
			}()
			url := proxy.URL(dbPooled, itest.PostgresUser)
			requireKnownBug(t, "proxySkip", tc.proxySkip, func(t testing.TB) { tc.run(t, url) })
			// A new direct session is always clean, so only the proxy run is checked.
			t.Run("hygiene", func(t *testing.T) {
				requireKnownBug(t, "hygieneSkip", tc.hygieneSkip, func(t testing.TB) { requireCleanBackend(t, url, parsed) })
			})
		})
	}
}

// requireKnownBug runs f, a part of a case. If reason is empty, or
// PG_PROXY_ITEST_RUN_KNOWN_BUGS is set, f runs as t. Otherwise reason names a
// known proxy bug that makes f fail: if f fails, the test is skipped with
// reason; if f passes, the test fails, so the change that fixes the bug also
// removes the case's field, proxySkip or hygieneSkip. Any failure of f counts,
// not only the one reason describes.
func requireKnownBug(t *testing.T, field, reason string, f func(t testing.TB)) {
	t.Helper()
	if reason == "" || runKnownBugs {
		f(t)
		return
	}
	if !fails(t, f) {
		t.Fatalf("passes, so the bug its %s names looks fixed; remove the %s: %s", field, field, reason)
	}
	t.Skip(reason)
}

// fails runs f with a testing.TB that records the failures f reports instead
// of failing t, and reports whether f failed.
func fails(t *testing.T, f func(t testing.TB)) bool {
	recorder := &failureRecorder{TB: t}
	var wg sync.WaitGroup
	// FailNow ends the goroutine that calls it, so f gets one of its own.
	wg.Go(func() { f(recorder) })
	wg.Wait()
	return recorder.failed
}

// failureRecorder records the failures reported to it and passes every other
// call to the test it wraps.
type failureRecorder struct {
	testing.TB
	failed bool
}

func (r *failureRecorder) Fail()                 { r.failed = true }
func (r *failureRecorder) Failed() bool          { return r.failed }
func (r *failureRecorder) FailNow()              { r.Fail(); runtime.Goexit() }
func (r *failureRecorder) Error(...any)          { r.Fail() }
func (r *failureRecorder) Errorf(string, ...any) { r.Fail() }
func (r *failureRecorder) Fatal(...any)          { r.FailNow() }
func (r *failureRecorder) Fatalf(string, ...any) { r.FailNow() }

// run runs the case's setup, sends its steps to url and checks the replies.
func (tc protocolCase) run(t testing.TB, url string) {
	t.Helper()
	if len(tc.setup) > 0 {
		require.NoError(t, backend.Exec(t.Context(), tc.setup...))
	}
	tc.check(t, exchange(t, url, tc.steps, tc.stepTimeout))
}

// clientStatements are the statements the clients of a series of cases parse
// with a name.
type clientStatements struct {
	// sql holds their SQL. The proxy keeps each such statement on the backend
	// under a name of its own, for reuse by any client.
	sql map[string]bool
	// names holds the names the clients give them, which belong to the
	// clients' sessions and must not outlive them on the backend.
	names map[string]bool
}

// requireCleanBackend connects a new client to url, the proxy's dbPooled, and
// checks that its backend holds nothing a previous client left behind. The
// client lands on the backend the case used, because dbPooled has a single
// backend connection; if the proxy replaced that connection, the state on it
// is gone with it. The client must:
//   - get its own replies in the simple and the extended protocol, so no
//     reply to a previous client is left over;
//   - find no LISTEN, session advisory lock, temporary relation or cursor;
//   - find only prepared statements whose SQL a client parsed with a name,
//     and none under a client's name;
//   - see the server's value of every parameter ParameterStatus reported.
func requireCleanBackend(t testing.TB, url string, parsed clientStatements) {
	t.Helper()
	c := connectRaw(t, url)
	defer c.close()

	requireReplies(t, [][]pgproto3.BackendMessage{
		c.do(t, simpleQuery("SELECT 'hygiene marker'")),
		c.do(t, ready(
			&pgproto3.Parse{Query: "SELECT $1::text"}, &pgproto3.Bind{Parameters: [][]byte{[]byte("hygiene marker 2")}},
			&pgproto3.Execute{}, &pgproto3.Sync{},
		)),
	},
		[]string{"RowDescription", "DataRow hygiene marker", "CommandComplete SELECT 1", "ReadyForQuery I"},
		[]string{"ParseComplete", "BindComplete", "DataRow hygiene marker 2", "CommandComplete SELECT 1", "ReadyForQuery I"},
	)

	// Each probe counts session state that outlives the transactions that
	// created it, and so must not reach another client.
	for _, probe := range []struct{ query, state string }{
		{"SELECT count(*) FROM pg_listening_channels()", "LISTEN channels"},
		{"SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND pid = pg_backend_pid()", "session advisory locks"},
		{"SELECT count(*) FROM pg_class WHERE relnamespace = pg_my_temp_schema()", "temporary relations"},
		{"SELECT count(*) FROM pg_cursors", "cursors"},
	} {
		assert.Equal(t, "0", dataRowValue(t, c.do(t, simpleQuery(probe.query))),
			"the backend holds %s", probe.state)
	}

	reply := c.do(t, simpleQuery("SELECT name, statement, from_sql FROM pg_prepared_statements"))
	requireNoError(t, reply)
	for _, msg := range reply {
		if row, ok := msg.(*pgproto3.DataRow); ok {
			name, statement := string(row.Values[0]), string(row.Values[1])
			switch {
			case string(row.Values[2]) == "t":
				t.Errorf("the backend holds a statement prepared with SQL PREPARE: %s", statement)
			case parsed.names[name]:
				t.Errorf("the backend holds a statement under a client's name %q: %s", name, statement)
			case !parsed.sql[statement]:
				t.Errorf("the backend holds a statement no client parsed with a name: %s", statement)
			}
		}
	}

	requireParameterView(t, c)
}

// requireParameterView checks that each parameter ParameterStatus reported to
// c has the value the server has.
func requireParameterView(t testing.TB, c *rawClient) {
	t.Helper()
	names := slices.Sorted(maps.Keys(c.parameters))
	settings := make([]string, len(names))
	for i, name := range names {
		settings[i] = "current_setting('" + strings.ReplaceAll(name, "'", "''") + "')"
	}
	reply := c.do(t, simpleQuery("SELECT "+strings.Join(settings, ", ")))
	requireNoError(t, reply)
	var values [][]byte
	for _, msg := range reply {
		if row, ok := msg.(*pgproto3.DataRow); ok {
			values = row.Values
		}
	}
	require.Len(t, values, len(names), "%v", summarize(reply))
	for i, name := range names {
		assert.Equal(t, string(values[i]), c.parameters[name], "the client's view of %s", name)
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
	before func(t testing.TB)
}

func ready(msgs ...pgproto3.FrontendMessage) step   { return step{send: msgs, until: untilReady} }
func failing(msgs ...pgproto3.FrontendMessage) step { return step{send: msgs, until: untilError} }
func quiet(msgs ...pgproto3.FrontendMessage) step   { return step{send: msgs, until: untilQuiet} }
func copyIn(msgs ...pgproto3.FrontendMessage) step  { return step{send: msgs, until: untilCopyIn} }

// simpleQuery returns a step that sends sql as a simple Query.
func simpleQuery(sql string) step { return ready(&pgproto3.Query{String: sql}) }

// onClient returns s sent by client instead of client 0.
func onClient(client int, s step) step {
	s.client = client
	return s
}

// exchange opens one connection to url per client the steps name, sends each
// step's messages on its client's connection and collects its replies. It
// waits up to timeout for each step's replies; zero means 20 seconds. It
// closes the connections before it returns.
func exchange(t testing.TB, url string, steps []step, timeout time.Duration) [][]pgproto3.BackendMessage {
	t.Helper()
	count := 1
	for _, step := range steps {
		count = max(count, step.client+1)
	}
	clients := make([]*rawClient, count)
	for i := range clients {
		clients[i] = connectRaw(t, url)
		defer clients[i].close()
		if timeout != 0 {
			clients[i].timeout = timeout
		}
	}

	var replies [][]pgproto3.BackendMessage
	for _, step := range steps {
		if step.before != nil {
			step.before(t)
		}
		replies = append(replies, clients[step.client].do(t, step))
	}
	return replies
}

// rawClient is a connection on which the test sends and reads protocol
// messages itself.
type rawClient struct {
	conn     net.Conn
	frontend *pgproto3.Frontend
	// parameters is the client's view of the server parameters: the last
	// value ParameterStatus reported for each.
	parameters map[string]string
	// closed is set once the server has sent an error that ends the session
	// and closed the connection.
	closed bool
	// timeout bounds the wait for each step's replies.
	timeout time.Duration
}

// connectRaw opens a connection to url and takes it over from pgconn after
// startup. Its steps wait up to 20 seconds for their replies.
func connectRaw(t testing.TB, url string) *rawClient {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	pgConn, err := pgconn.Connect(ctx, url)
	require.NoError(t, err)
	hijacked, err := pgConn.Hijack()
	require.NoError(t, err)
	return &rawClient{
		conn: hijacked.Conn, frontend: hijacked.Frontend, parameters: hijacked.ParameterStatuses, timeout: 20 * time.Second,
	}
}

// do sends the step's messages and reads their replies for up to c.timeout.
// An error that ends the session ends the replies, and after it do sends
// nothing and returns no replies.
func (c *rawClient) do(t testing.TB, step step) []pgproto3.BackendMessage {
	t.Helper()
	if c.closed {
		return nil
	}
	require.NoError(t, c.conn.SetDeadline(time.Now().Add(c.timeout)))
	for _, msg := range step.send {
		c.frontend.Send(msg)
	}
	require.NoError(t, c.frontend.Flush())
	if step.until == untilQuiet {
		require.NoError(t, c.conn.SetReadDeadline(time.Now().Add(500*time.Millisecond)))
	}
	var reply []pgproto3.BackendMessage
	for {
		msg, err := c.frontend.Receive()
		var netErr net.Error
		if step.until == untilQuiet && errors.As(err, &netErr) && netErr.Timeout() {
			break
		}
		require.NoError(t, err, "replies so far: %v", summarize(reply))
		msg = copyBackendMessage(t, msg)
		if status, ok := msg.(*pgproto3.ParameterStatus); ok {
			c.parameters[status.Name] = status.Value
		}
		reply = append(reply, msg)
		if errorResponse, ok := msg.(*pgproto3.ErrorResponse); ok && endsSession(errorResponse) {
			// The server closes the connection after it.
			c.closed = true
			break
		}
		_, isReady := msg.(*pgproto3.ReadyForQuery)
		_, isError := msg.(*pgproto3.ErrorResponse)
		_, isCopyIn := msg.(*pgproto3.CopyInResponse)
		if (step.until == untilReady && isReady) || (step.until == untilError && isError) ||
			(step.until == untilCopyIn && (isCopyIn || isError)) {
			break
		}
	}
	return reply
}

// endsSession reports whether the server closes the connection after
// errorResponse: its severity is FATAL or PANIC.
func endsSession(errorResponse *pgproto3.ErrorResponse) bool {
	// The localized severity depends on lc_messages. The proxy's own errors,
	// and servers before PostgreSQL 9.6, send no other.
	severity := errorResponse.SeverityUnlocalized
	if severity == "" {
		severity = errorResponse.Severity
	}
	return severity == "FATAL" || severity == "PANIC"
}

// close sends Terminate and closes the connection.
func (c *rawClient) close() {
	if !c.closed {
		_ = c.conn.SetDeadline(time.Now().Add(time.Second))
		c.frontend.Send(&pgproto3.Terminate{})
		_ = c.frontend.Flush()
	}
	_ = c.conn.Close()
}

// copyBackendMessage detaches msg from the frontend's reused messages and
// buffers, such as the single ReadyForQuery it decodes every one into.
func copyBackendMessage(t testing.TB, msg pgproto3.BackendMessage) pgproto3.BackendMessage {
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

// requireReplies checks the summary of each reply and reports every reply
// that differs.
func requireReplies(t testing.TB, replies [][]pgproto3.BackendMessage, want ...[]string) {
	t.Helper()
	require.Len(t, replies, len(want))
	equal := true
	for i := range want {
		equal = assert.Equal(t, want[i], summarize(replies[i]), "reply %d", i) && equal
	}
	if !equal {
		t.FailNow()
	}
}

func requireNoError(t testing.TB, reply []pgproto3.BackendMessage) {
	t.Helper()
	for _, msg := range reply {
		if errorResponse, ok := msg.(*pgproto3.ErrorResponse); ok {
			t.Fatalf("unexpected error %s: %s", errorResponse.Code, errorResponse.Message)
		}
	}
}

func requireErrorCode(t testing.TB, reply []pgproto3.BackendMessage, code string) {
	t.Helper()
	for _, msg := range reply {
		if errorResponse, ok := msg.(*pgproto3.ErrorResponse); ok {
			require.Equal(t, code, errorResponse.Code, errorResponse.Message)
			return
		}
	}
	t.Fatalf("expected error %s, got %#v", code, reply)
}

func dataRowValue(t testing.TB, reply []pgproto3.BackendMessage) string {
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
