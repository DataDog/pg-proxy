// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package session

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/DataDog/pg-proxy/pkg/link"
	"github.com/DataDog/pg-proxy/pkg/topology"
)

// The cost budget tests drive complete client protocol cycles through a
// Session, a link.New link on a pooled pgx connection, and a scripted Postgres.
// Both sockets are in-memory cycleConns served on the calling goroutine, so a
// cycle's allocations and socket writes are deterministic. The cycle includes
// the link checkout, the release reset, and the pool round trip, but not the
// client liveness monitor or a backend adapter's own metric tags.

var (
	errScriptExhausted = errors.New("cycleConn: read past the scripted bytes")
	errPartialMessage  = errors.New("cycleConn: a write ends inside a message; the harness counts whole messages only")
)

// cycleConn is a net.Conn that returns scripted bytes and counts the writes made to it.
// Each Write is one socket write in production.
type cycleConn struct {
	in        []byte // bytes the next Reads return
	readFrame int    // bytes left in the message the last Read started
	writes    int
	// Postgres side only, counted once frames is set after the startup handshake:
	frames     bool
	inReset    bool   // set by resetTracer while the release reset runs
	flushes    int    // writes of forwarded messages
	resets     int    // writes of the release reset
	statements int    // forwarded simple Query and Execute messages
	lastWrite  []byte // aliases the writer's buffer, which holds it until the next write
	err        error  // the first read past the script
}

// messageSize returns the size of the message that b starts with, or 0 if b
// is shorter than a message header.
func messageSize(b []byte) int {
	if len(b) < 5 {
		return 0
	}
	return 1 + int(binary.BigEndian.Uint32(b[1:5]))
}

// Read returns at most the rest of one message. The pgproto3 readers then
// buffer no message that their session or connection has not asked for, so
// bytes a cycle does not consume stay in in, where run finds them.
func (c *cycleConn) Read(p []byte) (int, error) {
	if len(c.in) == 0 {
		if !c.frames {
			return 0, errScriptExhausted
		}
		if c.err == nil {
			c.err = fmt.Errorf("%w: the script has no reply to the last write to Postgres: %s",
				errScriptExhausted, frontendMessages(c.lastWrite))
		}
		return 0, c.err
	}
	if c.readFrame == 0 {
		c.readFrame = messageSize(c.in)
		if c.readFrame == 0 || c.readFrame > len(c.in) {
			c.readFrame = len(c.in)
		}
	}
	n := copy(p[:min(len(p), c.readFrame)], c.in)
	c.in, c.readFrame = c.in[n:], c.readFrame-n
	return n, nil
}

func (c *cycleConn) Write(p []byte) (int, error) {
	c.writes++
	if !c.frames {
		return len(p), nil
	}
	c.lastWrite = p
	if c.inReset {
		c.resets++
		return len(p), nil
	}
	statements := 0
	for msg := p; len(msg) > 0; {
		size := messageSize(msg)
		if size == 0 || size > len(msg) {
			return 0, errPartialMessage
		}
		switch msg[0] {
		case 'X': // Terminate, sent when the pool closes the connection
			return len(p), nil
		case 'Q', 'E':
			statements++
		}
		msg = msg[size:]
	}
	c.flushes++
	c.statements += statements
	return len(p), nil
}

func (*cycleConn) Close() error                     { return nil }
func (*cycleConn) LocalAddr() net.Addr              { return dummyAddr{} }
func (*cycleConn) RemoteAddr() net.Addr             { return dummyAddr{} }
func (*cycleConn) SetDeadline(time.Time) error      { return nil }
func (*cycleConn) SetReadDeadline(time.Time) error  { return nil }
func (*cycleConn) SetWriteDeadline(time.Time) error { return nil }

// frontendMessages formats the messages that b encodes.
func frontendMessages(b []byte) string {
	backend := pgproto3.NewBackend(bytes.NewReader(b), io.Discard)
	var msgs []string
	for {
		msg, err := backend.Receive()
		if err != nil {
			return strings.Join(msgs, ", ")
		}
		msgs = append(msgs, fmt.Sprintf("%#v", msg))
	}
}

func (c *cycleConn) resetCounts() {
	c.writes, c.flushes, c.resets, c.statements = 0, 0, 0, 0
}

// resetTracer marks the scripted Postgres's writes during a pgx query as the
// release reset. In a cycle, only SessionLink.Release runs a query through pgx;
// the session forwards client messages through the connection's Frontend.
type resetTracer struct{ postgres *cycleConn }

func (t resetTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	t.postgres.inReset = true
	return ctx
}

func (t resetTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {
	t.postgres.inReset = false
}

// countingRoleSession manages the role without SQL and counts its calls.
type countingRoleSession struct{ sets, resets int }

func (r *countingRoleSession) Set(context.Context, *pgxpool.Conn, []string) (string, error) {
	r.sets++
	return "test-token", nil
}

func (r *countingRoleSession) Reset(context.Context, *pgxpool.Conn, string, []string) error {
	r.resets++
	return nil
}

type cycleHarness struct {
	postgres   *cycleConn
	pool       *pgxpool.Pool
	backend    *namedBackend
	role       countingRoleSession // its counts stay 0 without role management
	resetReply []byte
}

// newCycleHarness connects a one-connection pool to a scripted Postgres. With
// withRole, links use a counting RoleSession, so Release resets only GUCs;
// without one, it also resets the session authorization and role.
func newCycleHarness(tb testing.TB, withRole bool) *cycleHarness {
	tb.Helper()
	h := &cycleHarness{postgres: &cycleConn{in: encodeMessages[pgproto3.BackendMessage](tb,
		&pgproto3.AuthenticationOk{},
		&pgproto3.BackendKeyData{ProcessID: 1, SecretKey: []byte{0, 0, 0, 2}},
		cycleReadyForQuery(),
	)}}
	reset := &pgproto3.CommandComplete{CommandTag: []byte("RESET")}
	// RESET SESSION AUTHORIZATION; RESET ROLE; RESET ALL
	resetReplies := []pgproto3.BackendMessage{reset, reset, reset, cycleReadyForQuery()}
	var role link.RoleSession
	if withRole {
		role, resetReplies = &h.role, resetReplies[2:] // RESET ALL
	}
	h.resetReply = encodeMessages(tb, resetReplies...)
	// Checkouts follow the production backend adapter.
	h.backend = &namedBackend{name: "cycle", acquire: func(ctx context.Context, request LinkRequest) (link.Link, error) {
		conn, err := h.pool.Acquire(ctx)
		if err != nil {
			return nil, err
		}
		backendLink, err := link.New(conn, link.Options{
			MaxPreparedStmts: request.MaxPreparedStmts, MetricTags: request.MetricTags, Role: role,
		})
		if err != nil {
			conn.Release()
			return nil, err
		}
		return backendLink, nil
	}}

	config, err := pgxpool.ParseConfig("postgres://app_user@127.0.0.1/app_db?sslmode=disable")
	require.NoError(tb, err)
	config.MaxConns = 1
	config.HealthCheckPeriod = time.Hour
	// A ping on checkout would need a scripted reply; production pings only idle connections.
	config.ShouldPing = func(context.Context, pgxpool.ShouldPingParams) bool { return false }
	config.ConnConfig.Tracer = resetTracer{h.postgres}
	dialed := false
	config.ConnConfig.DialFunc = func(context.Context, string, string) (net.Conn, error) {
		if dialed {
			return nil, errors.New("the scripted Postgres accepts one connection")
		}
		dialed = true
		return h.postgres, nil
	}
	h.pool, err = pgxpool.NewWithConfig(context.Background(), config)
	require.NoError(tb, err)
	tb.Cleanup(h.pool.Close)

	// Connect before counting: the startup message has no type byte to frame.
	conn, err := h.pool.Acquire(context.Background())
	require.NoError(tb, err)
	conn.Release()
	require.Empty(tb, h.postgres.in, "startup handshake left scripted bytes unread")
	h.postgres.frames = true
	h.postgres.resetCounts()
	return h
}

// cycleClient is one client connection's session on the harness backend.
type cycleClient struct {
	h       *cycleHarness
	conn    *cycleConn
	session *Session
}

func (h *cycleHarness) newClient(configure ...func(*Config)) *cycleClient {
	conn := &cycleConn{}
	cfg := Config{
		Context: context.Background(), User: "app_user", Database: "app_db", LogicalDatabase: "app_db",
		Service: "app", Client: pgproto3.NewBackend(conn, conn), ClientConn: conn, Backend: h.backend,
		QueryLoadShedding:     true, // keep allocation budgets exercising the admission path
		PostgresMsgBufferSize: topology.DefaultPostgresMessageBufferSize,
		MetricTags:            []string{"database:app_db"},
	}
	for _, apply := range configure {
		apply(&cfg)
	}
	return &cycleClient{h: h, conn: conn, session: New(cfg)}
}

// cycleScript is one protocol cycle: the client's messages and Postgres's
// replies, followed by the reply to the reset that releasing the link runs.
type cycleScript struct {
	frontend, backend []byte
}

func (h *cycleHarness) script(tb testing.TB, frontend []pgproto3.FrontendMessage, backend []pgproto3.BackendMessage) cycleScript {
	tb.Helper()
	return cycleScript{frontend: encodeMessages(tb, frontend...), backend: append(encodeMessages(tb, backend...), h.resetReply...)}
}

// run serves one cycle. It returns once the session has released its link.
func (c *cycleClient) run(script cycleScript) error {
	c.conn.in = script.frontend
	c.h.postgres.in = script.backend
	if err := c.session.receiveAndHandleMessage(); err != nil {
		return err
	}
	switch {
	case c.h.postgres.err != nil:
		return c.h.postgres.err // the session logs a failed release instead of returning it
	case c.session.link != nil:
		return errors.New("cycle ended with the link still checked out")
	case len(c.conn.in) > 0 || len(c.h.postgres.in) > 0:
		return errors.New("cycle left scripted bytes unread")
	}
	return nil
}

// cycleCounts are the socket writes and checkouts of one or more cycles.
type cycleCounts struct {
	backendWrites, backendFlushes, backendStatements, releaseResets int
	clientWrites, checkouts, roleSets, roleResets                   int
}

func (h *cycleHarness) resetCounts(client *cycleClient) {
	h.postgres.resetCounts()
	client.conn.resetCounts()
	h.backend.acquisitions.Store(0)
	h.role = countingRoleSession{}
}

func (h *cycleHarness) counts(client *cycleClient) cycleCounts {
	return cycleCounts{
		backendWrites: h.postgres.writes, backendFlushes: h.postgres.flushes, backendStatements: h.postgres.statements,
		releaseResets: h.postgres.resets, clientWrites: client.conn.writes, checkouts: int(h.backend.acquisitions.Load()),
		roleSets: h.role.sets, roleResets: h.role.resets,
	}
}

func encodeMessages[M pgproto3.Message](tb testing.TB, msgs ...M) []byte {
	tb.Helper()
	var encoded []byte
	for _, msg := range msgs {
		var err error
		encoded, err = msg.Encode(encoded)
		require.NoError(tb, err)
	}
	return encoded
}

// Synthetic workload shapes: the extended statement has 600 B of SQL and a 400 B
// Bind with four text parameters.
const (
	cycleSQLSize  = 600
	cycleBindSize = 400 // encoded Bind, including the 5-byte message header
	cycleStmtName = "users_by_id"
)

var (
	cycleExtendedSQL = syntheticSelect("id = $1 AND tenant_id = $2 AND status = $3 AND created_at > $4", cycleSQLSize)
	cycleSimpleSQL   = syntheticSelect("id = 42 AND tenant_id = 7 AND status = 'active' AND created_at > '2026-01-01'", cycleSQLSize)
	cycleParamOIDs   = []uint32{20, 20, 25, 1184}
)

// syntheticSelect returns a query of exactly size bytes against an example table.
func syntheticSelect(where string, size int) string {
	sql := "SELECT id, name, email FROM users WHERE " + where + " /* "
	return sql + strings.Repeat("x", size-len(sql)-len(" */")) + " */"
}

// cycleBind returns a text-format Bind of cycleBindSize encoded bytes.
func cycleBind(statement string) *pgproto3.Bind {
	// The header is 5 bytes, the two names end in a zero byte, the three counts
	// take 2 bytes each, and each parameter has a 4-byte length.
	payload := cycleBindSize - 5 - 2 - len(statement) - 6 - 4*len(cycleParamOIDs)
	params := make([][]byte, len(cycleParamOIDs))
	for i := range params {
		params[i] = bytes.Repeat([]byte{'7'}, (payload+i)/len(params)) // the sizes sum to payload
	}
	return &pgproto3.Bind{PreparedStatement: statement, Parameters: params}
}

// cycleRowReplies is what Postgres returns for Describe(P) and Execute of one row.
func cycleRowReplies() []pgproto3.BackendMessage {
	return []pgproto3.BackendMessage{
		&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
			{Name: []byte("id"), DataTypeOID: 20, DataTypeSize: 8, TypeModifier: -1},
			{Name: []byte("name"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1},
			{Name: []byte("email"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1},
		}},
		&pgproto3.DataRow{Values: [][]byte{[]byte("42"), []byte("Example User"), []byte("user@example.com")}},
		&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")},
	}
}

func cycleReadyForQuery() pgproto3.BackendMessage { return &pgproto3.ReadyForQuery{TxStatus: 'I'} }

// protocolCycle is one measured cycle.
type protocolCycle struct {
	name       string
	statements int  // client statements per cycle
	prepared   bool // a first cycle prepares the named statement that this one reuses
	cycle      func(tb testing.TB, h *cycleHarness) cycleScript
}

func oneShotExtendedScript(tb testing.TB, h *cycleHarness, statements int) cycleScript {
	var frontend []pgproto3.FrontendMessage
	var backend []pgproto3.BackendMessage
	for range statements {
		frontend = append(frontend,
			&pgproto3.Parse{Query: cycleExtendedSQL, ParameterOIDs: cycleParamOIDs},
			cycleBind(""), &pgproto3.Describe{ObjectType: 'P'}, &pgproto3.Execute{})
		backend = append(append(backend, &pgproto3.ParseComplete{}, &pgproto3.BindComplete{}), cycleRowReplies()...)
	}
	return h.script(tb, append(frontend, &pgproto3.Sync{}), append(backend, cycleReadyForQuery()))
}

// namedStatementScript executes the cycleStmtName statement once. With parse,
// the client sends its Parse first. With parseComplete, Postgres answers that
// Parse, which it does unless the backend connection already has the statement.
func namedStatementScript(tb testing.TB, h *cycleHarness, parse, parseComplete bool) cycleScript {
	frontend := []pgproto3.FrontendMessage{
		cycleBind(cycleStmtName), &pgproto3.Describe{ObjectType: 'P'}, &pgproto3.Execute{}, &pgproto3.Sync{},
	}
	if parse {
		parseMsg := &pgproto3.Parse{Name: cycleStmtName, Query: cycleExtendedSQL, ParameterOIDs: cycleParamOIDs}
		frontend = append([]pgproto3.FrontendMessage{parseMsg}, frontend...)
	}
	backend := append([]pgproto3.BackendMessage{&pgproto3.BindComplete{}}, cycleRowReplies()...)
	if parseComplete {
		backend = append([]pgproto3.BackendMessage{&pgproto3.ParseComplete{}}, backend...)
	}
	return h.script(tb, frontend, append(backend, cycleReadyForQuery()))
}

var protocolCycles = []protocolCycle{
	{
		name:       "one_shot_extended",
		statements: 1,
		cycle:      func(tb testing.TB, h *cycleHarness) cycleScript { return oneShotExtendedScript(tb, h, 1) },
	},
	{
		name:       "simple_query",
		statements: 1,
		cycle: func(tb testing.TB, h *cycleHarness) cycleScript {
			return h.script(tb,
				[]pgproto3.FrontendMessage{&pgproto3.Query{String: cycleSimpleSQL}},
				append(cycleRowReplies(), cycleReadyForQuery()))
		},
	},
	{
		name:       "named_statement_reuse",
		statements: 1,
		prepared:   true,
		cycle: func(tb testing.TB, h *cycleHarness) cycleScript {
			return namedStatementScript(tb, h, false, false) // Bind only
		},
	},
	{
		name:       "pipeline_30",
		statements: 30,
		cycle:      func(tb testing.TB, h *cycleHarness) cycleScript { return oneShotExtendedScript(tb, h, 30) },
	},
}

// prepare returns a client ready for the cycle, and the cycle's script.
func (pc protocolCycle) prepare(tb testing.TB, h *cycleHarness) (*cycleClient, cycleScript) {
	tb.Helper()
	client := h.newClient()
	if pc.prepared {
		require.NoError(tb, client.run(namedStatementScript(tb, h, true, true)))
	}
	return client, pc.cycle(tb, h)
}

func TestProtocolCycleShapes(t *testing.T) {
	require.Len(t, cycleExtendedSQL, cycleSQLSize)
	require.Len(t, cycleSimpleSQL, cycleSQLSize)
	for _, statement := range []string{"", cycleStmtName} {
		require.Len(t, encodeMessages(t, cycleBind(statement)), cycleBindSize)
	}
}
