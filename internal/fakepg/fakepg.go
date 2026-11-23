// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

// Package fakepg is an in-process stand-in for a PostgreSQL server, for unit tests
// and benchmarks that need a real pgx connection without a database.
//
// A Conn plays one end of a connection on the caller's goroutine: its Reads return
// queued bytes, and as a server it answers each message written to it before Write
// returns. No goroutine serves it, so a test's allocation and socket-write counts
// are exact, and a reply that never comes fails the read after ReplyWait instead of
// hanging the test.
package fakepg

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DefaultReplyWait is how long a Read waits for bytes when a Conn's ReplyWait is 0.
const DefaultReplyWait = 5 * time.Second

// describedWrite is how much of a server write a missing-reply error can name. A
// Conn copies that much of each write, since the writer reuses its buffer, but no
// more, so a large message is not copied whole.
const describedWrite = 4 << 10

var (
	// ErrExhausted is returned by a Read that finds nothing queued: at once on a
	// client Conn, after ReplyWait on a server Conn. A server Conn's error also
	// names the messages of the last write, which got no reply.
	ErrExhausted = errors.New("fakepg: read with no reply queued")
	// ErrPartialMessage is returned by a server Write that ends inside a message.
	ErrPartialMessage = errors.New("fakepg: a write ends inside a message")
)

// Conn is an in-memory net.Conn. The zero value is a client end: it returns queued
// bytes and counts writes. NewServer returns the Postgres end.
type Conn struct {
	// ReplyWait bounds how long a server Read with nothing queued waits for bytes.
	// Set it before first use; 0 means DefaultReplyWait. A client Read with nothing
	// queued fails at once.
	//
	// A Read normally finds its reply queued, since the Write that asked for it has
	// already returned. But pgx starts a background reader when a write takes over
	// 15 ms, as on a loaded machine, and that reader can call Read before the Write
	// it awaits, or hold a Read open after the reply until the next request. Failing
	// that Read would break the conn, so a server Read waits instead.
	ReplyWait time.Duration
	// ServerVersion, if set before first use, is reported to the client as the
	// server_version parameter in the startup handshake.
	ServerVersion string

	// mu guards the fields below.
	mu        sync.Mutex
	ready     sync.Cond // broadcast when bytes are queued or the Conn closes
	in        []byte    // bytes the next Reads return
	joined    []byte    // reused to join queued bytes, so no caller's slice is written
	readFrame int       // bytes left in the message the last Read started
	writes    int
	server    bool
	started   bool // the startup message has been answered
	closed    bool
	respond   func(msg []byte) []byte // nil: replies are queued by the caller
	lastWrite []byte                  // the start of the last server write after the startup message
	missed    error                   // the first server Read that found no reply
}

// NewServer returns the Postgres end of a connection. It answers the startup
// handshake itself. After it, each message written to it is passed whole to
// respond, if set, and the reply respond returns is queued for the next Read.
// respond runs inside Write and must not call the Conn's methods.
func NewServer(respond func(msg []byte) []byte) *Conn {
	return &Conn{server: true, respond: respond}
}

// messageSize returns the size of the message b starts with, or 0 if b is shorter
// than a message header.
func messageSize(b []byte) int {
	if len(b) < 5 {
		return 0
	}
	return 1 + int(binary.BigEndian.Uint32(b[1:5]))
}

// Queue adds b to the bytes the next Reads return. The Conn reads b in place and
// never writes to it, so a script encoded once can be queued for every cycle.
func (c *Conn) Queue(b []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queue(b)
}

func (c *Conn) queue(b []byte) {
	if len(c.in) == 0 {
		c.in = b
	} else {
		c.joined = append(append(c.joined[:0], c.in...), b...)
		c.in = c.joined
	}
	c.ready.Broadcast()
}

// waitLocked waits, with mu held, until bytes are queued, the Conn closes, or
// ReplyWait passes.
func (c *Conn) waitLocked() {
	if c.ready.L == nil {
		c.ready.L = &c.mu
	}
	wait := c.ReplyWait
	if wait == 0 {
		wait = DefaultReplyWait
	}
	expired := false
	timer := time.AfterFunc(wait, func() {
		c.mu.Lock()
		expired = true
		c.mu.Unlock()
		c.ready.Broadcast()
	})
	defer timer.Stop()
	for len(c.in) == 0 && !c.closed && !expired {
		c.ready.Wait()
	}
}

// Pending returns the number of queued bytes no Read has returned yet.
func (c *Conn) Pending() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.in)
}

// Writes returns the number of Write calls since the last ResetWrites. Each is one
// socket write in production.
func (c *Conn) Writes() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writes
}

// ResetWrites zeroes the write count.
func (c *Conn) ResetWrites() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writes = 0
}

// Err returns the error of the first server Read that found no reply, or nil. It
// lets a test fail on a read error its code under test only logs, as a session
// does when a release fails.
func (c *Conn) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.missed
}

// Read returns at most the rest of one message, so the pgproto3 readers never
// buffer a message their caller has not asked for: bytes a test does not consume
// stay queued, where Pending finds them.
func (c *Conn) Read(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.in) == 0 && !c.closed && c.server {
		c.waitLocked()
	}
	switch {
	case len(c.in) > 0:
	case c.closed:
		return 0, io.EOF
	case !c.server:
		return 0, ErrExhausted
	default:
		err := fmt.Errorf("%w; the last write was %s", ErrExhausted, frontendMessages(c.lastWrite))
		if c.missed == nil {
			c.missed = err
		}
		return 0, err
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

func (c *Conn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writes++
	if !c.server {
		return len(p), nil
	}
	if !c.started {
		c.started = true // the startup message, which has no type byte
		reply := handshake
		if c.ServerVersion != "" {
			reply = mustEncode(&pgproto3.AuthenticationOk{},
				&pgproto3.ParameterStatus{Name: "server_version", Value: c.ServerVersion}, backendKeyData, readyForQuery)
		}
		c.queue(reply)
		return len(p), nil
	}
	c.lastWrite = append(c.lastWrite[:0], p[:min(len(p), describedWrite)]...)
	for msg := p; len(msg) > 0; {
		size := messageSize(msg)
		if size == 0 || size > len(msg) {
			return 0, ErrPartialMessage
		}
		if c.respond != nil {
			if reply := c.respond(msg[:size]); len(reply) > 0 {
				c.queue(reply)
			}
		}
		msg = msg[size:]
	}
	return len(p), nil
}

// Close wakes a waiting Read, which then returns what is queued, then io.EOF.
func (c *Conn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	c.ready.Broadcast()
	return nil
}

func (*Conn) LocalAddr() net.Addr              { return addr{} }
func (*Conn) RemoteAddr() net.Addr             { return addr{} }
func (*Conn) SetDeadline(time.Time) error      { return nil }
func (*Conn) SetReadDeadline(time.Time) error  { return nil }
func (*Conn) SetWriteDeadline(time.Time) error { return nil }

type addr struct{}

func (addr) Network() string { return "fakepg" }
func (addr) String() string  { return "fakepg" }

// Encode returns the wire encoding of msgs.
func Encode[M pgproto3.Message](tb testing.TB, msgs ...M) []byte {
	tb.Helper()
	var encoded []byte
	for _, msg := range msgs {
		var err error
		if encoded, err = msg.Encode(encoded); err != nil {
			tb.Fatal(err)
		}
	}
	return encoded
}

// Decode returns the frontend message msg encodes. Unlike Answer it allocates, so
// budgeted tests should not call it.
func Decode(msg []byte) (pgproto3.FrontendMessage, error) {
	return pgproto3.NewBackend(bytes.NewReader(msg), nil).Receive()
}

// frontendMessages formats the frontend messages b encodes, for example
// &pgproto3.Query{String:"SELECT 1"}.
func frontendMessages(b []byte) string {
	var msgs []string
	for len(b) > 0 {
		if size := messageSize(b); size > 0 && size <= len(b) {
			if msg, err := Decode(b[:size]); err == nil {
				msgs = append(msgs, fmt.Sprintf("%#v", msg))
				b = b[size:]
				continue
			}
		}
		// Cut off at describedWrite, or of a type pgproto3 does not decode.
		msgs = append(msgs, fmt.Sprintf("a %q message", b[0]))
		break
	}
	return strings.Join(msgs, ", ")
}

func mustEncode(msgs ...pgproto3.BackendMessage) []byte {
	var encoded []byte
	for _, msg := range msgs {
		var err error
		if encoded, err = msg.Encode(encoded); err != nil {
			panic(err)
		}
	}
	return encoded
}

var (
	readyForQuery  = &pgproto3.ReadyForQuery{TxStatus: 'I'}
	backendKeyData = &pgproto3.BackendKeyData{ProcessID: 1, SecretKey: []byte{0, 0, 0, 2}}
	handshake      = mustEncode(&pgproto3.AuthenticationOk{}, backendKeyData, readyForQuery)
	parseComplete  = mustEncode(&pgproto3.ParseComplete{})
	bindComplete   = mustEncode(&pgproto3.BindComplete{})
	describeStmt   = mustEncode(&pgproto3.ParameterDescription{ParameterOIDs: []uint32{23}}, &pgproto3.NoData{})
	noData         = mustEncode(&pgproto3.NoData{})
	executed       = mustEncode(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 0")})
	closeComplete  = mustEncode(&pgproto3.CloseComplete{})
	synced         = mustEncode(readyForQuery)
	queried        = mustEncode(&pgproto3.CommandComplete{CommandTag: []byte("RESET")}, readyForQuery)
)

// Answer replies to msg as an idle server with empty results: the extended query
// protocol, statement and portal Close, and simple queries such as RESET ALL. Its
// replies are encoded once, so it allocates nothing.
func Answer(msg []byte) []byte {
	switch msg[0] {
	case 'P':
		return parseComplete
	case 'B':
		return bindComplete
	case 'D':
		if len(msg) > 5 && msg[5] == 'S' {
			return describeStmt
		}
		return noData
	case 'E':
		return executed
	case 'C':
		return closeComplete
	case 'S':
		return synced
	case 'Q':
		return queried
	}
	return nil // Flush and Terminate get no reply
}

// NewPool returns a pool of at most one connection, to server, a Conn from
// NewServer. A second dial fails, so a test whose conn is destroyed cannot silently
// get a fresh one. configure, if set, adjusts the pool config first, for example to
// add a tracer. The pool closes at cleanup, and Close waits for acquired conns, so a
// test must release them in cleanups registered after NewPool.
func NewPool(tb testing.TB, server *Conn, configure ...func(*pgxpool.Config)) *pgxpool.Pool {
	tb.Helper()
	config, err := pgxpool.ParseConfig("postgres://app_user@127.0.0.1/app_db?sslmode=disable")
	if err != nil {
		tb.Fatal(err)
	}
	config.MaxConns = 1
	config.HealthCheckPeriod = time.Hour
	// A ping on checkout would need a reply; production pings only idle connections.
	config.ShouldPing = func(context.Context, pgxpool.ShouldPingParams) bool { return false }
	dialed := false
	config.ConnConfig.DialFunc = func(context.Context, string, string) (net.Conn, error) {
		if dialed {
			return nil, errors.New("fakepg: the server accepts one connection")
		}
		dialed = true
		return server, nil
	}
	for _, apply := range configure {
		apply(config)
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(pool.Close)
	return pool
}
