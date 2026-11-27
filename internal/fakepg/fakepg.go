// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

// Package fakepg is an in-process stand-in for a PostgreSQL server, for unit tests
// and benchmarks that need a real pgx connection without a database.
//
// A Conn plays the server end of a connection on the caller's goroutine: it answers
// each message written to it before Write returns, and its Reads return the queued
// replies. No goroutine serves it, so a test's allocation counts are exact, and a
// reply that never comes fails the read after ReplyWait instead of hanging the test.
package fakepg

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DefaultReplyWait is how long a Read waits for bytes when a Conn's ReplyWait is 0.
const DefaultReplyWait = 5 * time.Second

var (
	// ErrExhausted is returned by a Read that finds nothing queued after ReplyWait.
	ErrExhausted = errors.New("fakepg: read with no reply queued")
	// ErrPartialMessage is returned by a Write that ends inside a message.
	ErrPartialMessage = errors.New("fakepg: a write ends inside a message")
)

// Conn is the Postgres end of an in-memory net.Conn. NewServer returns one.
type Conn struct {
	// ReplyWait bounds how long a Read with nothing queued waits for bytes. Set it
	// before first use; 0 means DefaultReplyWait.
	//
	// A Read normally finds its reply queued, since the Write that asked for it has
	// already returned. But pgx starts a background reader when a write takes over
	// 15 ms, as on a loaded machine, and that reader can call Read before the Write
	// it awaits, or hold a Read open after the reply until the next request. Failing
	// that Read would break the conn, so a Read waits instead.
	ReplyWait time.Duration

	// mu guards the fields below.
	mu        sync.Mutex
	ready     sync.Cond // broadcast when bytes are queued or the Conn closes
	in        []byte    // bytes the next Reads return
	joined    []byte    // reused to join queued bytes, so no reply's slice is written
	readFrame int       // bytes left in the message the last Read started
	started   bool      // the startup message has been answered
	closed    bool
	respond   func(msg []byte) []byte
}

// NewServer returns the Postgres end of a connection. It answers the startup
// handshake itself. After it, each message written to it is passed whole to
// respond, if set, and the reply respond returns is queued for the next Read.
// respond runs inside Write and must not call the Conn's methods.
func NewServer(respond func(msg []byte) []byte) *Conn {
	c := &Conn{respond: respond}
	c.ready.L = &c.mu
	return c
}

// messageSize returns the size of the message b starts with, or 0 if b is shorter
// than a message header.
func messageSize(b []byte) int {
	if len(b) < 5 {
		return 0
	}
	return 1 + int(binary.BigEndian.Uint32(b[1:5]))
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

// Read returns at most the rest of one message, so pgx never buffers a reply its
// caller has not asked for.
func (c *Conn) Read(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.in) == 0 && !c.closed {
		c.waitLocked()
	}
	switch {
	case len(c.in) > 0:
	case c.closed:
		return 0, io.EOF
	default:
		return 0, ErrExhausted
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
	if !c.started {
		c.started = true // the startup message, which has no type byte
		c.queue(handshake)
		return len(p), nil
	}
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
	readyForQuery = &pgproto3.ReadyForQuery{TxStatus: 'I'}
	handshake     = mustEncode(&pgproto3.AuthenticationOk{},
		&pgproto3.BackendKeyData{ProcessID: 1, SecretKey: []byte{0, 0, 0, 2}}, readyForQuery)
	parseComplete = mustEncode(&pgproto3.ParseComplete{})
	bindComplete  = mustEncode(&pgproto3.BindComplete{})
	describeStmt  = mustEncode(&pgproto3.ParameterDescription{ParameterOIDs: []uint32{23}}, &pgproto3.NoData{})
	noData        = mustEncode(&pgproto3.NoData{})
	executed      = mustEncode(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 0")})
	closeComplete = mustEncode(&pgproto3.CloseComplete{})
	synced        = mustEncode(readyForQuery)
	queried       = mustEncode(&pgproto3.CommandComplete{CommandTag: []byte("RESET")}, readyForQuery)
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
// get a fresh one. The pool closes at cleanup, and Close waits for acquired conns,
// so a test must release them in cleanups registered after NewPool.
func NewPool(tb testing.TB, server *Conn) *pgxpool.Pool {
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
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(pool.Close)
	return pool
}
