// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

// Package fakepg is an in-process stand-in for a PostgreSQL server, for unit tests
// and benchmarks that need a real pgx connection without a database.
package fakepg

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Pipe returns the two ends of an in-memory connection. Unlike net.Pipe it buffers
// writes, so both ends may write before either reads, as a proxy flushing to Postgres
// while Postgres flushes replies does. It reuses its buffers, so a benchmark through
// it measures the code under test rather than the transport. Closing either end
// closes both: reads drain what was written, then return io.EOF.
func Pipe() (net.Conn, net.Conn) {
	a, b := newBuffer(), newBuffer()
	return &end{in: a, out: b}, &end{in: b, out: a}
}

type buffer struct {
	mu     sync.Mutex
	ready  sync.Cond
	data   []byte
	closed bool
}

func newBuffer() *buffer {
	b := &buffer{}
	b.ready.L = &b.mu
	return b
}

func (b *buffer) write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return 0, io.ErrClosedPipe
	}
	b.data = append(b.data, p...)
	b.ready.Signal()
	return len(p), nil
}

func (b *buffer) read(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for len(b.data) == 0 {
		if b.closed {
			return 0, io.EOF
		}
		b.ready.Wait()
	}
	n := copy(p, b.data)
	b.data = b.data[:copy(b.data, b.data[n:])]
	return n, nil
}

func (b *buffer) close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	b.ready.Broadcast()
}

type end struct {
	in, out *buffer
}

func (e *end) Read(p []byte) (int, error)  { return e.in.read(p) }
func (e *end) Write(p []byte) (int, error) { return e.out.write(p) }

func (e *end) Close() error {
	e.in.close()
	e.out.close()
	return nil
}

func (*end) LocalAddr() net.Addr              { return addr{} }
func (*end) RemoteAddr() net.Addr             { return addr{} }
func (*end) SetDeadline(time.Time) error      { return nil }
func (*end) SetReadDeadline(time.Time) error  { return nil }
func (*end) SetWriteDeadline(time.Time) error { return nil }

type addr struct{}

func (addr) Network() string { return "fakepg" }
func (addr) String() string  { return "fakepg" }

// serve answers the startup handshake on conn, then passes each frontend message to
// handle until the connection closes or handle returns false.
func serve(conn net.Conn, handle func(*pgproto3.Backend, pgproto3.FrontendMessage) bool) {
	backend := pgproto3.NewBackend(conn, conn)
	if _, err := backend.ReceiveStartupMessage(); err != nil {
		return
	}
	backend.Send(&pgproto3.AuthenticationOk{})
	backend.Send(&pgproto3.BackendKeyData{ProcessID: 1, SecretKey: []byte{0, 0, 0, 2}})
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if backend.Flush() != nil {
		return
	}
	for {
		msg, err := backend.Receive()
		if err != nil || !handle(backend, msg) {
			return
		}
	}
}

// Replies are only encoded, never modified, so Answer shares one of each instead of
// allocating per message.
var (
	parseComplete        = &pgproto3.ParseComplete{}
	bindComplete         = &pgproto3.BindComplete{}
	parameterDescription = &pgproto3.ParameterDescription{ParameterOIDs: []uint32{23}}
	noData               = &pgproto3.NoData{}
	selectComplete       = &pgproto3.CommandComplete{CommandTag: []byte("SELECT 0")}
	closeComplete        = &pgproto3.CloseComplete{}
	resetComplete        = &pgproto3.CommandComplete{CommandTag: []byte("RESET")}
	readyForQuery        = &pgproto3.ReadyForQuery{TxStatus: 'I'}
)

// Answer replies to msg as an idle server with empty results: the extended query
// protocol, statement and portal Close, and simple queries such as RESET ALL. It
// flushes on Sync, Flush and Query, as Postgres does, and stops at Terminate.
func Answer(backend *pgproto3.Backend, msg pgproto3.FrontendMessage) bool {
	flush := false
	switch m := msg.(type) {
	case *pgproto3.Parse:
		backend.Send(parseComplete)
	case *pgproto3.Bind:
		backend.Send(bindComplete)
	case *pgproto3.Describe:
		if m.ObjectType == 'S' {
			backend.Send(parameterDescription)
		}
		backend.Send(noData)
	case *pgproto3.Execute:
		backend.Send(selectComplete)
	case *pgproto3.Close:
		backend.Send(closeComplete)
	case *pgproto3.Sync:
		backend.Send(readyForQuery)
		flush = true
	case *pgproto3.Query:
		backend.Send(resetComplete)
		backend.Send(readyForQuery)
		flush = true
	case *pgproto3.Flush:
		flush = true
	case *pgproto3.Terminate:
		return false
	}
	return !flush || backend.Flush() == nil
}

// NewPool returns a pool of at most one connection, served by a fake Postgres that
// passes each frontend message to handle (Answer, or a wrapper of it) until handle
// returns false. A second dial gets the same, by then closed, pipe, so a test whose
// conn is destroyed cannot silently get a fresh one. The pool closes at cleanup and
// Close waits for acquired conns, so a test must release them in cleanups registered
// after NewPool, or a failed assertion hangs instead of failing.
func NewPool(tb testing.TB, handle func(*pgproto3.Backend, pgproto3.FrontendMessage) bool) *pgxpool.Pool {
	tb.Helper()
	client, server := Pipe()
	tb.Cleanup(func() { _ = client.Close() })
	go serve(server, handle)
	config, err := pgxpool.ParseConfig("postgres://test@127.0.0.1/test?sslmode=disable")
	if err != nil {
		tb.Fatal(err)
	}
	config.MaxConns = 1
	config.ConnConfig.DialFunc = func(context.Context, string, string) (net.Conn, error) {
		return client, nil
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(pool.Close)
	return pool
}
