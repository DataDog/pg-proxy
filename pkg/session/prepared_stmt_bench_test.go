// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package session

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/DataDog/pg-proxy/pkg/link"
	"github.com/DataDog/pg-proxy/pkg/prepared"
)

const benchQuery = "SELECT id, name FROM users WHERE id = $1"

// serveBenchPostgres answers the extended and simple query protocol with empty results.
func serveBenchPostgres(conn net.Conn) {
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
		if err != nil {
			return
		}
		flush := false
		switch m := msg.(type) {
		case *pgproto3.Parse:
			backend.Send(&pgproto3.ParseComplete{})
		case *pgproto3.Bind:
			backend.Send(&pgproto3.BindComplete{})
		case *pgproto3.Describe:
			if m.ObjectType == 'S' {
				backend.Send(&pgproto3.ParameterDescription{ParameterOIDs: []uint32{23}})
			}
			backend.Send(&pgproto3.NoData{})
		case *pgproto3.Execute:
			backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 0")})
		case *pgproto3.Close:
			backend.Send(&pgproto3.CloseComplete{})
		case *pgproto3.Sync:
			backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			flush = true
		case *pgproto3.Query:
			backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("RESET")})
			backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			flush = true
		case *pgproto3.Flush:
			flush = true
		case *pgproto3.Terminate:
			return
		}
		if flush && backend.Flush() != nil {
			return
		}
	}
}

// benchPipeConn is one end of an in-memory duplex connection. Unlike net.Pipe it
// buffers writes, so both ends may write before either reads (the proxy flushes to
// Postgres while Postgres is still flushing replies), and unlike loopback TCP it adds
// no kernel round trip that would drown the proxy's own cost.
type benchPipeConn struct {
	in, out   chan []byte
	pending   []byte
	done      chan struct{}
	closeOnce *sync.Once
}

func newBenchPipe() (net.Conn, net.Conn) {
	a, b := make(chan []byte, 256), make(chan []byte, 256)
	done, once := make(chan struct{}), &sync.Once{}
	return &benchPipeConn{in: a, out: b, done: done, closeOnce: once},
		&benchPipeConn{in: b, out: a, done: done, closeOnce: once}
}

func (c *benchPipeConn) Read(p []byte) (int, error) {
	if len(c.pending) == 0 {
		select {
		case c.pending = <-c.in:
		case <-c.done:
			return 0, io.EOF
		}
	}
	n := copy(p, c.pending)
	c.pending = c.pending[n:]
	return n, nil
}

func (c *benchPipeConn) Write(p []byte) (int, error) {
	select {
	case c.out <- append([]byte(nil), p...):
		return len(p), nil
	case <-c.done:
		return 0, io.ErrClosedPipe
	}
}

func (c *benchPipeConn) Close() error {
	c.closeOnce.Do(func() { close(c.done) })
	return nil
}
func (*benchPipeConn) LocalAddr() net.Addr              { return dummyAddr{} }
func (*benchPipeConn) RemoteAddr() net.Addr             { return dummyAddr{} }
func (*benchPipeConn) SetDeadline(time.Time) error      { return nil }
func (*benchPipeConn) SetReadDeadline(time.Time) error  { return nil }
func (*benchPipeConn) SetWriteDeadline(time.Time) error { return nil }

// poolBackend checks out real SessionLinks from a single-connection pool, so
// connection-scoped prepared statement state persists across transactions.
type poolBackend struct {
	pool *pgxpool.Pool
	shed passthroughLoadShedder
}

func (b *poolBackend) Acquire(ctx context.Context, req LinkRequest) (link.Link, error) {
	conn, err := b.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	return link.New(conn, link.Options{MaxPreparedStmts: req.MaxPreparedStmts, MetricTags: req.MetricTags})
}

func (b *poolBackend) LoadShedder() LoadShedder { return &b.shed }

// newBenchSession runs a session against a fake Postgres and returns the client end.
func newBenchSession(b *testing.B) *pgproto3.Frontend {
	b.Helper()
	pgClient, pgServer := newBenchPipe()
	go serveBenchPostgres(pgServer)
	config, err := pgxpool.ParseConfig("postgres://test@127.0.0.1/test?sslmode=disable")
	if err != nil {
		b.Fatal(err)
	}
	config.MaxConns = 1
	config.ConnConfig.DialFunc = func(context.Context, string, string) (net.Conn, error) {
		return pgClient, nil
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		b.Fatal(err)
	}

	fakeClient, client, clientConn := newFrontendBackendPair()
	s := New(Config{
		Context:               context.Background(),
		User:                  "testuser",
		Database:              "testdb",
		Service:               "testservice",
		PID:                   1234,
		Client:                client,
		ClientConn:            clientConn,
		Backend:               &poolBackend{pool: pool},
		PostgresMsgBufferSize: 8192,
		PreparedStatements:    prepared.CreateProxyPreparedStmtsState(nil),
		MetricTags:            []string{"user:testuser", "database:app_db"},
	})
	done := make(chan error, 1)
	go func() { done <- s.Run() }()
	b.Cleanup(func() {
		fakeClient.Send(&pgproto3.Terminate{})
		_ = fakeClient.Flush()
		<-done
		pool.Close()
		_ = pgClient.Close()
	})
	return fakeClient
}

// roundTrip sends msgs and reads replies through ReadyForQuery.
func roundTrip(b *testing.B, client *pgproto3.Frontend, msgs ...pgproto3.FrontendMessage) {
	for _, msg := range msgs {
		client.Send(msg)
	}
	if err := client.Flush(); err != nil {
		b.Fatal(err)
	}
	for {
		msg, err := client.Receive()
		if err != nil {
			b.Fatal(err)
		}
		switch m := msg.(type) {
		case *pgproto3.ReadyForQuery:
			return
		case *pgproto3.ErrorResponse:
			b.Fatalf("unexpected error: %s", m.Message)
		}
	}
}

// BenchmarkSessionBindExistingNamedStmt binds a named statement that is already on
// the connection: one checkout, Bind/Execute/Sync, one release per iteration.
func BenchmarkSessionBindExistingNamedStmt(b *testing.B) {
	client := newBenchSession(b)
	roundTrip(b, client, &pgproto3.Parse{Name: "s1", Query: benchQuery, ParameterOIDs: []uint32{23}}, &pgproto3.Sync{})
	bind := &pgproto3.Bind{PreparedStatement: "s1", Parameters: [][]byte{[]byte("1")}}
	execute := &pgproto3.Execute{}
	sync := &pgproto3.Sync{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		roundTrip(b, client, bind, execute, sync)
	}
}

// BenchmarkSessionParseExistingNamedStmt parses a statement the connection already
// holds, which the proxy answers with a local ParseComplete, then closes it.
func BenchmarkSessionParseExistingNamedStmt(b *testing.B) {
	client := newBenchSession(b)
	roundTrip(b, client, &pgproto3.Parse{Name: "seed", Query: benchQuery, ParameterOIDs: []uint32{23}}, &pgproto3.Sync{})
	parse := &pgproto3.Parse{Name: "again", Query: benchQuery, ParameterOIDs: []uint32{23}}
	closeStmt := &pgproto3.Close{ObjectType: 'S', Name: "again"}
	sync := &pgproto3.Sync{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// The session rewrites Parse.Name to the proxy name; restore the client name.
		parse.Name = "again"
		roundTrip(b, client, parse, closeStmt, sync)
	}
}

// BenchmarkSessionUnnamedPipeline runs the unnamed Parse/Bind/Execute/Sync pipeline
// most drivers send for a one-off parameterized query.
func BenchmarkSessionUnnamedPipeline(b *testing.B) {
	client := newBenchSession(b)
	parse := &pgproto3.Parse{Query: benchQuery, ParameterOIDs: []uint32{23}}
	bind := &pgproto3.Bind{Parameters: [][]byte{[]byte("1")}}
	execute := &pgproto3.Execute{}
	sync := &pgproto3.Sync{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		roundTrip(b, client, parse, bind, execute, sync)
	}
}
