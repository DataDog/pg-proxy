// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package session

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/DataDog/pg-proxy/internal/fakepg"
	"github.com/DataDog/pg-proxy/pkg/link"
	"github.com/DataDog/pg-proxy/pkg/prepared"
)

const benchQuery = "SELECT id, name FROM users WHERE id = $1"

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
// Both ends use fakepg pipes, which reuse their buffers, so B/op and allocs/op are
// the session's plus the fake client's and fake Postgres's protocol decoding, which
// is the same before and after a change to the session.
func newBenchSession(b *testing.B) *pgproto3.Frontend {
	b.Helper()
	pgClient, pgServer := fakepg.Pipe()
	go fakepg.Serve(pgServer, fakepg.Answer)
	pool := fakepg.NewPool(b, pgClient)

	clientEnd, sessionEnd := fakepg.Pipe()
	b.Cleanup(func() { _ = clientEnd.Close() })
	fakeClient := pgproto3.NewFrontend(clientEnd, clientEnd)
	s := New(Config{
		Context:               context.Background(),
		User:                  "testuser",
		Database:              "testdb",
		Service:               "testservice",
		PID:                   1234,
		Client:                pgproto3.NewBackend(sessionEnd, sessionEnd),
		ClientConn:            sessionEnd,
		Backend:               &poolBackend{pool: pool},
		PostgresMsgBufferSize: 8192,
		PreparedStatements:    prepared.CreateProxyPreparedStmtsState(nil),
		MetricTags:            []string{"user:testuser", "database:app_db"},
	})
	done := make(chan error, 1)
	go func() { done <- s.Run() }()
	// Runs before the pool's cleanup, which waits for the session to give its conn back.
	b.Cleanup(func() {
		fakeClient.Send(&pgproto3.Terminate{})
		_ = fakeClient.Flush()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			b.Error("session did not exit after Terminate")
			_ = clientEnd.Close() // unblock its client read so the pool's cleanup can finish
		}
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
