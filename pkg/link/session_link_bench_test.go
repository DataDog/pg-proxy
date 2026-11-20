// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package link

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/DataDog/pg-proxy/pkg/prepared"
)

// newBenchPool returns a single-connection pool whose connection talks to a fake
// Postgres that answers RESET and Deallocate, which is all Release sends.
func newBenchPool(b *testing.B) *pgxpool.Pool {
	b.Helper()
	clientConn, serverConn := net.Pipe()
	b.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	})
	go func() {
		backend := pgproto3.NewBackend(serverConn, serverConn)
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
			switch msg.(type) {
			case *pgproto3.Close:
				backend.Send(&pgproto3.CloseComplete{})
			case *pgproto3.Query:
				backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("RESET")})
				backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
				if backend.Flush() != nil {
					return
				}
			case *pgproto3.Sync:
				backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
				if backend.Flush() != nil {
					return
				}
			case *pgproto3.Terminate:
				return
			}
		}
	}()

	config, err := pgxpool.ParseConfig("postgres://test@127.0.0.1/test?sslmode=disable")
	if err != nil {
		b.Fatal(err)
	}
	config.MaxConns = 1
	config.ConnConfig.DialFunc = func(context.Context, string, string) (net.Conn, error) {
		return clientConn, nil
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(pool.Close)
	return pool
}

func benchStmts(prefix string, n int) []prepared.PreparedStatement {
	stmts := make([]prepared.PreparedStatement, n)
	for i := range stmts {
		name := fmt.Sprintf("%s_%d", prefix, i)
		stmts[i] = prepared.CreatePreparedStatement(name, name, "SELECT id, name FROM users WHERE id = $1", []uint32{23})
	}
	return stmts
}

func newBenchLink(b *testing.B, pool *pgxpool.Pool) Link {
	b.Helper()
	conn, err := pool.Acquire(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	l, err := New(conn, Options{MaxPreparedStmts: DefaultPreparedStmtsPerConnection, MetricTags: []string{"database:app_db"}})
	if err != nil {
		b.Fatal(err)
	}
	return l
}

// BenchmarkBindExistingNamedStmt is the per-Bind work for a named statement already
// on the connection: the presence check, then the usage update.
func BenchmarkBindExistingNamedStmt(b *testing.B) {
	l := newBenchLink(b, newBenchPool(b))
	b.Cleanup(func() { _ = l.Release() })
	stmts := benchStmts("warm", 50)
	for _, st := range stmts {
		if err := l.RecordNewNamedPreparedStmt(st); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		st := stmts[i%len(stmts)]
		if !l.HasExistingNamedPreparedStmt(st) {
			b.Fatal("statement missing")
		}
		if err := l.RecordExistingNamedPreparedStmtUsed(st); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkParseExistingNamedStmt is the presence check a Parse runs before the
// proxy answers ParseComplete locally.
func BenchmarkParseExistingNamedStmt(b *testing.B) {
	l := newBenchLink(b, newBenchPool(b))
	b.Cleanup(func() { _ = l.Release() })
	stmts := benchStmts("warm", 50)
	for _, st := range stmts {
		if err := l.RecordNewNamedPreparedStmt(st); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !l.HasExistingNamedPreparedStmt(stmts[i%len(stmts)]) {
			b.Fatal("statement missing")
		}
	}
}

// BenchmarkRecordNewNamedStmt is the ParseComplete bookkeeping for a new statement.
func BenchmarkRecordNewNamedStmt(b *testing.B) {
	l := newBenchLink(b, newBenchPool(b))
	b.Cleanup(func() { _ = l.Release() })
	stmts := benchStmts("new", 64)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := l.RecordNewNamedPreparedStmt(stmts[i%len(stmts)]); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkReleasePreparedStmts checks out a link, uses or creates statements, and
// releases it. Below the cap Release only resets the session; above it Release also
// deallocates the overflow, one round trip per statement.
func BenchmarkReleasePreparedStmts(b *testing.B) {
	for _, tc := range []struct {
		name     string
		resident int // statements already on the connection
		perLink  int // new statements each checkout creates
	}{
		{name: "under_cap", resident: 50, perLink: 0},
		{name: "over_cap", resident: DefaultPreparedStmtsPerConnection, perLink: 10},
	} {
		b.Run(tc.name, func(b *testing.B) {
			pool := newBenchPool(b)
			resident := benchStmts("resident", tc.resident)
			l := newBenchLink(b, pool)
			for _, st := range resident {
				if err := l.RecordNewNamedPreparedStmt(st); err != nil {
					b.Fatal(err)
				}
			}
			if err := l.Release(); err != nil {
				b.Fatal(err)
			}
			// A long cycle of unique names, so each checkout adds statements the
			// connection no longer holds and Release has to evict them.
			fresh := make([][]prepared.PreparedStatement, 4096)
			for i := range fresh {
				fresh[i] = benchStmts(fmt.Sprintf("fresh%d", i), tc.perLink)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				l := newBenchLink(b, pool)
				st := resident[i%len(resident)]
				if l.HasExistingNamedPreparedStmt(st) {
					if err := l.RecordExistingNamedPreparedStmtUsed(st); err != nil {
						b.Fatal(err)
					}
				}
				for _, st := range fresh[i%len(fresh)] {
					if err := l.RecordNewNamedPreparedStmt(st); err != nil {
						b.Fatal(err)
					}
				}
				if err := l.Release(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
