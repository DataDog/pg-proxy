// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package link

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/DataDog/pg-proxy/pkg/prepared"
)

// fakePg is a single-connection pool whose connection talks to a fake Postgres that
// answers RESET and Deallocate, which is all Release sends.
type fakePg struct {
	pool   *pgxpool.Pool
	closes atomic.Int64 // statement Close messages received, one per Deallocate
}

func newFakePgPool(tb testing.TB) *fakePg {
	tb.Helper()
	fake := &fakePg{}
	clientConn, serverConn := net.Pipe()
	tb.Cleanup(func() {
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
				fake.closes.Add(1)
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
		tb.Fatal(err)
	}
	config.MaxConns = 1
	config.ConnConfig.DialFunc = func(context.Context, string, string) (net.Conn, error) {
		return clientConn, nil
	}
	fake.pool, err = pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		tb.Fatal(err)
	}
	// Close waits for every acquired conn, so a link still holding one when a test
	// fails would hang here; tests release their links in their own cleanups first.
	tb.Cleanup(fake.pool.Close)
	return fake
}

// newFakePgLink checks out the pool's connection. The caller must release the link,
// or the pool's cleanup blocks on it.
func newFakePgLink(tb testing.TB, pool *pgxpool.Pool, maxPreparedStmts int) Link {
	tb.Helper()
	conn, err := pool.Acquire(context.Background())
	if err != nil {
		tb.Fatal(err)
	}
	l, err := New(conn, Options{MaxPreparedStmts: maxPreparedStmts, MetricTags: []string{"database:app_db"}})
	if err != nil {
		tb.Fatal(err)
	}
	return l
}

// testStmts returns n named statements whose proxy names are distinct.
func testStmts(prefix string, n int) []prepared.PreparedStatement {
	stmts := make([]prepared.PreparedStatement, n)
	for i := range stmts {
		name := fmt.Sprintf("%s_%d", prefix, i)
		stmts[i] = prepared.CreatePreparedStatement(name, name, "SELECT id, name FROM users WHERE id = $1", []uint32{23})
	}
	return stmts
}
