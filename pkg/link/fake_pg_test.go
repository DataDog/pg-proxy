// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package link

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/DataDog/pg-proxy/internal/fakepg"
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
	client, server := fakepg.Pipe()
	go fakepg.Serve(server, func(backend *pgproto3.Backend, msg pgproto3.FrontendMessage) bool {
		if _, ok := msg.(*pgproto3.Close); ok {
			fake.closes.Add(1)
		}
		return fakepg.Answer(backend, msg)
	})
	fake.pool = fakepg.NewPool(tb, client)
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
		conn.Release()
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
