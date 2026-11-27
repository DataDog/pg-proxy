// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package link

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/DataDog/pg-proxy/internal/fakepg"
	"github.com/DataDog/pg-proxy/pkg/prepared"
)

// fakePg is a single-connection pool whose connection talks to a fake Postgres that
// answers RESET and Deallocate, which is all Release sends.
type fakePg struct {
	pool   *pgxpool.Pool
	closes int // statement Close messages received, one per Deallocate
}

func newFakePgPool(tb testing.TB) *fakePg {
	tb.Helper()
	fake := &fakePg{}
	fake.pool = fakepg.NewPool(tb, fakepg.NewServer(func(msg []byte) []byte {
		if msg[0] == 'C' {
			fake.closes++
		}
		return fakepg.Answer(msg)
	}))
	return fake
}

// newFakePgLink checks out the pool's connection and releases it at cleanup, which
// is a no-op if the test already released it.
func newFakePgLink(tb testing.TB, pool *pgxpool.Pool, maxPreparedStmts int) Link {
	tb.Helper()
	l, err := checkOut(pool, maxPreparedStmts)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = l.Release() })
	return l
}

// checkOut is newFakePgLink without the test bookkeeping, for timed loops.
func checkOut(pool *pgxpool.Pool, maxPreparedStmts int) (Link, error) {
	conn, err := pool.Acquire(context.Background())
	if err != nil {
		return nil, err
	}
	l, err := New(conn, Options{MaxPreparedStmts: maxPreparedStmts, MetricTags: []string{"database:app_db"}})
	if err != nil {
		conn.Release()
		return nil, err
	}
	return l, nil
}

// recordAll records stmts as created on the link's connection.
func recordAll(tb testing.TB, l Link, stmts []prepared.PreparedStatement) {
	tb.Helper()
	for _, st := range stmts {
		if err := l.RecordNewNamedPreparedStmt(st); err != nil {
			tb.Fatal(err)
		}
	}
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
