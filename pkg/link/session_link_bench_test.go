// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package link

import (
	"fmt"
	"testing"

	"github.com/DataDog/pg-proxy/pkg/prepared"
)

// newWarmLink checks out a link whose connection already holds n statements.
func newWarmLink(b *testing.B, n int) (Link, []prepared.PreparedStatement) {
	l := newFakePgLink(b, newFakePgPool(b).pool, DefaultPreparedStmtsPerConnection)
	stmts := testStmts("warm", n)
	recordAll(b, l, stmts)
	return l, stmts
}

// BenchmarkBindExistingNamedStmt is the per-Bind work for a named statement already
// on the connection: the presence check, then the usage update.
func BenchmarkBindExistingNamedStmt(b *testing.B) {
	l, stmts := newWarmLink(b, 50)
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
	l, stmts := newWarmLink(b, 50)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !l.HasExistingNamedPreparedStmt(stmts[i%len(stmts)]) {
			b.Fatal("statement missing")
		}
	}
}

// BenchmarkRecordNewNamedStmt is the ParseComplete bookkeeping for a statement new to
// the connection. Each name is removed again, so every record is an insert, as in
// production, where a Parse of a held statement is answered without Postgres; the
// removal is part of the measured cost.
func BenchmarkRecordNewNamedStmt(b *testing.B) {
	l, _ := newWarmLink(b, 50)
	held := l.(*SessionLink).connState.stmts
	stmts := testStmts("new", 64)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		st := stmts[i%len(stmts)]
		if err := l.RecordNewNamedPreparedStmt(st); err != nil {
			b.Fatal(err)
		}
		delete(held, st.GetProxyName())
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
			pool := newFakePgPool(b).pool
			resident := testStmts("resident", tc.resident)
			l := newFakePgLink(b, pool, DefaultPreparedStmtsPerConnection)
			recordAll(b, l, resident)
			if err := l.Release(); err != nil {
				b.Fatal(err)
			}
			// A long cycle of unique names, so each checkout adds statements the
			// connection no longer holds and Release has to evict them.
			fresh := make([][]prepared.PreparedStatement, 4096)
			for i := range fresh {
				fresh[i] = testStmts(fmt.Sprintf("fresh%d", i), tc.perLink)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				l, err := checkOut(pool, DefaultPreparedStmtsPerConnection)
				if err != nil {
					b.Fatal(err)
				}
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
