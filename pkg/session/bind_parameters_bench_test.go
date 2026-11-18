// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package session

import (
	"testing"

	"github.com/DataDog/pg-proxy/pkg/actions"
	"github.com/DataDog/pg-proxy/pkg/prepared"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

// BenchmarkObserveDescribe measures the type tracking that runs on every
// statement Describe while capture is enabled, sampled or not: the Describe,
// its ParameterDescription and the cycle's ReadyForQuery.
func BenchmarkObserveDescribe(b *testing.B) {
	oids := []uint32{pgtype.Int8OID, pgtype.TextOID, pgtype.UUIDOID, pgtype.TimestamptzOID}
	describe := func(b *testing.B, s *Session, name string) {
		b.Helper()
		msg := &pgproto3.Describe{ObjectType: 'S', Name: name}
		reply := &pgproto3.ParameterDescription{ParameterOIDs: oids}
		ready := &pgproto3.ReadyForQuery{TxStatus: 'I'}
		b.ReportAllocs()
		for b.Loop() {
			s.observeClientMessage(msg)
			s.observeBackendMessage(reply)
			s.observeBackendMessage(ready)
		}
	}
	b.Run("unnamed", func(b *testing.B) {
		describe(b, New(Config{EnableTraceParameters: true}), "")
	})
	b.Run("named", func(b *testing.B) {
		state := prepared.CreateProxyPreparedStmtsState(nil)
		const sql = "SELECT $1, $2, $3, $4"
		stmt := actions.CreateProxyPreparedStmt("stmt", sql, nil, false)
		if err := state.AddConfirmedNamedStmt(stmt); err != nil {
			b.Fatal(err)
		}
		describe(b, New(Config{EnableTraceParameters: true, PreparedStatements: state}), "stmt")
	})
}
