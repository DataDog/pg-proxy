// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package session

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/DataDog/pg-proxy/pkg/prepared"
)

// replayUnnamedStmt queues stmt the way handleEnsureUnnamedPreparedStmt does and
// confirms it the way flushPostgres does on ParseComplete.
func replayUnnamedStmt(s *Session, stmt prepared.PreparedStatement) {
	s.pStmtState.EnqueuePendingStmt(stmt, true)
	s.unnamedPendingGens = append(s.unnamedPendingGens, s.unnamedGen)
	s.confirmUnnamedStmt(s.pStmtState.ConfirmPendingStmt())
}

func TestConfirmedUnnamedStmtIsNotWrappedAgainOnReuse(t *testing.T) {
	s := &Session{pStmtState: prepared.CreateProxyPreparedStmtsState(nil)}
	stmt := prepared.CreatePreparedStatement("", "", "SELECT $1::int", []uint32{23})
	replayUnnamedStmt(s, stmt)
	for range 5 {
		replayUnnamedStmt(s, s.unnamedStmt)
	}
	_, wrapped := s.unnamedStmt.(prepared.QueuedPreparedStatement)
	require.False(t, wrapped, "the saved statement must be a plain copy, not the pending-queue wrapper")
	require.Equal(t, "SELECT $1::int", s.unnamedStmt.GetSQLQuery())
	require.Equal(t, []uint32{23}, s.unnamedStmt.GetParamOIDs())
}

func TestSupersededParseCompleteDoesNotRestoreUnnamedStmt(t *testing.T) {
	s := &Session{pStmtState: prepared.CreateProxyPreparedStmtsState(nil)}
	// An unnamed Parse is sent, then a Close, Query or newer Parse destroys the
	// statement before the first ParseComplete arrives.
	s.invalidateUnnamedStmt()
	s.pStmtState.EnqueuePendingStmt(prepared.CreatePreparedStatement("", "", "SELECT 'old'", nil), false)
	s.unnamedPendingGens = append(s.unnamedPendingGens, s.unnamedGen)
	s.invalidateUnnamedStmt()

	s.confirmUnnamedStmt(s.pStmtState.ConfirmPendingStmt())
	require.Nil(t, s.unnamedStmt)
	require.Empty(t, s.unnamedPendingGens)
}
