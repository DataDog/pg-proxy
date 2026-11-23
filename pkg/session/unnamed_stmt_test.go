// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package session

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/DataDog/pg-proxy/pkg/actions"
	"github.com/DataDog/pg-proxy/pkg/link"
	"github.com/DataDog/pg-proxy/pkg/prepared"
	"github.com/DataDog/pg-proxy/pkg/query"
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

// sendThroughSession has a session holding backendLink handle msgs in order, and
// returns what the link was sent and what the client received.
func sendThroughSession(t *testing.T, backendLink *testLink, msgs ...pgproto3.FrontendMessage) (sent []pgproto3.FrontendMessage, received []pgproto3.BackendMessage) {
	t.Helper()
	_, received = sendThroughSessionState(t, backendLink, &sent, msgs...)
	return sent, received
}

// sendThroughSessionState is sendThroughSession that also returns the session.
func sendThroughSessionState(t *testing.T, backendLink *testLink, sent *[]pgproto3.FrontendMessage, msgs ...pgproto3.FrontendMessage) (*Session, []pgproto3.BackendMessage) {
	t.Helper()
	backendLink.send = func(msg pgproto3.FrontendMessage, _ query.ReplyExpectation) { *sent = append(*sent, msg) }
	fakeClient, client, _ := newFrontendBackendPair()
	s := New(Config{Context: context.Background(), Client: client, InitialLink: backendLink})
	var received []pgproto3.BackendMessage
	for _, msg := range msgs {
		require.NoError(t, s.handleClientMessage(msg))
		if s.discardUntilSync {
			reply, err := fakeClient.Receive()
			require.NoError(t, err)
			received = append(received, reply)
			break
		}
	}
	return s, received
}

// TestForeignUnnamedStmtGuard: a Bind or Describe of the unnamed statement from a
// client that has none reaches Postgres, which answers, unless the connection's
// last release skipped the reset. Then the session answers, because Postgres
// would run or describe the statement another client left.
func TestForeignUnnamedStmtGuard(t *testing.T) {
	for _, msg := range []pgproto3.FrontendMessage{&pgproto3.Bind{}, &pgproto3.Describe{ObjectType: 'S'}} {
		sent, received := sendThroughSession(t, &testLink{}, msg)
		assert.Equal(t, []pgproto3.FrontendMessage{msg}, sent, "%T on a connection that reset", msg)
		assert.Empty(t, received)

		sent, received = sendThroughSession(t, &testLink{mayHoldForeignUnnamed: true}, msg)
		assert.Equal(t, []pgproto3.FrontendMessage{&pgproto3.Flush{}}, sent, "%T on a connection that skipped the reset", msg)
		require.Len(t, received, 1)
		assert.Equal(t, actions.BuildUnnamedStatementDoesNotExistErrorResponse(), received[0])
	}
}

// TestForeignUnnamedStmtGuardLetsClientsOwnStatementsThrough: a client's own unnamed
// statement, parsed earlier in the pipeline, and a Describe of a portal reach
// Postgres even on a connection whose last release skipped the reset.
func TestForeignUnnamedStmtGuardLetsClientsOwnStatementsThrough(t *testing.T) {
	parse := &pgproto3.Parse{Query: "SELECT 1"}
	bind := &pgproto3.Bind{}
	sent, received := sendThroughSession(t, &testLink{mayHoldForeignUnnamed: true}, parse, bind)
	assert.Equal(t, []pgproto3.FrontendMessage{parse, bind}, sent, "a Parse pending in the pipeline is the client's statement")
	assert.Empty(t, received)

	portal := &pgproto3.Describe{ObjectType: 'P'}
	sent, received = sendThroughSession(t, &testLink{mayHoldForeignUnnamed: true}, portal)
	assert.Equal(t, []pgproto3.FrontendMessage{portal}, sent, "a portal is not a statement")
	assert.Empty(t, received)
}

// TestSessionMarksOnlyForLinksThatUseThem: the session judges a message only for
// a link whose release reads the marks.
func TestSessionMarksOnlyForLinksThatUseThem(t *testing.T) {
	setter := &pgproto3.Query{String: "SET search_path = app"}
	ignoring := &testLink{}
	sendThroughSession(t, ignoring, setter)
	assert.Zero(t, ignoring.marks)

	using := &testLink{usesMarks: true}
	sendThroughSession(t, using, setter)
	assert.Equal(t, link.MarkClientSQL, using.marks)
}

// TestPreparedStatementsUnjudgedForLinksWithoutMarks: Parse and Bind through a link
// that does not use session marks scan no statement text, named or unnamed.
func TestPreparedStatementsUnjudgedForLinksWithoutMarks(t *testing.T) {
	for _, name := range []string{"named", ""} {
		for _, usesMarks := range []bool{false, true} {
			backendLink := &testLink{usesMarks: usesMarks}
			var sent []pgproto3.FrontendMessage
			s, _ := sendThroughSessionState(t, backendLink, &sent,
				&pgproto3.Parse{Name: name, Query: "SET search_path = app"},
				&pgproto3.Bind{PreparedStatement: name})
			require.Len(t, sent, 2)
			stmt := s.pStmtState.FindPendingNamedStmtFromClientName(name)
			require.NotNil(t, stmt, "statement %q", name)
			assert.Equal(t, usesMarks, prepared.Judged(stmt), "statement %q, link uses marks: %v", name, usesMarks)
			if usesMarks {
				assert.Equal(t, link.MarkPreparedStmt, backendLink.marks)
			}
		}
	}
}
