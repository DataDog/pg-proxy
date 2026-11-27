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

// TestDiscardedReparseIsSentAgain: when an earlier error makes Postgres skip the
// Parse that re-creates the client's unnamed statement, the next Bind sends it
// again, rather than reach whatever unnamed statement the connection holds.
func TestDiscardedReparseIsSentAgain(t *testing.T) {
	var sent []pgproto3.FrontendMessage
	s := &Session{
		pStmtState:  prepared.CreateProxyPreparedStmtsState(nil),
		unnamedStmt: prepared.CreatePreparedStatement("", "", "SELECT 1", nil),
		link: &testLink{send: func(msg pgproto3.FrontendMessage, _ query.ReplyExpectation) {
			sent = append(sent, msg)
		}},
	}
	bind := actions.ClientMessageAction{ProxyMsgToPg: &pgproto3.Bind{}, EnsureUnnamedPreparedStmt: true}

	s.handleEnsureUnnamedPreparedStmt(bind)
	s.invalidatePendingStmts(true)
	s.handleEnsureUnnamedPreparedStmt(bind)
	require.Len(t, sent, 2)
	assert.Equal(t, sent[0], sent[1])

	s.confirmUnnamedStmt(s.pStmtState.ConfirmPendingStmt())
	s.invalidatePendingStmts(true)
	s.handleEnsureUnnamedPreparedStmt(bind)
	assert.Len(t, sent, 2, "an answered Parse left the statement on the connection")
}

// sendThroughSession has a session holding backendLink handle msgs in order until
// it starts discarding, and returns the session, what the link was sent and what
// the client received.
func sendThroughSession(t *testing.T, backendLink *testLink, msgs ...pgproto3.FrontendMessage) (s *Session, sent []pgproto3.FrontendMessage, received []pgproto3.BackendMessage) {
	t.Helper()
	observe := backendLink.send
	backendLink.send = func(msg pgproto3.FrontendMessage, expectation query.ReplyExpectation) {
		sent = append(sent, msg)
		if observe != nil {
			observe(msg, expectation)
		}
	}
	client := &testClient{}
	s = New(Config{Context: context.Background(), Client: client, InitialLink: backendLink})
	for _, msg := range msgs {
		require.NoError(t, s.handleClientMessage(msg))
		if s.discardUntilSync {
			break
		}
	}
	return s, sent, client.sent
}

// TestForeignUnnamedStmtGuard: a Bind or Describe of the unnamed statement from a
// client that has none reaches Postgres, which answers. When the connection's
// last release skipped the reset, a Close of the unnamed statement goes first,
// so Postgres answers as on a connection that has none instead of running or
// describing the statement another client left. Its CloseComplete is the proxy's.
func TestForeignUnnamedStmtGuard(t *testing.T) {
	for _, msg := range []pgproto3.FrontendMessage{&pgproto3.Bind{}, &pgproto3.Describe{ObjectType: 'S'}} {
		_, sent, received := sendThroughSession(t, &testLink{}, msg)
		assert.Equal(t, []pgproto3.FrontendMessage{msg}, sent, "%T on a connection that reset", msg)
		assert.Empty(t, received)

		var expectations []query.ReplyExpectation
		backendLink := &testLink{mayHoldForeignUnnamed: true, send: func(_ pgproto3.FrontendMessage, e query.ReplyExpectation) {
			expectations = append(expectations, e)
		}}
		_, sent, received = sendThroughSession(t, backendLink, msg)
		assert.Equal(t, []pgproto3.FrontendMessage{&pgproto3.Close{ObjectType: 'S'}, msg}, sent, "%T on a connection that skipped the reset", msg)
		assert.Equal(t, query.ProxyCloseReply, expectations[0])
		assert.Empty(t, received)
	}
}

// TestForeignUnnamedStmtGuardLetsClientsOwnStatementsThrough: a client's own unnamed
// statement, parsed earlier in the pipeline, and a Describe of a portal reach
// Postgres even on a connection whose last release skipped the reset.
func TestForeignUnnamedStmtGuardLetsClientsOwnStatementsThrough(t *testing.T) {
	parse := &pgproto3.Parse{Query: "SELECT 1"}
	bind := &pgproto3.Bind{}
	_, sent, received := sendThroughSession(t, &testLink{mayHoldForeignUnnamed: true}, parse, bind)
	assert.Equal(t, []pgproto3.FrontendMessage{parse, bind}, sent, "a Parse pending in the pipeline is the client's statement")
	assert.Empty(t, received)

	portal := &pgproto3.Describe{ObjectType: 'P'}
	_, sent, received = sendThroughSession(t, &testLink{mayHoldForeignUnnamed: true}, portal)
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
			s, sent, _ := sendThroughSession(t, backendLink,
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
