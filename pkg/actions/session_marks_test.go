// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package actions

import (
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/DataDog/pg-proxy/pkg/link"
	"github.com/DataDog/pg-proxy/pkg/prepared"
)

func TestQuerySessionMarks(t *testing.T) {
	state := State{PreparedStatements: prepared.CreateProxyPreparedStmtsState(nil)}
	for sql, want := range map[string]link.SessionMarks{
		"SELECT 1":                        0,
		"SET search_path = app":           link.MarkClientSQL,
		"SELECT 1; SET work_mem = '1MB'":  link.MarkClientSQL,
		"SELECT currval('s')":             0,
		"SET LOCAL statement_timeout = 0": 0,
	} {
		action, err := DecideClientMessageAction(&pgproto3.Query{String: sql}, state, Hooks{})
		require.NoError(t, err)
		assert.Equal(t, want, action.SessionMarks(state.PreparedStatements, nil), "%q", sql)
	}
}

func TestBindSessionMarks(t *testing.T) {
	clean := CreateProxyPreparedStmt("clean", "SELECT $1", nil, false)
	setter := CreateProxyPreparedStmt("setter", "SELECT set_config('app.tenant_id', $1, false)", nil, false)
	pStmts := prepared.CreateProxyPreparedStmtsState(nil)
	for _, stmt := range []prepared.PreparedStatement{clean, setter} {
		require.NoError(t, pStmts.AddConfirmedNamedStmt(stmt))
	}
	state := State{PreparedStatements: pStmts}
	for name, want := range map[string]link.SessionMarks{
		"clean":  0,
		"setter": link.MarkPreparedStmt,
	} {
		action, err := DecideClientMessageAction(&pgproto3.Bind{PreparedStatement: name}, state, Hooks{})
		require.NoError(t, err)
		assert.Equal(t, want, action.SessionMarks(pStmts, nil), "Bind of %s", name)
	}

	// A statement parsed earlier in the same pipeline is not confirmed yet. The
	// Bind runs it, so its own verdict applies, not "unmatched".
	pending := prepared.CreateProxyPreparedStmtsState(nil)
	pending.EnqueuePendingStmt(CreateProxyPreparedStmt("fresh", "SET search_path = app", nil, false), false)
	action, err := DecideClientMessageAction(&pgproto3.Bind{PreparedStatement: "fresh"}, State{PreparedStatements: pending}, Hooks{})
	require.NoError(t, err)
	assert.Equal(t, link.MarkPreparedStmt, action.SessionMarks(pending, nil))
}

func TestUnnamedBindSessionMarks(t *testing.T) {
	setter := prepared.CreatePreparedStatement("", "", "SET search_path = app", nil)
	clean := prepared.CreatePreparedStatement("", "", "SELECT 1", nil)
	stmts := prepared.CreateProxyPreparedStmtsState(nil)
	state := State{PreparedStatements: stmts}

	action, err := DecideClientMessageAction(&pgproto3.Bind{}, state, Hooks{})
	require.NoError(t, err)
	assert.True(t, action.EnsureUnnamedPreparedStmt)
	assert.Equal(t, link.MarkPreparedStmt, action.SessionMarks(stmts, setter),
		"the unnamed statement parsed in an earlier checkout carries its verdict")
	assert.Equal(t, link.MarkPreparedStmt, action.SessionMarks(stmts, nil), "a Bind of no known statement may run anything")

	// A newer unnamed Parse in this pipeline replaces the confirmed one.
	stmts.EnqueuePendingStmt(clean, false)
	assert.Zero(t, action.SessionMarks(stmts, setter))
}

// TestUnnamedStatementReachesPostgres: the decision forwards a Bind or Describe of
// the unnamed statement even when the client has none. Only the session, which
// knows whether the connection skipped its reset, may answer it instead.
func TestUnnamedStatementReachesPostgres(t *testing.T) {
	state := State{PreparedStatements: prepared.CreateProxyPreparedStmtsState(nil)}
	for _, msg := range []pgproto3.FrontendMessage{
		&pgproto3.Bind{},
		&pgproto3.Describe{ObjectType: 'S'},
	} {
		action, err := DecideClientMessageAction(msg, state, Hooks{})
		require.NoError(t, err)
		assert.Same(t, msg, action.ProxyMsgToPg)
		assert.True(t, action.EnsureUnnamedPreparedStmt)
	}
	assert.Equal(t, "26000", BuildUnnamedStatementDoesNotExistErrorResponse().Code)
}

func TestSessionMarksOfOtherMessages(t *testing.T) {
	state := State{PreparedStatements: prepared.CreateProxyPreparedStmtsState(nil)}
	for _, msg := range []pgproto3.FrontendMessage{
		&pgproto3.Describe{ObjectType: 'S'},
		&pgproto3.Execute{},
		&pgproto3.Sync{},
	} {
		action, err := DecideClientMessageAction(msg, state, Hooks{})
		require.NoError(t, err)
		assert.Zero(t, action.SessionMarks(state.PreparedStatements, nil), "%T runs nothing new", msg)
	}
}
