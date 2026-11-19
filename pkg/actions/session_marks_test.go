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
		"SET search_path = app":           link.MarkClientSQL | link.MarkReadsSequenceState,
		"SELECT 1; SET work_mem = '1MB'":  link.MarkClientSQL | link.MarkReadsSequenceState,
		"SELECT currval('s')":             link.MarkReadsSequenceState,
		"SET LOCAL statement_timeout = 0": 0,
	} {
		action, err := DecideClientMessageAction(&pgproto3.Query{String: sql}, state, Hooks{})
		require.NoError(t, err)
		assert.Equal(t, want, action.SessionMarks, "%q", sql)
	}
}

func TestBindSessionMarks(t *testing.T) {
	clean := CreateProxyPreparedStmt("clean", "SELECT $1", nil, false)
	setter := CreateProxyPreparedStmt("setter", "SELECT set_config('app.tenant_id', $1, false)", nil, false)
	reader := CreateProxyPreparedStmt("reader", "SELECT currval('s')", nil, false)
	pStmts := prepared.CreateProxyPreparedStmtsState(nil)
	for _, stmt := range []prepared.PreparedStatement{clean, setter, reader} {
		require.NoError(t, pStmts.AddConfirmedNamedStmt(stmt))
	}
	state := State{PreparedStatements: pStmts}
	for name, want := range map[string]link.SessionMarks{
		"clean":  0,
		"setter": link.MarkPreparedStmt | link.MarkReadsSequenceState,
		"reader": link.MarkReadsSequenceState,
	} {
		action, err := DecideClientMessageAction(&pgproto3.Bind{PreparedStatement: name}, state, Hooks{})
		require.NoError(t, err)
		assert.Equal(t, want, action.SessionMarks, "Bind of %s", name)
	}

	// A statement parsed earlier in the same pipeline is not confirmed yet. The
	// Bind runs it, so its own verdict applies, not "unmatched".
	pending := prepared.CreateProxyPreparedStmtsState(nil)
	pending.EnqueuePendingStmt(CreateProxyPreparedStmt("fresh", "SELECT $1", nil, false), false)
	action, err := DecideClientMessageAction(&pgproto3.Bind{PreparedStatement: "fresh"}, State{PreparedStatements: pending}, Hooks{})
	require.NoError(t, err)
	assert.Zero(t, action.SessionMarks)
}

func TestUnnamedBindSessionMarks(t *testing.T) {
	setter := prepared.CreatePreparedStatement("", "", "SET search_path = app", nil)
	clean := prepared.CreatePreparedStatement("", "", "SELECT 1", nil)
	state := State{PreparedStatements: prepared.CreateProxyPreparedStmtsState(nil), UnnamedStmt: setter, HasUnnamedStmt: true}

	action, err := DecideClientMessageAction(&pgproto3.Bind{}, state, Hooks{})
	require.NoError(t, err)
	assert.True(t, action.EnsureUnnamedPreparedStmt)
	assert.Equal(t, link.MarkPreparedStmt|link.MarkReadsSequenceState, action.SessionMarks,
		"the unnamed statement parsed in an earlier checkout carries its verdict")

	// A newer unnamed Parse in this pipeline replaces the confirmed one.
	state.PreparedStatements.EnqueuePendingStmt(clean, false)
	action, err = DecideClientMessageAction(&pgproto3.Bind{}, state, Hooks{})
	require.NoError(t, err)
	assert.Zero(t, action.SessionMarks)
}

func TestUnnamedStatementGuard(t *testing.T) {
	none := State{PreparedStatements: prepared.CreateProxyPreparedStmtsState(nil)}
	for _, msg := range []pgproto3.FrontendMessage{
		&pgproto3.Bind{},
		&pgproto3.Describe{ObjectType: 'S'},
	} {
		action, err := DecideClientMessageAction(msg, none, Hooks{ErrorActionCreated: func(*pgproto3.ErrorResponse, error) {}})
		require.NoError(t, err)
		assert.Equal(t, &pgproto3.Flush{}, action.ProxyMsgToPg, "%T must not reach Postgres", msg)
		require.NotNil(t, action.AfterFlushAction)
		assert.True(t, action.AfterFlushAction.DiscardUntilSync, "the client gets the extended-query error flow")
		assert.Equal(t, []pgproto3.BackendMessage{BuildUnnamedStatementDoesNotExistErrorResponse()}, action.AfterFlushAction.ReplyMsgsToClient)
	}
	assert.Equal(t, "26000", BuildUnnamedStatementDoesNotExistErrorResponse().Code)

	// A portal is not a statement, and auto-cached statements take the named path.
	action, err := DecideClientMessageAction(&pgproto3.Describe{ObjectType: 'P'}, none, Hooks{})
	require.NoError(t, err)
	assert.IsType(t, &pgproto3.Describe{}, action.ProxyMsgToPg)

	// A Parse pending in the same pipeline counts as having one.
	pending := none
	pending.HasUnnamedStmt = true
	action, err = DecideClientMessageAction(&pgproto3.Bind{}, pending, Hooks{})
	require.NoError(t, err)
	assert.IsType(t, &pgproto3.Bind{}, action.ProxyMsgToPg)
	assert.True(t, action.EnsureUnnamedPreparedStmt)
}
