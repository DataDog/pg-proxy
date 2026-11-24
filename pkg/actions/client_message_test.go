// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package actions

import (
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/DataDog/pg-proxy/pkg/classifier"
	"github.com/DataDog/pg-proxy/pkg/prepared"
	"github.com/DataDog/pg-proxy/pkg/query"
)

func TestDecideClientMessageAction(t *testing.T) {
	state := State{
		DynamicRoutingEnabled: true,
		PreparedStatements:    prepared.CreateProxyPreparedStmtsState(nil),
	}
	queryMessage := &pgproto3.Query{String: "SELECT 1"}
	action, err := DecideClientMessageAction(queryMessage, state, Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	want := ClientMessageAction{
		ProxyMsgToPg:                  queryMessage,
		AddReplyExpectation:           query.QueryReply,
		FlushPostgres:                 true,
		HandleReadQuery:               true,
		InvalidateUnnamedPreparedStmt: true,
	}
	if !reflect.DeepEqual(action, want) {
		t.Fatalf("action mismatch:\n got: %#v\nwant: %#v", action, want)
	}
}

func TestDecideClientMessageActionDiscardSyncWithoutLink(t *testing.T) {
	action, err := DecideClientMessageAction(&pgproto3.Sync{}, State{
		DiscardUntilSync:   true,
		PreparedStatements: prepared.CreateProxyPreparedStmtsState(nil),
	}, Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	want := ClientMessageAction{
		ReplyMsgsToClient: []pgproto3.BackendMessage{&pgproto3.ReadyForQuery{TxStatus: 'I'}},
		DiscardUntilSync:  false,
	}
	if !reflect.DeepEqual(action, want) {
		t.Fatalf("action mismatch:\n got: %#v\nwant: %#v", action, want)
	}
}

func TestDecideClientMessageActionRewritesNamedPreparedStatement(t *testing.T) {
	message := &pgproto3.Parse{Name: "client-name", Query: "SELECT $1", ParameterOIDs: []uint32{23}}
	state := State{PreparedStatements: prepared.CreateProxyPreparedStmtsState(nil)}
	action, err := DecideClientMessageAction(message, state, Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	if message.Name == "client-name" || message.Name == "" {
		t.Fatalf("expected proxy statement name, got %q", message.Name)
	}
	if action.RecordNewPreparedStmt.GetClientName() != "client-name" {
		t.Fatalf("client name changed: %q", action.RecordNewPreparedStmt.GetClientName())
	}
	if action.RecordNewPreparedStmt.GetProxyName() != message.Name {
		t.Fatalf("action proxy name %q differs from message %q", action.RecordNewPreparedStmt.GetProxyName(), message.Name)
	}
}

func TestDecideClientMessageActionReportsPortableHooks(t *testing.T) {
	var observedMessage pgproto3.FrontendMessage
	var observedSQL string
	var observedProtocol PGQueryProtocolType
	var statementCount int
	message := &pgproto3.Query{String: "SELECT 1; SELECT 2"}
	_, err := DecideClientMessageAction(message, State{
		DynamicRoutingEnabled: true,
		PreparedStatements:    prepared.CreateProxyPreparedStmtsState(nil),
	}, Hooks{
		MessageObserved: func(msg pgproto3.FrontendMessage) { observedMessage = msg },
		QueryObserved: func(sql string, protocol PGQueryProtocolType) {
			observedSQL, observedProtocol = sql, protocol
		},
		QueryClassified: func(_ string, _ PGQueryProtocolType, classification classifier.Classification) {
			statementCount = classification.StatementCount
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if observedMessage != message || observedSQL != message.String || observedProtocol != SimpleQueryProtocol || statementCount != 2 {
		t.Fatalf("hooks not called as expected: message=%T sql=%q protocol=%q statements=%d", observedMessage, observedSQL, observedProtocol, statementCount)
	}
}

type ClientMessageActionTest struct {
	name                          string
	msg                           pgproto3.FrontendMessage
	replyMsgs                     []pgproto3.BackendMessage
	proxyMsgToPg                  pgproto3.FrontendMessage
	wantExp                       query.ReplyExpectation
	wantFlush                     bool
	wantErrContains               string
	wantErrIs                     error
	wantTraceStarted              bool
	wantHandleReadQuery           bool
	wantReply                     bool
	wantNewPreparedStmt           bool
	newPreparedStmt               prepared.PreparedStatement
	afterFlushAction              *ClientMessageAction
	enableNamedPreparedStatements bool
	passthroughUser               bool
	wantDeallocate                bool
	wantEnsureUnnamed             bool
	wantInvalidateUnnamed         bool
	deallocatePreparedStmt        prepared.PreparedStatement
	dynamicRoutingEnabled         bool
	autoCachedEnabled             bool
}

func TestClientMessageActions(t *testing.T) {
	assert := assert.New(t)

	tests := []ClientMessageActionTest{
		{
			name:                  "simple query",
			msg:                   &pgproto3.Query{String: "SELECT 1"},
			proxyMsgToPg:          &pgproto3.Query{String: "SELECT 1"},
			dynamicRoutingEnabled: true,
			wantExp:               query.QueryReply,
			wantFlush:             true,
			wantTraceStarted:      true,
			wantHandleReadQuery:   true,
			wantInvalidateUnnamed: true,
		},
		{
			name:                  "simple query update",
			msg:                   &pgproto3.Query{String: "UPDATE dogs set upvotes = upvotes + 1 WHERE id = 1"},
			proxyMsgToPg:          &pgproto3.Query{String: "UPDATE dogs set upvotes = upvotes + 1 WHERE id = 1"},
			dynamicRoutingEnabled: true,
			wantExp:               query.QueryReply,
			wantFlush:             true,
			wantTraceStarted:      true,
			wantHandleReadQuery:   false, // Should be false since INSERT is a write query
			wantInvalidateUnnamed: true,
		},
		{
			name:                  "simple parse",
			msg:                   &pgproto3.Parse{Query: "SELECT 2"},
			proxyMsgToPg:          &pgproto3.Parse{Query: "SELECT 2"},
			dynamicRoutingEnabled: true,
			wantExp:               query.ParseReply,
			wantTraceStarted:      true,
			wantHandleReadQuery:   true,
			wantNewPreparedStmt:   true,
			newPreparedStmt:       prepared.CreatePreparedStatement("", "", "SELECT 2", nil),
		},
		{
			name:                  "query with dynamic routing disabled - no parsing",
			msg:                   &pgproto3.Query{String: "SELECT 1"},
			proxyMsgToPg:          &pgproto3.Query{String: "SELECT 1"},
			dynamicRoutingEnabled: false,
			wantExp:               query.QueryReply,
			wantFlush:             true,
			wantTraceStarted:      true,
			wantHandleReadQuery:   false, // Should be false even though it's a read-only query since no parsing occurred
			wantInvalidateUnnamed: true,
		},
		{
			// Answered locally, but still a simple Query, which destroys the unnamed statement.
			name:                  "pgx ping",
			msg:                   &pgproto3.Query{String: "-- ping"},
			wantReply:             true,
			replyMsgs:             []pgproto3.BackendMessage{&pgproto3.EmptyQueryResponse{}, &pgproto3.ReadyForQuery{TxStatus: 'I'}},
			wantInvalidateUnnamed: true,
		},
		{
			name:                          "parse named statement",
			msg:                           &pgproto3.Parse{Query: "SELECT 3", Name: "MyNamedPreparedStatement"},
			proxyMsgToPg:                  &pgproto3.Parse{Query: "SELECT 3", Name: "1d6a12252ca55db7a202d76b7afdcdacb9b0c90734f73d9c88f27eba4bf67288"},
			wantExp:                       query.ParseReply,
			wantNewPreparedStmt:           true,
			newPreparedStmt:               prepared.CreatePreparedStatement("MyNamedPreparedStatement", "1d6a12252ca55db7a202d76b7afdcdacb9b0c90734f73d9c88f27eba4bf67288", "SELECT 3", nil),
			wantTraceStarted:              true,
			enableNamedPreparedStatements: true,
		},
		{
			name:              "simple describe",
			msg:               &pgproto3.Describe{ObjectType: 'S'},
			proxyMsgToPg:      &pgproto3.Describe{ObjectType: 'S'},
			wantExp:           query.DescribeReply,
			wantEnsureUnnamed: true,
		},
		{
			name:                  "simple close",
			msg:                   &pgproto3.Close{ObjectType: 'S', Name: ""},
			proxyMsgToPg:          &pgproto3.Close{ObjectType: 'S', Name: ""},
			wantExp:               query.CloseReply,
			wantDeallocate:        true,
			wantInvalidateUnnamed: true,
		},
		{
			// Closing the unnamed portal leaves the unnamed statement in place.
			name:         "close unnamed portal",
			msg:          &pgproto3.Close{ObjectType: 'P', Name: ""},
			proxyMsgToPg: &pgproto3.Close{ObjectType: 'P', Name: ""},
			wantExp:      query.CloseReply,
		},
		{
			name:         "close named prepared statement",
			msg:          &pgproto3.Close{ObjectType: 'S', Name: "foo"},
			proxyMsgToPg: &pgproto3.Flush{},
			wantFlush:    true,
			afterFlushAction: &ClientMessageAction{
				ReplyMsgsToClient:      []pgproto3.BackendMessage{&pgproto3.CloseComplete{}},
				DeallocatePreparedStmt: nil, // nil because test case contains non-existent prepared stmt
			},
		},
		{
			name:              "simple bind",
			msg:               &pgproto3.Bind{},
			proxyMsgToPg:      &pgproto3.Bind{},
			wantExp:           query.BindReply,
			wantEnsureUnnamed: true,
		},
		{
			name:         "simple execute",
			msg:          &pgproto3.Execute{},
			proxyMsgToPg: &pgproto3.Execute{},
			wantExp:      query.ExecuteReply,
		},
		{
			name:         "simple flush",
			msg:          &pgproto3.Flush{},
			proxyMsgToPg: &pgproto3.Flush{},
			wantFlush:    true,
		},
		{
			name:         "simple sync",
			msg:          &pgproto3.Sync{},
			proxyMsgToPg: &pgproto3.Sync{},
			wantExp:      query.SyncReply,
			wantFlush:    true,
			afterFlushAction: &ClientMessageAction{
				DiscardUntilSync: false,
			},
		},
		{
			name:         "simple copy data",
			msg:          &pgproto3.CopyData{},
			proxyMsgToPg: &pgproto3.CopyData{},
			wantExp:      query.NoReply,
		},
		{
			name:         "simple copy done",
			msg:          &pgproto3.CopyDone{},
			proxyMsgToPg: &pgproto3.CopyDone{},
			wantExp:      query.QueryReply,
			wantFlush:    true,
		},
		{
			name:         "simple copy fail",
			msg:          &pgproto3.CopyFail{},
			proxyMsgToPg: &pgproto3.CopyFail{},
			wantExp:      query.NoReply,
		},
		{
			name:            "unexpected FunctionCall",
			msg:             &pgproto3.FunctionCall{},
			wantErrContains: "received unsupported message from client",
		},
		{
			name:                          "parse unnamed statement with autoCached enabled",
			msg:                           &pgproto3.Parse{Query: "SELECT * FROM users WHERE id = $1"},
			proxyMsgToPg:                  &pgproto3.Parse{Query: "SELECT * FROM users WHERE id = $1", Name: "843923b9a0257cf80f1dff554e7dc8fdfc05f489328e8376513124dfb42996e3"},
			dynamicRoutingEnabled:         true,
			autoCachedEnabled:             true,
			wantExp:                       query.ParseReply,
			wantTraceStarted:              true,
			wantNewPreparedStmt:           true,
			enableNamedPreparedStatements: true,
			newPreparedStmt:               prepared.CreatePreparedStatement("", "843923b9a0257cf80f1dff554e7dc8fdfc05f489328e8376513124dfb42996e3", "SELECT * FROM users WHERE id = $1", nil),
		},
		{
			name:              "close unnamed autocached prepared statement",
			msg:               &pgproto3.Close{ObjectType: 'S', Name: ""},
			autoCachedEnabled: true,
			proxyMsgToPg:      &pgproto3.Flush{},
			wantFlush:         true,
			afterFlushAction: &ClientMessageAction{
				ReplyMsgsToClient:      []pgproto3.BackendMessage{&pgproto3.CloseComplete{}},
				DeallocatePreparedStmt: nil, // nil because test case contains non-existent prepared stmt
			},
		},
	}

	for _, test := range tests {
		pStmtState := prepared.CreateProxyPreparedStmtsState([]string{})

		// Create a test sess with the required fields
		state := State{
			DynamicRoutingEnabled:    test.dynamicRoutingEnabled,
			AutoPreparedStmtsEnabled: test.autoCachedEnabled,
			PreparedStatements:       pStmtState,
		}

		action, err := DecideClientMessageAction(test.msg, state, Hooks{})
		if test.wantErrContains != "" {
			assert.Empty(action, "for test %s", test.name)
			assert.ErrorContains(err, test.wantErrContains, "for test %s", test.name)
		} else if test.wantErrIs != nil {
			assert.Empty(action, "for test %s", test.name)
			assert.ErrorIs(err, test.wantErrIs, "for test %s", test.name)
		} else {
			wantAction := ClientMessageAction{}
			if test.proxyMsgToPg != nil {
				wantAction.ProxyMsgToPg = test.proxyMsgToPg
			}
			if test.wantExp != query.NoReply {
				wantAction.AddReplyExpectation = test.wantExp
			}
			if test.wantFlush {
				wantAction.FlushPostgres = true
			}
			if test.afterFlushAction != nil {
				wantAction.AfterFlushAction = test.afterFlushAction
			}
			if test.wantReply {
				wantAction.ReplyMsgsToClient = test.replyMsgs
			}
			if test.wantNewPreparedStmt {
				wantAction.RecordNewPreparedStmt = test.newPreparedStmt
			}
			if test.wantDeallocate {
				wantAction.DeallocatePreparedStmt = test.deallocatePreparedStmt
			}
			wantAction.EnsureUnnamedPreparedStmt = test.wantEnsureUnnamed
			wantAction.InvalidateUnnamedPreparedStmt = test.wantInvalidateUnnamed
			wantAction.HandleReadQuery = test.wantHandleReadQuery
			assert.EqualValues(wantAction, action, "for test %s", test.name)
		}
	}
}

func TestSyncAfterLoadSheddingDiscard(t *testing.T) {
	assert := assert.New(t)
	messageObserved := false

	state := State{
		DiscardUntilSync:   true,
		PreparedStatements: prepared.CreateProxyPreparedStmtsState([]string{}),
	}

	action, err := DecideClientMessageAction(&pgproto3.Sync{}, state, Hooks{
		MessageObserved: func(pgproto3.FrontendMessage) {
			messageObserved = true
		},
	})
	assert.NoError(err)
	assert.Equal(ClientMessageAction{
		ReplyMsgsToClient: []pgproto3.BackendMessage{&pgproto3.ReadyForQuery{TxStatus: 'I'}},
		DiscardUntilSync:  false,
	}, action)
	assert.False(messageObserved, "no observation hook should run for the Sync fast-path")
}

// Different SQL/OID pairs must not share a proxy name, or the proxy would
// reuse one statement for another.
func TestCreateProxyPreparedStmtNameKeepsPairsApart(t *testing.T) {
	assert.NotEqual(t, CreateProxyPreparedStmtName("SELECT $1, $2", []uint32{16, 38400}),
		CreateProxyPreparedStmtName("SELECT $1, $2", []uint32{16384, 0}))
	assert.NotEqual(t, CreateProxyPreparedStmtName("SELECT 1", []uint32{23}),
		CreateProxyPreparedStmtName("SELECT 12", []uint32{3}))
	assert.NotEqual(t, CreateProxyPreparedStmtName("SELECT $1", nil),
		CreateProxyPreparedStmtName("SELECT $1", []uint32{0}), "a declared unspecified type is still declared")
}

func TestNewFlushErrorAction(t *testing.T) {
	response := BuildPreparedStatementDoesNotExistErrorResponse("missing")
	action := NewFlushErrorAction(response)
	assert.Equal(t, &pgproto3.Flush{}, action.ProxyMsgToPg)
	assert.True(t, action.FlushPostgres)
	assert.Empty(t, action.ReplyMsgsToClient, "earlier backend errors take precedence")
	require.NotNil(t, action.AfterFlushAction)
	assert.Same(t, response, action.AfterFlushAction.AbortTransactionWithError)
	assert.Empty(t, action.AfterFlushAction.ReplyMsgsToClient, "error is sent only after the backend fails")

	var observed *pgproto3.ErrorResponse
	observeErrorAction(&action, Hooks{ErrorActionCreated: func(response *pgproto3.ErrorResponse, err error) {
		assert.NoError(t, err)
		observed = response
	}})
	assert.Same(t, response, observed)
}

func TestFindPotentiallyExistingPreparedStatement(t *testing.T) {
	tests := []struct {
		testName             string
		clientName           string
		pendingStmts         []prepared.PreparedStatement
		confirmedStmts       []prepared.PreparedStatement
		expectedProxyName    string
		expectedPreparedStmt prepared.PreparedStatement
		expectErrorAction    bool
	}{
		{
			testName:          "empty preparedStatementState",
			clientName:        "client_name",
			pendingStmts:      []prepared.PreparedStatement{},
			confirmedStmts:    []prepared.PreparedStatement{},
			expectErrorAction: true,
		},
		{
			testName:   "no matching client name",
			clientName: "client_name",
			pendingStmts: []prepared.PreparedStatement{
				prepared.CreatePreparedStatement("no_match", "proxy_name", "", []uint32{}),
			},
			confirmedStmts: []prepared.PreparedStatement{
				prepared.CreatePreparedStatement("no_match_2", "proxy_name", "", []uint32{}),
			},
			expectErrorAction: true,
		},
		{
			testName:   "matching pending named prepared stmt",
			clientName: "client_name",
			pendingStmts: []prepared.PreparedStatement{
				prepared.CreatePreparedStatement("client_name", "proxy_name", "", []uint32{}),
			},
			confirmedStmts:    []prepared.PreparedStatement{},
			expectedProxyName: "proxy_name",
		},
		{
			testName:   "matching pending named prepared stmt, non-empty confirmed stmts",
			clientName: "client_name",
			pendingStmts: []prepared.PreparedStatement{
				prepared.CreatePreparedStatement("client_name", "proxy_name", "", []uint32{}),
			},
			confirmedStmts: []prepared.PreparedStatement{
				prepared.CreatePreparedStatement("no_match", "proxy_name", "", []uint32{}),
			},
			expectedProxyName: "proxy_name",
		},
		{
			testName:     "matching confirmed named prepared stmt",
			clientName:   "client_name",
			pendingStmts: []prepared.PreparedStatement{},
			confirmedStmts: []prepared.PreparedStatement{
				prepared.CreatePreparedStatement("client_name", "proxy_name", "", []uint32{}),
			},
			expectedProxyName:    "proxy_name",
			expectedPreparedStmt: prepared.CreatePreparedStatement("client_name", "proxy_name", "", []uint32{}),
		},
		{
			testName:   "matching confirmed named prepared stmt, non-empty pending stmts",
			clientName: "client_name",
			pendingStmts: []prepared.PreparedStatement{
				prepared.CreatePreparedStatement("no_match", "proxy_name", "", []uint32{}),
			},
			confirmedStmts: []prepared.PreparedStatement{
				prepared.CreatePreparedStatement("client_name", "proxy_name", "", []uint32{}),
			},
			expectedProxyName:    "proxy_name",
			expectedPreparedStmt: prepared.CreatePreparedStatement("client_name", "proxy_name", "", []uint32{}),
		},
		{
			testName:   "matching pending unnamed prepared stmt with autoCached",
			clientName: prepared.UnnamedStmtName,
			pendingStmts: []prepared.PreparedStatement{
				prepared.CreatePreparedStatement(prepared.UnnamedStmtName, "proxy_name", "SELECT * FROM test", []uint32{}),
			},
			confirmedStmts:    []prepared.PreparedStatement{},
			expectedProxyName: "proxy_name",
			expectErrorAction: false,
		},
		{
			testName:     "matching confirmed unnamed prepared stmt with autoCached",
			clientName:   prepared.UnnamedStmtName,
			pendingStmts: []prepared.PreparedStatement{},
			confirmedStmts: []prepared.PreparedStatement{
				prepared.CreatePreparedStatement(prepared.UnnamedStmtName, "proxy_name", "SELECT * FROM test", []uint32{}),
			},
			expectedProxyName:    "proxy_name",
			expectedPreparedStmt: prepared.CreatePreparedStatement(prepared.UnnamedStmtName, "proxy_name", "SELECT * FROM test", []uint32{}),
			expectErrorAction:    false,
		},
		{
			testName:   "multiple pending unnamed prepared stmts with autoCached",
			clientName: prepared.UnnamedStmtName,
			pendingStmts: []prepared.PreparedStatement{
				prepared.CreatePreparedStatement(prepared.UnnamedStmtName, "proxy_name1", "SELECT * FROM test1", []uint32{}),
				prepared.CreatePreparedStatement(prepared.UnnamedStmtName, "proxy_name2", "SELECT * FROM test2", []uint32{}),
			},
			confirmedStmts:    []prepared.PreparedStatement{},
			expectedProxyName: "proxy_name2", // Should match the last one in the queue
			expectErrorAction: false,
		},
		{
			testName:   "mix of unnamed and named prepared stmts with autoCached",
			clientName: prepared.UnnamedStmtName,
			pendingStmts: []prepared.PreparedStatement{
				prepared.CreatePreparedStatement("named_stmt", "proxy_named", "SELECT * FROM named", []uint32{}),
				prepared.CreatePreparedStatement(prepared.UnnamedStmtName, "proxy_unnamed", "SELECT * FROM unnamed", []uint32{}),
			},
			confirmedStmts:    []prepared.PreparedStatement{},
			expectedProxyName: "proxy_unnamed",
			expectErrorAction: false,
		},
		{
			testName:     "mix of confirmed unnamed and named prepared stmts with autoCached",
			clientName:   prepared.UnnamedStmtName,
			pendingStmts: []prepared.PreparedStatement{},
			confirmedStmts: []prepared.PreparedStatement{
				prepared.CreatePreparedStatement("named_stmt", "proxy_named", "SELECT * FROM named", []uint32{}),
				prepared.CreatePreparedStatement(prepared.UnnamedStmtName, "proxy_unnamed", "SELECT * FROM unnamed", []uint32{}),
			},
			expectedProxyName:    "proxy_unnamed",
			expectedPreparedStmt: prepared.CreatePreparedStatement(prepared.UnnamedStmtName, "proxy_unnamed", "SELECT * FROM unnamed", []uint32{}),
			expectErrorAction:    false,
		},
	}

	for _, test := range tests {
		preparedStatementState := prepared.CreateProxyPreparedStmtsState([]string{})
		for _, stmt := range test.confirmedStmts {
			preparedStatementState.EnqueuePendingStmt(stmt, false)
			preparedStatementState.ConfirmPendingStmt()
		}
		for _, stmt := range test.pendingStmts {
			preparedStatementState.EnqueuePendingStmt(stmt, false)
		}
		actualProxyName, actualPreparedStmt, actualErrorAction := FindPotentiallyExistingPreparedStatement(test.clientName, preparedStatementState)
		assert.Equalf(t, test.expectedProxyName, actualProxyName, "for test %s", test.testName)
		assert.Truef(t, prepared.PreparedStatementEquivalent(test.expectedPreparedStmt, actualPreparedStmt), "for test %s", test.testName)
		if test.expectErrorAction {
			assert.NotNilf(t, actualErrorAction, "for test %s", test.testName)
		} else {
			assert.Nilf(t, actualErrorAction, "for test %s", test.testName)
		}
	}
}

// A held link covers both buffered protocol work and active/failed transactions.
func TestPingShortcutBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name                        string
		msg                         pgproto3.FrontendMessage
		hasLink, discard, autoCache bool
		wantExpectation             query.ReplyExpectation
	}{
		{name: "idle", msg: &pgproto3.Query{String: "-- ping"}},
		{name: "held_link", msg: &pgproto3.Query{String: "-- ping"}, hasLink: true, wantExpectation: query.QueryReply},
		{name: "unnamed_parse", msg: &pgproto3.Parse{Query: "-- ping"}, wantExpectation: query.ParseReply},
		{name: "named_parse", msg: &pgproto3.Parse{Name: "ping", Query: "-- ping"}, wantExpectation: query.ParseReply},
		{name: "auto_cached_parse", msg: &pgproto3.Parse{Query: "-- ping"}, autoCache: true, wantExpectation: query.ParseReply},
		{name: "recovery_without_link", msg: &pgproto3.Query{String: "-- ping"}, discard: true},
		{name: "recovery_with_link", msg: &pgproto3.Query{String: "-- ping"}, hasLink: true, discard: true},
		{name: "parse_during_recovery", msg: &pgproto3.Parse{Query: "-- ping"}, hasLink: true, discard: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			action, err := DecideClientMessageAction(tc.msg, State{
				HasLink: tc.hasLink, DiscardUntilSync: tc.discard,
				AutoPreparedStmtsEnabled: tc.autoCache,
				PreparedStatements:       prepared.CreateProxyPreparedStmtsState(nil),
			}, Hooks{})
			assert.NoError(t, err)
			// The session's shedder bypass must agree with the local-reply decision.
			repliedLocally := !tc.discard && tc.wantExpectation == query.NoReply
			assert.Equal(t, repliedLocally, IsIdlePGXPing(tc.msg, State{HasLink: tc.hasLink, DiscardUntilSync: tc.discard}))
			if tc.discard {
				// Discarded messages do not reach Postgres, so they keep the unnamed statement.
				assert.Equal(t, ClientMessageAction{DiscardUntilSync: true}, action)
			} else if tc.wantExpectation != query.NoReply {
				assert.Equal(t, tc.msg, action.ProxyMsgToPg)
				assert.Equal(t, tc.wantExpectation, action.AddReplyExpectation)
				assert.Empty(t, action.ReplyMsgsToClient)
				assert.Equal(t, tc.wantExpectation == query.QueryReply, action.FlushPostgres)
				_, simpleQuery := tc.msg.(*pgproto3.Query)
				assert.Equal(t, simpleQuery, action.InvalidateUnnamedPreparedStmt)
			} else {
				// Answered locally, but a simple Query still destroys the unnamed statement.
				assert.Equal(t, ClientMessageAction{ReplyMsgsToClient: []pgproto3.BackendMessage{
					&pgproto3.EmptyQueryResponse{}, &pgproto3.ReadyForQuery{TxStatus: 'I'},
				}, InvalidateUnnamedPreparedStmt: true}, action)
			}
		})
	}
}
