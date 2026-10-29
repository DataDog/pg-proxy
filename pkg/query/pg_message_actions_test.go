package query

import (
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
)

func TestPgMessageActions(t *testing.T) {
	assert := assert.New(t)

	tests := []struct {
		name              string
		msg               pgproto3.BackendMessage
		expectation       ReplyExpectation
		wantTraceFinished bool
		// One of the following should be set:
		wantProxyAndResolve    bool
		wantJustProxy          bool
		wantErrContains        string
		wantAction             PgMessageAction
		wantAddNewPreparedStmt bool
		wantDeallocate         bool
	}{
		{
			name:                   "simple parse complete",
			msg:                    &pgproto3.ParseComplete{},
			expectation:            ParseReply,
			wantProxyAndResolve:    true,
			wantAddNewPreparedStmt: true,
		},
		{
			name:            "unexpected parse complete",
			msg:             &pgproto3.ParseComplete{},
			expectation:     QueryReply,
			wantErrContains: "expected a QueryReply",
		},
		{
			name:                "simple bind complete",
			msg:                 &pgproto3.BindComplete{},
			expectation:         BindReply,
			wantProxyAndResolve: true,
		},
		{
			name:            "unexpected bind complete",
			msg:             &pgproto3.BindComplete{},
			expectation:     QueryReply,
			wantErrContains: "expected a QueryReply",
		},
		{
			name:                "simple close complete",
			msg:                 &pgproto3.CloseComplete{},
			expectation:         CloseReply,
			wantProxyAndResolve: true,
			wantDeallocate:      true,
		},
		{
			name:            "unexpected close complete",
			msg:             &pgproto3.CloseComplete{},
			expectation:     QueryReply,
			wantErrContains: "expected a QueryReply",
		},
		{
			name:                "command complete for execution",
			msg:                 &pgproto3.CommandComplete{CommandTag: []byte{1, 2, 3, 4, 5}},
			expectation:         ExecuteReply,
			wantProxyAndResolve: true,
		},
		{
			name:          "command complete within query",
			msg:           &pgproto3.CommandComplete{},
			expectation:   QueryReply,
			wantJustProxy: true,
		},
		{
			name:            "unexpected command complete",
			msg:             &pgproto3.CommandComplete{},
			expectation:     ParseReply,
			wantErrContains: "expected a ParseReply",
		},
		{
			name:                "empty response for execution",
			msg:                 &pgproto3.EmptyQueryResponse{},
			expectation:         ExecuteReply,
			wantProxyAndResolve: true,
		},
		{
			name:          "empty response within query",
			msg:           &pgproto3.EmptyQueryResponse{},
			expectation:   QueryReply,
			wantJustProxy: true,
		},
		{
			name:            "unexpected empty response complete",
			msg:             &pgproto3.EmptyQueryResponse{},
			expectation:     ParseReply,
			wantErrContains: "expected a ParseReply",
		},
		{
			name:                "row description for describe",
			msg:                 &pgproto3.RowDescription{},
			expectation:         DescribeReply,
			wantProxyAndResolve: true,
		},
		{
			name:          "row description within query",
			msg:           &pgproto3.RowDescription{},
			expectation:   QueryReply,
			wantJustProxy: true,
		},
		{
			name:            "unexpected row description complete",
			msg:             &pgproto3.RowDescription{},
			expectation:     ParseReply,
			wantErrContains: "expected a ParseReply",
		},
		{
			name:                "no data for describe",
			msg:                 &pgproto3.NoData{},
			expectation:         DescribeReply,
			wantProxyAndResolve: true,
		},
		{
			name:            "unexpected no data",
			msg:             &pgproto3.NoData{},
			expectation:     QueryReply,
			wantErrContains: "expected a QueryReply",
		},
		{
			name:          "parameter description within describe",
			msg:           &pgproto3.ParameterDescription{ParameterOIDs: []uint32{1, 2, 3, 4, 5, 6}},
			expectation:   DescribeReply,
			wantJustProxy: true,
		},
		{
			name:            "unexpected parameter description",
			msg:             &pgproto3.ParameterDescription{},
			expectation:     QueryReply,
			wantErrContains: "expected a QueryReply",
		},
		{
			name:                "portal suspended for execution",
			msg:                 &pgproto3.PortalSuspended{},
			expectation:         ExecuteReply,
			wantProxyAndResolve: true,
		},
		{
			name:            "unexpected portal suspended",
			msg:             &pgproto3.PortalSuspended{},
			expectation:     QueryReply,
			wantErrContains: "expected a QueryReply",
		},
		{
			name:          "data row for simple query",
			msg:           &pgproto3.DataRow{Values: [][]byte{[]byte("test")}},
			expectation:   QueryReply,
			wantJustProxy: true,
		},
		{
			name:          "data row for extended query",
			msg:           &pgproto3.DataRow{Values: [][]byte{[]byte("test2")}},
			expectation:   ExecuteReply,
			wantJustProxy: true,
		},
		{
			name:            "unexpected data row",
			msg:             &pgproto3.DataRow{},
			expectation:     ParseReply,
			wantErrContains: "expected a ParseReply",
		},
		{
			name:        "ready for query after simple, idle",
			msg:         &pgproto3.ReadyForQuery{TxStatus: 'I'},
			expectation: QueryReply,
			wantAction: PgMessageAction{
				ProxyMsgToClient:        &pgproto3.ReadyForQuery{TxStatus: 'I'},
				ResolveReplyExpectation: true,
				ReleaseLinkIfIdle:       true,
				SetTxStatus:             Idle,
				ProxyMsgSize:            6,
			},
			wantTraceFinished: true,
		},
		{
			name:        "ready for query after extended, idle",
			msg:         &pgproto3.ReadyForQuery{TxStatus: 'I'},
			expectation: SyncReply,
			wantAction: PgMessageAction{
				ProxyMsgToClient:        &pgproto3.ReadyForQuery{TxStatus: 'I'},
				ResolveReplyExpectation: true,
				ReleaseLinkIfIdle:       true,
				SetTxStatus:             Idle,
				ProxyMsgSize:            6,
			},
			wantTraceFinished: true,
		},
		{
			name:        "ready for query after simple, in txn",
			msg:         &pgproto3.ReadyForQuery{TxStatus: 'T'},
			expectation: QueryReply,
			wantAction: PgMessageAction{
				ProxyMsgToClient:        &pgproto3.ReadyForQuery{TxStatus: 'T'},
				ResolveReplyExpectation: true,
				ReleaseLinkIfIdle:       true,
				SetTxStatus:             InTransaction,
				ProxyMsgSize:            6,
			},
			wantTraceFinished: true,
		},
		{
			name:        "ready for query after simple, in err txn",
			msg:         &pgproto3.ReadyForQuery{TxStatus: 'E'},
			expectation: QueryReply,
			wantAction: PgMessageAction{
				ProxyMsgToClient:        &pgproto3.ReadyForQuery{TxStatus: 'E'},
				ResolveReplyExpectation: true,
				ReleaseLinkIfIdle:       true,
				SetTxStatus:             InErroredTransaction,
				ProxyMsgSize:            6,
			},
			wantTraceFinished: true,
		},
		{
			name:            "bad tx status in ready for query",
			msg:             &pgproto3.ReadyForQuery{TxStatus: '?'},
			expectation:     QueryReply,
			wantErrContains: "failed to handle ReadyForQuery: unexpected TxStatus: ?",
		},
		{
			name:            "empty tx status in ready for query",
			msg:             &pgproto3.ReadyForQuery{},
			expectation:     QueryReply,
			wantErrContains: "failed to handle ReadyForQuery: unexpected TxStatus:",
		},
		{
			name:            "unexpected ready for query",
			msg:             &pgproto3.ReadyForQuery{TxStatus: 'I'},
			expectation:     ParseReply,
			wantErrContains: "expected a ParseReply",
		},
		{
			name:          "notice during any exp, e.g. ParseReply",
			msg:           &pgproto3.NoticeResponse{},
			expectation:   ParseReply,
			wantJustProxy: true,
		},
		{
			name:          "notice during any exp, e.g. QueryReply",
			msg:           &pgproto3.NoticeResponse{},
			expectation:   QueryReply,
			wantJustProxy: true,
		},
		{
			name:          "parameter status during any exp, e.g. ParseReply",
			msg:           &pgproto3.ParameterStatus{},
			expectation:   ParseReply,
			wantJustProxy: true,
		},
		{
			name:          "parameter status during any exp, e.g. QueryReply",
			msg:           &pgproto3.ParameterStatus{},
			expectation:   QueryReply,
			wantJustProxy: true,
		},
		{
			name:              "fatal error response",
			msg:               &pgproto3.ErrorResponse{Severity: "FATAL", Message: "testing"},
			expectation:       QueryReply,
			wantErrContains:   "FATAL: testing",
			wantTraceFinished: true,
		},
		{
			name:        "extended query error response, e.g. for ParseReply",
			msg:         &pgproto3.ErrorResponse{Message: "testing"},
			expectation: ParseReply,
			wantAction: PgMessageAction{
				ProxyMsgToClient: &pgproto3.ErrorResponse{Message: "testing"},
				DiscardUntilSync: true,
				ProxyMsgSize:     15,
			},
			wantTraceFinished: true,
		},
		{
			name:        "extended query error response, e.g. for BindReply",
			msg:         &pgproto3.ErrorResponse{Message: "testing"},
			expectation: BindReply,
			wantAction: PgMessageAction{
				ProxyMsgToClient: &pgproto3.ErrorResponse{Message: "testing"},
				DiscardUntilSync: true,
				ProxyMsgSize:     15,
			},
			wantTraceFinished: true,
		},
		{
			name:              "simple query error response",
			msg:               &pgproto3.ErrorResponse{Message: "testing"},
			expectation:       QueryReply,
			wantJustProxy:     true,
			wantTraceFinished: true,
		},
		{
			name:              "commit error response to sync",
			msg:               &pgproto3.ErrorResponse{Message: "testing"},
			expectation:       SyncReply,
			wantJustProxy:     true,
			wantTraceFinished: true,
		},
		{
			name:          "simple copy data",
			msg:           &pgproto3.CopyData{},
			expectation:   QueryReply,
			wantJustProxy: true,
		},
		{
			name:            "unexpected copy data",
			msg:             &pgproto3.CopyData{},
			expectation:     SyncReply,
			wantErrContains: "expected a SyncReply",
		},
		{
			name:          "simple copy done",
			msg:           &pgproto3.CopyDone{},
			expectation:   QueryReply,
			wantJustProxy: true,
		},
		{
			name:            "unexpected copy done",
			msg:             &pgproto3.CopyDone{},
			expectation:     SyncReply,
			wantErrContains: "expected a SyncReply",
		},
		{
			name:                "simple copy in response",
			msg:                 &pgproto3.CopyInResponse{},
			expectation:         QueryReply,
			wantProxyAndResolve: true,
		},
		{
			name:            "unexpected copy in response",
			msg:             &pgproto3.CopyInResponse{},
			expectation:     SyncReply,
			wantErrContains: "expected a SyncReply",
		},
		{
			name:          "simple copy out response",
			msg:           &pgproto3.CopyOutResponse{},
			expectation:   QueryReply,
			wantJustProxy: true,
		},
		{
			name:            "unexpected copy out response",
			msg:             &pgproto3.CopyOutResponse{},
			expectation:     SyncReply,
			wantErrContains: "expected a SyncReply",
		},
		{
			name:            "unsupported CopyBothResponse",
			msg:             &pgproto3.CopyBothResponse{},
			expectation:     QueryReply,
			wantErrContains: "received unsupported message",
		},
		{
			name:            "unsupported FunctionCallResponse",
			msg:             &pgproto3.FunctionCallResponse{},
			expectation:     QueryReply,
			wantErrContains: "received unsupported message",
		},
		{
			name:            "unsupported NotificationResponse",
			msg:             &pgproto3.NotificationResponse{},
			expectation:     QueryReply,
			wantErrContains: "received unsupported message",
		},
	}

	for _, test := range tests {
		traceState := &TraceState{}
		action, err := DecidePgMessageAction(test.msg, test.expectation, traceState)
		if test.wantErrContains != "" {
			assert.Empty(action, "for test %s", test.name)
			assert.ErrorContains(err, test.wantErrContains, "for test %s", test.name)
		} else if test.wantProxyAndResolve {
			assert.NoError(err, "for test %s", test.name)
			wantAction := PgMessageAction{
				ProxyMsgToClient:        test.msg,
				ResolveReplyExpectation: true,
				ProxyMsgSize:            getMessageSize(test.msg),
				AddNewPreparedStatement: test.wantAddNewPreparedStmt,
			}
			assert.EqualValues(wantAction, action, "for test %s", test.name)
		} else if test.wantJustProxy {
			assert.NoError(err, "for test %s", test.name)
			wantAction := PgMessageAction{
				ProxyMsgToClient:        test.msg,
				ProxyMsgSize:            getMessageSize(test.msg),
				AddNewPreparedStatement: test.wantAddNewPreparedStmt,
			}
			assert.EqualValues(wantAction, action, "for test %s", test.name)
		} else {
			assert.NoError(err, "for test %s", test.name)
			assert.EqualValues(test.wantAction, action, "for test %s", test.name)
		}
	}
}

func TestIsSwitchoverSignature(t *testing.T) {
	tests := []struct {
		name             string
		in               *pgproto3.ErrorResponse
		expectSwitchover bool
	}{
		{
			name:             "25006 cannot execute INSERT — matches",
			in:               &pgproto3.ErrorResponse{Code: "25006", Message: "cannot execute INSERT in a read-only transaction"},
			expectSwitchover: true,
		},
		{
			name:             "25006 cannot execute UPDATE — matches",
			in:               &pgproto3.ErrorResponse{Code: "25006", Message: "cannot execute UPDATE in a read-only transaction"},
			expectSwitchover: true,
		},
		{
			name:             "57P03 shutting down — matches",
			in:               &pgproto3.ErrorResponse{Code: "57P03", Message: "the database system is shutting down"},
			expectSwitchover: true,
		},
		{
			name:             "57P03 starting up — does not match (steady-state standby, not switchover)",
			in:               &pgproto3.ErrorResponse{Code: "57P03", Message: "the database system is starting up"},
			expectSwitchover: false,
		},
		{
			name:             "57P01 admin shutdown — matches",
			in:               &pgproto3.ErrorResponse{Code: "57P01", Message: "terminating connection due to administrator command"},
			expectSwitchover: true,
		},
		{
			name:             "57P01 with unrelated message — does not match",
			in:               &pgproto3.ErrorResponse{Code: "57P01", Message: "some other admin shutdown reason"},
			expectSwitchover: false,
		},
		{
			name:             "25006 with unrelated message — does not match",
			in:               &pgproto3.ErrorResponse{Code: "25006", Message: "some other read-only problem"},
			expectSwitchover: false,
		},
		{
			name:             "different SQLSTATE — does not match",
			in:               &pgproto3.ErrorResponse{Code: "42501", Message: "permission denied"},
			expectSwitchover: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expectSwitchover, IsSwitchoverSignature(tc.in.Code, tc.in.Message))
		})
	}
}
