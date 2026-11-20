// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package query

import (
	"fmt"
	"strings"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// LeaderSwitchoverHint is appended to error messages that match IsSwitchoverSignature,
// both when annotating a proxied ErrorResponse and when building a FATAL response to
// the client directly (see closeErrorResponse in pkg/session).
const LeaderSwitchoverHint = " (a PostgreSQL leader switchover may be in progress; retry shortly)"

// IsSwitchoverSignature matches the three switchover SQLSTATEs by code+message.
// Message check is required: same code can mean different root causes (e.g. 57P03 "starting up" vs "shutting down").
func IsSwitchoverSignature(code, message string) bool {
	switch code {
	case pgerrcode.ReadOnlySQLTransaction:
		return strings.HasPrefix(message, "cannot execute") && strings.Contains(message, "in a read-only transaction")
	case pgerrcode.AdminShutdown:
		return strings.Contains(message, "terminating connection due to administrator command")
	case pgerrcode.CannotConnectNow:
		return strings.Contains(message, "the database system is shutting down")
	}
	return false
}

// annotateIfSwitchoverError appends the hint to a non-FATAL ErrorResponse in place.
func annotateIfSwitchoverError(m *pgproto3.ErrorResponse) {
	if IsSwitchoverSignature(m.Code, m.Message) {
		m.Message += LeaderSwitchoverHint
	}
}

// PgMessageAction contains a set of actions the proxy will take to handle
// a BackendMessage received from Postgres to a Session's linked connection.
type PgMessageAction struct {
	// If ProxyMsgToClient is non-nil, the contained message will be proxied
	// to the client connected to this Session.
	ProxyMsgToClient pgproto3.BackendMessage
	// If ResolveReplyExpectation is true, the next reply expectation on the
	// SessionLink is now resolved.
	ResolveReplyExpectation bool
	// If DiscardUntilSync is true, we've encountered an extended query mode
	// error condition and need to discard all reply expectations until the
	// next client Sync. See SessionLink.DiscardUntilSync() for more detail.
	DiscardUntilSync bool
	// If SetTxStatus is not NoTxStatus, update the TxStatus of the SessionLink.
	SetTxStatus TxStatus
	// If ReleaseLinkIfIdle is true, we should check if the SessionLink is now
	// idle and release it if so. This flag is necessary because there are some
	// situations that a SessionLink might be "idle", but not releasable (e.g.
	// after a client sends a FlushMessage). We essentially only want to release
	// after Postgres sends back a ReadyForQuery{I} reply.
	ReleaseLinkIfIdle bool
	// Add a new prepared statement to the list of successfully parsed named prepared statements.
	AddNewPreparedStatement bool
}

// DecidePgMessageAction returns the set of actions to take to handle a message received
// from Postgres to a Session's linked connection. This function also requires the next
// reply expectation, to determine if the message resolves the expectation or not.
// (See PgMessageAction docs for the set of actions.) It does not record rows: a
// caller that forwards a DataRow reports it with traceState.AddRow.
func DecidePgMessageAction(msg pgproto3.BackendMessage, expectation ReplyExpectation, traceState *TraceState) (PgMessageAction, error) {
	action := PgMessageAction{
		ProxyMsgToClient: msg, // currently, we always transparently proxy the msg back to the client.
	}
	traceState.AddMessage(msg)

	switch m := msg.(type) {
	case *pgproto3.ParseComplete:
		if expectation == ParseReply {
			action.ResolveReplyExpectation = true
		} else {
			return PgMessageAction{}, fmt.Errorf("expected a %s, but received %#v", expectation, m)
		}
		action.AddNewPreparedStatement = true

	case *pgproto3.BindComplete:
		if expectation == BindReply {
			action.ResolveReplyExpectation = true
		} else {
			return PgMessageAction{}, fmt.Errorf("expected a %s, but received %#v", expectation, m)
		}

	case *pgproto3.CloseComplete:
		if expectation == CloseReply {
			action.ResolveReplyExpectation = true
		} else {
			return PgMessageAction{}, fmt.Errorf("expected a %s, but received %#v", expectation, m)
		}

	case *pgproto3.CommandComplete:
		if expectation == ExecuteReply {
			action.ResolveReplyExpectation = true
		} else if expectation == QueryReply {
			// don't clear expectation, this CommandComplete is part of the larger QueryReply.
		} else {
			return PgMessageAction{}, fmt.Errorf("expected a %s, but received %#v", expectation, m)
		}

	case *pgproto3.EmptyQueryResponse:
		if expectation == ExecuteReply {
			action.ResolveReplyExpectation = true
		} else if expectation == QueryReply {
			// don't clear expectation, this EmptyQueryResponse is part of the larger QueryReply.
		} else {
			return PgMessageAction{}, fmt.Errorf("expected a %s, but received %#v", expectation, m)
		}

	case *pgproto3.RowDescription:
		// In case we have a Batch, we group the exchange of message in the "Batch Execution" trace
		if !traceState.isBatch {
			traceState.FinishTrace()
			traceState.StartTrace(DataRoundTrips)
		}
		if expectation == DescribeReply {
			action.ResolveReplyExpectation = true
		} else if expectation == QueryReply {
			// don't clear expectation, this RowDescription is part of the larger QueryReply.
		} else {
			return PgMessageAction{}, fmt.Errorf("expected a %s, but received %#v", expectation, m)
		}

	case *pgproto3.NoData:
		if expectation == DescribeReply {
			action.ResolveReplyExpectation = true
		} else {
			return PgMessageAction{}, fmt.Errorf("expected a %s, but received %#v", expectation, m)
		}

	case *pgproto3.ParameterDescription:
		if expectation == DescribeReply {
			// don't clear expectation, this ParameterDescription is part of a larger DescribeReply
		} else {
			return PgMessageAction{}, fmt.Errorf("expected a %s, but received %#v", expectation, m)
		}

	case *pgproto3.PortalSuspended:
		if expectation == ExecuteReply {
			action.ResolveReplyExpectation = true
		} else {
			return PgMessageAction{}, fmt.Errorf("expected a %s, but received %#v", expectation, m)
		}

	case *pgproto3.DataRow:
		if expectation == ExecuteReply || expectation == QueryReply {
			// don't clear expectation, this DataRow is part of a larger ExecuteReply or QueryReply
		} else {
			return PgMessageAction{}, fmt.Errorf("expected a %s, but received %#v", expectation, m)
		}

	case *pgproto3.ReadyForQuery:
		if expectation == QueryReply || expectation == SyncReply {
			var err error
			action.SetTxStatus, err = TxStatusFromByte(m.TxStatus)
			if err != nil {
				return PgMessageAction{}, fmt.Errorf("failed to handle ReadyForQuery: %w", err)
			}
			action.ReleaseLinkIfIdle = true
			action.ResolveReplyExpectation = true
		} else {
			return PgMessageAction{}, fmt.Errorf("expected a %s, but received %#v", expectation, m)
		}

	case *pgproto3.NoticeResponse:
		// A Notice can occur at any time. It is not part of any expectation. Proxy it on.

	case *pgproto3.ParameterStatus:
		// A ParameterStatus update can occur at any time, or as part of a side effect from a client's SET query.
		// It is not part of any expectation. Proxy it on.

	case *pgproto3.ErrorResponse:
		traceState.FinishTrace(fmt.Errorf("code %s: %s", m.Code, m.Message))
		if m.Severity == "FATAL" {
			// FATAL always closes the session; annotation happens in closeErrorResponse()
			// to avoid double-appending the hint when the error is forwarded to the client.
			return PgMessageAction{}, pgconn.ErrorResponseToPgError(m)
		}
		annotateIfSwitchoverError(m)
		if expectation == ParseReply || expectation == BindReply || expectation == DescribeReply || expectation == ExecuteReply || expectation == CloseReply {
			// This error was in response to an Extended Query Mode message. Postgres will discard all messages until a sync
			action.DiscardUntilSync = true
		} else if expectation == QueryReply || expectation == SyncReply {
			// PostgreSQL still sends ReadyForQuery after a Query error or a commit
			// failure at Sync. Forward the error, but wait for ReadyForQuery before
			// treating the Query or Sync as complete.
		} else {
			return PgMessageAction{}, fmt.Errorf("expected a %s, but received %#v", expectation, m)
		}

	case *pgproto3.CopyData:
		if expectation == QueryReply {
			// don't clear expectation, this CopyData is part of a larger QueryReply
		} else {
			return PgMessageAction{}, fmt.Errorf("expected a %s, but received %#v", expectation, m)
		}

	case *pgproto3.CopyDone:
		if expectation == QueryReply {
			// don't clear expectation, this CopyDone is part of a larger QueryReply
		} else {
			return PgMessageAction{}, fmt.Errorf("expected a %s, but received %#v", expectation, m)
		}

	case *pgproto3.CopyInResponse:
		if expectation == QueryReply {
			action.ResolveReplyExpectation = true // must flush Query, so need to resolve expectation
		} else {
			return PgMessageAction{}, fmt.Errorf("expected a %s, but received %#v", expectation, m)
		}

	case *pgproto3.CopyOutResponse:
		if expectation == QueryReply {
			// don't clear expectation, this CopyOutResponse is part of a larger QueryReply
		} else {
			return PgMessageAction{}, fmt.Errorf("expected a %s, but received %#v", expectation, m)
		}

	case *pgproto3.CopyBothResponse:
		// we don't use CopyBoth for replication, so we should never see this
		return PgMessageAction{}, fmt.Errorf("received unsupported message from postgres: %#v", msg)

	case *pgproto3.FunctionCallResponse: // No plans to support, "a legacy feature that is probably best avoided in new code."
		return PgMessageAction{}, fmt.Errorf("received unsupported message from postgres: %#v", msg)

	case *pgproto3.NotificationResponse: // No plans to support LISTEN
		return PgMessageAction{}, fmt.Errorf("received unsupported message from postgres: %#v", msg)

	default:
		return PgMessageAction{}, fmt.Errorf("received unexpected message from postgres: %#v", msg)
	}

	return action, nil
}
