// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package session

import (
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"

	pgproxy "github.com/DataDog/pg-proxy"
	"github.com/DataDog/pg-proxy/pkg/actions"
	"github.com/DataDog/pg-proxy/pkg/classifier"
)

// ClientMessageAction is the portable action selected for a frontend message.
type ClientMessageAction = actions.ClientMessageAction

// PGQueryProtocolType represents the Postgres query protocol used by a client message.
type PGQueryProtocolType = actions.PGQueryProtocolType

const (
	SimpleQueryProtocol   = actions.SimpleQueryProtocol
	ExtendedQueryProtocol = actions.ExtendedQueryProtocol
)

// DecideClientMessageAction returns the set of actions to take to handle a message received
// from a client to their Session. (See ClientMessageAction docs for the set of actions.)
func (s *Session) DecideClientMessageAction(msg pgproto3.FrontendMessage) (ClientMessageAction, error) {
	// The parameter observer resolves statements from the same state the
	// decision uses, so both agree on how a name maps to a statement.
	state := s.actionState()
	return actions.DecideClientMessageAction(msg, state, actions.Hooks{
		MessageObserved: func(msg pgproto3.FrontendMessage) {
			s.obsVals.traceState.AddMessage(msg)
			if s.enableTraceParameters && observesClientMessage(msg) {
				s.observeClientMessage(msg, state)
			}
			if s.hooks.FrontendMessage != nil {
				s.hooks.FrontendMessage(msg)
			}
		},
		QueryObserved: func(_ string, _ actions.PGQueryProtocolType) {
			s.recordQueryObserved()
		},
		QueryClassified: func(sql string, protocol actions.PGQueryProtocolType, classification classifier.Classification) {
			s.recordQueryClassified(sql, protocol, classification)
		},
		QueryClassificationFailed: func(sql string, _ actions.PGQueryProtocolType, err error) {
			recordQueryClassificationFailed(sql, err)
		},
		QueryParseFinished: func(_ string, _ actions.PGQueryProtocolType, elapsed time.Duration) {
			s.recordQueryParseFinished(elapsed)
		},
		NamedPreparedStmtObserved: func(name, sql string) {
			recordNamedPreparedStatement(name, sql, s.obsVals.traceState, s.obsVals.clientServiceAccount)
		},
		ErrorActionCreated: logFlushError,
	})
}

// actionState snapshots the session state the actions layer decides on.
func (s *Session) actionState() actions.State {
	return actions.State{
		DiscardUntilSync:         s.discardUntilSync,
		HasLink:                  s.link != nil,
		DynamicRoutingEnabled:    s.dynamicRoutingEnabled,
		AutoPreparedStmtsEnabled: s.autoPreparedStatementsEnabled(),
		PreparedStatements:       s.pStmtState,
		UnnamedStmt:              s.unnamedStmt,
		HasUnnamedStmt:           s.unnamedStmt != nil || len(s.unnamedPendingGens) > 0,
	}
}

// NewFlushErrorAction returns a ClientMessageAction that ensures PostgreSQL flushes pending messages
// before sending an error response to the client.
func NewFlushErrorAction(response *pgproto3.ErrorResponse, err error) ClientMessageAction {
	logFlushError(response, err)
	return actions.NewFlushErrorAction(response)
}

func logFlushError(response *pgproto3.ErrorResponse, err error) {
	if err != nil {
		pgproxy.Logger().Error(fmt.Sprintf("%s - %s, err: %v", response.Message, response.Detail, err))
	} else {
		pgproxy.Logger().Error(fmt.Sprintf("%s - %s", response.Message, response.Detail))
	}
}
