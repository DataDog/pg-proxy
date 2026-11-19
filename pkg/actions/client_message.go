// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package actions

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"

	pgproxy "github.com/DataDog/pg-proxy"
	"github.com/DataDog/pg-proxy/pkg/classifier"
	"github.com/DataDog/pg-proxy/pkg/prepared"
	"github.com/DataDog/pg-proxy/pkg/query"
)

// ClientMessageAction contains a set of actions the proxy will take to handle
// a FrontendMessage received from a client to a Session.
type ClientMessageAction struct {
	// If ProxyMsgToPg is non-nil, the contained message will be proxied to the
	// Postgres server via the SessionLink connection.
	ProxyMsgToPg pgproto3.FrontendMessage
	// If AddReplyExpectation is anything other than `NoReply`, a "reply expectation"
	// will be added to the SessionLink. Once `FlushPostgres` is triggered, the
	// Session will block until *all* reply expectations are resolved.
	AddReplyExpectation query.ReplyExpectation
	// ReplyMsgsToClient is sent in order and flushed back to the client as one reply.
	// This allows the proxy to answer locally instead of proxying to Postgres.
	ReplyMsgsToClient []pgproto3.BackendMessage
	// If FlushPostgres is true, any buffered proxied messages are flushed on the
	// SessionLink connection to Postgres, and then the Session enters a blocking loop
	// to receive messages from the PostgresServer until *all* reply expectations are resolved.
	FlushPostgres bool
	// If HandleReadQuery is true, the SessionLink may acquire a connection with either a state
	// with which read traffic is distributed or a replica pool of a state's cluster
	HandleReadQuery bool
	// If AfterFlushAction is not nil and FlushPostgres is true, it will first flush all buffered messages to PG and then execute this action.
	// This is useful for sending ReplyMsgsToClient after processing all previously received messages.
	AfterFlushAction *ClientMessageAction
	// If DiscardUntilSync is true, the Session should proceed to discard all incoming messages from the client until a Sync is received.
	DiscardUntilSync bool
	// If RecordNewPreparedStmt is not nil, then on a non-error expected reply, check whether the SessionLink already
	// has this prepared statement created, otherwise add it to a queue of pending prepared statements to be created.
	RecordNewPreparedStmt prepared.PreparedStatement
	// If CheckExistingPreparedStmt is not nil, check that the SessionLink's connection has this prepared statement
	// created, otherwise create it.
	CheckExistingPreparedStmt prepared.PreparedStatement
	// If EnsureUnnamedPreparedStmt is true, the client's unnamed prepared statement is re-created on the
	// SessionLink's connection before the message is proxied, if that connection does not have it.
	EnsureUnnamedPreparedStmt bool
	// If InvalidateUnnamedPreparedStmt is true, the message destroys the client's unnamed prepared
	// statement in PostgreSQL: a simple Query, including one the proxy answers locally, or a Close
	// of the unnamed statement. The Session must forget it before replying or proxying the message.
	InvalidateUnnamedPreparedStmt bool
	// If DeallocatePreparedStmt is not nil, deallocate the prepared statement with the given client name.
	DeallocatePreparedStmt prepared.PreparedStatement
	// If UpdateAutoCachedUnnamedPreparedStmt is not nil, update the prepared statement in the Session's state.
	// This is only valid for replacing auto-cached unnamed prepared statements, as named prepared statements must be deallocated
	// before the name can be reused.
	UpdateAutoCachedUnnamedPreparedStmt prepared.PreparedStatement
}

// PGQueryProtocolType represents the Postgres query protocol used by a client message.
type PGQueryProtocolType string

const (
	// SimpleQueryProtocol indicates the simple query protocol (e.g. pgproto3.Query).
	SimpleQueryProtocol PGQueryProtocolType = "simple"

	// ExtendedQueryProtocol indicates the extended query protocol (e.g. pgproto3.Parse, Bind, Execute, etc)
	ExtendedQueryProtocol PGQueryProtocolType = "extended"
)

// State is the session state consulted while deciding an action.
type State struct {
	DiscardUntilSync         bool
	HasLink                  bool
	DynamicRoutingEnabled    bool
	AutoPreparedStmtsEnabled bool
	PreparedStatements       prepared.PreparedStatementState
}

// Hooks keeps embedder-specific observability outside the decision engine.
// Every callback is optional.
type Hooks struct {
	MessageObserved           func(pgproto3.FrontendMessage)
	QueryObserved             func(string, PGQueryProtocolType)
	QueryClassified           func(string, PGQueryProtocolType, classifier.Classification)
	QueryClassificationFailed func(string, PGQueryProtocolType, error)
	QueryParseFinished        func(string, PGQueryProtocolType, time.Duration)
	NamedPreparedStmtObserved func(string, string)
	// Deprecated: never called. Sessions record Bind values as db.parameters.N
	// root-span tags instead.
	BindParametersObserved func(string)
	ErrorActionCreated     func(*pgproto3.ErrorResponse, error)
}

// DecideClientMessageAction returns the set of actions to take to handle a message received
// from a client to their Session. (See ClientMessageAction docs for the set of actions.)
func DecideClientMessageAction(msg pgproto3.FrontendMessage, state State, hooks Hooks) (ClientMessageAction, error) {
	if ShouldDiscardUntilSync(state.DiscardUntilSync, msg) {
		return ClientMessageAction{DiscardUntilSync: state.DiscardUntilSync}, nil
	}
	if state.DiscardUntilSync && !state.HasLink {
		// ShouldDiscardUntilSync returned false, so msg must be a Sync.
		// There is nothing to flush to Postgres (no session link). Send
		// ReadyForQuery directly to the client and clear the discard flag.
		return ClientMessageAction{
			ReplyMsgsToClient: []pgproto3.BackendMessage{&pgproto3.ReadyForQuery{TxStatus: 'I'}},
			DiscardUntilSync:  false,
		}, nil
	}

	if hooks.MessageObserved != nil {
		hooks.MessageObserved(msg)
	}

	switch m := msg.(type) {
	case *pgproto3.Query:
		action := ClientMessageAction{
			ProxyMsgToPg:        m,
			AddReplyExpectation: query.QueryReply,
			FlushPostgres:       true,
		}
		action, err := handleClientQuery(m, m.String, SimpleQueryProtocol, action, state, hooks)
		action.InvalidateUnnamedPreparedStmt = true
		return action, err

	case *pgproto3.Parse:
		clientName := m.Name
		proxyPreparedStmt := CreateProxyPreparedStmt(m.Name, m.Query, m.ParameterOIDs, state.AutoPreparedStmtsEnabled)
		m.Name = proxyPreparedStmt.GetProxyName()
		action := ClientMessageAction{
			ProxyMsgToPg:          m,
			AddReplyExpectation:   query.ParseReply,
			RecordNewPreparedStmt: proxyPreparedStmt,
		}

		// handle unnamed prepared statements (and that are not autocached)
		if !prepared.IsNamedPreparedStmt(m.Name) && !state.AutoPreparedStmtsEnabled {
			// TODO: track unnamed prepared statements across backend releases
			// instead of treating them as one-off queries.
			return handleClientQuery(m, m.Query, ExtendedQueryProtocol, action, state, hooks)
		}
		if hooks.NamedPreparedStmtObserved != nil {
			hooks.NamedPreparedStmtObserved(m.Name, m.Query)
		}
		// Check if the session already has named prepared statement prepared based on the client name.
		// It must be deallocated before its name can be reused for a new statement.
		// It is valid to reuse unnamed prepared statements, so we don't have to check if it's auto-cached.
		if proxyPreparedStmt.IsClientNamedPreparedStmt() && state.PreparedStatements.FindKnownNamedStmtFromClientName(clientName) != nil {
			response := BuildPreparedStatementAlreadyExistsErrorResponse(clientName)
			return newObservedFlushErrorAction(response, nil, hooks), nil
		}
		// Check if the session already has the named prepared statement queued to be prepared based on the proxy name.
		// From the client perspective, it is valid to prepared two prepared statements with different names but the same query/param oids.
		// From the proxy perspective, it will hash the query/param oids to create the name that will actually be passed to Postgres,
		// so it needs to detect this edge case so it won't prepare the same prepared statement twice, which would result in an error.
		// We only need to check the pending prepared statements since we know they are prepared on the same underlying SessionLink.
		// Confirmed named prepared statements are checked when we actually attempt to send the message on the SessionLink.
		if state.PreparedStatements.FindPendingNamedStmtFromProxyName(m.Name) != nil {
			action = ClientMessageAction{
				ProxyMsgToPg:  &pgproto3.Flush{},
				FlushPostgres: true,
				AfterFlushAction: &ClientMessageAction{
					ReplyMsgsToClient: []pgproto3.BackendMessage{&pgproto3.ParseComplete{}},
				},
			}
			// if it's an auto-cached unnamed prepared statement, it's valid to reuse its name
			// we need to update the sql query associated with the unnamed prepared statement
			if proxyPreparedStmt.IsAutoCachedStmt() {
				action.AfterFlushAction.UpdateAutoCachedUnnamedPreparedStmt = proxyPreparedStmt
			}
		}
		return action, nil

	case *pgproto3.Describe:
		action := ClientMessageAction{
			ProxyMsgToPg: m, // TODO: transforms actually needed here, but ignoring for now
			// May involve multiple messages if gname stmt not setup on conn
			AddReplyExpectation: query.DescribeReply,
		}
		// if attempting to describe an unnamed statement or a named portal, no action needed
		// portals only exist for the duration of the transaction, so there is no risk of conflict
		clientName := m.Name
		if m.ObjectType == 'P' {
			return action, nil
		}
		if !prepared.IsNamedPreparedStmt(clientName) && !state.AutoPreparedStmtsEnabled {
			action.EnsureUnnamedPreparedStmt = true
			return action, nil
		}
		proxyName, stmt, errorAction := FindPotentiallyExistingPreparedStatement(clientName, state.PreparedStatements)
		if errorAction != nil {
			observeErrorAction(errorAction, hooks)
			return *errorAction, nil
		}
		m.Name = proxyName
		action.CheckExistingPreparedStmt = stmt
		return action, nil

	case *pgproto3.Close:
		// It is not an error to issue Close against a nonexistent statement or portal name, so do not need to check
		clientName := m.Name
		if (prepared.IsNamedPreparedStmt(clientName) || state.AutoPreparedStmtsEnabled) && m.ObjectType == 'S' { // named prepared statement
			return ClientMessageAction{
				ProxyMsgToPg:  &pgproto3.Flush{},
				FlushPostgres: true,
				AfterFlushAction: &ClientMessageAction{
					ReplyMsgsToClient:      []pgproto3.BackendMessage{&pgproto3.CloseComplete{}},
					DeallocatePreparedStmt: state.PreparedStatements.FindKnownNamedStmtFromClientName(clientName),
				},
			}, nil
		}
		return ClientMessageAction{
			ProxyMsgToPg:                  m,
			AddReplyExpectation:           query.CloseReply,
			InvalidateUnnamedPreparedStmt: m.ObjectType == 'S' && clientName == "",
		}, nil

	case *pgproto3.Bind:
		action := ClientMessageAction{ProxyMsgToPg: m, AddReplyExpectation: query.BindReply}
		// handle unnamed prepared statements with no auto-caching
		clientName := m.PreparedStatement
		if !prepared.IsNamedPreparedStmt(clientName) && !state.AutoPreparedStmtsEnabled {
			action.EnsureUnnamedPreparedStmt = true
			return action, nil
		}
		proxyName, stmt, errorAction := FindPotentiallyExistingPreparedStatement(clientName, state.PreparedStatements)
		if errorAction != nil {
			observeErrorAction(errorAction, hooks)
			return *errorAction, nil
		}
		m.PreparedStatement = proxyName
		action.CheckExistingPreparedStmt = stmt
		return action, nil

	case *pgproto3.Execute:
		// TODO: in theory, we could validate that the SessionLink actually has the expected portal bind setup
		return ClientMessageAction{
			ProxyMsgToPg:        m, // TODO: transforms actually needed here, but ignoring for now
			AddReplyExpectation: query.ExecuteReply,
		}, nil
	case *pgproto3.Flush:
		return ClientMessageAction{ProxyMsgToPg: m, FlushPostgres: true}, nil
	case *pgproto3.Sync:
		return ClientMessageAction{
			ProxyMsgToPg:        m,
			AddReplyExpectation: query.SyncReply,
			FlushPostgres:       true,
			AfterFlushAction: &ClientMessageAction{
				// notify Session that it can resume processing any incoming message
				DiscardUntilSync: false,
			},
		}, nil
	case *pgproto3.CopyData:
		return ClientMessageAction{ProxyMsgToPg: m, AddReplyExpectation: query.NoReply}, nil
	case *pgproto3.CopyDone:
		return ClientMessageAction{ProxyMsgToPg: m, AddReplyExpectation: query.QueryReply, FlushPostgres: true}, nil
	case *pgproto3.CopyFail:
		return ClientMessageAction{ProxyMsgToPg: m, AddReplyExpectation: query.NoReply}, nil
	case *pgproto3.FunctionCall: // No plans to support, "a legacy feature that is probably best avoided in new code."
		return ClientMessageAction{}, fmt.Errorf("received unsupported message from client: %#v", msg)
	default:
		return ClientMessageAction{}, fmt.Errorf("received unexpected message from client: %#v", msg)
	}
}

// handleClientQuery processes a sql string received as either a Query or Parse FrontendMessage.
// Classification failures still return the original action so the query is forwarded.
func handleClientQuery(msg pgproto3.FrontendMessage, sql string, protocol PGQueryProtocolType, action ClientMessageAction, state State, hooks Hooks) (ClientMessageAction, error) {
	if hooks.QueryObserved != nil {
		hooks.QueryObserved(sql, protocol)
	}
	if IsIdlePGXPing(msg, state) {
		return ClientMessageAction{ReplyMsgsToClient: []pgproto3.BackendMessage{
			&pgproto3.EmptyQueryResponse{},
			&pgproto3.ReadyForQuery{TxStatus: 'I'},
		}}, nil
	}
	// Only parse the query if dynamic routing is enabled, as we only use the parsing result
	// to determine if it's a read query for routing purposes
	if !state.DynamicRoutingEnabled {
		return action, nil
	}

	start := time.Now()
	if hooks.QueryParseFinished != nil {
		defer func() {
			hooks.QueryParseFinished(sql, protocol, time.Since(start))
		}()
	}
	classification, err := classifier.Classify(sql)
	if err != nil {
		if hooks.QueryClassificationFailed != nil {
			hooks.QueryClassificationFailed(sql, protocol, err)
		} else {
			pgproxy.Logger().Warn("query classification failed", "error", err, "query", sql)
		}
		return action, nil
	}
	if hooks.QueryClassified != nil {
		hooks.QueryClassified(sql, protocol, classification)
	}
	action.HandleReadQuery = classification.ReadOnly
	return action, nil
}

// ShouldDiscardUntilSync reports whether the proxy should skip the message.
// When an error occurs, the proxy will discard all future messages until a Sync is properly handled.
func ShouldDiscardUntilSync(discard bool, msg pgproto3.FrontendMessage) bool {
	_, sync := msg.(*pgproto3.Sync)
	return discard && !sync
}

// CreateProxyPreparedStmt translates a prepared statement from the client's representation to the proxy's representation.
// If it is a named prepared statement, the proxy will create a prepared statement with Postgres using a pseudo-unique
// hash in order to avoid clashing with other clients that may use the same name.
func CreateProxyPreparedStmt(clientName, sql string, parameterOIDs []uint32, autoCached bool) prepared.PreparedStatement {
	proxyName := clientName
	if prepared.IsNamedPreparedStmt(clientName) || autoCached {
		proxyName = CreateProxyPreparedStmtName(sql, parameterOIDs)
	}
	return prepared.CreatePreparedStatement(clientName, proxyName, sql, parameterOIDs)
}

// CreateProxyPreparedStmtName returns the stable proxy name for a SQL/OID pair.
func CreateProxyPreparedStmtName(sql string, parameterOIDs []uint32) string {
	var oids strings.Builder
	if len(parameterOIDs) > 0 {
		// SQL cannot contain NUL, and each OID ends with a comma, so different
		// SQL/OID pairs cannot write the same bytes. Statements without
		// declared types keep their earlier names.
		oids.WriteByte(0)
		for _, oid := range parameterOIDs {
			oids.WriteString(strconv.FormatUint(uint64(oid), 10))
			oids.WriteByte(',')
		}
	}
	// create hash of query + oids to have pseudo-unique name
	sum := sha256.Sum256([]byte(sql + oids.String()))
	return hex.EncodeToString(sum[:])
}

// LookupPreparedStatement returns the statement a client name denotes: its
// latest pending Parse, else its confirmed statement, else nil. confirmed
// reports which one it is.
func LookupPreparedStatement(clientName string, state prepared.PreparedStatementState) (stmt prepared.PreparedStatement, confirmed bool) {
	if stmt := state.FindPendingNamedStmtFromClientName(clientName); stmt != nil {
		return stmt, false
	}
	if stmt := state.FindConfirmedNamedStmtFromClientName(clientName); stmt != nil {
		return stmt, true
	}
	return nil, false
}

// FindPotentiallyExistingPreparedStatement checks if a prepared statement with the given client name
// already exists in the prepared statement state.
// It returns the proxy name and the prepared statement if it was confirmed already.
// It returns a non-nil error action if the prepared statement does not exist.
func FindPotentiallyExistingPreparedStatement(clientName string, state prepared.PreparedStatementState) (string, prepared.PreparedStatement, *ClientMessageAction) {
	stmt, confirmed := LookupPreparedStatement(clientName, state)
	if stmt == nil {
		action := NewFlushErrorAction(BuildPreparedStatementDoesNotExistErrorResponse(clientName))
		return "", nil, &action
	}
	if !confirmed {
		// we only want to check if the stmt is confirmed
		// since if its pending we already checked that the underlying connection does not have the prepared statement
		return stmt.GetProxyName(), nil, nil
	}
	return stmt.GetProxyName(), stmt, nil
}

// GetParameterString formats Bind parameters for optional diagnostics.
//
// Deprecated: sessions record Bind values as db.parameters.N root-span tags
// instead; nothing in this module calls it.
func GetParameterString(msg *pgproto3.Bind) string {
	return string(bytes.Join(msg.Parameters, []byte(",")))
}

// NewFlushErrorAction returns a ClientMessageAction that ensures PostgreSQL flushes pending messages
// before sending an error response to the client.
func NewFlushErrorAction(response *pgproto3.ErrorResponse) ClientMessageAction {
	return ClientMessageAction{
		ProxyMsgToPg:  &pgproto3.Flush{},
		FlushPostgres: true,
		AfterFlushAction: &ClientMessageAction{
			ReplyMsgsToClient: []pgproto3.BackendMessage{response},
			DiscardUntilSync:  true,
		},
	}
}

func newObservedFlushErrorAction(response *pgproto3.ErrorResponse, err error, hooks Hooks) ClientMessageAction {
	action := NewFlushErrorAction(response)
	if hooks.ErrorActionCreated != nil {
		hooks.ErrorActionCreated(response, err)
	} else {
		pgproxy.Logger().Error("client message action created PostgreSQL error", "message", response.Message, "detail", response.Detail, "error", err)
	}
	return action
}

func observeErrorAction(action *ClientMessageAction, hooks Hooks) {
	if action == nil || action.AfterFlushAction == nil || len(action.AfterFlushAction.ReplyMsgsToClient) == 0 {
		return
	}
	response, _ := action.AfterFlushAction.ReplyMsgsToClient[0].(*pgproto3.ErrorResponse)
	if response != nil {
		if hooks.ErrorActionCreated != nil {
			hooks.ErrorActionCreated(response, nil)
		} else {
			pgproxy.Logger().Error("client message action created PostgreSQL error", "message", response.Message, "detail", response.Detail)
		}
	}
}

// BuildPreparedStatementAlreadyExistsErrorResponse builds PostgreSQL error 42P05.
func BuildPreparedStatementAlreadyExistsErrorResponse(name string) *pgproto3.ErrorResponse {
	return &pgproto3.ErrorResponse{
		Severity: "ERROR",
		Code:     "42P05",
		Message:  "Named prepared statement with this name already exists. It must be deallocated before reusing it.",
		Detail:   fmt.Sprintf("Received named prepared statement with name: `%s`", name),
		Hint:     "Please use a new named prepared statement name or deallocate the existing named prepared statement.",
	}
}

// BuildPreparedStatementDoesNotExistErrorResponse builds PostgreSQL error 26000.
func BuildPreparedStatementDoesNotExistErrorResponse(name string) *pgproto3.ErrorResponse {
	return &pgproto3.ErrorResponse{
		Severity: "ERROR",
		Code:     "26000",
		Message:  "Named prepared statement with this name does not exist.",
		Detail:   fmt.Sprintf("Received named prepared statement with name: `%s`", name),
		Hint:     "Please prepare the statement name with the given name before attempting to use it.",
	}
}

// IsPGXPing reports whether sql is pgx's health-check query.
func IsPGXPing(sql string) bool { return sql == "-- ping" }

// IsIdlePGXPing reports whether msg is a pgx health-check ping that the proxy
// answers locally. Only pings at an idle boundary qualify: a held link may have
// an active/failed transaction or buffered extended-protocol work, so those
// pings go to Postgres in order and report its transaction state; during error
// recovery pings are discarded until Sync.
//
// Session uses this to bypass the load shedder, and DecideClientMessageAction
// uses it to reply locally, so the two decisions cannot drift apart.
func IsIdlePGXPing(msg pgproto3.FrontendMessage, state State) bool {
	q, ok := msg.(*pgproto3.Query)
	return ok && !state.HasLink && !state.DiscardUntilSync && IsPGXPing(q.String)
}
