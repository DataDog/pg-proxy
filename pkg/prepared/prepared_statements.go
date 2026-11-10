// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package prepared

import (
	"fmt"
	"slices"
	"strings"

	pgproxy "github.com/DataDog/pg-proxy"
)

const (
	// UnnamedStmtName is the Postgres empty prepared-statement name.
	UnnamedStmtName = ""

	confirmedCountMetric = "pg_proxy.session.prepared_stmt.confirmed.size"
	enqueuedCountMetric  = "pg_proxy.session.prepared_stmt.enqueued.size"

	preparedStmtNamedTrue       = "named:true"
	preparedStmtNamedFalse      = "named:false"
	preparedStmtAutocachedTrue  = "autocached:true"
	preparedStmtAutocachedFalse = "autocached:false"
	preparedStmtExistingTrue    = "existing:true"
	preparedStmtExistingFalse   = "existing:false"
	preparedStmtFromErrorTrue   = "from_error:true"
	preparedStmtFromErrorFalse  = "from_error:false"
	preparedStmtExistedTrue     = "existed:true"
	preparedStmtExistedFalse    = "existed:false"
)

// PreparedStatement represents a prepared statement from the perspective of the proxy.
// It contains the name that the client gave it (clientName) and
// the name that the proxy used to create it in its connection with PG (poxyName).
// These are different names in order to avoid clashing due to connection pooling.
// Additionally, it represents whether the prepared statement was given a name because
// the client explicitly created a named prepared statement, or if it was "auto-cached",
// meaning that the proxy chose to make a client's unnamed statement a named prepared statement.
// The current use case for this is if the client is using unnamed prepared statements in a way
// that is not compatible with tx-level pooling, such as the PQ driver with binary_parameters=false
type PreparedStatement interface {
	// IsClientNamedPreparedStmt returns whether the client created this prepared statement with a name or not
	IsClientNamedPreparedStmt() bool
	// IsProxyNamedPreparedStmt returns whether the proxy created this prepared statement with a name or not
	IsProxyNamedPreparedStmt() bool
	IsAutoCachedStmt() bool
	GetClientName() string
	GetProxyName() string
	GetSQLQuery() string
	GetParamOIDs() []uint32
	GetErrorTags() []string
	GetMetricTags() []string
}

func PreparedStatementEquivalent(a, b PreparedStatement) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.IsClientNamedPreparedStmt() == b.IsClientNamedPreparedStmt() &&
		a.GetClientName() == b.GetClientName() &&
		a.GetProxyName() == b.GetProxyName() &&
		a.GetSQLQuery() == b.GetSQLQuery() &&
		a.IsAutoCachedStmt() == b.IsAutoCachedStmt() &&
		a.IsProxyNamedPreparedStmt() == b.IsProxyNamedPreparedStmt() &&
		slices.Equal(a.GetParamOIDs(), b.GetParamOIDs())
}

func CreatePreparedStatement(clientName string, proxyName string, sql string, paramOIDs []uint32) PreparedStatement {
	return &proxyPreparedStmt{
		clientName: clientName,
		proxyName:  proxyName,
		sql:        sql,
		paramOIDs:  paramOIDs,
	}
}

// contains the client name and the corresponding prepared statement created between the proxy and PG
type proxyPreparedStmt struct {
	clientName string
	proxyName  string
	sql        string
	paramOIDs  []uint32
}

func (p proxyPreparedStmt) IsClientNamedPreparedStmt() bool {
	return IsNamedPreparedStmt(p.clientName)
}

func (p proxyPreparedStmt) IsProxyNamedPreparedStmt() bool {
	return IsNamedPreparedStmt(p.proxyName)
}

func (p proxyPreparedStmt) IsAutoCachedStmt() bool {
	return !IsNamedPreparedStmt(p.clientName) && IsNamedPreparedStmt(p.proxyName)
}

func (p proxyPreparedStmt) GetClientName() string {
	return p.clientName
}

func (p proxyPreparedStmt) GetProxyName() string {
	return p.proxyName
}

func (p proxyPreparedStmt) GetSQLQuery() string {
	return p.sql
}

func (p proxyPreparedStmt) GetParamOIDs() []uint32 {
	return p.paramOIDs
}

func (p proxyPreparedStmt) GetErrorTags() []string {
	return append(p.GetMetricTags(),
		fmt.Sprintf("client_name:%s", p.clientName),
		fmt.Sprintf("proxy_name:%s", p.proxyName),
		fmt.Sprintf("sql:%s", p.sql),
	)
}

// GetMetricTags gets non-high-cardinality tags intended for metrics.
func (p proxyPreparedStmt) GetMetricTags() []string {
	return []string{preparedStmtNamedTag(p.IsClientNamedPreparedStmt()), preparedStmtAutocachedTag(p.IsAutoCachedStmt())}
}

func preparedStmtNamedTag(named bool) string {
	if named {
		return preparedStmtNamedTrue
	}
	return preparedStmtNamedFalse
}

func preparedStmtAutocachedTag(autocached bool) string {
	if autocached {
		return preparedStmtAutocachedTrue
	}
	return preparedStmtAutocachedFalse
}

func preparedStmtExistingTag(existing bool) string {
	if existing {
		return preparedStmtExistingTrue
	}
	return preparedStmtExistingFalse
}

func preparedStmtFromErrorTag(fromError bool) string {
	if fromError {
		return preparedStmtFromErrorTrue
	}
	return preparedStmtFromErrorFalse
}

func preparedStmtExistedTag(existed bool) string {
	if existed {
		return preparedStmtExistedTrue
	}
	return preparedStmtExistedFalse
}

// IsNamedPreparedStmt reports whether name is a non-empty Postgres prepared
// statement name (the empty string is the unnamed statement).
func IsNamedPreparedStmt(name string) bool {
	return name != UnnamedStmtName
}

// QueuedPreparedStatement represents a prepared statement that will be sent to Postgres.
// It is existing if it is a named statement that was previously created in the Session but will be created on a different connection
// than the one it was originally created on.
type QueuedPreparedStatement interface {
	PreparedStatement
	IsExistingPreparedStmt() bool
}

type queuedPreparedStmt struct {
	PreparedStatement
	existing bool
}

func (q queuedPreparedStmt) IsExistingPreparedStmt() bool {
	return q.existing
}

func createQueuedPreparedStmt(preparedStmt PreparedStatement, existing bool) QueuedPreparedStatement {
	return queuedPreparedStmt{PreparedStatement: preparedStmt, existing: existing}
}

// PreparedStatementState contains the state of prepared statements that a proxy session is aware of. Because of the proxy's tx-level pooling,
// it must keep track of the mapping between:
// - how the client create the prepared statement AND
// - how the underlying connections between the proxy and PG create the prepared statement
// as these are done in different ways to avoid clashing between different client sessions.
//
// Prepared statements are divided into 2 different categories:
//
// 1. pending prepared statements. These are statements that have been received in a Parse message from the client,
// but no corresponding ParseComplete has been received from PG.
// When a ParseComplete it received, the pending prepared statement will be promoted to a confirmed prepared statement.
// Pending prepared statements are valid to reference for the remainder of the tx block.
//
// 2. confirmed prepared statements. These are statements that have been received and acknowledged with a ParseComplete from PG
// by any of the connections in the connection pool associated with the proxy session.
type PreparedStatementState interface {
	// AddConfirmedNamedStmt records that a named prepared statement was confirmed.
	// This happens when a prepared statement with the same query and paramOIDs has already been created on a connection from a different session,
	// and this session happens to be using the same prepared statement.
	// It will throw an error if the client name of the prepared statement is unnamed or the client name already exists in this state.
	AddConfirmedNamedStmt(preparedStmt PreparedStatement) error

	// EnqueuePendingStmt records that a prepared statement was received and that a corresponding ParseComplete is expected,
	// and if it is a named prepared statement, whether it has already been created in this Session.
	EnqueuePendingStmt(pendingPreparedStmt PreparedStatement, existing bool)

	// ConfirmPendingStmt dequeues a pending prepared statement and records it as confirmed, if named.
	// It then returns the prepared statement.
	ConfirmPendingStmt() QueuedPreparedStatement

	// InvalidatePendingStmts clears out all pending prepared statements.
	// This occurs because a client message resulted in an error or
	// as a cleanup operation when the connection is released back to the pool.
	InvalidatePendingStmts(fromError bool)

	// FindKnownNamedStmtFromClientName returns the proxy's named prepared statement associated with the client name for it, nil otherwise.
	// It includes both confirmed and pending prepared statements.
	FindKnownNamedStmtFromClientName(clientName string) PreparedStatement

	// FindConfirmedNamedStmtFromClientName finds the proxy's confirmed named prepared statement associated with the client name for it, nil otherwise.
	FindConfirmedNamedStmtFromClientName(clientName string) PreparedStatement

	// FindPendingNamedStmtFromClientName finds the proxy's pending prepared statement associated with the client name for it, nil otherwise.
	FindPendingNamedStmtFromClientName(clientName string) PreparedStatement

	// FindPendingNamedStmtFromProxyName finds the proxy's pending prepared statement associated with the proxy name for it, nil otherwise.
	FindPendingNamedStmtFromProxyName(proxyName string) PreparedStatement

	// DeallocateNamedStmtFromClientName deletes the named prepared statement associated with the client name
	// from the PreparedStatementState confirmed named statements. It is a no-op if it does not exist.
	DeallocateNamedStmtFromClientName(clientName string)
}

func CreateProxyPreparedStmtsState(metricTags []string) PreparedStatementState {
	return &proxyPreparedStmtsState{
		confirmedNamedPreparedStmtsMap: make(map[string]PreparedStatement),
		allPendingPreparedStmtsQueue:   make([]QueuedPreparedStatement, 0),
		metricTags:                     metricTags,
	}
}

// When prepared statements are received in a Parse message from the client, they are initially added to a queue of pending prepared statements (named or unnamed).
// Once a ParseComplete is received from PG, it is promoted to a map of confirmed named prepared stmts.
//
// If the underlying connection already created the prepared statement from a different session, but the current session had not yet known about it,
// it should be added directly to the confirmed named prepared statements.
//
// Any errors received from PG would result in all remaining pending prepared statements to be discarded as a
// result of PG's discard-until-sync behavior.
type proxyPreparedStmtsState struct {
	// mapping of "confirmed" named prepared statements from the client's statement name to the proxy's version of the prepared statement.
	// unnamed prepared statements are not included since they do not need to deallocated before preparing a new unnamed prepared statement
	confirmedNamedPreparedStmtsMap map[string]PreparedStatement

	// a queue is required since ParseComplete messages are received in the same order as the corresponding Parse message
	allPendingPreparedStmtsQueue []QueuedPreparedStatement

	metricTags []string
}

func (p *proxyPreparedStmtsState) AddConfirmedNamedStmt(preparedStmt PreparedStatement) error {
	if !preparedStmt.IsProxyNamedPreparedStmt() {
		return fmt.Errorf("attempting to add a confirmed unnamed prepared statement when a named one was expected %s", strings.Join(append(preparedStmt.GetErrorTags(), p.metricTags...), " "))
	}

	// Named prepared statements cannot be reused.
	// However, if we auto-cached it, this is the equivalent of reusing the unnamed prepared statement, which is valid.
	clientName := preparedStmt.GetClientName()
	if _, ok := p.confirmedNamedPreparedStmtsMap[clientName]; preparedStmt.IsClientNamedPreparedStmt() && ok {
		return fmt.Errorf("attempting to add a confirmed named prepared statement that already exists in the map %s", strings.Join(append(preparedStmt.GetErrorTags(), p.metricTags...), " "))
	}
	p.confirmedNamedPreparedStmtsMap[clientName] = preparedStmt
	_ = pgproxy.Metrics().Incr("pg_proxy.session.prepared_stmt.confirmed_added", p.metricTags, 1)
	_ = pgproxy.Metrics().Gauge(confirmedCountMetric, float64(len(p.confirmedNamedPreparedStmtsMap)), p.metricTags, 1)
	return nil
}

func (p *proxyPreparedStmtsState) EnqueuePendingStmt(pendingPreparedStmt PreparedStatement, existing bool) {
	p.allPendingPreparedStmtsQueue = append(p.allPendingPreparedStmtsQueue, createQueuedPreparedStmt(pendingPreparedStmt, existing))

	tags := append(p.metricTags, pendingPreparedStmt.GetMetricTags()...)
	tags = append(tags, preparedStmtExistingTag(existing))
	_ = pgproxy.Metrics().Incr("pg_proxy.session.prepared_stmt.enqueued", tags, 1)
	_ = pgproxy.Metrics().Gauge(enqueuedCountMetric, float64(len(p.allPendingPreparedStmtsQueue)), p.metricTags, 1)
}

func (p *proxyPreparedStmtsState) ConfirmPendingStmt() QueuedPreparedStatement {
	if len(p.allPendingPreparedStmtsQueue) == 0 {
		// this signifies a bug in the code
		pgproxy.Logger().Error("unexpectedly attempted to confirm pending prepared stmt when there are no known pending stmts; this is a pg-proxy bug")
		return nil
	}
	successfulStmt := p.allPendingPreparedStmtsQueue[0]

	// if it was an unnamed prepared statement, don't record it as we always want to re-prepare it
	// we added it to pendingPreparedStatements so we need to deque it still
	if successfulStmt.IsProxyNamedPreparedStmt() {
		p.confirmedNamedPreparedStmtsMap[successfulStmt.GetClientName()] = successfulStmt
	}
	p.allPendingPreparedStmtsQueue = slices.Delete(p.allPendingPreparedStmtsQueue, 0, 1)
	_ = pgproxy.Metrics().Incr("pg_proxy.session.prepared_stmt.confirmed_enqueued", append(p.metricTags, successfulStmt.GetMetricTags()...), 1)
	_ = pgproxy.Metrics().Gauge(confirmedCountMetric, float64(len(p.confirmedNamedPreparedStmtsMap)), p.metricTags, 1)
	return successfulStmt
}

func (p *proxyPreparedStmtsState) InvalidatePendingStmts(fromError bool) {
	p.allPendingPreparedStmtsQueue = nil
	_ = pgproxy.Metrics().Incr("pg_proxy.session.prepared_stmt.invalidated", append(p.metricTags, preparedStmtFromErrorTag(fromError)), 1)
	_ = pgproxy.Metrics().Gauge(enqueuedCountMetric, float64(len(p.allPendingPreparedStmtsQueue)), p.metricTags, 1)
}

func (p *proxyPreparedStmtsState) FindKnownNamedStmtFromClientName(clientName string) PreparedStatement {
	if stmt := p.FindConfirmedNamedStmtFromClientName(clientName); stmt != nil {
		return stmt
	}

	return p.FindPendingNamedStmtFromClientName(clientName)
}

func (p *proxyPreparedStmtsState) FindConfirmedNamedStmtFromClientName(clientName string) PreparedStatement {
	stmt, _ := p.confirmedNamedPreparedStmtsMap[clientName]
	return stmt
}

func (p *proxyPreparedStmtsState) FindPendingNamedStmtFromClientName(clientName string) PreparedStatement {
	// It is important for auto-cached prepared statements to search backwards through the queue
	// since when the client bind/describe/closes an unnamed prepared statement,
	// it will be associated with the last prepared statement created with that client name,
	// as there may be multiple unnamed prepared statements in the queue.
	for i := len(p.allPendingPreparedStmtsQueue) - 1; i >= 0; i-- {
		stmt := p.allPendingPreparedStmtsQueue[i]
		if stmt.GetClientName() == clientName {
			return stmt
		}
	}
	return nil
}

func (p *proxyPreparedStmtsState) FindPendingNamedStmtFromProxyName(proxyName string) PreparedStatement {
	stmtIndex := slices.IndexFunc(p.allPendingPreparedStmtsQueue, func(stmt QueuedPreparedStatement) bool {
		return stmt.GetProxyName() == proxyName
	})
	if stmtIndex != -1 {
		return p.allPendingPreparedStmtsQueue[stmtIndex]
	}
	return nil
}

func (p *proxyPreparedStmtsState) DeallocateNamedStmtFromClientName(clientName string) {
	preparedStmt, existed := p.confirmedNamedPreparedStmtsMap[clientName]
	delete(p.confirmedNamedPreparedStmtsMap, clientName)

	tags := append(p.metricTags, preparedStmtExistedTag(existed))
	if existed {
		tags = append(tags, preparedStmt.GetMetricTags()...)
	}
	_ = pgproxy.Metrics().Incr("pg_proxy.session.prepared_stmt.deallocated", tags, 1)
	_ = pgproxy.Metrics().Gauge(confirmedCountMetric, float64(len(p.confirmedNamedPreparedStmtsMap)), p.metricTags, 1)
}
