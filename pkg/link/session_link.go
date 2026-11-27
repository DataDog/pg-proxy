// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package link

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"

	pgproxy "github.com/DataDog/pg-proxy"
	"github.com/DataDog/pg-proxy/pkg/prepared"
	"github.com/DataDog/pg-proxy/pkg/query"
)

// A SessionLink is an association between a Session and a Postgres Conn.
//
// When a message is sent to the linked Postgres connection, the caller typically adds a
// ReplyExpectation indicating the reply we expect from Postgres (the protocol has a well defined
// call-and-response message flow, other than a few messages that can be received at any time).
// The SessionLink maintains a queue of expectations that can be peeked at using NextExpectation,
// and popped using ResolveNextExpectation. There is necessary special handling of an error response
// during an extended query using the DiscardUntilSync method.
//
// SessionLinks will generally be short lived (e.g. living for the duration of a query/transaction),
// and therefore frequently created/GCed. In contrast, the Session owning a SessionLink is generally
// long lived (living for the lifetime of a client's connection), and Postgres connections are also
// long lived (returning to the connection pool when a SessionLink is released).
//
// A SessionLink is typically only accessed synchronously by the Session that owns it. However, in the case
// that a tx is cancelled, the SessionLink will be accessed concurrently.
// Therefore, a mutex is introduced to prevent race conditions between the SessionLink being released and the tx being cancelled.
type SessionLink struct {
	conn     *pgxpool.Conn
	pgConn   *pgproto3.Frontend // type is Frontend as the pg-proxy is acting as the "frontend" for this pg conn
	txStatus query.TxStatus
	lock     sync.Mutex

	replyExpectations   []query.ReplyExpectation // expected replies from the linked pg conn
	discardingUntilSync bool                     // if true, in an extended query error state where all messages will be discarded until a Sync
	readyForQuery       bool                     // true after Postgres has sent ReadyForQuery for the latest request
	marks               SessionMarks             // what this checkout may have done to session state
	// foreignUnnamed: the connection's last release skipped the reset, and no
	// request has been sent on this link yet.
	foreignUnnamed bool

	namedPreparedStmtThreshold int
	createdTime                int
	sessionLinkTags            []string
	token                      string // opaque token from RoleSession.Set when role management is enabled
	role                       RoleSession
	reset                      ResetPolicy
}

// MarkSession records what this checkout may have done to session state.
func (link *SessionLink) MarkSession(marks SessionMarks) { link.marks |= marks }

// UsesSessionMarks reports whether Release reads the marks: only in the reset
// modes that decide per checkout.
func (link *SessionLink) UsesSessionMarks() bool { return link.reset.usesMarks() }

// Conn returns the pooled Postgres connection associated with this link.
func (link *SessionLink) Conn() *pgxpool.Conn {
	return link.conn
}

// RoleToken returns the opaque role token from RoleSession.Set, if any.
func (link *SessionLink) RoleToken() string {
	return link.token
}

// SetRoleToken sets the opaque role token. Intended for tests that stub role management.
func (link *SessionLink) SetRoleToken(token string) {
	link.token = token
}

// Send sends a message to the linked Postgres connection, and optionally (but typically) adds
// a ReplyExpectation to the queue.
//
// The first request after a release that skipped the reset, which only
// ResetDynamic and ResetOff do, is preceded by a Close of the unnamed statement:
// the last checkout's statement may still be there, and no client may reach it.
// The Close rides the request's round trip, nothing before it can fail and make
// Postgres skip it, and its CloseComplete is not forwarded. A simple Query drops
// the statement itself.
func (link *SessionLink) Send(msg pgproto3.FrontendMessage, expectation query.ReplyExpectation) {
	if link.foreignUnnamed && expectation != query.NoReply {
		link.foreignUnnamed = false
		if _, ok := msg.(*pgproto3.Query); !ok {
			link.pgConn.Send(&pgproto3.Close{ObjectType: 'S'})
			link.replyExpectations = append(link.replyExpectations, query.ProxyCloseReply)
		}
	}
	link.pgConn.Send(msg)
	if expectation != query.NoReply {
		link.readyForQuery = false
		if expectation == query.SyncReply {
			link.discardingUntilSync = false
		}
		if !link.discardingUntilSync {
			link.replyExpectations = append(link.replyExpectations, expectation)
		}
	}
}

// Flush ensures any buffered messages are sent over the Postgres connection.
func (link *SessionLink) Flush() error {
	return link.pgConn.Flush()
}

// Receive blocks until the Postgres connection sends a message. A ParameterStatus
// marks the checkout: the backend reports a changed setting this way, including
// one changed inside a function.
func (link *SessionLink) Receive() (pgproto3.BackendMessage, error) {
	msg, err := link.pgConn.Receive()
	if _, ok := msg.(*pgproto3.ParameterStatus); ok {
		link.marks |= MarkParameterStatus
	}
	return msg, err
}

// resetTags returns the link's metric tags plus its reset mode.
func (link *SessionLink) resetTags(extra ...string) []string {
	tags := make([]string, 0, len(link.sessionLinkTags)+1+len(extra))
	tags = append(tags, link.sessionLinkTags...)
	tags = append(tags, "reset_mode:"+string(link.reset.Mode()))
	return append(tags, extra...)
}

// NextExpectation peeks at the next expectation in the queue, or NoReply if
// there are no expectations.
func (link *SessionLink) NextExpectation() query.ReplyExpectation {
	if len(link.replyExpectations) == 0 {
		return query.NoReply
	}
	return link.replyExpectations[0]
}

func (link *SessionLink) ResolveNextExpectation() {
	link.replyExpectations = link.replyExpectations[1:]
}

// DiscardUntilSync drops all expectations until the next Sync call. This is the
// necessary error handling response in Postgres' extended query mode, see
// https://www.postgresql.org/docs/15/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY:
//
//	> When an error is detected while processing any extended-query message,
//	> the backend issues ErrorResponse, then reads and discards messages until
//	> a Sync is reached
//
// If we don't yet have a reply expectation for a SyncReply, we need to set
// an addition flag discardingUntilSync indicating we also need to drop future
// expectations until we see a Sync.
// DiscardingUntilSync reports the extended-query error state: Postgres is dropping
// everything until a Sync, so nothing sent now reaches it.
func (link *SessionLink) DiscardingUntilSync() bool { return link.discardingUntilSync }

func (link *SessionLink) DiscardUntilSync() {
	nDrop := 0
	for ; nDrop < len(link.replyExpectations); nDrop++ {
		if link.replyExpectations[nDrop] == query.SyncReply {
			break
		}
	}
	link.replyExpectations = link.replyExpectations[nDrop:]
	link.discardingUntilSync = len(link.replyExpectations) == 0 // we still have more in the future to discard
}

const (
	sessionLinkStateIdle                = "idle"
	sessionLinkStateTxOpen              = "tx_open"
	sessionLinkStateDiscarding          = "discarding"
	sessionLinkStateExpectationsPending = "expectations_pending"
	sessionLinkStateNoReadyForQuery     = "no_ready_for_query"
)

func (link *SessionLink) getComputedState() string {
	switch {
	case link.txStatus != query.Idle:
		return sessionLinkStateTxOpen
	case link.discardingUntilSync:
		return sessionLinkStateDiscarding
	case len(link.replyExpectations) > 0:
		return sessionLinkStateExpectationsPending
	case !link.readyForQuery:
		return sessionLinkStateNoReadyForQuery
	default:
		return sessionLinkStateIdle
	}
}

// Idle reports whether the backend link is safe to release/reuse. It is
// intentionally stricter than "no pending reply expectations": Postgres must
// also have returned ReadyForQuery for the latest request.
func (link *SessionLink) Idle() bool {
	return link.getComputedState() == sessionLinkStateIdle
}

// Cancel cancels a request
func (link *SessionLink) Cancel() error {
	link.lock.Lock()
	defer link.lock.Unlock()
	if link.conn == nil {
		// The connection has already been released, so there's nothing to cancel
		return nil
	}
	pgConn := link.conn.Conn().PgConn()
	pgproxy.Logger().Debug("Canceling PG connection", "PID", pgConn.PID())
	_ = pgproxy.Metrics().Incr("pg_proxy.sessions.connections.cancellation_attempt", link.sessionLinkTags, 1)
	return pgConn.CancelRequest(context.Background())
}

// Release returns the Postgres connection to the pool. Once released, a SessionLink must be destroyed and
// not used again.
func (link *SessionLink) Release() error {
	link.lock.Lock()
	defer func() {
		if link.conn != nil {
			link.conn.Release()
			link.conn = nil
		}
		link.pgConn = nil
		link.lock.Unlock()
	}()

	// Destroy a link that is not at a reusable command boundary before anything else
	// runs. This has to come first: both the role reset and the RESET ALL below issue
	// SQL on this link, and on a desynced protocol stream — a backend still in COPY
	// mode, or one holding replies we never read — that SQL fails and returns early,
	// which would skip this accounting entirely and report the destruction as a
	// role-reset failure instead.
	if computedState := link.getComputedState(); computedState != sessionLinkStateIdle && link.conn != nil {
		metricTags := append(slices.Clone(link.sessionLinkTags), "state:"+computedState)
		_ = pgproxy.Metrics().Count("pg_proxy.sessions.connections.destroyed.release_non_idle", 1, metricTags, 1)
		pid := link.conn.Conn().PgConn().PID() // read before Close so the log can be matched against Postgres' own logs
		link.conn.Conn().Close(context.Background())
		link.logNonIdleRelease(computedState, pid)
	}

	// We should only reset the role if RoleSession is configured, and only while we
	// still have a live connection to run it on — the check above may have closed it,
	// and a destroyed connection carries no role to leak.
	if link.role != nil && link.conn != nil && !link.conn.Conn().IsClosed() {
		err := link.role.Reset(context.TODO(), link.conn, link.token, link.sessionLinkTags)
		if err != nil {
			// we failed to reset the role of the connection so we can't return it to the pool
			// closing the underlying PgConn makes it so that it is evicted despite the call to Release()
			// see https://github.com/jackc/pgx/blob/master/pgxpool/conn.go#L33
			link.conn.Conn().Close(context.Background())
			pgproxy.Logger().Warn("Closing connection", "error", err)
			return err
		}
	}

	// The reset clears any session GUCs the client SET (e.g. default_transaction_read_only,
	// search_path) so they don't leak to the next client to check out this conn from the pool.
	// Startup-packet GUCs (application_name, -c options from ConnectionOptions) are preserved
	// because RESET ALL resets to pg_settings.reset_val, which captures startup values.
	// postgres docs on reset all: https://www.postgresql.org/docs/current/sql-reset.html
	// pgx docs on client config startup packets: https://pkg.go.dev/github.com/jackc/pgx#ConnConfig
	//
	// The policy decides whether this release sends it: always, never, or only when the
	// checkout's marks say something may have changed; and in audit mode, whether it
	// also runs the check. resetStatements builds the statement, the same in every mode,
	// including the identity reset when no RoleSession ran above. ResetAlways sends it
	// and reports nothing, as before reset_mode existed.
	if link.conn != nil && !link.conn.Conn().IsClosed() {
		// A RoleSession reset the role above, whatever the server does with it.
		// Only the modes that read the marks look at the server's version.
		rolePersists := link.role != nil || !link.reset.usesMarks() ||
			!resetDropsRole(link.conn.Conn().PgConn().ParameterStatus("server_version"))
		action, reason := link.reset.resetDecision(link.marks, rolePersists)
		reports := link.reset.reports()
		var start time.Time
		if reports {
			start = time.Now()
		}
		var err error
		if action == resetSkip {
			// The reset is what discards the unnamed statement; without it, the next
			// checkout's client could run this one's.
			link.conn.Conn().PgConn().CustomData()[customDataResetSkippedKey] = true
		} else {
			err = link.runReset(action)
		}
		if reports {
			link.reportReset(action, reason, start, err)
		}
		if err != nil {
			return err
		}
	}

	// A closed connection has nothing left to deallocate: its prepared statements, and
	// the metadata tracking them, die with it.
	if link.conn == nil || link.conn.Conn().IsClosed() {
		return nil
	}

	if err := link.deallocateExcessPreparedStatements(); err != nil {
		return err
	}

	return nil
}

// runReset sends the reset query (pgx sends a zero-argument Exec as one simple
// query). An audited action wraps the reset in the audit check. A failure closes
// the connection, so nothing it might have left behind reaches the pool.
func (link *SessionLink) runReset(action resetAction) error {
	statements, resetQuery := resetStatements(link.role != nil)
	resetCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var err error
	if action == resetSend {
		_, err = link.conn.Exec(resetCtx, resetQuery)
	} else {
		err = link.runAuditedReset(resetCtx, statements, action == resetFullAudited)
	}
	if err != nil {
		_ = pgproxy.Metrics().Count("pg_proxy.sessions.connections.guc_reset_failure", 1, link.sessionLinkTags, 1)
		link.conn.Conn().Close(context.Background())
		pgproxy.Logger().Warn("Closing connection due to session reset failure", "statement", resetQuery, "error", err)
	}
	return err
}

// reportReset emits a release's reset count and, for a reset that ran without
// error, its latency since start.
func (link *SessionLink) reportReset(action resetAction, reason string, start time.Time, err error) {
	tags := reasonTags(action, reason)
	outcome := "outcome:reset"
	if action == resetSkip {
		outcome = "outcome:skipped"
	}
	_ = pgproxy.Metrics().Count(metricReset, 1, link.resetTags(append([]string{outcome}, tags...)...), 1)
	if action != resetSkip && err == nil {
		_ = pgproxy.Metrics().Distribution(pgproxy.MetricResetLatency, float64(time.Since(start).Microseconds()), link.resetTags(tags...), 1)
	}
}

// runAuditedReset runs the audit check and the reset in one simple query, and
// counts each audited setting the checkout left changed: a setting that
// ResetDynamic would have leaked to the next client.
//
// The check reads the audited settings before the reset and compares them with
// the connection's baseline. A connection without one, or whose last check found
// a change, also reads them after the reset and keeps that row as its baseline,
// so a changed server default costs one count per connection. With fullCheck
// the query starts with fullCheckSQL, which names settings the list misses.
// Values are never logged; they can hold tenant ids. A failure closes the
// connection.
func (link *SessionLink) runAuditedReset(ctx context.Context, reset []string, fullCheck bool) error {
	pgConn := link.conn.Conn().PgConn()
	baseline, haveBaseline := pgConn.CustomData()[customDataAuditBaselineKey].(auditBaseline)
	haveBaseline = haveBaseline && baseline.sql == link.reset.auditSQL

	var statements []string
	if fullCheck {
		statements = append(statements, fullCheckSQL)
	}
	statements = append(statements, link.reset.auditSQL)
	statements = append(statements, reset...)
	if !haveBaseline {
		statements = append(statements, link.reset.auditSQL)
	}
	// pgx's Exec discards rows, so read the raw results, one per statement.
	results, err := pgConn.Exec(ctx, strings.Join(statements, "; ")).ReadAll()
	if err != nil {
		return err
	}
	if len(results) != len(statements) {
		return fmt.Errorf("reset audit check returned %d results, want %d", len(results), len(statements))
	}

	var fullNames []string
	if fullCheck {
		row, err := singleRow(results[0], 1)
		if err != nil {
			return fmt.Errorf("reset full check: %w", err)
		}
		if row[0] != nil {
			fullNames = strings.Split(string(row[0]), ",")
		}
		results = results[1:]
	}
	before, err := singleRow(results[0], len(link.reset.auditColumns))
	if err != nil {
		return fmt.Errorf("reset audit check: %w", err)
	}
	after := baseline.values
	if !haveBaseline {
		if after, err = singleRow(results[len(results)-1], len(link.reset.auditColumns)); err != nil {
			return fmt.Errorf("reset audit check: %w", err)
		}
	}
	changed := link.reset.changedColumns(before, after)
	switch {
	case !haveBaseline:
		pgConn.CustomData()[customDataAuditBaselineKey] = auditBaseline{sql: link.reset.auditSQL, values: after}
	case len(changed) > 0:
		// Read the baseline again next time, in case a server default changed.
		delete(pgConn.CustomData(), customDataAuditBaselineKey)
	}

	for _, setting := range changed {
		_ = pgproxy.Metrics().Count(metricResetWouldLeak, 1, link.resetTags("setting:"+setting, "check:list"), 1)
	}
	var unlisted []string
	if fullCheck {
		for _, setting := range fullNames {
			_ = pgproxy.Metrics().Count(metricResetWouldLeak, 1, link.resetTags("setting:"+setting, "check:full"), 1)
			if !slices.Contains(link.reset.auditColumns, setting) {
				unlisted = append(unlisted, setting)
			}
		}
	}
	if len(changed) > 0 || len(unlisted) > 0 {
		pgproxy.Logger().Warn("reset_would_leak",
			"settings", changed,
			"unlisted_settings", unlisted,
			"backend_pid", pgConn.PID(),
			"tags", link.sessionLinkTags,
		)
	}
	return nil
}

// singleRow returns the only row of result, which must have columns values.
func singleRow(result *pgconn.Result, columns int) ([][]byte, error) {
	if len(result.Rows) != 1 || len(result.Rows[0]) != columns {
		return nil, fmt.Errorf("got %d rows, want 1 row of %d values", len(result.Rows), columns)
	}
	return result.Rows[0], nil
}

// logNonIdleRelease reports a backend destroyed because its link was not at a
// reusable command boundary.
//
// no_ready_for_query is a warning rather than an error: it is the expected outcome of
// legitimate client behaviour (an abandoned COPY, or an extended-query batch that
// ended in Flush with no Sync), and at Error severity a single COPY-heavy client
// would produce a steady stream of them. The other states mean a link was released
// with work the proxy still believed was in flight, which is a bug worth an error.
//
// The state, backend PID, pending expectations and session metric tags are logged —
// never the link itself, whose %+v renders unexported fields including the OSRM
// set_user token.
func (link *SessionLink) logNonIdleRelease(computedState string, pid uint32) {
	message := link.nonIdleReleaseLogMessage(computedState, pid)
	if computedState == sessionLinkStateNoReadyForQuery {
		pgproxy.Logger().Warn(message)
		return
	}
	pgproxy.Logger().Error(message)
}

func (link *SessionLink) nonIdleReleaseLogMessage(computedState string, pid uint32) string {
	return fmt.Sprintf(
		"Releasing non-idle session link. state: %s, backend PID: %d, expectations: %q, tags: %q.",
		computedState,
		pid,
		link.DebugExpectationString(),
		link.sessionLinkTags,
	)
}

// ForceClose force-closes the underlying PG connection. It returns true only
// when it successfully closes a live connection; it returns false if there is
// no live connection or the close fails. This unblocks any pending Receive()
// call and ensures the connection is not returned to the pool. It is safe to
// call concurrently with Receive() — net.Conn.Close() is goroutine-safe and
// will cause a blocking Read to return immediately.
func (link *SessionLink) ForceClose() bool {
	link.lock.Lock()
	defer link.lock.Unlock()
	if link.conn == nil || link.conn.Conn().IsClosed() {
		return false
	}
	// Close the raw TCP connection to PG.
	if err := link.conn.Conn().PgConn().Conn().Close(); err != nil {
		return false
	}
	return true
}

func (link *SessionLink) DebugExpectationString() string {
	exps := []string{}
	if link.discardingUntilSync {
		exps = append(exps, "[DiscardingUntilSync]")
	}
	for _, exp := range link.replyExpectations {
		exps = append(exps, exp.String())
	}
	return strings.Join(exps, "->")
}

// TxStatus returns link tx status
func (link *SessionLink) TxStatus() query.TxStatus {
	return link.txStatus
}

// SetTxStatus sets link tx status
func (link *SessionLink) SetTxStatus(txStatus query.TxStatus) {
	link.txStatus = txStatus
}

// HandleReadyForQuery records the transaction status reported by Postgres and
// marks the latest request as having reached a reusable command boundary.
func (link *SessionLink) HandleReadyForQuery(txStatus query.TxStatus) {
	link.SetTxStatus(txStatus)
	link.readyForQuery = true
}

// RecordNewNamedPreparedStmt records in the session link's data that it has created a new named prepared statement.
func (link *SessionLink) RecordNewNamedPreparedStmt(preparedStatement prepared.PreparedStatement) error {
	if preparedStatement == nil {
		return fmt.Errorf("attempting to record a new named prepared statement that is nil")
	}
	if !preparedStatement.IsProxyNamedPreparedStmt() {
		return nil
	}
	return link.recordPreparedStatement(preparedStatement, false)
}

// RecordExistingNamedPreparedStmtUsed records that an existing named prepared statement on the connection is being used.
func (link *SessionLink) RecordExistingNamedPreparedStmtUsed(preparedStatement prepared.PreparedStatement) error {
	if preparedStatement == nil {
		return fmt.Errorf("attempting to record an existing named prepared statement that is nil")
	}
	if !preparedStatement.IsProxyNamedPreparedStmt() {
		return nil
	}
	return link.recordPreparedStatement(preparedStatement, true)
}

// HasExistingNamedPreparedStmt checks whether a session link has created a named prepared statement.
func (link *SessionLink) HasExistingNamedPreparedStmt(preparedStatement prepared.PreparedStatement) bool {
	if preparedStatement == nil || !preparedStatement.IsProxyNamedPreparedStmt() {
		return false
	}
	_, hasExistingNamedPreparedStmt, _ := link.checkForValidSessionLinkAndExistingNamedPreparedStmt(preparedStatement)
	return hasExistingNamedPreparedStmt
}

func (link *SessionLink) checkForValidSessionLinkAndExistingNamedPreparedStmt(preparedStatement prepared.PreparedStatement) (map[string]ConnPreparedStmt, bool, error) {
	if preparedStatement == nil {
		// this signifies a bug in the code
		return nil, false, fmt.Errorf("unexpectedly attempting to check for a nil prepared stmt on a session link")
	}

	connPreparedStmts, err := getConnPreparedStmts(link.conn)
	if err != nil {
		return nil, false, err
	}
	stmtName := preparedStatement.GetProxyName()
	_, connHasPreparedStmt := connPreparedStmts[stmtName]
	return connPreparedStmts, connHasPreparedStmt, nil
}

// Checks whether the SessionLink has already created a prepared statement. If it has not been created, it will
// record on the underlying connection that it has. If the statement was already created, increment its usage count.
// Check its calling methods for an explanation on when these different scenarios occur.
// In the case that a prepared statement will be created, it checks whether it has exceeded the max number of prepared statements
// on the connection, and if so, deallocates one.
func (link *SessionLink) recordPreparedStatement(preparedStatement prepared.PreparedStatement, existingOnConn bool) error {
	connPreparedStmts, err := getConnPreparedStmts(link.conn)
	if err != nil {
		return err
	}
	stats, err := getConnStats(link.conn)
	if err != nil {
		return err
	}

	stmtName := preparedStatement.GetProxyName()

	if existingOnConn {
		metaData := connPreparedStmts[stmtName]
		metaData.UsageCount++
		metaData.TimeLastUsed = time.Now().Nanosecond()
		connPreparedStmts[stmtName] = metaData
		if metaData.UsageCount > stats.preparedStmtsMaxUsage {
			stats.preparedStmtsMaxUsage = metaData.UsageCount
		}
		link.exportStats()
		return nil
	}

	stats.preparedStmtsCreatedCount++
	connPreparedStmts[stmtName] = ConnPreparedStmt{Name: stmtName, TimeLastUsed: time.Now().Nanosecond()}
	link.exportStats()

	return nil
}

// exportStats reports metrics on the SessionLink and its underlying connection.
// A gauge is used for reporting metrics in order to understand the state of prepared statements per connection rather
// than aggregated across all connections.
// TODO- stats for the underlying connection should probably be emitted for its lifetime, which would require
// using a onCreate hook. Since we do not yet have a standardized way of creating a connection pool,
// we will only export stats when a connection is linked to a SessionLink.
// SessionLinks are expected to be relatively short lived due to the nature of tx-level pooling. As a result,
// this is expected to be called on events such as checkout & release rather than at time intervals.
func (link *SessionLink) exportStats() {
	connStats, err := getConnStats(link.conn)
	if err != nil {
		pgproxy.Logger().Error("error getting conn's statistics", "error", err)
		return
	}
	_ = pgproxy.Metrics().Gauge(connPreparedStmtDeallocatedCount, float64(connStats.preparedStmtsDeallocatedCount), link.sessionLinkTags, 1)
	_ = pgproxy.Metrics().Gauge(connPreparedStmtCreatedCount, float64(connStats.preparedStmtsCreatedCount), link.sessionLinkTags, 1)
	_ = pgproxy.Metrics().Gauge(connPreparedStmtMaxUsage, float64(connStats.preparedStmtsMaxUsage), link.sessionLinkTags, 1)
}

// deallocateExcessPreparedStatements removes the least-used prepared statements from the connection
// if the total number of prepared statements exceeds the maximum allowed. It ensures that the
// connection stays within the configured limit of prepared statements to avoid memory overuse.
// There is a roundtrip for each message that needs to be deallocated, which is a potential point of optimization.
// However, if there is an error that occurs, it will not block subsequent error messages from being sent.
func (link *SessionLink) deallocateExcessPreparedStatements() error {
	connPreparedStmts, err := getConnPreparedStmts(link.conn)
	if err != nil {
		return err
	}
	preparedStmtCount := len(connPreparedStmts)
	if preparedStmtCount <= link.namedPreparedStmtThreshold {
		return nil
	}
	stmtsToDeallocateCount := preparedStmtCount - link.namedPreparedStmtThreshold
	for _, stmtName := range StmtsToDeallocate(connPreparedStmts, stmtsToDeallocateCount, link.createdTime) {
		if err := link.conn.Conn().PgConn().Deallocate(context.Background(), stmtName); err != nil {
			return err
		}
		delete(connPreparedStmts, stmtName)
		stats, err := getConnStats(link.conn)
		if err != nil {
			return err
		}
		stats.preparedStmtsDeallocatedCount++
	}
	return nil
}
