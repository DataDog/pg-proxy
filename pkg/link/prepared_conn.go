// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package link

import (
	"errors"
	"maps"
	"slices"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	customDataConnStateKey = "connState"

	connPreparedStmtDeallocatedCount = "pg_proxy.sessions.connections.prepared_stmt.deallocated.count"
	connPreparedStmtCreatedCount     = "pg_proxy.sessions.connections.prepared_stmt.created.count"
	connPreparedStmtMaxUsage         = "pg_proxy.sessions.connections.prepared_stmt.max_usage"
)

// errNoConnState is returned when a link has no connection, either because it was
// built without one or because it has been released.
var errNoConnState = errors.New("session link has no connection: never acquired or already released")

// statistics associated with a connection
type connStats struct {
	// how many prepared stmts have been deallocated on this connection due to max number of prepared stmts being reached
	preparedStmtsDeallocatedCount int
	// how many prepared stmts have been created on this connection
	preparedStmtsCreatedCount int
	// the max number of times a prepared stmt has been used out of all prepared stmts on the connection
	preparedStmtsMaxUsage int
}

// connState is the state of one Postgres connection: who it logged in as, its
// prepared statements, and what its releases left on it. It lives in the
// connection's pgx CustomData, so it outlasts each SessionLink and stays with the
// connection while it sits in the pool.
//
// Only the link that holds the connection touches it, on the session goroutine. An
// AbortHandle's Cancel and ForceClose never do, so its lock does not guard it.
type connState struct {
	loginUser string // the user in the connection's config
	// Keyed by proxy statement name. Pointers, so a Bind updates usage in place.
	stmts map[string]*stmtUsage
	stats connStats
	// resetSkipped is set while the connection may hold another checkout's unnamed
	// statement: its last release skipped the reset, and no request has been sent
	// on it since.
	resetSkipped bool
	// auditBaseline is the zero value while the connection has none. Its empty sql
	// matches no audit query.
	auditBaseline auditBaseline
}

// stmtUsage is what eviction needs to know about one statement on a connection. Its
// name is the map key, so the two can never disagree.
type stmtUsage struct {
	usageCount   int
	timeLastUsed int
}

// ConnPreparedStmt is metadata associated with a prepared statement on a connection.
type ConnPreparedStmt struct {
	Name         string
	UsageCount   int
	TimeLastUsed int
}

// connStateOf returns the connection's state, creating it on the connection's first
// checkout. A pool hook cannot own this: links accept conns from any pool.Pool, and
// the state only matters once a link uses the conn.
func connStateOf(conn *pgxpool.Conn) *connState {
	pgxConn := conn.Conn()
	customData := pgxConn.PgConn().CustomData()
	if state, ok := customData[customDataConnStateKey].(*connState); ok {
		return state
	}
	// Config deep-copies the connection's config, so read it once per connection.
	state := &connState{loginUser: pgxConn.Config().User, stmts: make(map[string]*stmtUsage)}
	customData[customDataConnStateKey] = state
	return state
}

// LoginUser returns the user conn logged in as, from conn's config. It copies the
// config once per connection, not per call. Call it only while holding conn.
func LoginUser(conn *pgxpool.Conn) string {
	return connStateOf(conn).loginUser
}

// A sorting function for ConnPreparedStmt that is based on a statement's deallocation priority.
// Statements that were created in the current Session have the least priority.
// Afterward, usage count is considered and then least-recently-used.
func sortConnPreparedStmtBasedOnDeallocationPriority(sessionLinkCreatedTime int) func(i, j ConnPreparedStmt) int {
	return func(i, j ConnPreparedStmt) int {
		// If statement i was used after session link created and j was used before, j should be prioritized for deallocation
		iUsedAfter := i.TimeLastUsed > sessionLinkCreatedTime
		jUsedAfter := j.TimeLastUsed > sessionLinkCreatedTime
		if iUsedAfter && !jUsedAfter {
			return 1
		}
		if !iUsedAfter && jUsedAfter {
			return -1
		}
		// Both used before or both used after session, use LFU behavior
		usageDiff := i.UsageCount - j.UsageCount
		if usageDiff != 0 {
			return usageDiff
		}
		// Both used before or both used after session with the same usage count, use LRU behavior
		return i.TimeLastUsed - j.TimeLastUsed
	}
}

// StmtsToDeallocate returns the names of the n least-used prepared statements.
func StmtsToDeallocate(connPreparedStmts map[string]ConnPreparedStmt, stmtsToDeallocate int, sessionLinkCreatedTime int) []string {
	return leastValuableStmts(slices.Collect(maps.Values(connPreparedStmts)), stmtsToDeallocate, sessionLinkCreatedTime)
}

// stmtsToDeallocate returns the names of the n statements Release should evict first.
func (s *connState) stmtsToDeallocate(n int, sessionLinkCreatedTime int) []string {
	stmts := make([]ConnPreparedStmt, 0, len(s.stmts))
	for name, usage := range s.stmts {
		stmts = append(stmts, ConnPreparedStmt{Name: name, UsageCount: usage.usageCount, TimeLastUsed: usage.timeLastUsed})
	}
	return leastValuableStmts(stmts, n, sessionLinkCreatedTime)
}

// leastValuableStmts sorts stmts by deallocation priority, in place, and returns the
// names of the first stmtsToDeallocate.
func leastValuableStmts(stmts []ConnPreparedStmt, stmtsToDeallocate int, sessionLinkCreatedTime int) []string {
	if stmtsToDeallocate <= 0 || len(stmts) == 0 {
		return nil
	}
	slices.SortFunc(stmts, sortConnPreparedStmtBasedOnDeallocationPriority(sessionLinkCreatedTime))

	result := make([]string, 0, min(stmtsToDeallocate, len(stmts)))
	for i := 0; i < stmtsToDeallocate && i < len(stmts); i++ {
		result = append(result, stmts[i].Name)
	}
	return result
}
