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
	customDataConnStateKey     = "preparedStmtsState"
	customDataAuditBaselineKey = "resetAuditBaseline" // an auditBaseline
	// customDataResetSkippedKey is set while the connection's last release skipped
	// the reset, so it may hold another checkout's unnamed statement, until the
	// next link takes it over.
	customDataResetSkippedKey = "resetSkipped"

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

// connState is the prepared statement state of one Postgres connection. It lives in
// the connection's pgx CustomData, so it outlasts each SessionLink and stays with the
// connection while it sits in the pool.
type connState struct {
	// Pointers, so a Bind updates usage in place instead of a read and a write-back.
	stmts map[string]*ConnPreparedStmt
	stats connStats
}

// ConnPreparedStmt is metadata associated with a prepared statement on a connection.
type ConnPreparedStmt struct {
	Name         string
	UsageCount   int
	TimeLastUsed int
}

// setUpConnMetaData returns the connection's prepared statement state, creating it
// on the connection's first checkout.
// TODO standardize how we create pools and add beforeConnect hook
func setUpConnMetaData(conn *pgxpool.Conn) *connState {
	customData := conn.Conn().PgConn().CustomData()
	if state, ok := customData[customDataConnStateKey].(*connState); ok {
		return state
	}
	state := &connState{stmts: make(map[string]*ConnPreparedStmt)}
	customData[customDataConnStateKey] = state
	return state
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
	if stmtsToDeallocate <= 0 || len(connPreparedStmts) == 0 {
		return nil
	}

	return leastValuableStmts(slices.Collect(maps.Values(connPreparedStmts)), stmtsToDeallocate, sessionLinkCreatedTime)
}

// stmtsToDeallocate returns the map keys of the n statements Release should evict
// first. Names come from the keys, which are what Deallocate and delete must use,
// rather than from each entry's copy of its name.
func (s *connState) stmtsToDeallocate(n int, sessionLinkCreatedTime int) []string {
	if n <= 0 || len(s.stmts) == 0 {
		return nil
	}
	stmts := make([]ConnPreparedStmt, 0, len(s.stmts))
	for name, stmt := range s.stmts {
		entry := *stmt
		entry.Name = name
		stmts = append(stmts, entry)
	}
	return leastValuableStmts(stmts, n, sessionLinkCreatedTime)
}

// leastValuableStmts sorts stmts by deallocation priority, in place, and returns the
// names of the first stmtsToDeallocate.
func leastValuableStmts(stmts []ConnPreparedStmt, stmtsToDeallocate int, sessionLinkCreatedTime int) []string {
	slices.SortFunc(stmts, sortConnPreparedStmtBasedOnDeallocationPriority(sessionLinkCreatedTime))

	result := make([]string, 0, stmtsToDeallocate)
	for i := 0; i < stmtsToDeallocate && i < len(stmts); i++ {
		result = append(result, stmts[i].Name)
	}
	return result
}
