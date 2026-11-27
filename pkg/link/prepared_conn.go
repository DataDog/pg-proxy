// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package link

import (
	"fmt"
	"maps"
	"slices"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	customDataPreparedStmtsKey = "preparedStmts"
	customDataStatsKey         = "preparedStmtsStats"
	customDataAuditBaselineKey = "resetAuditBaseline" // an auditBaseline
	// customDataResetSkippedKey is set while the connection's last release skipped
	// the reset, so it may hold another checkout's unnamed statement, until the
	// next link takes it over.
	customDataResetSkippedKey = "resetSkipped"

	connPreparedStmtDeallocatedCount = "pg_proxy.sessions.connections.prepared_stmt.deallocated.count"
	connPreparedStmtCreatedCount     = "pg_proxy.sessions.connections.prepared_stmt.created.count"
	connPreparedStmtMaxUsage         = "pg_proxy.sessions.connections.prepared_stmt.max_usage"
)

// statistics associated with a connection
type connStats struct {
	// how many prepared stmts have been deallocated on this connection due to max number of prepared stmts being reached
	preparedStmtsDeallocatedCount int
	// how many prepared stmts have been created on this connection
	preparedStmtsCreatedCount int
	// the max number of times a prepared stmt has been used out of all prepared stmts on the connection
	preparedStmtsMaxUsage int
}

// ConnPreparedStmt is metadata associated with a prepared statement on a connection.
type ConnPreparedStmt struct {
	Name         string
	UsageCount   int
	TimeLastUsed int
}

// Adds metadata to a connection, if it does not already exist.
// TODO standardize how we create pools and add beforeConnect hook
func setUpConnMetaData(conn *pgxpool.Conn) {
	customData := conn.Conn().PgConn().CustomData()
	if _, ok := customData[customDataPreparedStmtsKey]; !ok {
		customData[customDataPreparedStmtsKey] = make(map[string]ConnPreparedStmt)
	}
	if _, ok := customData[customDataStatsKey]; !ok {
		customData[customDataStatsKey] = &connStats{}
	}
}

// safely get a connection's custom data
func getConnCustomData(conn *pgxpool.Conn) (map[string]any, error) {
	if conn == nil {
		return nil, fmt.Errorf("pool connection is nil")
	}
	pgxConn := conn.Conn()
	if pgxConn == nil {
		return nil, fmt.Errorf("pool connection did not have an underlying conn")
	}
	pgConn := pgxConn.PgConn()
	if pgConn == nil {
		return nil, fmt.Errorf("pgx connection did not have an underlying pg conn")
	}
	connData := pgConn.CustomData()
	return connData, nil
}

// safely get a connection's stats
func getConnStats(conn *pgxpool.Conn) (*connStats, error) {
	connData, err := getConnCustomData(conn)
	if err != nil {
		return nil, err
	}
	statsData, ok := connData[customDataStatsKey]
	if !ok {
		return nil, fmt.Errorf("expected connection to have data on which prepared statements it has created")
	}
	stats, ok := statsData.(*connStats)
	if !ok {
		return nil, fmt.Errorf("expected a pointer to a connStats struct in the connection's data")
	}
	return stats, nil
}

// safely get a connection's prepared statement data
func getConnPreparedStmts(conn *pgxpool.Conn) (map[string]ConnPreparedStmt, error) {
	connData, err := getConnCustomData(conn)
	if err != nil {
		return nil, err
	}
	connPreparedData, ok := connData[customDataPreparedStmtsKey]
	if !ok {
		return nil, fmt.Errorf("expected connection to have data on which prepared statements it has created")
	}
	connPreparedStmts, ok := connPreparedData.(map[string]ConnPreparedStmt)
	if !ok {
		return nil, fmt.Errorf("expected a map of prepared statement in the connection's data")
	}
	return connPreparedStmts, nil
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

	stmts := slices.Collect(maps.Values(connPreparedStmts))
	slices.SortFunc(stmts, sortConnPreparedStmtBasedOnDeallocationPriority(sessionLinkCreatedTime))

	result := make([]string, 0, stmtsToDeallocate)
	for i := 0; i < stmtsToDeallocate && i < len(stmts); i++ {
		result = append(result, stmts[i].Name)
	}
	return result
}
