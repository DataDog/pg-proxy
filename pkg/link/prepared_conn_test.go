// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package link

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStmtsToDeallocate(t *testing.T) {
	tests := []struct {
		name               string
		connPreparedStmts  map[string]ConnPreparedStmt
		sessionCreatedTime int
		stmtsToDeallocateN int
		expectedStmtNames  []string
	}{
		{
			name:               "Deallocate from empty map",
			connPreparedStmts:  map[string]ConnPreparedStmt{},
			sessionCreatedTime: 0,
			stmtsToDeallocateN: 2,
			expectedStmtNames:  []string{},
		},
		{
			name: "Deallocate one statement: 3 -> 2",
			connPreparedStmts: map[string]ConnPreparedStmt{
				"stmt1": {Name: "stmt1", UsageCount: 5},
				"stmt2": {Name: "stmt2", UsageCount: 1},
				"stmt3": {Name: "stmt3", UsageCount: 3},
			},
			sessionCreatedTime: 0,
			stmtsToDeallocateN: 1,
			expectedStmtNames:  []string{"stmt2"},
		},
		{
			name: "Deallocate multiple statements: 5 -> 2",
			connPreparedStmts: map[string]ConnPreparedStmt{
				"stmt1": {Name: "stmt1", UsageCount: 10},
				"stmt2": {Name: "stmt2", UsageCount: 1},
				"stmt3": {Name: "stmt3", UsageCount: 3},
				"stmt4": {Name: "stmt4", UsageCount: 2},
				"stmt5": {Name: "stmt5", UsageCount: 5},
			},
			sessionCreatedTime: 0,
			stmtsToDeallocateN: 3,
			expectedStmtNames:  []string{"stmt2", "stmt4", "stmt3"},
		},
		{
			name: "Use LRU for tie-breaking: same usage counts",
			connPreparedStmts: map[string]ConnPreparedStmt{
				"stmt1": {Name: "stmt1", UsageCount: 5, TimeLastUsed: 300},
				"stmt2": {Name: "stmt2", UsageCount: 5, TimeLastUsed: 100},
				"stmt3": {Name: "stmt3", UsageCount: 5, TimeLastUsed: 200},
			},
			sessionCreatedTime: 0,
			stmtsToDeallocateN: 2,
			expectedStmtNames:  []string{"stmt2", "stmt3"},
		},
		{
			name: "Mix of usage count and LRU",
			connPreparedStmts: map[string]ConnPreparedStmt{
				"stmt1": {Name: "stmt1", UsageCount: 5, TimeLastUsed: 300},
				"stmt2": {Name: "stmt2", UsageCount: 1, TimeLastUsed: 500},
				"stmt3": {Name: "stmt3", UsageCount: 3, TimeLastUsed: 200},
				"stmt4": {Name: "stmt4", UsageCount: 3, TimeLastUsed: 100},
			},
			sessionCreatedTime: 0,
			stmtsToDeallocateN: 3,
			expectedStmtNames:  []string{"stmt2", "stmt4", "stmt3"},
		},
		{
			name: "Deallocate all statements",
			connPreparedStmts: map[string]ConnPreparedStmt{
				"stmt1": {Name: "stmt1", UsageCount: 10},
				"stmt2": {Name: "stmt2", UsageCount: 5},
			},
			sessionCreatedTime: 0,
			stmtsToDeallocateN: 2,
			expectedStmtNames:  []string{"stmt2", "stmt1"},
		},
		{
			name: "Request more statements than available",
			connPreparedStmts: map[string]ConnPreparedStmt{
				"stmt1": {Name: "stmt1", UsageCount: 10},
				"stmt2": {Name: "stmt2", UsageCount: 5},
			},
			sessionCreatedTime: 0,
			stmtsToDeallocateN: 5,
			expectedStmtNames:  []string{"stmt2", "stmt1"},
		},
		{
			name: "Zero statements to deallocate",
			connPreparedStmts: map[string]ConnPreparedStmt{
				"stmt1": {Name: "stmt1", UsageCount: 10},
				"stmt2": {Name: "stmt2", UsageCount: 5},
			},
			sessionCreatedTime: 0,
			stmtsToDeallocateN: 0,
			expectedStmtNames:  []string{},
		},
		{
			name: "Negative statements to deallocate",
			connPreparedStmts: map[string]ConnPreparedStmt{
				"stmt1": {Name: "stmt1", UsageCount: 10},
				"stmt2": {Name: "stmt2", UsageCount: 5},
			},
			sessionCreatedTime: 0,
			stmtsToDeallocateN: -1,
			expectedStmtNames:  []string{},
		},
		{
			name: "Multiple statements with same low usage count - tiebreak by timeLastUsed",
			connPreparedStmts: map[string]ConnPreparedStmt{
				"stmt1": {Name: "stmt1", UsageCount: 1, TimeLastUsed: 150},
				"stmt2": {Name: "stmt2", UsageCount: 1, TimeLastUsed: 100},
				"stmt3": {Name: "stmt3", UsageCount: 1, TimeLastUsed: 200},
				"stmt4": {Name: "stmt4", UsageCount: 1, TimeLastUsed: 120},
				"stmt5": {Name: "stmt5", UsageCount: 5, TimeLastUsed: 50},
			},
			sessionCreatedTime: 0,
			stmtsToDeallocateN: 3,
			expectedStmtNames:  []string{"stmt2", "stmt4", "stmt1"},
		},
		{
			name: "All statements have same usage count - pure LRU ordering",
			connPreparedStmts: map[string]ConnPreparedStmt{
				"stmt1": {Name: "stmt1", UsageCount: 3, TimeLastUsed: 500},
				"stmt2": {Name: "stmt2", UsageCount: 3, TimeLastUsed: 100},
				"stmt3": {Name: "stmt3", UsageCount: 3, TimeLastUsed: 300},
				"stmt4": {Name: "stmt4", UsageCount: 3, TimeLastUsed: 200},
				"stmt5": {Name: "stmt5", UsageCount: 3, TimeLastUsed: 400},
			},
			sessionCreatedTime: 0,
			stmtsToDeallocateN: 3,
			expectedStmtNames:  []string{"stmt2", "stmt4", "stmt3"},
		},
		{
			name: "Identical timeLastUsed values - should still be deterministic",
			connPreparedStmts: map[string]ConnPreparedStmt{
				"stmt1": {Name: "stmt1", UsageCount: 1, TimeLastUsed: 100},
				"stmt2": {Name: "stmt2", UsageCount: 1, TimeLastUsed: 100},
				"stmt3": {Name: "stmt3", UsageCount: 2, TimeLastUsed: 100},
				"stmt4": {Name: "stmt4", UsageCount: 2, TimeLastUsed: 100},
			},
			sessionCreatedTime: 0,
			stmtsToDeallocateN: 2,
			expectedStmtNames:  []string{"stmt1", "stmt2"},
		},
		{
			name: "Statements created before session prioritized over those created after",
			connPreparedStmts: map[string]ConnPreparedStmt{
				"stmt_before_1": {Name: "stmt_before_1", UsageCount: 2, TimeLastUsed: 50},
				"stmt_before_2": {Name: "stmt_before_2", UsageCount: 2, TimeLastUsed: 80},
				"stmt_after_1":  {Name: "stmt_after_1", UsageCount: 2, TimeLastUsed: 150},
				"stmt_after_2":  {Name: "stmt_after_2", UsageCount: 2, TimeLastUsed: 200},
			},
			sessionCreatedTime: 100,
			stmtsToDeallocateN: 2,
			expectedStmtNames:  []string{"stmt_before_1", "stmt_before_2"},
		},
		{
			name: "Session time prioritized over usage count",
			connPreparedStmts: map[string]ConnPreparedStmt{
				"stmt1": {Name: "stmt1", UsageCount: 1, TimeLastUsed: 150},
				"stmt2": {Name: "stmt2", UsageCount: 2, TimeLastUsed: 150},
				"stmt3": {Name: "stmt3", UsageCount: 2, TimeLastUsed: 50},
				"stmt4": {Name: "stmt4", UsageCount: 3, TimeLastUsed: 50},
			},
			sessionCreatedTime: 100,
			stmtsToDeallocateN: 3,
			expectedStmtNames:  []string{"stmt3", "stmt4", "stmt1"},
		},
		{
			name: "Complex scenario - session time prioritized over usage count",
			connPreparedStmts: map[string]ConnPreparedStmt{
				"low_before":    {Name: "low_before", UsageCount: 1, TimeLastUsed: 50},
				"low_after":     {Name: "low_after", UsageCount: 1, TimeLastUsed: 150},
				"medium_before": {Name: "medium_before", UsageCount: 2, TimeLastUsed: 70},
				"medium_after":  {Name: "medium_after", UsageCount: 2, TimeLastUsed: 180},
				"high_before":   {Name: "high_before", UsageCount: 5, TimeLastUsed: 80},
				"high_after":    {Name: "high_after", UsageCount: 5, TimeLastUsed: 200},
			},
			sessionCreatedTime: 100,
			stmtsToDeallocateN: 4,
			expectedStmtNames:  []string{"low_before", "medium_before", "high_before", "low_after"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmtNames := StmtsToDeallocate(tt.connPreparedStmts, tt.stmtsToDeallocateN, tt.sessionCreatedTime)
			assert.ElementsMatch(t, tt.expectedStmtNames, stmtNames, "Statements selected for deallocation should match expected")

			// Release selects through the connection's own state; it must agree.
			state := &connState{stmts: make(map[string]*stmtUsage, len(tt.connPreparedStmts))}
			for name, stmt := range tt.connPreparedStmts {
				state.stmts[name] = &stmtUsage{usageCount: stmt.UsageCount, timeLastUsed: stmt.TimeLastUsed}
			}
			assert.ElementsMatch(t, tt.expectedStmtNames, state.stmtsToDeallocate(tt.stmtsToDeallocateN, tt.sessionCreatedTime))
		})
	}
}
