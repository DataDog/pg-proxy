// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package classifier

import (
	pg_query "github.com/pganalyze/pg_query_go/v6"
)

const txReadOnly = "transaction_read_only"

// knownReadOnlyFunctions lists functions that are known not to modify data.
// TODO: Allow for more read-only functions, possibly with a less rigid method.
var knownReadOnlyFunctions = [][]string{
	// AT TIME ZONE internally calls pg_catalog.timezone(<timezone>, <time>).
	{"pg_catalog", "timezone"},
	{"timezone"},
	{"count"},
	{"sum"},
	{"avg"},
	{"min"},
	{"max"},
	{"string_agg"},
	{"array_agg"},
	{"json_agg"},
	{"jsonb_agg"},
	{"json_build_object"},
	{"jsonb_build_object"},
	{"bool_and"},
	{"bool_or"},
}

// Classification contains the routing-relevant properties of a SQL string.
type Classification struct {
	// ReadOnly is true when every parsed statement is safe to route to a read replica.
	ReadOnly bool
	// StatementCount is the number of statements parsed from the SQL string.
	StatementCount int
}

// Classify parses sql and returns its routing-relevant properties.
func Classify(sql string) (Classification, error) {
	tree, err := pg_query.Parse(sql)
	if err != nil {
		return Classification{}, err
	}

	classification := Classification{StatementCount: len(tree.GetStmts())}
	if classification.StatementCount == 0 {
		return classification, nil
	}

	classification.ReadOnly = true
	for _, stmt := range tree.GetStmts() {
		if !isReadQuery(stmt.GetStmt()) {
			classification.ReadOnly = false
			break
		}
	}

	return classification, nil
}

func isReadQuery(node *pg_query.Node) bool {
	if node == nil {
		return false
	}

	switch n := node.GetNode().(type) {
	case *pg_query.Node_TransactionStmt:
		return isBeginReadOnlyTx(n.TransactionStmt)
	case *pg_query.Node_SelectStmt:
		sel := n.SelectStmt
		if sel == nil {
			return false
		}
		// Locking clauses look like SELECT ... FOR ...
		if len(sel.GetLockingClause()) > 0 {
			return false
		}

		// WITH <CTE(s)> SELECT ... may contain CTEs that modify data.
		if with := sel.GetWithClause(); with != nil {
			for _, cte := range with.GetCtes() {
				if !isReadQuery(cte.GetCommonTableExpr().GetCtequery()) {
					return false
				}
			}
		}

		// It is difficult to discern whether an arbitrary function modifies data.
		if funcCall := getFuncCall(sel); funcCall != nil {
			return isKnownReadOnlyFunction(funcCall)
		}

		// SELECT ... INTO ... writes to a table.
		return sel.GetIntoClause() == nil
	case *pg_query.Node_VariableShowStmt:
		return true
	default:
		// In particular, do not route EXPLAIN statements to replicas.
		return false
	}
}

func getFuncCall(selectStmt *pg_query.SelectStmt) *pg_query.FuncCall {
	if selectStmt == nil {
		return nil
	}

	for _, target := range selectStmt.GetTargetList() {
		res := target.GetResTarget()
		if res == nil || res.GetVal() == nil {
			continue
		}
		if funcCall := res.GetVal().GetFuncCall(); funcCall != nil {
			return funcCall
		}
	}

	return nil
}

func isKnownReadOnlyFunction(funcCall *pg_query.FuncCall) bool {
	if funcCall == nil {
		return false
	}

	function := funcCall.GetFuncname()
	for _, known := range knownReadOnlyFunctions {
		if len(known) == len(function) && matchesKnownReadOnlyFunction(function, known) {
			return true
		}
	}
	return false
}

func matchesKnownReadOnlyFunction(functionNodes []*pg_query.Node, known []string) bool {
	for i, node := range functionNodes {
		funcName, ok := node.GetNode().(*pg_query.Node_String_)
		if !ok || funcName.String_.Sval != known[i] {
			return false
		}
	}
	return true
}

func isBeginReadOnlyTx(txStmt *pg_query.TransactionStmt) bool {
	if txStmt == nil || txStmt.GetKind() != pg_query.TransactionStmtKind_TRANS_STMT_BEGIN {
		return false
	}

	for _, opt := range txStmt.GetOptions() {
		elem := opt.GetDefElem()
		if elem != nil && elem.GetDefname() == txReadOnly {
			return true
		}
	}
	return false
}
