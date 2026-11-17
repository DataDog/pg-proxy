// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

// Package purego tries a pure-Go Postgres-dialect parser (CockroachDB's)
// with the same AST rules as the pg_query detector. It is its own module so
// that its large dependency stays out of pg-proxy's go.mod.
package purego

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/parser"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
)

// Dirty applies the AST rules: a session-level SET, SET SESSION
// CHARACTERISTICS, set_config or set_limit anywhere in expressions, an
// UPDATE of pg_settings, DO, EXECUTE and CALL. Text CockroachDB's grammar
// rejects is dirty.
func Dirty(sql string) (dirty bool) {
	stmts, err := parse(sql)
	if err != nil {
		return true
	}
	for _, s := range stmts {
		if stmtDirty(s.AST) {
			return true
		}
	}
	return false
}

// Panics counts inputs on which the parser panicked.
var Panics int

// parse recovers from the parser's panics, which some inputs trigger, and
// reports them as errors.
func parse(sql string) (stmts statements.Statements, err error) {
	defer func() {
		if r := recover(); r != nil {
			Panics++
			err = fmt.Errorf("parser panic: %v", r)
		}
	}()
	return parser.Parse(sql)
}

func stmtDirty(stmt tree.Statement) bool {
	switch n := stmt.(type) {
	case *tree.SetVar:
		return !n.Local && !n.Reset && !n.ResetAll
	case *tree.SetSessionCharacteristics, *tree.DoBlock, *tree.Execute, *tree.Call:
		return true
	case *tree.Update:
		if t, ok := n.Table.(*tree.AliasedTableExpr); ok {
			if tn, ok := t.Expr.(*tree.TableName); ok && strings.EqualFold(string(tn.ObjectName), "pg_settings") {
				return true
			}
		}
	case *tree.Explain:
		return stmtDirty(n.Statement)
	case *tree.ExplainAnalyze:
		return stmtDirty(n.Statement)
	}
	dirty := false
	_, _ = tree.SimpleStmtVisit(stmt, func(expr tree.Expr) (bool, tree.Expr, error) {
		if f, ok := expr.(*tree.FuncExpr); ok {
			name := strings.ToLower(f.Func.String())
			if i := strings.LastIndexByte(name, '.'); i >= 0 {
				name = name[i+1:]
			}
			name = strings.Trim(name, `"`)
			if name == "set_config" || name == "set_limit" {
				dirty = true
				return false, expr, nil
			}
		}
		if sub, ok := expr.(*tree.Subquery); ok {
			if sel, ok := sub.Select.(*tree.ParenSelect); ok && stmtDirty(sel.Select) {
				dirty = true
				return false, expr, nil
			}
		}
		return true, expr, nil
	})
	if dirty {
		return true
	}
	// CTEs and subqueries in FROM are not expressions; check the text of
	// the formatted statement as a fallback for nested statements.
	if sel, ok := stmt.(*tree.Select); ok {
		if sel.With != nil {
			for _, cte := range sel.With.CTEList {
				if stmtDirty(cte.Stmt) {
					return true
				}
			}
		}
		if sc, ok := sel.Select.(*tree.SelectClause); ok {
			for _, from := range sc.From.Tables {
				if fromDirty(from) {
					return true
				}
			}
		}
	}
	return false
}

func fromDirty(t tree.TableExpr) bool {
	switch x := t.(type) {
	case *tree.AliasedTableExpr:
		return fromDirty(x.Expr)
	case *tree.Subquery:
		if p, ok := x.Select.(*tree.ParenSelect); ok {
			return stmtDirty(p.Select)
		}
	case *tree.RowsFromExpr:
		for _, e := range x.Items {
			if f, ok := e.(*tree.FuncExpr); ok {
				name := strings.ToLower(f.Func.String())
				if strings.HasSuffix(name, "set_config") || strings.HasSuffix(name, "set_limit") {
					return true
				}
			}
		}
	case *tree.JoinTableExpr:
		return fromDirty(x.Left) || fromDirty(x.Right)
	}
	return false
}
