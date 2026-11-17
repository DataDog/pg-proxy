// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package setdetect

import (
	pg_query "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/proto"
)

// Detectors built on pg_query (cgo). The AST rule: dirty on a session-level
// SET (not LOCAL, not a transaction form, not a reset), SET SESSION
// CHARACTERISTICS, a call to set_config or set_limit anywhere, an UPDATE of
// pg_settings, and DO, EXECUTE and CALL. Text that does not parse is dirty.

// PgQueryParse parses every query.
type PgQueryParse struct{}

// Name implements Detector.
func (PgQueryParse) Name() string { return "pgquery-parse" }

// Classify implements Detector.
func (PgQueryParse) Classify(sql string, _ bool) Verdict { return verdict(astDirty(sql)) }

func astDirty(sql string) bool {
	tree, err := pg_query.Parse(sql)
	if err != nil {
		return true
	}
	walk := astNeedsWalk(sql)
	for _, raw := range tree.GetStmts() {
		if astStmtDirty(raw.GetStmt()) || walk && astWalkDirty(raw.GetStmt()) {
			return true
		}
	}
	return false
}

func astStmtDirty(node *pg_query.Node) bool {
	switch n := node.GetNode().(type) {
	case *pg_query.Node_VariableSetStmt:
		v := n.VariableSetStmt
		switch v.GetKind() {
		case pg_query.VariableSetKind_VAR_SET_VALUE:
			return !v.GetIsLocal()
		case pg_query.VariableSetKind_VAR_SET_MULTI:
			return v.GetName() == "SESSION CHARACTERISTICS"
		}
		return false
	case *pg_query.Node_DoStmt, *pg_query.Node_ExecuteStmt, *pg_query.Node_CallStmt:
		return true
	}
	return false
}

// astNeedsWalk is a verdict-preserving shortcut: the walk below can only
// find a name the text spells (set_config, set_limit, pg_settings, an
// EXECUTE) or one a U& escape hides. Without those it would find nothing.
func astNeedsWalk(sql string) bool {
	return containsFold(sql, "set_config") || containsFold(sql, "set_limit") || containsFold(sql, "pg_settings") ||
		containsFold(sql, "execute") || containsFold(sql, "u&")
}

func astWalkDirty(node *pg_query.Node) bool {
	dirty := false
	WalkAST(node, func(m proto.Message) bool {
		switch x := m.(type) {
		case *pg_query.FuncCall:
			if n := LastName(x); n == "set_config" || n == "set_limit" {
				dirty = true
			}
		case *pg_query.UpdateStmt:
			if x.GetRelation().GetRelname() == "pg_settings" {
				dirty = true
			}
		case *pg_query.ExecuteStmt:
			dirty = true // EXPLAIN ANALYZE EXECUTE, CREATE TABLE AS EXECUTE
		}
		return !dirty
	})
	return dirty
}

// PgQueryPrefilter parses only text that a cheap test cannot clear: the
// test passes text with no "set", "u&", "exec", "call" or "do" in it.
type PgQueryPrefilter struct{}

// Name implements Detector.
func (PgQueryPrefilter) Name() string { return "pgquery-prefilter" }

// Classify implements Detector.
func (PgQueryPrefilter) Classify(sql string, _ bool) Verdict {
	if !prefilterHit(sql) {
		return Clean
	}
	return verdict(astDirty(sql))
}

// prefilterHit reports whether sql contains, ignoring ASCII case, any of
// "set", "u&", "exec", "call" or "do". Without one of those no rule of the
// AST detector can fire: every SET form, set_config, set_limit and
// pg_settings contain "set", and a U& escape is the only way to spell them
// otherwise.
func prefilterHit(sql string) bool {
	n := len(sql)
	for i := 0; i+1 < n; i++ {
		if !prefilterFirst[sql[i]] {
			continue
		}
		switch sql[i] | 0x20 {
		case 's':
			if i+2 < n && sql[i+1]|0x20 == 'e' && sql[i+2]|0x20 == 't' {
				return true
			}
		case 'u':
			if sql[i+1] == '&' {
				return true
			}
		case 'e':
			if i+3 < n && sql[i+1]|0x20 == 'x' && sql[i+2]|0x20 == 'e' && sql[i+3]|0x20 == 'c' {
				return true
			}
		case 'c':
			if i+3 < n && sql[i+1]|0x20 == 'a' && sql[i+2]|0x20 == 'l' && sql[i+3]|0x20 == 'l' {
				return true
			}
		case 'd':
			if sql[i+1]|0x20 == 'o' {
				return true
			}
		}
	}
	return false
}

// prefilterFirst marks the first bytes of the prefilter's patterns.
var prefilterFirst = func() (t [256]bool) {
	for _, c := range []byte("sSuUeEcCdD") {
		t[c] = true
	}
	return t
}()

// PgQueryScan uses pg_query's scanner (cgo, no parse tree) and the lexer's
// keyword rules over its tokens.
type PgQueryScan struct{}

// Name implements Detector.
func (PgQueryScan) Name() string { return "pgquery-scan" }

// Classify implements Detector.
func (PgQueryScan) Classify(sql string, _ bool) Verdict {
	res, err := pg_query.Scan(sql)
	if err != nil {
		return Dirty
	}
	start, afterSet, afterSession, afterScope := true, false, false, false
	for _, t := range res.GetTokens() {
		tok := t.GetToken()
		if tok == pg_query.Token_SQL_COMMENT || tok == pg_query.Token_C_COMMENT {
			continue
		}
		if tok == pg_query.Token_ASCII_59 { // ;
			start, afterSet, afterSession, afterScope = true, false, false, false
			continue
		}
		text := sql[t.GetStart():t.GetEnd()]
		if afterScope {
			// SET local.x, SET local = x, SET local TO x: a setting named local.
			afterScope = false
			if text == "." || text == "=" || tok == pg_query.Token_TO {
				return Dirty
			}
		}
		if afterSession {
			if tok == pg_query.Token_TRANSACTION {
				afterSession, afterScope = false, true
				continue
			}
			return Dirty
		}
		if afterSet {
			afterSet = false
			switch tok {
			case pg_query.Token_LOCAL, pg_query.Token_TRANSACTION, pg_query.Token_CONSTRAINTS:
				afterScope = true
				continue
			case pg_query.Token_SESSION:
				afterSession = true
				continue
			}
			return Dirty
		}
		if start {
			start = false
			switch tok {
			case pg_query.Token_SET:
				afterSet = true
				continue
			case pg_query.Token_DO, pg_query.Token_CALL, pg_query.Token_EXECUTE:
				return Dirty
			}
		}
		switch tok {
		case pg_query.Token_EXECUTE:
			return Dirty
		case pg_query.Token_IDENT, pg_query.Token_UIDENT:
			if len(text) > 1 && (text[0] == 'U' || text[0] == 'u') && text[1] == '&' {
				return Dirty
			}
			if text[0] == '"' {
				text = text[1 : len(text)-1]
			}
			if isSetterName(text) {
				return Dirty
			}
		}
	}
	return verdict(afterSet || afterSession)
}
