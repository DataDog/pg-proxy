// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package setdetect

import (
	"regexp"
	"unicode/utf8"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// A port of pgdog's session-state detection at 96c83f3:
//
//   - pgdog/src/frontend/regex_parser.rs: a cheap prefix test, then a regex
//     set over the first regex_parser_limit (1000) bytes, each pattern a
//     leading keyword after optional comments. In query_parser: auto mode
//     without sharding, a match turns on the parser for that request.
//   - pgdog/src/frontend/router/parser/query/{mod,set,set_config,split}.rs:
//     what the parser then treats as a session change: a SET of a value,
//     DEFAULT or RESET (not SET TRANSACTION or SESSION CHARACTERISTICS),
//     RESET ALL, and SELECT set_config(...) when it is the only target and
//     all three arguments are constants. A multi-statement query is split
//     when it holds a SET; otherwise only its first statement is routed.
//
// A SET LOCAL is tracked for the transaction only, so it does not count.

const pgdogRegexLimit = 1000

const pgdogCommentPrefix = `(?i)^\s*(?:(?:--[^\n]*\n|/\*[\s\S]*?\*/)\s*)*`

var (
	pgdogSetRE = regexp.MustCompile(pgdogCommentPrefix + `(RE)?SET`)
	// The whole CMD_BASE set, which decides whether auto mode parses.
	pgdogCmdRE = regexp.MustCompile(pgdogCommentPrefix + `(?:(RE)?SET|BEGIN|START|COMMIT|END|ROLLBACK|LISTEN|UNLISTEN|NOTIFY|DISCARD|ABORT)`)
)

var pgdogCommon = []string{"SELECT", "INSERT", "UPDATE", "DELETE", "WITH"}

func pgdogIsCommon(q string) bool {
	for _, k := range pgdogCommon {
		if len(q) >= len(k) && eqFoldUpper(q[:len(k)], k) {
			return true
		}
	}
	return false
}

func eqFoldUpper(w, upper string) bool {
	for i := 0; i < len(w); i++ {
		c := w[i]
		if c >= 'a' && c <= 'z' {
			c -= 32
		}
		if c != upper[i] {
			return false
		}
	}
	return true
}

func truncateUTF8(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	for limit > 0 && !utf8.RuneStart(s[limit]) {
		limit--
	}
	return s[:limit]
}

// PgdogRegex is the regex gate alone: dirty when the leading keyword is SET
// or RESET.
type PgdogRegex struct{}

// Name implements Detector.
func (PgdogRegex) Name() string { return "pgdog-regex" }

// Classify implements Detector.
func (PgdogRegex) Classify(sql string, _ bool) Verdict {
	if pgdogIsCommon(sql) {
		return Clean
	}
	return verdict(pgdogPrefixMatch(truncateUTF8(sql, pgdogRegexLimit), pgdogSetWords))
}

// PgdogAuto is query_parser: auto on a cluster without sharding: the regex
// gate decides whether to parse, and the parser decides.
type PgdogAuto struct{}

// Name implements Detector.
func (PgdogAuto) Name() string { return "pgdog-auto" }

// Classify implements Detector.
func (PgdogAuto) Classify(sql string, _ bool) Verdict {
	if pgdogIsCommon(sql) || !pgdogPrefixMatch(truncateUTF8(sql, pgdogRegexLimit), pgdogCmdWords) {
		return Clean
	}
	return verdict(pgdogParse(sql))
}

// PgdogOn is query_parser: on: every query is parsed.
type PgdogOn struct{}

// Name implements Detector.
func (PgdogOn) Name() string { return "pgdog-on" }

// Classify implements Detector.
func (PgdogOn) Classify(sql string, _ bool) Verdict { return verdict(pgdogParse(sql)) }

func pgdogParse(sql string) bool {
	tree, err := pg_query.Parse(sql)
	if err != nil {
		// pgdog returns the parse error; the server would reject the text too.
		return false
	}
	stmts := tree.GetStmts()
	if len(stmts) == 0 {
		return false
	}
	if len(stmts) > 1 {
		hasSet := false
		for _, s := range stmts {
			if v := s.GetStmt().GetVariableSetStmt(); v != nil && v.GetKind() != pg_query.VariableSetKind_VAR_SET_MULTI {
				hasSet = true
			}
		}
		if hasSet {
			// try_multi_set: all SETs, or split and route each statement.
			for _, s := range stmts {
				if pgdogRoot(s.GetStmt()) {
					return true
				}
			}
			return false
		}
	}
	return pgdogRoot(stmts[0].GetStmt())
}

func pgdogRoot(node *pg_query.Node) bool {
	if v := node.GetVariableSetStmt(); v != nil {
		switch v.GetKind() {
		case pg_query.VariableSetKind_VAR_SET_MULTI:
			return false
		case pg_query.VariableSetKind_VAR_RESET_ALL:
			return true
		}
		return !v.GetIsLocal()
	}
	if sel := node.GetSelectStmt(); sel != nil {
		targets := sel.GetTargetList()
		if len(targets) != 1 {
			return false
		}
		fc := targets[0].GetResTarget().GetVal().GetFuncCall()
		if fc == nil {
			return false
		}
		name := FuncName(fc)
		if name != "set_config" && name != "pg_catalog.set_config" {
			return false
		}
		local, ok := pgdogConstArgs(fc)
		return ok && !local
	}
	return false
}

// pgdogConstArgs mirrors set_config.rs parse_args: a constant string name, a
// constant string or NULL value, and a constant boolean is_local.
func pgdogConstArgs(fc *pg_query.FuncCall) (local, ok bool) {
	args := fc.GetArgs()
	if len(args) < 3 {
		return false, false
	}
	if c := args[0].GetAConst(); c == nil || c.GetSval() == nil {
		return false, false
	}
	if c := args[1].GetAConst(); c == nil || !c.GetIsnull() && c.GetSval() == nil {
		return false, false
	}
	c := args[2].GetAConst()
	if c == nil || c.GetBoolval() == nil {
		return false, false
	}
	return c.GetBoolval().GetBoolval(), true
}

// The regexes above, as byte loops. pgdogSetRE and pgdogCmdRE stay as the
// reference the fuzz test compares against.
var (
	pgdogSetWords = []string{"SET", "RESET"}
	pgdogCmdWords = []string{"SET", "RESET", "BEGIN", "START", "COMMIT", "END", "ROLLBACK", "LISTEN", "UNLISTEN", "NOTIFY", "DISCARD", "ABORT"}
)

// pgdogPrefixMatch reports whether q matches pgdogCommentPrefix followed by
// one of words (upper case, compared ASCII case-insensitively, no word
// boundary), with RE2 semantics: \s is [\t\n\f\r ], a -- comment needs its
// newline, and a lazy /* ... */ may end at any later */ if that lets the
// rest match.
func pgdogPrefixMatch(q string, words []string) bool {
	var seen [pgdogRegexLimit + 1]bool
	var try func(p int) bool
	try = func(p int) bool {
		for p < len(q) && isRE2Space(q[p]) {
			p++
		}
		if seen[p] {
			return false
		}
		seen[p] = true
		rest := q[p:]
		for _, w := range words {
			if len(rest) >= len(w) && eqFoldUpper(rest[:len(w)], w) {
				return true
			}
		}
		switch {
		case len(rest) >= 2 && rest[0] == '-' && rest[1] == '-':
			for i := 2; i < len(rest); i++ {
				if rest[i] == '\n' {
					return try(p + i + 1)
				}
			}
		case len(rest) >= 2 && rest[0] == '/' && rest[1] == '*':
			for i := 2; i+1 < len(rest); i++ {
				if rest[i] == '*' && rest[i+1] == '/' && try(p+i+2) {
					return true
				}
			}
		}
		return false
	}
	return try(0)
}

func isRE2Space(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\f' || c == '\r' }
