// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package main

import (
	"regexp"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/proto"

	sd "github.com/DataDog/pg-proxy/internal/setdetect"
)

// The rule labeler. It applies the category rules to the parse tree of text
// whose label was not written by hand (regression SQL and other projects'
// tests). The execution oracle checks its output independently.

type label struct {
	Label    string
	Category string
	Note     string
}

var (
	dirty = func(cat, note string) label { return label{sd.LabelDirty, cat, note} }
	clean = func(cat, note string) label { return label{sd.LabelClean, cat, note} }
	// either: a detector may answer either way without penalty.
	either = func(cat, note string) label { return label{sd.LabelEither, cat, note} }
)

// labelSQL labels the full text of one message. ok is false when the text
// does not parse; the caller labels those (Postgres rejects the message, so
// nothing in it runs).
func labelSQL(sql string) (label, bool) {
	tree, err := pg_query.Parse(sql)
	if err != nil {
		return label{}, false
	}
	stmts := tree.GetStmts()
	rolledBack := rollbackFrom(stmts)
	var first *label
	var eitherL *label
	var cleans []label
	for i, raw := range stmts {
		text := stmtText(sql, raw)
		l := labelStmt(raw.GetStmt(), text)
		switch l.Label {
		case sd.LabelDirty:
			if rolledBack[i] {
				l = either(l.Category, "inside a transaction the message rolls back: "+l.Note)
				if eitherL == nil {
					eitherL = &l
				}
				continue
			}
			if first == nil {
				if i > 0 {
					l = dirty("P5", "statement "+itoa(i+1)+" is "+l.Category+": "+l.Note)
				}
				first = &l
			}
		case sd.LabelEither:
			if eitherL == nil {
				eitherL = &l
			}
		default:
			cleans = append(cleans, l)
		}
	}
	if first != nil {
		return *first, true
	}
	if eitherL != nil {
		return *eitherL, true
	}
	if len(stmts) == 0 {
		return clean(lexicalCategory(sql), "empty query"), true
	}
	best := cleans[0]
	for _, l := range cleans[1:] {
		if cleanRank(l.Category) < cleanRank(best.Category) {
			best = l
		}
	}
	if best.Category == "N7" {
		best.Category = lexicalCategory(sql)
	}
	return best, true
}

// rollbackFrom marks statements inside an explicit transaction block that a
// later ROLLBACK in the same message ends.
func rollbackFrom(stmts []*pg_query.RawStmt) []bool {
	out := make([]bool, len(stmts))
	open := -1
	for i, raw := range stmts {
		ts := raw.GetStmt().GetTransactionStmt()
		if ts == nil {
			continue
		}
		switch ts.GetKind() {
		case pg_query.TransactionStmtKind_TRANS_STMT_BEGIN, pg_query.TransactionStmtKind_TRANS_STMT_START:
			open = i
		case pg_query.TransactionStmtKind_TRANS_STMT_ROLLBACK:
			if open >= 0 {
				for j := open; j < i; j++ {
					out[j] = true
				}
			}
			open = -1
		case pg_query.TransactionStmtKind_TRANS_STMT_COMMIT:
			open = -1
		}
	}
	return out
}

func cleanRank(cat string) int {
	return strings.Index("N4N1N6N5N2N3N7", cat)
}

func stmtText(sql string, raw *pg_query.RawStmt) string {
	start := int(raw.GetStmtLocation())
	end := len(sql)
	if raw.GetStmtLen() > 0 {
		end = start + int(raw.GetStmtLen())
	}
	if start > len(sql) || end > len(sql) || start > end {
		return sql
	}
	return sql[start:end]
}

func labelStmt(node *pg_query.Node, text string) label {
	switch n := node.GetNode().(type) {
	case *pg_query.Node_VariableSetStmt:
		return labelVariableSet(n.VariableSetStmt)
	case *pg_query.Node_ConstraintsSetStmt:
		return clean("N4", "SET CONSTRAINTS lasts until the transaction ends")
	case *pg_query.Node_DiscardStmt:
		return either("N5", "DISCARD resets state")
	case *pg_query.Node_DoStmt:
		return labelDo(n.DoStmt)
	case *pg_query.Node_ExecuteStmt:
		return either("P4", "EXECUTE: depends on the prepared statement")
	case *pg_query.Node_CallStmt:
		if l, found := labelExecuted(node); found {
			return l
		}
		return either("P4", "CALL: depends on the procedure body")
	case *pg_query.Node_PrepareStmt:
		if l, found := labelExecuted(n.PrepareStmt.GetQuery()); found && l.Label != sd.LabelClean {
			return either("P4", "PREPARE does not run its query: "+l.Note)
		}
		return clean("N7", "PREPARE")
	case *pg_query.Node_ExplainStmt:
		if n.ExplainStmt.GetQuery().GetExecuteStmt() != nil {
			if explainAnalyzes(n.ExplainStmt) {
				return either("P4", "EXPLAIN ANALYZE EXECUTE: depends on the prepared statement")
			}
			return clean("N7", "EXPLAIN EXECUTE")
		}
		inner, found := labelExecuted(n.ExplainStmt.GetQuery())
		if !found || inner.Label == sd.LabelClean {
			return clean("N7", "EXPLAIN")
		}
		if explainAnalyzes(n.ExplainStmt) {
			if inner.Label == sd.LabelDirty {
				return dirty("P4", "EXPLAIN ANALYZE runs "+inner.Note)
			}
			return inner
		}
		return either("P4", "EXPLAIN without ANALYZE does not run "+inner.Note)
	case *pg_query.Node_CopyStmt:
		if q := n.CopyStmt.GetQuery(); q != nil {
			if l, found := labelExecuted(q); found {
				if l.Label == sd.LabelDirty {
					return dirty("P4", "COPY (query) runs "+l.Note)
				}
				return l
			}
		}
		return clean("N7", "COPY")
	case *pg_query.Node_DeclareCursorStmt:
		if l, found := labelExecuted(n.DeclareCursorStmt.GetQuery()); found && l.Label != sd.LabelClean {
			return either("P4", "DECLARE runs its query at FETCH: "+l.Note)
		}
		return clean("N7", "DECLARE")
	case *pg_query.Node_VariableShowStmt:
		return clean("N6", "SHOW")
	case *pg_query.Node_SelectStmt, *pg_query.Node_InsertStmt, *pg_query.Node_UpdateStmt,
		*pg_query.Node_DeleteStmt, *pg_query.Node_MergeStmt:
		if l, found := labelExecuted(node); found {
			return l
		}
		return labelDML(node, text)
	case *pg_query.Node_CreateTableAsStmt:
		ctas := n.CreateTableAsStmt
		if ctas.GetQuery().GetExecuteStmt() != nil && !ctas.GetInto().GetSkipData() {
			return either("P4", "CREATE TABLE AS EXECUTE: depends on the prepared statement")
		}
		if l, found := labelExecuted(ctas.GetQuery()); found {
			if ctas.GetInto().GetSkipData() {
				return either(l.Category, "WITH NO DATA does not run "+l.Note)
			}
			return l
		}
		return clean("N7", "CREATE TABLE AS")
	case *pg_query.Node_AlterRoleSetStmt, *pg_query.Node_AlterDatabaseSetStmt, *pg_query.Node_AlterSystemStmt:
		return clean("N4", "stored setting, not this session's")
	case *pg_query.Node_CreateFunctionStmt, *pg_query.Node_AlterFunctionStmt:
		if hasFuncs(node) {
			return either("N4", "function definition mentions a setter it does not run")
		}
		return ddlCategory(text, "function definition")
	case *pg_query.Node_TransactionStmt:
		return clean("N7", "transaction control")
	}
	// Other utility statements. A setter call inside one is not run by the
	// statement itself (view bodies, defaults, rules), so the label allows
	// either answer.
	if hasFuncs(node) {
		return either("N4", "DDL mentions a setter it does not run")
	}
	return ddlCategory(text, "utility statement")
}

func ddlCategory(text, note string) label {
	if containsSetToken(text) {
		return clean("N4", note+" with a SET clause")
	}
	return clean("N7", note)
}

func labelVariableSet(v *pg_query.VariableSetStmt) label {
	name := v.GetName()
	switch v.GetKind() {
	case pg_query.VariableSetKind_VAR_RESET, pg_query.VariableSetKind_VAR_RESET_ALL:
		return either("N5", "RESET "+name)
	case pg_query.VariableSetKind_VAR_SET_DEFAULT, pg_query.VariableSetKind_VAR_SET_CURRENT:
		// SET x TO DEFAULT is RESET x; FROM CURRENT keeps the current value.
		return either("N5", "SET "+name+" TO DEFAULT or FROM CURRENT")
	case pg_query.VariableSetKind_VAR_SET_MULTI:
		if name == "SESSION CHARACTERISTICS" {
			return dirty("P1", "SET SESSION CHARACTERISTICS")
		}
		return clean("N4", "SET "+name)
	}
	if v.GetIsLocal() {
		return clean("N4", "SET LOCAL "+name)
	}
	if name == "role" && len(v.GetArgs()) == 1 && strings.EqualFold(v.GetArgs()[0].GetAConst().GetSval().GetSval(), "none") {
		return either("N5", "SET ROLE NONE is RESET ROLE")
	}
	return dirty("P1", "SET "+name)
}

// labelExecuted looks for setters the statement runs. found is false when
// there are none.
func labelExecuted(node proto.Message) (label, bool) {
	var out *label
	note := func(l label) bool {
		if out == nil || out.Label != sd.LabelDirty && l.Label == sd.LabelDirty {
			out = &l
		}
		return out.Label != sd.LabelDirty
	}
	sd.WalkAST(node, func(m proto.Message) bool {
		switch x := m.(type) {
		case *pg_query.FuncCall:
			switch LastName(x) {
			case "set_config":
				if isLocalTrue(x) {
					return note(either("P2", "set_config with literal is_local = true has no effect in autocommit"))
				}
				return note(dirty("P2", "set_config"))
			case "set_limit":
				return note(dirty("P7", "pg_trgm set_limit"))
			}
		case *pg_query.UpdateStmt:
			if x.GetRelation().GetRelname() == "pg_settings" {
				return note(dirty("P3", "UPDATE pg_settings"))
			}
		case *pg_query.CreateFunctionStmt, *pg_query.ViewStmt:
			// Bodies are not run here.
			return true
		}
		return true
	})
	if out == nil {
		return label{}, false
	}
	return *out, true
}

// LastName mirrors setdetect.LastName with the parser's case folding.
func LastName(fc *pg_query.FuncCall) string { return sd.LastName(fc) }

func isLocalTrue(fc *pg_query.FuncCall) bool {
	args := fc.GetArgs()
	if len(args) != 3 {
		return false
	}
	arg := args[2]
	if tc := arg.GetTypeCast(); tc != nil {
		arg = tc.GetArg()
	}
	c := arg.GetAConst()
	if c == nil || c.GetIsnull() {
		return false
	}
	if b := c.GetBoolval(); b != nil {
		return b.GetBoolval()
	}
	if s := c.GetSval(); s != nil {
		switch strings.ToLower(strings.TrimSpace(s.GetSval())) {
		case "true", "t", "on", "yes", "y", "1":
			return true
		}
	}
	return false
}

func hasFuncs(node proto.Message) bool {
	found := false
	sd.WalkAST(node, func(m proto.Message) bool {
		if fc, ok := m.(*pg_query.FuncCall); ok {
			switch LastName(fc) {
			case "set_config", "set_limit":
				found = true
				return false
			}
		}
		return true
	})
	return found
}

func explainAnalyzes(e *pg_query.ExplainStmt) bool {
	for _, o := range e.GetOptions() {
		d := o.GetDefElem()
		if d.GetDefname() != "analyze" {
			continue
		}
		if d.GetArg() == nil {
			return true
		}
		if s := d.GetArg().GetString_(); s != nil {
			v := strings.ToLower(s.GetSval())
			return v == "true" || v == "on" || v == "1"
		}
		if b := d.GetArg().GetBoolean(); b != nil {
			return b.GetBoolval()
		}
		if i := d.GetArg().GetInteger(); i != nil {
			return i.GetIval() != 0
		}
		return true
	}
	return false
}

func labelDML(node *pg_query.Node, text string) label {
	switch n := node.GetNode().(type) {
	case *pg_query.Node_UpdateStmt:
		return clean("N1", "UPDATE ... SET")
	case *pg_query.Node_InsertStmt:
		if oc := n.InsertStmt.GetOnConflictClause(); oc != nil && oc.GetAction() == pg_query.OnConflictAction_ONCONFLICT_UPDATE {
			return clean("N1", "upsert")
		}
	case *pg_query.Node_MergeStmt:
		return clean("N1", "MERGE")
	}
	if readsSettings(node) {
		return clean("N6", "reads settings")
	}
	return clean("N7", "")
}

func readsSettings(node proto.Message) bool {
	found := false
	sd.WalkAST(node, func(m proto.Message) bool {
		switch x := m.(type) {
		case *pg_query.FuncCall:
			if n := LastName(x); n == "current_setting" || n == "show_limit" {
				found = true
			}
		case *pg_query.RangeVar:
			if x.GetRelname() == "pg_settings" {
				found = true
			}
		}
		return !found
	})
	return found
}

var (
	doSetRE     = regexp.MustCompile(`(?is)set_config|pg_settings|set_limit|(?:^|;|\bbegin|\bthen|\bloop|\belse)\s*set\s+(?:session\s+)?([a-z_."]+)`)
	doExecuteRE = regexp.MustCompile(`(?i)\bexecute\b`)
)

// labelDo labels a DO block by its body. The oracle checks it: plpgsql can
// change settings in ways no rule over the text sees.
func labelDo(d *pg_query.DoStmt) label {
	var body string
	for _, a := range d.GetArgs() {
		if de := a.GetDefElem(); de.GetDefname() == "as" {
			body = de.GetArg().GetString_().GetSval()
		}
	}
	for _, m := range doSetRE.FindAllStringSubmatch(body, -1) {
		if m[1] == "" {
			return dirty("P4", "DO body may change settings: "+strings.TrimSpace(m[0]))
		}
		switch strings.ToLower(m[1]) {
		case "local", "constraints", "transaction":
			continue
		}
		return dirty("P4", "DO body runs SET "+m[1])
	}
	if doExecuteRE.MatchString(body) {
		return either("P4", "DO body runs dynamic SQL: oracle decides")
	}
	return clean("N7", "DO block without setters")
}

// lexicalCategory picks N2 or N3 for clean text from where "set" appears.
func lexicalCategory(sql string) string {
	res, err := pg_query.Scan(sql)
	if err != nil {
		return "N7"
	}
	inIdent, inLit := false, false
	for _, t := range res.GetTokens() {
		s := strings.ToLower(sql[t.GetStart():t.GetEnd()])
		if !strings.Contains(s, "set") {
			continue
		}
		switch t.GetToken() {
		case pg_query.Token_SCONST, pg_query.Token_USCONST, pg_query.Token_BCONST, pg_query.Token_XCONST,
			pg_query.Token_SQL_COMMENT, pg_query.Token_C_COMMENT:
			inLit = true
		default:
			inIdent = true
		}
	}
	switch {
	case inIdent:
		return "N2"
	case inLit:
		return "N3"
	}
	return "N7"
}

func containsSetToken(sql string) bool {
	res, err := pg_query.Scan(sql)
	if err != nil {
		return false
	}
	for _, t := range res.GetTokens() {
		if t.GetToken() == pg_query.Token_SET {
			return true
		}
	}
	return false
}

func itoa(i int) string {
	const digits = "0123456789"
	if i < 10 {
		return digits[i : i+1]
	}
	return itoa(i/10) + digits[i%10:i%10+1]
}
