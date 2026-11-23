// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package sessionsql

import (
	"strings"
	"testing"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// FuzzScanVsParseTree checks the scanner against rules over the pg_query parse
// tree, on text that parses. The scanner is coarser, so it may report a flag
// the tree rules do not, but never the reverse: that would be a missed change.
// pg_query scans as if standard_conforming_strings were on, so this only
// fuzzes that value. Inputs that fail are saved under testdata/fuzz.
//
//	go test -run '^$' -fuzz FuzzScanVsParseTree -fuzztime 10m ./pkg/sessionsql
func FuzzScanVsParseTree(f *testing.F) {
	for _, c := range loadCorpus(f) {
		if !c.SCSOff {
			f.Add(c.SQL)
		}
	}
	f.Fuzz(func(t *testing.T, sql string) {
		// A Query or Parse message cannot carry a NUL (it ends the string),
		// and pg_query would truncate the text there.
		if strings.IndexByte(sql, 0) >= 0 {
			return
		}
		tree, err := pg_query.Parse(sql)
		if err != nil {
			return
		}
		want := treeFlags(tree)
		if got := Scan(sql, true); !got.Has(want) {
			t.Errorf("scanner says %b, parse tree %b: %q", got, want, sql)
		}
	})
}

// cursorOptHold is CURSOR_OPT_HOLD from PostgreSQL's parsenodes.h.
const cursorOptHold = 0x0020

// treeFlags applies the scanner's rules to a parse tree: a session-level SET
// (not LOCAL, not a transaction form), SET SESSION CHARACTERISTICS, DO,
// EXECUTE, CALL, a WITH HOLD cursor, a call to set_config or set_limit
// anywhere, and an UPDATE of pg_settings change the session.
func treeFlags(tree *pg_query.ParseResult) Flags {
	var flags Flags
	for _, raw := range tree.GetStmts() {
		switch n := raw.GetStmt().GetNode().(type) {
		case *pg_query.Node_VariableSetStmt:
			v := n.VariableSetStmt
			switch v.GetKind() {
			case pg_query.VariableSetKind_VAR_SET_VALUE:
				if !v.GetIsLocal() {
					flags |= MayChangeSession
				}
			case pg_query.VariableSetKind_VAR_SET_MULTI:
				if v.GetName() == "SESSION CHARACTERISTICS" {
					flags |= MayChangeSession
				}
			}
		case *pg_query.Node_DoStmt, *pg_query.Node_ExecuteStmt, *pg_query.Node_CallStmt:
			flags |= MayChangeSession
		case *pg_query.Node_DeclareCursorStmt:
			if n.DeclareCursorStmt.GetOptions()&cursorOptHold != 0 {
				flags |= MayChangeSession
			}
		}
		walkTree(raw.GetStmt(), func(m proto.Message) {
			switch x := m.(type) {
			case *pg_query.FuncCall:
				switch lastName(x) {
				case "set_config", "set_limit":
					flags |= MayChangeSession
				}
			case *pg_query.UpdateStmt:
				if x.GetRelation().GetRelname() == "pg_settings" {
					flags |= MayChangeSession
				}
			case *pg_query.ExecuteStmt:
				flags |= MayChangeSession // EXPLAIN ANALYZE EXECUTE, CREATE TABLE AS EXECUTE
			}
		})
	}
	return flags
}

// walkTree calls fn for every message under m, depth first, m included.
func walkTree(m proto.Message, fn func(proto.Message)) {
	if m != nil {
		walkReflect(m.ProtoReflect(), fn)
	}
}

func walkReflect(m protoreflect.Message, fn func(proto.Message)) {
	if !m.IsValid() {
		return
	}
	fn(m.Interface())
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if fd.Kind() != protoreflect.MessageKind || fd.IsMap() {
			return true
		}
		if fd.IsList() {
			l := v.List()
			for i := 0; i < l.Len(); i++ {
				walkReflect(l.Get(i).Message(), fn)
			}
			return true
		}
		walkReflect(v.Message(), fn)
		return true
	})
}

// lastName returns a function call's unqualified name. The parser has already
// folded unquoted names to lower case.
func lastName(fc *pg_query.FuncCall) string {
	names := fc.GetFuncname()
	if len(names) == 0 {
		return ""
	}
	return names[len(names)-1].GetString_().GetSval()
}
