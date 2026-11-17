// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package setdetect

import (
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// WalkAST calls fn for every message under m, depth first, m included. fn
// returns false to stop the walk; WalkAST then returns false too.
func WalkAST(m proto.Message, fn func(proto.Message) bool) bool {
	if m == nil {
		return true
	}
	return walkReflect(m.ProtoReflect(), fn)
}

func walkReflect(m protoreflect.Message, fn func(proto.Message) bool) bool {
	if !m.IsValid() {
		return true
	}
	if !fn(m.Interface()) {
		return false
	}
	ok := true
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if fd.Kind() != protoreflect.MessageKind {
			return true
		}
		switch {
		case fd.IsList():
			l := v.List()
			for i := 0; i < l.Len(); i++ {
				if !walkReflect(l.Get(i).Message(), fn) {
					ok = false
					return false
				}
			}
		case fd.IsMap():
		default:
			if !walkReflect(v.Message(), fn) {
				ok = false
				return false
			}
		}
		return true
	})
	return ok
}

// FuncName returns the dotted, lowercased name of a function call, e.g.
// "pg_catalog.set_config". Identifiers are already case-folded or unquoted by
// the parser, so "SET_CONFIG" and "set_config" agree and "Set_Config" (quoted)
// does not.
func FuncName(fc *pg_query.FuncCall) string {
	parts := make([]string, 0, len(fc.GetFuncname()))
	for _, n := range fc.GetFuncname() {
		parts = append(parts, n.GetString_().GetSval())
	}
	return strings.Join(parts, ".")
}

// LastName returns the unqualified function name.
func LastName(fc *pg_query.FuncCall) string {
	names := fc.GetFuncname()
	if len(names) == 0 {
		return ""
	}
	return names[len(names)-1].GetString_().GetSval()
}
