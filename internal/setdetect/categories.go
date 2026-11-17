// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package setdetect

// Categories, in report order. P* cases are dirty (or acceptable-either),
// N* cases clean (or acceptable-either). P9 is out of reach of any text
// detector and is reported separately.
var Categories = []struct{ ID, Name string }{
	{"P1", "Direct SET forms"},
	{"P2", "set_config anywhere"},
	{"P3", "UPDATE pg_settings"},
	{"P4", "Indirect execution (DO, EXECUTE, EXPLAIN ANALYZE, COPY, CALL)"},
	{"P5", "SET after the first statement"},
	{"P6", "Lexical tricks"},
	{"P7", "Extension setters (set_limit)"},
	{"P9", "Server-side code (out of scope)"},
	{"N1", "UPDATE ... SET and upserts"},
	{"N2", "OFFSET and identifiers containing set"},
	{"N3", "Literals and comments containing SET"},
	{"N4", "Scoped SET forms"},
	{"N5", "RESET"},
	{"N6", "Reads of settings"},
	{"N7", "General traffic"},
}

// InScope reports whether a category counts toward in-scope false negatives.
func InScope(category string) bool { return category != "P9" }
