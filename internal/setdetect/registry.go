// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package setdetect

// Detectors returns every text detector, in report order.
func Detectors() []Info {
	return []Info{
		{Detector: Substring{}, Family: "baseline", Files: []string{"simple.go"}, Note: `"set" anywhere (the proposal)`},
		{Detector: WordBoundary{}, Family: "baseline", Files: []string{"simple.go"}, Note: `"set" as a whole word`},
		{Detector: PgdogRegex{}, Family: "pgdog", Files: []string{"pgdog.go"}, Note: "leading (RE)SET regex after comments"},
		{Detector: PgdogAuto{}, Family: "pgdog", Cgo: true, Files: []string{"pgdog.go", "astwalk.go"}, Note: "regex gate, then pg_query rules (query_parser: auto)"},
		{Detector: PgdogOn{}, Family: "pgdog", Cgo: true, Files: []string{"pgdog.go", "astwalk.go"}, Note: "pg_query rules on every query (query_parser: on)"},
		{Detector: ProxySQL{}, Family: "proxysql", Files: []string{"proxysql.go"}, Note: "digest prefix SET/RESET/DISCARD, tracked-variable regex, pinning prefixes"},
		{Detector: PgQueryParse{}, Family: "pg_query", Cgo: true, Files: []string{"pgquery.go", "astwalk.go"}, Note: "parse every query, walk the AST"},
		{Detector: PgQueryPrefilter{}, Family: "pg_query", Cgo: true, Files: []string{"pgquery.go", "astwalk.go"}, Note: "substring prefilter, then parse"},
		{Detector: PgQueryScan{}, Family: "pg_query", Cgo: true, Files: []string{"pgquery.go"}, Note: "pg_query scanner tokens + lexer rules"},
		{Detector: NewCached(PgQueryParse{}, 4096), Family: "pg_query", Cgo: true, Files: []string{"pgquery.go", "astwalk.go", "cache.go"}, Note: "verdict cache keyed by text hash, then parse"},
		{Detector: Lexer{}, Family: "lexer", Files: []string{"lexer.go"}, Note: "hand-written token-aware lexer"},
		{Detector: TwoStage{}, Family: "lexer", Files: []string{"lexer.go", "twostage.go"}, Note: "byte-loop prefilter, then lexer"},
		{Detector: TwoStageSWAR{}, Family: "lexer", Files: []string{"lexer.go", "twostage.go", "twostage_swar.go"}, Note: "SWAR byte-pair prefilter, then lexer"},
		{Detector: TwoStageDFA{}, Family: "lexer", Files: []string{"lexer.go", "twostage.go", "twostage_dfa.go"}, Note: "Aho-Corasick prefilter, then lexer"},
	}
}
