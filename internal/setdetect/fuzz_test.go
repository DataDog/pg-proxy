// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package setdetect

import (
	"strings"
	"testing"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// FuzzLexerVsAST checks the lexer against the pg_query AST detector on text
// that parses. The lexer may say dirty where the AST says clean (it is
// coarser), but never the reverse: that would be a false negative.
// Inputs that fail are saved under testdata/fuzz by go test.
//
//	go test -run '^$' -fuzz FuzzLexerVsAST -fuzztime 10m ./internal/setdetect
func FuzzLexerVsAST(f *testing.F) {
	cases, err := LoadCorpus("corpus/corpus.jsonl")
	if err != nil {
		f.Fatal(err)
	}
	for _, c := range cases {
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
		if _, err := pg_query.Parse(sql); err != nil {
			return
		}
		if astDirty(sql) && !lexDirty(sql, true) {
			t.Errorf("lexer clean, AST dirty: %q", sql)
		}
		// The two-stage variants must agree with the lexer exactly.
		lex := verdict(lexDirty(sql, true))
		for _, d := range []Detector{TwoStage{}, TwoStageSWAR{}, TwoStageDFA{}} {
			if v := d.Classify(sql, true); v != lex {
				t.Errorf("%s says %v, lexer %v: %q", d.Name(), v, lex, sql)
			}
		}
	})
}

var fuzzCache = NewCached(PgQueryParse{}, 64)

// FuzzOptimizedEqualsReference checks each optimized function against its
// pre-optimization copy in reference_test.go on arbitrary text.
//
//	go test -run '^$' -fuzz FuzzOptimizedEqualsReference -fuzztime 10m ./internal/setdetect
func FuzzOptimizedEqualsReference(f *testing.F) {
	cases, err := LoadCorpus("corpus/corpus.jsonl")
	if err != nil {
		f.Fatal(err)
	}
	for _, c := range cases {
		f.Add(c.SQL, !c.SCSOff)
	}
	f.Fuzz(func(t *testing.T, sql string, scsOn bool) {
		if strings.IndexByte(sql, 0) >= 0 {
			return
		}
		check := func(name string, got, want bool) {
			if got != want {
				t.Errorf("%s: optimized %v, reference %v: %q", name, got, want, sql)
			}
		}
		check("lexer", lexDirty(sql, scsOn), lexDirtyV0(sql, scsOn))
		check("substring", Substring{}.Classify(sql, scsOn) == Dirty, MayChangeSession(sql))
		check("stage1Loop", stage1Loop(sql), stage1LoopV0(sql))
		check("stage1SWAR", stage1SWAR(sql), stage1SWARV0(sql))
		check("stage1DFA", stage1DFA(sql), stage1LoopV0(sql))
		check("pgdog-regex", PgdogRegex{}.Classify(sql, scsOn) == Dirty, pgdogRegexV0(sql))
		check("pgdog-gate", !pgdogIsCommon(sql) && pgdogPrefixMatch(truncateUTF8(sql, pgdogRegexLimit), pgdogCmdWords), pgdogGateV0(sql))
		check("proxysql", proxysqlCaught(sql), proxysqlCaughtV0(sql))
		check("pgquery-parse", astDirty(sql), astDirtyV0(sql))
		check("word-boundary", WordBoundary{}.Classify(sql, scsOn) == Dirty, wordBoundaryV0(sql) == Dirty)
		check("prefilter", prefilterHit(sql), prefilterHitV0(sql))
		check("cache", fuzzCache.Classify(sql, scsOn) == Dirty, astDirty(sql))
	})
}
