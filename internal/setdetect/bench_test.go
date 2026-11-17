// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package setdetect

import (
	"fmt"
	"hash/maphash"
	"os"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// Size classes. Every one but "set" is clean, so detectors cannot exit early
// on a match.
var sizeClasses = func() []struct{ name, sql string } {
	ids := make([]string, 0, 2000)
	for i := 0; len(strings.Join(ids, ", ")) < 10_000; i++ {
		ids = append(ids, fmt.Sprint(100000+i*7))
	}
	return []struct{ name, sql string }{
		{"select1", "SELECT 1"},
		{"select200", "/*dddbs='orders-api',dde='prod'*/ SELECT u.id, u.email, u.name, o.total FROM users u JOIN orders o ON o.user_id = u.id WHERE u.id = $1 AND o.status = $2 ORDER BY o.created_at DESC LIMIT 20"},
		{"update400", "/*dddbs='orders-api',dde='prod',traceparent='00-0000000000000000c0ffee1234567890-1234567890abcdef-01'*/ UPDATE orders SET status = $1, updated_at = now(), total = total + $2, offset_ms = $3 WHERE id = $4 AND user_id IN (SELECT id FROM users WHERE status = 'active' AND reset_at IS NULL) RETURNING id, status, total, updated_at, created_at, user_id /*controller='orders',action='update'*/"},
		{"inlist10k", "SELECT id, user_id, total, status FROM orders WHERE id IN (" + strings.Join(ids, ", ") + ")"},
		{"multi", "BEGIN; UPDATE accounts SET balance = balance - 10 WHERE id = 1; UPDATE accounts SET balance = balance + 10 WHERE id = 2; INSERT INTO audit_log (msg) VALUES ('transfer'); COMMIT"},
		{"set", "SET search_path = tenant_1, public"},
	}
}()

func benchDetectors() []Detector {
	var ds []Detector
	for _, info := range Detectors() {
		ds = append(ds, info.Detector)
	}
	return ds
}

var sink Verdict

// BenchmarkClassify reports ns, B and allocs per query for every detector
// and size class. Run with GOMAXPROCS=1 -benchmem -count=10 and compare with
// benchstat.
func BenchmarkClassify(b *testing.B) {
	for _, d := range benchDetectors() {
		for _, sc := range sizeClasses {
			b.Run(d.Name()+"/"+sc.name, func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(sc.sql)))
				for b.Loop() {
					sink = d.Classify(sc.sql, true)
				}
			})
		}
	}
}

// BenchmarkTraffic classifies the traffic sample of the corpus in order;
// ns/op is the mean cost per query of that mix.
func BenchmarkTraffic(b *testing.B) {
	cases, err := LoadCorpus("corpus/corpus.jsonl")
	if err != nil {
		b.Fatal(err)
	}
	var qs []string
	for _, c := range cases {
		if isTraffic(c) {
			qs = append(qs, c.SQL)
		}
	}
	for _, d := range benchDetectors() {
		b.Run(d.Name(), func(b *testing.B) {
			b.ReportAllocs()
			i := 0
			for b.Loop() {
				sink = d.Classify(qs[i], true)
				i++
				if i == len(qs) {
					i = 0
				}
			}
		})
	}
}

// BenchmarkCgo isolates the fixed cost of pg_query's cgo calls.
func BenchmarkCgo(b *testing.B) {
	b.Run("HashXXH3_64", func(b *testing.B) {
		in := []byte("x")
		for b.Loop() {
			_ = pg_query.HashXXH3_64(in, 0)
		}
	})
	b.Run("Scan/select1", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, _ = pg_query.Scan("SELECT 1")
		}
	})
	b.Run("Parse/select1", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, _ = pg_query.Parse("SELECT 1")
		}
	})
	b.Run("ParseToJSON/select1", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, _ = pg_query.ParseToJSON("SELECT 1")
		}
	})
}

// TestLatency measures per-call latency distributions: for each detector and
// size class it times many batches of calls and reports the median and p99
// of the per-call mean within a batch. Batches keep timer overhead below a
// few percent. Gated by LATENCY=1; writes results/latency.json.
func TestLatency(t *testing.T) {
	if os.Getenv("LATENCY") == "" {
		t.Skip("set LATENCY=1")
	}
	runtime.GOMAXPROCS(1)
	type row struct {
		Detector string  `json:"detector"`
		Class    string  `json:"class"`
		Median   float64 `json:"median_ns"`
		P99      float64 `json:"p99_ns"`
	}
	var rows []row
	for _, d := range benchDetectors() {
		for _, sc := range sizeClasses {
			// Size the batch to about 5 µs.
			start := time.Now()
			calls := 0
			for time.Since(start) < 2*time.Millisecond {
				sink = d.Classify(sc.sql, true)
				calls++
			}
			per := float64(time.Since(start)) / float64(calls)
			k := max(1, int(5000/per))
			const batches = 3000
			samples := make([]float64, batches)
			for i := range samples {
				t0 := time.Now()
				for j := 0; j < k; j++ {
					sink = d.Classify(sc.sql, true)
				}
				samples[i] = float64(time.Since(t0)) / float64(k)
			}
			sort.Float64s(samples)
			rows = append(rows, row{d.Name(), sc.name, samples[batches/2], samples[batches*99/100]})
		}
	}
	writeJSON(t, "results/latency.json", rows)
}

// BenchmarkBeforeAfter times each optimized function against its
// pre-optimization copy in reference_test.go, on the same inputs in the same
// run, for the optimization log.
func BenchmarkBeforeAfter(b *testing.B) {
	v0cache := &cachedV0{Inner: PgQueryParse{}, seed: maphash.MakeSeed(), slots: make([]cacheSlot, 4096)}
	pairs := []struct {
		name          string
		before, after func(string) bool
	}{
		{"substring", MayChangeSession, func(s string) bool { return Substring{}.Classify(s, true) == Dirty }},
		{"word-boundary", func(s string) bool { return wordBoundaryV0(s) == Dirty }, func(s string) bool { return WordBoundary{}.Classify(s, true) == Dirty }},
		{"pgdog-regex", pgdogRegexV0, func(s string) bool { return PgdogRegex{}.Classify(s, true) == Dirty }},
		{"proxysql", proxysqlCaughtV0, proxysqlCaught},
		{"pgquery-parse", astDirtyV0, astDirty},
		{"pgquery-prefilter", func(s string) bool { return prefilterHitV0(s) && astDirtyV0(s) }, func(s string) bool { return PgQueryPrefilter{}.Classify(s, true) == Dirty }},
		{"pgquery-parse+cache", func(s string) bool { return v0cache.Classify(s, true) == Dirty }, func(s string) bool { return benchCache.Classify(s, true) == Dirty }},
		{"lexer", func(s string) bool { return lexDirtyV0(s, true) }, func(s string) bool { return lexDirty(s, true) }},
		{"two-stage", func(s string) bool {
			return stage1LoopV0(s) && lexDirtyV0(s, true) || !stage1LoopV0(s) && leadingDoCall(s)
		}, func(s string) bool { return TwoStage{}.Classify(s, true) == Dirty }},
		{"two-stage-swar", func(s string) bool {
			return stage1SWARV0(s) && lexDirtyV0(s, true) || !stage1SWARV0(s) && leadingDoCall(s)
		}, func(s string) bool { return TwoStageSWAR{}.Classify(s, true) == Dirty }},
	}
	var bsink bool
	for _, p := range pairs {
		for _, sc := range sizeClasses {
			for _, side := range []struct {
				name string
				fn   func(string) bool
			}{{"before", p.before}, {"after", p.after}} {
				b.Run(p.name+"/"+sc.name+"/side="+side.name, func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						bsink = side.fn(sc.sql)
					}
				})
			}
		}
	}
	_ = bsink
}

var benchCache = NewCached(PgQueryParse{}, 4096)
