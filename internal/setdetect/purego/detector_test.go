// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package purego

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
)

type corpusCase struct {
	ID     string `json:"id"`
	SQL    string `json:"sql"`
	SCSOff bool   `json:"scs_off"`
	Mix    string `json:"mix"`
	Source string `json:"source"`
}

func loadCorpus(t testing.TB) []corpusCase {
	f, err := os.Open("../corpus/corpus.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []corpusCase
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var c corpusCase
		if err := json.Unmarshal(sc.Bytes(), &c); err != nil {
			t.Fatal(err)
		}
		out = append(out, c)
	}
	return out
}

// TestVerdicts writes ../results/purego-verdicts.json, which the main
// accuracy test scores as the "crdb-parser" detector.
func TestVerdicts(t *testing.T) {
	out := map[string]string{}
	for _, c := range loadCorpus(t) {
		v := "clean"
		if Dirty(c.SQL) {
			v = "dirty"
		}
		out[c.ID] = v
	}
	t.Logf("parser panicked on %d inputs", Panics)
	data, _ := json.MarshalIndent(out, "", " ")
	if err := os.WriteFile("../results/purego-verdicts.json", data, 0o644); err != nil {
		t.Fatal(err)
	}
}

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

var sink bool

func BenchmarkClassify(b *testing.B) {
	for _, sc := range sizeClasses {
		b.Run("crdb-parser/"+sc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(sc.sql)))
			for b.Loop() {
				sink = Dirty(sc.sql)
			}
		})
	}
}
