// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

// Command gen builds the labeled corpus in ../corpus/corpus.jsonl.
//
//	go run ./internal/setdetect/gen -regress <postgresql>/src/test/regress \
//	    -proxysql <proxysql> -pgdog <pgdog> -pgbouncer <pgbouncer>
//
// Every source path is optional; a missing one drops its cases.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"

	sd "github.com/DataDog/pg-proxy/internal/setdetect"
)

func main() {
	regress := flag.String("regress", "", "PostgreSQL src/test/regress directory")
	proxysql := flag.String("proxysql", "", "ProxySQL checkout")
	pgdog := flag.String("pgdog", "", "pgdog checkout")
	pgbouncer := flag.String("pgbouncer", "", "PgBouncer checkout")
	out := flag.String("out", "internal/setdetect/corpus/corpus.jsonl", "output file")
	sample := flag.Int("regress-sample", 1500, "regression statements without \"set\" to sample")
	oltp := flag.Int("oltp", 2000, "synthesized OLTP statements")
	flag.Parse()

	var cases []sd.Case
	if *regress != "" {
		cs, err := regressCases(*regress, *sample)
		if err != nil {
			log.Fatal(err)
		}
		cases = append(cases, cs...)
	}
	for _, src := range []struct {
		name, root string
		dirs       []string
		globs      []string
	}{
		{"proxysql", *proxysql, []string{"test/tap/tests"}, []string{"pgsql-set_*.cpp", "pgsql-reg_test_*set*.cpp", "pgsql-*parameter*.cpp"}},
		{"pgdog", *pgdog, []string{"integration", "pgdog/src/frontend/router/parser/query/test", "pgdog/src/frontend/router/parser/test"}, []string{"*.rs", "*.py", "*.rb", "*.go", "*.java", "*.js", "*.sql"}},
		{"pgbouncer", *pgbouncer, []string{"test"}, []string{"*.py", "*.sql", "*.c"}},
	} {
		if src.root == "" {
			continue
		}
		for _, d := range src.dirs {
			dir := filepath.Join(src.root, d)
			if _, err := os.Stat(dir); err != nil {
				continue
			}
			lits, err := literalsIn(dir, src.globs)
			if err != nil {
				log.Fatal(err)
			}
			for _, l := range lits {
				lab, ok := labelSQL(l.text)
				if !ok {
					continue
				}
				cases = append(cases, sd.Case{
					SQL: l.text, Label: lab.Label, Category: lab.Category,
					Source: src.name + ":" + filepath.ToSlash(filepath.Join(d, l.file)),
					Exec:   true, Note: "rule: " + lab.Note,
				})
			}
		}
	}

	var s synth
	s.directSet()
	s.setConfig()
	s.pgSettings()
	s.indirect()
	s.multiStatement()
	s.lexical()
	s.extensionAndServer()
	s.cleanForms()
	s.drivers()
	s.oltp(*oltp)
	cases = append(cases, s.cases...)

	cases = dedupe(cases)
	resolveWithOracle(cases, filepath.Dir(*out))
	if err := checkHandLabels(cases, filepath.Join(filepath.Dir(*out), "labeler-vs-hand.txt")); err != nil {
		log.Fatal(err)
	}
	if err := sd.WriteJSONL(*out, cases); err != nil {
		log.Fatal(err)
	}
	stats(cases)
}

// regressCases keeps every statement that contains "set" or runs code
// indirectly, plus a fixed sample of the rest.
func regressCases(dir string, sample int) ([]sd.Case, error) {
	files, err := filepath.Glob(filepath.Join(dir, "sql", "*.sql"))
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	var keep, rest []sd.Case
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}
		base := filepath.Base(f)
		for i, stmt := range sd.RegressStatements(string(data)) {
			if sd.UsesPsqlVar(stmt) {
				continue
			}
			lab, ok := labelSQL(stmt)
			if !ok {
				continue
			}
			c := sd.Case{
				SQL: stmt, Label: lab.Label, Category: lab.Category,
				Source: fmt.Sprintf("pg-regress:%s#%d", base, i),
				Exec:   true, Note: "rule: " + lab.Note,
			}
			lower := strings.ToLower(stmt)
			if strings.Contains(lower, "set") || lab.Label != sd.LabelClean ||
				strings.HasPrefix(lower, "do") || strings.HasPrefix(lower, "execute") || strings.HasPrefix(lower, "call") {
				keep = append(keep, c)
			} else {
				rest = append(rest, c)
			}
		}
	}
	r := rand.New(rand.NewSource(14))
	r.Shuffle(len(rest), func(i, j int) { rest[i], rest[j] = rest[j], rest[i] })
	if len(rest) > sample {
		rest = rest[:sample]
	}
	return append(keep, rest...), nil
}

// dedupe keeps the first case for each (SQL, scs) pair. IDs hash the
// source and text, so they survive changes elsewhere in the corpus and the
// oracle files stay joinable.
func dedupe(cases []sd.Case) []sd.Case {
	seen := map[string]bool{}
	out := cases[:0]
	for _, c := range cases {
		key := fmt.Sprint(c.SCSOff, "\x00", c.SQL)
		if seen[key] {
			continue
		}
		seen[key] = true
		prefix := c.Source
		if i := strings.IndexAny(prefix, ":"); i >= 0 {
			prefix = prefix[:i]
		}
		sum := sha256.Sum256([]byte(key + "\x00" + c.Source + "\x00" + strings.Join(c.Context, ";")))
		c.ID = prefix + "-" + hex.EncodeToString(sum[:5])
		out = append(out, c)
	}
	return out
}

// checkHandLabels runs the rule labeler over the hand-labeled cases and
// writes every disagreement, as a check on both.
func checkHandLabels(cases []sd.Case, path string) error {
	var b strings.Builder
	n := 0
	for _, c := range cases {
		if !strings.HasPrefix(c.Source, "synth") || c.SCSOff {
			continue
		}
		lab, ok := labelSQL(c.SQL)
		switch {
		case !ok:
			fmt.Fprintf(&b, "%s\tunparsed\thand=%s/%s\t%q\n", c.ID, c.Label, c.Category, c.SQL)
			n++
		case lab.Label != c.Label:
			fmt.Fprintf(&b, "%s\trule=%s/%s (%s)\thand=%s/%s\t%q\n", c.ID, lab.Label, lab.Category, lab.Note, c.Label, c.Category, c.SQL)
			n++
		}
	}
	fmt.Fprintf(os.Stderr, "labeler vs hand: %d disagreements, see %s\n", n, path)
	return os.WriteFile(path, []byte(b.String()), 0o644)
}

func stats(cases []sd.Case) {
	byCat := map[string]map[string]int{}
	bySrc := map[string]int{}
	for _, c := range cases {
		if byCat[c.Category] == nil {
			byCat[c.Category] = map[string]int{}
		}
		byCat[c.Category][c.Label]++
		src := c.Source
		if i := strings.IndexAny(src, ":"); i >= 0 {
			src = src[:i]
		}
		bySrc[src]++
	}
	fmt.Fprintf(os.Stderr, "%d cases\n", len(cases))
	for _, cat := range sd.Categories {
		fmt.Fprintf(os.Stderr, "  %-3s %v\n", cat.ID, byCat[cat.ID])
	}
	fmt.Fprintf(os.Stderr, "sources: %v\n", bySrc)
}

type oracleRow struct {
	ID      string   `json:"id"`
	Changed []string `json:"changed"`
	Error   string   `json:"error"`
}

// resolveWithOracle settles rule labels that depend on code the text does
// not show (EXECUTE, CALL, DO with dynamic SQL), using what execution did on
// every server version that ran them. It also turns a rule-clean statement
// that changed state into P9, since its text shows no setter, and lists them
// for review. Hand labels are never changed.
func resolveWithOracle(cases []sd.Case, dir string) {
	files, _ := filepath.Glob(filepath.Join(dir, "oracle-*.jsonl"))
	if len(files) == 0 {
		return
	}
	runs := map[string][]oracleRow{}
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			log.Fatal(err)
		}
		for _, line := range strings.Split(string(data), "\n") {
			var r oracleRow
			if json.Unmarshal([]byte(line), &r) == nil && r.ID != "" {
				runs[r.ID] = append(runs[r.ID], r)
			}
		}
	}
	resolved, p9 := 0, 0
	for i := range cases {
		c := &cases[i]
		rows := runs[c.ID]
		if strings.HasPrefix(c.Source, "synth") || len(rows) == 0 {
			continue
		}
		changed, clean := []string{}, true
		for _, r := range rows {
			changed = append(changed, r.Changed...)
			if r.Error != "" {
				clean = false
			}
		}
		pending := c.Label == sd.LabelEither && (strings.Contains(c.Note, "oracle decides") || strings.Contains(c.Note, "depends on"))
		switch {
		case pending && len(changed) > 0:
			c.Label, c.Note = sd.LabelDirty, c.Note+"; oracle: changed "+strings.Join(dedupeStrings(changed), ",")
			resolved++
		case pending && clean:
			c.Label, c.Category, c.Note = sd.LabelClean, "N7", c.Note+"; oracle: no change"
			resolved++
		case c.Label == sd.LabelClean && len(changed) > 0:
			c.Label, c.Category, c.Note = sd.LabelDirty, "P9", c.Note+"; oracle: changed "+strings.Join(dedupeStrings(changed), ",")
			p9++
			fmt.Fprintf(os.Stderr, "review P9 %s %q\n", c.ID, c.SQL)
		}
	}
	fmt.Fprintf(os.Stderr, "oracle: resolved %d pending labels, %d clean labels became P9\n", resolved, p9)
}

func dedupeStrings(xs []string) []string {
	sort.Strings(xs)
	out := xs[:0]
	for i, x := range xs {
		if i == 0 || x != xs[i-1] {
			out = append(out, x)
		}
	}
	return out
}
