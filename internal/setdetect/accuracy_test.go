// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package setdetect

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

var (
	updateGolden = flag.Bool("update", false, "rewrite results/verdicts.json")
	writeResults = flag.Bool("write", false, "write results/accuracy.json and the oracle disagreement files")
)

// oltpMix weights the false positive rate by statement kind.
var oltpMix = map[string]float64{"select": 0.70, "update": 0.15, "insert": 0.10, "other": 0.05}

type cell struct {
	Caught int `json:"caught"` // dirty answered dirty
	Missed int `json:"missed"` // dirty answered clean
	Clean  int `json:"clean"`  // clean answered clean
	FP     int `json:"fp"`     // clean answered dirty
	Either int `json:"either"` // acceptable-either, not scored
	// EitherDirty counts acceptable-either cases answered dirty.
	EitherDirty int `json:"either_dirty"`
}

type miss struct {
	ID       string `json:"id"`
	Category string `json:"category"`
	SQL      string `json:"sql"`
	Note     string `json:"note,omitempty"`
	SCSOff   bool   `json:"scs_off,omitempty"`
}

type detectorResult struct {
	Name       string            `json:"name"`
	Family     string            `json:"family"`
	Cgo        bool              `json:"cgo"`
	Note       string            `json:"note"`
	LOC        int               `json:"loc"`
	FNInScope  int               `json:"fn_in_scope"`
	FNP9       int               `json:"fn_p9"`
	Dirty      int               `json:"dirty_in_scope"`
	CleanTotal int               `json:"clean_total"`
	FPTotal    int               `json:"fp_total"`
	FPRate     float64           `json:"fp_rate"`
	FPWeighted float64           `json:"fp_weighted"`
	FPByMix    map[string][2]int `json:"fp_by_mix"` // [fp, n]
	Categories map[string]*cell  `json:"categories"`
	Misses     []miss            `json:"false_negatives"`
	Verdicts   string            `json:"-"`
}

// loadOracle reads corpus/oracle-<tag>.jsonl, or returns nil.
func loadOracle(tag string) map[string]oracleRow {
	f, err := os.Open(filepath.Join("corpus", "oracle-"+tag+".jsonl"))
	if err != nil {
		return nil
	}
	defer f.Close()
	out := map[string]oracleRow{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var r oracleRow
		if json.Unmarshal(sc.Bytes(), &r) == nil {
			out[r.ID] = r
		}
	}
	return out
}

type oracleRow struct {
	ID       string   `json:"id"`
	Changed  []string `json:"changed"`
	Reported []string `json:"reported"`
	Error    string   `json:"error"`
}

func loadTestCorpus(t testing.TB) []Case {
	cases, err := LoadCorpus("corpus/corpus.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	return cases
}

func score(info Info, cases []Case) detectorResult {
	d := info.Detector
	r := detectorResult{
		Name: d.Name(), Family: info.Family, Cgo: info.Cgo, Note: info.Note,
		Categories: map[string]*cell{}, FPByMix: map[string][2]int{},
	}
	for _, f := range info.Files {
		r.LOC += countLOC(f)
	}
	var vb strings.Builder
	for _, c := range cases {
		v := d.Classify(c.SQL, !c.SCSOff)
		vb.WriteByte('0' + byte(v))
		cl := r.Categories[c.Category]
		if cl == nil {
			cl = &cell{}
			r.Categories[c.Category] = cl
		}
		switch c.Label {
		case LabelDirty:
			if v == Dirty {
				cl.Caught++
			} else {
				cl.Missed++
				if InScope(c.Category) {
					r.FNInScope++
				} else {
					r.FNP9++
				}
				r.Misses = append(r.Misses, miss{ID: c.ID, Category: c.Category, SQL: c.SQL, Note: c.Note, SCSOff: c.SCSOff})
			}
			if InScope(c.Category) {
				r.Dirty++
			}
		case LabelClean:
			r.CleanTotal++
			if v == Dirty {
				cl.FP++
				r.FPTotal++
			} else {
				cl.Clean++
			}
			if isTraffic(c) {
				m := r.FPByMix[c.Mix]
				m[1]++
				if v == Dirty {
					m[0]++
				}
				r.FPByMix[c.Mix] = m
			}
		default:
			cl.Either++
			if v == Dirty {
				cl.EitherDirty++
			}
		}
	}
	if r.CleanTotal > 0 {
		r.FPRate = float64(r.FPTotal) / float64(r.CleanTotal)
	}
	for mix, w := range oltpMix {
		if m := r.FPByMix[mix]; m[1] > 0 {
			r.FPWeighted += w * float64(m[0]) / float64(m[1])
		}
	}
	sum := sha256.Sum256([]byte(vb.String()))
	r.Verdicts = hex.EncodeToString(sum[:8])
	return r
}

// isTraffic reports whether a case belongs to the application-traffic
// sample that the weighted false positive rate uses: synthesized OLTP and
// driver statements, not the adversarial category cases.
func isTraffic(c Case) bool {
	return c.Mix != "" && (c.Source == "synth-oltp" || c.Source == "synth-driver")
}

func countLOC(file string) int {
	data, err := os.ReadFile(file)
	if err != nil {
		return 0
	}
	n := 0
	for _, line := range strings.Split(string(data), "\n") {
		l := strings.TrimSpace(line)
		if l != "" && !strings.HasPrefix(l, "//") {
			n++
		}
	}
	return n
}

// TestAccuracy scores every detector; with -write it writes
// results/accuracy.json.
func TestAccuracy(t *testing.T) {
	cases := loadTestCorpus(t)
	var results []detectorResult
	ids := sha256.New()
	for _, c := range cases {
		ids.Write([]byte(c.ID + "\n"))
	}
	golden := map[string]string{"_corpus": hex.EncodeToString(ids.Sum(nil)[:8])}
	for _, info := range Detectors() {
		r := score(info, cases)
		results = append(results, r)
		golden[r.Name] = r.Verdicts
		t.Logf("%-18s FN in scope %3d  P9 %2d  FP %5.1f%%  weighted %5.1f%%", r.Name, r.FNInScope, r.FNP9, 100*r.FPRate, 100*r.FPWeighted)
	}
	if info, ok := pureGoDetector(t, cases); ok {
		r := score(info, cases)
		results = append(results, r)
		t.Logf("%-18s FN in scope %3d  P9 %2d  FP %5.1f%%  weighted %5.1f%%", r.Name, r.FNInScope, r.FNP9, 100*r.FPRate, 100*r.FPWeighted)
	}
	out := map[string]any{
		"detectors": results,
		"pgbouncer": pgbouncerModel(cases),
		"corpus":    corpusStats(cases),
		"oracle":    oracleSummary(t, cases),
	}
	writeResult(t, "results/accuracy.json", out)
	checkGolden(t, golden)
}

// checkGolden fails when a detector's verdicts changed, which is how an
// optimization that changes behavior gets rejected. Run with -update after
// a deliberate change.
func checkGolden(t *testing.T, got map[string]string) {
	path := "results/verdicts.json"
	if *updateGolden {
		writeJSON(t, path, got)
		return
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Logf("no %s; run with -update", path)
		return
	}
	var want map[string]string
	if err := json.Unmarshal(data, &want); err != nil {
		t.Fatal(err)
	}
	if want["_corpus"] != got["_corpus"] {
		// Hashes cover the whole corpus; after a corpus change they say
		// nothing. FuzzOptimizedEqualsReference still compares every case.
		t.Logf("%s is for another corpus; run with -update", path)
		return
	}
	for name, h := range got {
		if w, ok := want[name]; ok && w != h {
			t.Errorf("%s: verdicts changed (%s, was %s)", name, h, w)
		}
	}
}

// writeResult writes a results file when -write is set, so that plain
// go test (as in CI) leaves the tree alone.
func writeResult(t *testing.T, path string, v any) {
	if *writeResults {
		writeJSON(t, path, v)
	}
}

func writeJSON(t *testing.T, path string, v any) {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}

type pgbouncerResult struct {
	Tag string `json:"tag"`
	// Of the dirty cases whose execution changed session state, how many
	// changed a reported setting (which PgBouncer and the proposal's
	// ParameterStatus signal see).
	ChangedDirty int               `json:"changed_dirty"`
	Reported     int               `json:"reported"`
	ByCategory   map[string][2]int `json:"by_category"` // [reported, changed]
	// For the lexer plus ParameterStatus: P9 cases caught by either.
	P9Changed        int    `json:"p9_changed"`
	P9Reported       int    `json:"p9_reported"`
	LexerPlusMissing []miss `json:"lexer_plus_parameter_status_missed"`
}

// pgbouncerModel scores ParameterStatus as a detector, from the oracle.
func pgbouncerModel(cases []Case) []pgbouncerResult {
	var out []pgbouncerResult
	for _, tag := range []string{"pg14", "pg16"} {
		o := loadOracle(tag)
		if o == nil {
			continue
		}
		r := pgbouncerResult{Tag: tag, ByCategory: map[string][2]int{}}
		for _, c := range cases {
			row, ok := o[c.ID]
			if !ok || c.Label != LabelDirty || len(row.Changed) == 0 {
				continue
			}
			r.ChangedDirty++
			b := r.ByCategory[c.Category]
			b[1]++
			if len(row.Reported) > 0 {
				r.Reported++
				b[0]++
			}
			r.ByCategory[c.Category] = b
			if c.Category == "P9" {
				r.P9Changed++
				if len(row.Reported) > 0 {
					r.P9Reported++
				}
			}
			if lexDirty(c.SQL, !c.SCSOff) || len(row.Reported) > 0 {
				continue
			}
			r.LexerPlusMissing = append(r.LexerPlusMissing, miss{ID: c.ID, Category: c.Category, SQL: c.SQL, Note: strings.Join(row.Changed, ",")})
		}
		out = append(out, r)
	}
	return out
}

func corpusStats(cases []Case) map[string]any {
	byCat := map[string]map[string]int{}
	bySource := map[string]int{}
	byMix := map[string]int{}
	exec := 0
	for _, c := range cases {
		if byCat[c.Category] == nil {
			byCat[c.Category] = map[string]int{}
		}
		byCat[c.Category][c.Label]++
		src := c.Source
		if i := strings.IndexByte(src, ':'); i >= 0 {
			src = src[:i]
		}
		if strings.HasPrefix(src, "synth") {
			src = "synthesized"
		}
		bySource[src]++
		if isTraffic(c) && c.Label == LabelClean {
			byMix[c.Mix]++
		}
		if c.Exec {
			exec++
		}
	}
	return map[string]any{"total": len(cases), "by_category": byCat, "by_source": bySource, "traffic_mix_clean": byMix, "executable": exec}
}

type disagreement struct {
	Tag      string   `json:"tag"`
	ID       string   `json:"id"`
	Kind     string   `json:"kind"`
	Label    string   `json:"label"`
	Category string   `json:"category"`
	SQL      string   `json:"sql"`
	Changed  []string `json:"changed,omitempty"`
}

// oracleSummary compares labels with what execution did.
func oracleSummary(t *testing.T, cases []Case) map[string]any {
	out := map[string]any{}
	for _, tag := range []string{"pg14", "pg16"} {
		o := loadOracle(tag)
		if o == nil {
			continue
		}
		counts := map[string]int{}
		var dis []disagreement
		for _, c := range cases {
			row, ok := o[c.ID]
			if !ok {
				continue
			}
			counts["ran"]++
			changed := len(row.Changed) > 0
			switch {
			case changed && c.Label == LabelClean:
				// The serious kind: a clean label that changed state.
				counts["clean_but_changed"]++
				dis = append(dis, disagreement{tag, c.ID, "clean_but_changed", c.Label, c.Category, c.SQL, row.Changed})
			case row.Error != "":
				counts["error"]++
			case changed && c.Label == LabelDirty:
				counts["dirty_confirmed"]++
			case !changed && c.Label == LabelDirty:
				counts["dirty_but_unchanged"]++
				dis = append(dis, disagreement{tag, c.ID, "dirty_but_unchanged", c.Label, c.Category, c.SQL, nil})
			case !changed && c.Label == LabelClean:
				counts["clean_confirmed"]++
			default:
				counts["either"]++
			}
		}
		sort.Slice(dis, func(i, j int) bool { return dis[i].Kind+dis[i].ID < dis[j].Kind+dis[j].ID })
		writeResult(t, fmt.Sprintf("results/oracle-disagreements-%s.json", tag), dis)
		out[tag] = counts
	}
	return out
}

// pureGoDetector replays the verdicts the purego module wrote, so the
// CockroachDB parser is scored without entering this module's go.mod.
func pureGoDetector(t *testing.T, cases []Case) (Info, bool) {
	data, err := os.ReadFile("results/purego-verdicts.json")
	if err != nil {
		return Info{}, false
	}
	var byID map[string]string
	if err := json.Unmarshal(data, &byID); err != nil {
		t.Fatal(err)
	}
	byText := map[string]Verdict{}
	for _, c := range cases {
		v, ok := byID[c.ID]
		if !ok {
			t.Fatalf("results/purego-verdicts.json is stale: no verdict for %s", c.ID)
		}
		byText[fmt.Sprint(c.SCSOff, "\x00", c.SQL)] = verdict(v == "dirty")
	}
	d := NewFunc("crdb-parser", func(sql string, scsOn bool) Verdict { return byText[fmt.Sprint(!scsOn, "\x00", sql)] })
	return Info{Detector: d, Family: "pure-Go parser", Files: []string{"purego/detector.go"},
		Note: "CockroachDB's Go parser, same AST rules (separate module, no cgo)"}, true
}
