// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

//go:build integration

package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/DataDog/pg-proxy/internal/itest"
)

// Run is one measured pgbench run against one fresh proxy process.
type Run struct {
	Variant      string       `json:"variant"`
	Clients      int          `json:"clients"`
	Repeat       int          `json:"repeat"`
	Dir          string       `json:"dir"`
	Transactions uint64       `json:"transactions"`
	TPS          float64      `json:"tps"`
	LatencyAvgMS float64      `json:"latency_avg_ms"`
	LatencyP99MS float64      `json:"latency_p99_ms"`
	Proxy        ProxyMetrics `json:"proxy"`
}

// ProxyMetrics are proxy costs over the measured run.
type ProxyMetrics struct {
	CPUUSPerQ        float64 `json:"cpu_us_per_query"`
	UserUSPerQ       float64 `json:"user_cpu_us_per_query"`
	SysUSPerQ        float64 `json:"sys_cpu_us_per_query"`
	AllocBytesPerQ   float64 `json:"alloc_bytes_per_query"`
	AllocObjectsPerQ float64 `json:"alloc_objects_per_query"`
	PeakRSSBytes     uint64  `json:"peak_rss_bytes"`
	HeapLiveBytes    uint64  `json:"heap_live_bytes"`
}

func (r Run) oneLine() string {
	return fmt.Sprintf("tps=%.0f lat_avg=%.3fms p99=%.3fms cpu=%.2fus/q (user %.2f, sys %.2f) alloc=%.0fB/q %.2fobj/q peak_rss=%.1fMiB",
		r.TPS, r.LatencyAvgMS, r.LatencyP99MS, r.Proxy.CPUUSPerQ, r.Proxy.UserUSPerQ, r.Proxy.SysUSPerQ,
		r.Proxy.AllocBytesPerQ, r.Proxy.AllocObjectsPerQ, float64(r.Proxy.PeakRSSBytes)*mib)
}

// metric is one reported quantity. A metric with a threshold gets a verdict:
// B regresses when it is worse in every pair, with no ties, and the mean change
// exceeds the threshold, in percent of A. Allocations are nearly deterministic, so their
// threshold is tight; timing metrics need more room for host noise.
type metric struct {
	name, unit    string
	lowerIsBetter bool
	threshold     float64
	value         func(Run) float64
}

const mib = 1.0 / (1 << 20)

var metrics = []metric{
	{"TPS", "tx/s", false, 3, func(r Run) float64 { return r.TPS }},
	{"latency avg", "ms", true, 0, func(r Run) float64 { return r.LatencyAvgMS }},
	{"latency p99", "ms", true, 10, func(r Run) float64 { return r.LatencyP99MS }},
	{"proxy CPU", "µs/query", true, 3, func(r Run) float64 { return r.Proxy.CPUUSPerQ }},
	{"proxy user CPU", "µs/query", true, 0, func(r Run) float64 { return r.Proxy.UserUSPerQ }},
	{"proxy system CPU", "µs/query", true, 0, func(r Run) float64 { return r.Proxy.SysUSPerQ }},
	{"alloc bytes", "B/query", true, 1, func(r Run) float64 { return r.Proxy.AllocBytesPerQ }},
	{"alloc objects", "objects/query", true, 1, func(r Run) float64 { return r.Proxy.AllocObjectsPerQ }},
	{"peak RSS", "MiB", true, 10, func(r Run) float64 { return float64(r.Proxy.PeakRSSBytes) * mib }},
	{"Go heap live", "MiB", true, 0, func(r Run) float64 { return float64(r.Proxy.HeapLiveBytes) * mib }},
}

const (
	verdictRegression  = "regression"
	verdictImprovement = "improvement"
	verdictNoChange    = "no change"
)

// Results is the content of results.json.
type Results struct {
	Manifest Manifest `json:"manifest"`
	Verdict  string   `json:"verdict,omitempty"`
	Paired   []Paired `json:"paired,omitempty"`
	Runs     []Run    `json:"runs"`
}

// Paired holds B−A differences of runs that share a client count and repeat.
type Paired struct {
	Clients     int       `json:"clients"`
	Metric      string    `json:"metric"`
	Unit        string    `json:"unit"`
	AMean       float64   `json:"a_mean"`
	Diffs       []float64 `json:"diffs"`
	Mean        float64   `json:"mean"`
	MeanPercent float64   `json:"mean_percent_of_a"`
	// BetterCount counts pairs where B improved on A.
	BetterCount int `json:"b_better_count"`
	// WorseCount counts pairs where B was worse than A; ties count in neither.
	WorseCount int `json:"b_worse_count"`
	// Verdict is empty for metrics without a threshold.
	Verdict string `json:"verdict,omitempty"`
}

func (p Paired) line() string {
	return fmt.Sprintf("%s %+.1f%% at %d clients (B better in %d/%d pairs)", p.Metric, p.MeanPercent, p.Clients, p.BetterCount, len(p.Diffs))
}

func (res *Results) summarize() {
	var clients []int
	for _, r := range res.Runs {
		if !slices.Contains(clients, r.Clients) {
			clients = append(clients, r.Clients)
		}
	}
	res.Paired = nil
	for _, c := range clients {
		res.Paired = append(res.Paired, pairRuns(res.Runs, c)...)
	}
	res.Verdict = verdictNoChange
	for _, p := range res.Paired {
		if p.Verdict == verdictRegression {
			res.Verdict = verdictRegression
			return
		}
		if p.Verdict == verdictImprovement {
			res.Verdict = verdictImprovement
		}
	}
}

func pairRuns(runs []Run, clients int) []Paired {
	byRepeat := map[int]map[string]Run{}
	for _, r := range runs {
		if r.Clients == clients {
			if byRepeat[r.Repeat] == nil {
				byRepeat[r.Repeat] = map[string]Run{}
			}
			byRepeat[r.Repeat][r.Variant] = r
		}
	}
	var repeats []int
	for repeat, pair := range byRepeat {
		if len(pair) == 2 {
			repeats = append(repeats, repeat)
		}
	}
	if len(repeats) == 0 {
		return nil
	}
	slices.Sort(repeats)
	out := make([]Paired, 0, len(metrics))
	for _, m := range metrics {
		p := Paired{Clients: clients, Metric: m.name, Unit: m.unit}
		var aSum float64
		for _, repeat := range repeats {
			a, b := m.value(byRepeat[repeat]["A"]), m.value(byRepeat[repeat]["B"])
			d := b - a
			p.Diffs = append(p.Diffs, d)
			aSum += a
			switch {
			case (m.lowerIsBetter && d < 0) || (!m.lowerIsBetter && d > 0):
				p.BetterCount++
			case d != 0:
				p.WorseCount++
			}
		}
		n := float64(len(p.Diffs))
		p.AMean = aSum / n
		for _, d := range p.Diffs {
			p.Mean += d / n
		}
		if p.AMean != 0 {
			p.MeanPercent = 100 * p.Mean / p.AMean
		}
		if m.threshold > 0 {
			worsePercent := p.MeanPercent
			if !m.lowerIsBetter {
				worsePercent = -worsePercent
			}
			switch {
			case p.WorseCount == len(p.Diffs) && worsePercent > m.threshold:
				p.Verdict = verdictRegression
			case p.BetterCount == len(p.Diffs) && -worsePercent > m.threshold:
				p.Verdict = verdictImprovement
			default:
				p.Verdict = verdictNoChange
			}
		}
		out = append(out, p)
	}
	return out
}

func (res *Results) write(out string) error {
	data, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(out, "results.json"), append(data, '\n'), 0o644); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(out, "summary.md"), []byte(res.markdown()), 0o644)
}

func (res *Results) markdown() string {
	var b strings.Builder
	b.WriteString("# pg-proxy pgbench A/B\n\n")
	if res.Verdict != "" {
		fmt.Fprintf(&b, "**Verdict: %s**\n\n", strings.ToUpper(res.Verdict))
		listed := false
		for _, p := range res.Paired {
			if p.Verdict == verdictRegression || p.Verdict == verdictImprovement {
				fmt.Fprintf(&b, "- %s: %s\n", p.Verdict, p.line())
				listed = true
			}
		}
		if listed {
			b.WriteString("\n")
		}
		b.WriteString("A metric regresses when B is worse in every pair, with no ties, and the mean change exceeds its threshold. Thresholds: ")
		var thresholds []string
		for _, m := range metrics {
			if m.threshold > 0 {
				thresholds = append(thresholds, fmt.Sprintf("%s %g%%", m.name, m.threshold))
			}
		}
		b.WriteString(strings.Join(thresholds, ", ") + ".\n\n")
	} else {
		fmt.Fprintf(&b, "In progress: %d runs completed.\n\n", len(res.Runs))
	}
	m := res.Manifest
	o := m.Options
	fmt.Fprintf(&b, "A = `%s` (%s), B = `%s` (%s). %s/%s, %d CPUs, %s, %s.\n\n",
		m.Variants[0].Spec, m.Variants[0].Commit[:12], m.Variants[1].Spec, m.Variants[1].Commit[:12], m.OS, m.Arch, m.CPUs, m.Go, m.Pgbench)
	fmt.Fprintf(&b, "`SELECT 1`, %s protocol, pool size %d, GOMAXPROCS=%s, %s measured after %s warmup, %d repeats with alternating order.\n\n",
		o.Protocol, o.PoolSize, o.GOMAXPROCS, o.Duration, o.Warmup, o.Repeats)
	if len(res.Paired) > 0 {
		b.WriteString("| Clients | Metric | Unit | A mean | B − A | % of A | B better | Per repeat | Verdict |\n| ---: | --- | --- | ---: | ---: | ---: | ---: | --- | --- |\n")
		for _, p := range res.Paired {
			diffs := make([]string, len(p.Diffs))
			for i, d := range p.Diffs {
				diffs[i] = num(d)
			}
			fmt.Fprintf(&b, "| %d | %s | %s | %s | %s | %+.1f%% | %d/%d | %s | %s |\n",
				p.Clients, p.Metric, p.Unit, num(p.AMean), num(p.Mean), p.MeanPercent, p.BetterCount, len(p.Diffs), strings.Join(diffs, ", "), p.Verdict)
		}
		b.WriteString("\nPer-run values are in results.json.\n")
	}
	return b.String()
}

func num(v float64) string {
	switch a := math.Abs(v); {
	case a >= 1000:
		return strconv.FormatFloat(v, 'f', 0, 64)
	case a >= 10:
		return strconv.FormatFloat(v, 'f', 1, 64)
	default:
		return strconv.FormatFloat(v, 'f', 3, 64)
	}
}

// Manifest records the host, tools, settings, and commits of an invocation.
type Manifest struct {
	Started  time.Time  `json:"started"`
	OS       string     `json:"os"`
	Arch     string     `json:"arch"`
	CPUs     int        `json:"cpus"`
	Go       string     `json:"go"`
	Pgbench  string     `json:"pgbench"`
	Postgres string     `json:"postgres_image"`
	Options  options    `json:"options"`
	Variants []*variant `json:"variants"`
}

func newManifest(opts options, variants []*variant) Manifest {
	return Manifest{
		Started: time.Now().UTC(), OS: runtime.GOOS, Arch: runtime.GOARCH, CPUs: runtime.NumCPU(),
		Go: commandOutput("go", "version"), Pgbench: commandOutput("pgbench", "--version"),
		Postgres: itest.PostgresImage(), Options: opts, Variants: variants,
	}
}

func commandOutput(name string, args ...string) string {
	out, err := exec.Command(name, args...).Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}
