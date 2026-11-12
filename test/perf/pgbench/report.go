// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// Run is one measured pgbench run against one fresh proxy process.
type Run struct {
	Variant      string       `json:"variant"`
	Clients      int          `json:"clients"`
	Repeat       int          `json:"repeat"`
	Started      time.Time    `json:"started"`
	Dir          string       `json:"dir"`
	Transactions uint64       `json:"transactions"`
	TPS          float64      `json:"tps"`
	LatencyAvgMS float64      `json:"latency_avg_ms"`
	LatencyP50MS *float64     `json:"latency_p50_ms"`
	LatencyP99MS *float64     `json:"latency_p99_ms"`
	Proxy        ProxyMetrics `json:"proxy"`
}

// ProxyMetrics are proxy costs over the measured run. Pointer fields are null
// when the binary lacks the runtime metrics overlay.
type ProxyMetrics struct {
	CPUSource        string   `json:"cpu_source"`
	CPUUSPerQ        float64  `json:"cpu_us_per_query"`
	UserUSPerQ       float64  `json:"user_cpu_us_per_query"`
	SysUSPerQ        float64  `json:"sys_cpu_us_per_query"`
	RSSBytes         uint64   `json:"rss_bytes"`
	RuntimeMetric    bool     `json:"runtime_metrics"`
	MaxRSSBytes      *uint64  `json:"max_rss_bytes"`
	HeapLiveBytes    *uint64  `json:"heap_live_bytes"`
	HeapObjectsBytes *uint64  `json:"heap_objects_bytes"`
	GoTotalBytes     *uint64  `json:"go_total_bytes"`
	AllocBytesPerQ   *float64 `json:"alloc_bytes_per_query"`
	AllocObjectsPerQ *float64 `json:"alloc_objects_per_query"`
	GCCycles         *float64 `json:"gc_cycles"`
	GOMAXPROCS       *uint64  `json:"gomaxprocs"`
}

func (r Run) oneLine() string {
	s := fmt.Sprintf("tps=%.0f lat_avg=%.3fms", r.TPS, r.LatencyAvgMS)
	if r.LatencyP99MS != nil {
		s += fmt.Sprintf(" p99=%.3fms", *r.LatencyP99MS)
	}
	s += fmt.Sprintf(" cpu=%.2fus/q (user %.2f, sys %.2f) rss=%.1fMiB", r.Proxy.CPUUSPerQ, r.Proxy.UserUSPerQ, r.Proxy.SysUSPerQ, float64(r.Proxy.RSSBytes)/(1<<20))
	if r.Proxy.AllocBytesPerQ != nil {
		s += fmt.Sprintf(" alloc=%.0fB/q %.2fobj/q", *r.Proxy.AllocBytesPerQ, *r.Proxy.AllocObjectsPerQ)
	}
	return s
}

// metric is one reported quantity. lowerIsBetter orders paired differences.
type metric struct {
	name, unit    string
	lowerIsBetter bool
	value         func(Run) (float64, bool)
}

func always(f func(Run) float64) func(Run) (float64, bool) {
	return func(r Run) (float64, bool) { return f(r), true }
}

func optional[T uint64 | float64](f func(Run) *T, scale float64) func(Run) (float64, bool) {
	return func(r Run) (float64, bool) {
		if v := f(r); v != nil {
			return float64(*v) * scale, true
		}
		return 0, false
	}
}

const mib = 1.0 / (1 << 20)

var metrics = []metric{
	{"TPS", "tx/s", false, always(func(r Run) float64 { return r.TPS })},
	{"latency avg", "ms", true, always(func(r Run) float64 { return r.LatencyAvgMS })},
	{"latency p50", "ms", true, optional(func(r Run) *float64 { return r.LatencyP50MS }, 1)},
	{"latency p99", "ms", true, optional(func(r Run) *float64 { return r.LatencyP99MS }, 1)},
	{"proxy CPU", "µs/query", true, always(func(r Run) float64 { return r.Proxy.CPUUSPerQ })},
	{"proxy user CPU", "µs/query", true, always(func(r Run) float64 { return r.Proxy.UserUSPerQ })},
	{"proxy system CPU", "µs/query", true, always(func(r Run) float64 { return r.Proxy.SysUSPerQ })},
	{"alloc bytes", "B/query", true, optional(func(r Run) *float64 { return r.Proxy.AllocBytesPerQ }, 1)},
	{"alloc objects", "objects/query", true, optional(func(r Run) *float64 { return r.Proxy.AllocObjectsPerQ }, 1)},
	{"RSS at end", "MiB", true, always(func(r Run) float64 { return float64(r.Proxy.RSSBytes) * mib })},
	{"peak RSS", "MiB", true, optional(func(r Run) *uint64 { return r.Proxy.MaxRSSBytes }, mib)},
	{"Go heap live", "MiB", true, optional(func(r Run) *uint64 { return r.Proxy.HeapLiveBytes }, mib)},
	{"Go runtime total", "MiB", true, optional(func(r Run) *uint64 { return r.Proxy.GoTotalBytes }, mib)},
}

// Results is the content of results.json.
type Results struct {
	Manifest Manifest  `json:"manifest"`
	Runs     []Run     `json:"runs"`
	Summary  []Summary `json:"summary,omitempty"`
	Paired   []Paired  `json:"paired,omitempty"`
}

// Summary is the median and range of one metric for one variant.
type Summary struct {
	Variant string  `json:"variant"`
	Clients int     `json:"clients"`
	Metric  string  `json:"metric"`
	Unit    string  `json:"unit"`
	N       int     `json:"n"`
	Median  float64 `json:"median"`
	Min     float64 `json:"min"`
	Max     float64 `json:"max"`
}

// Paired holds B−A differences of runs that share a client count and repeat.
type Paired struct {
	Clients     int       `json:"clients"`
	Metric      string    `json:"metric"`
	Unit        string    `json:"unit"`
	Diffs       []float64 `json:"diffs"`
	Mean        float64   `json:"mean"`
	Min         float64   `json:"min"`
	Max         float64   `json:"max"`
	MeanPercent float64   `json:"mean_percent_of_a"`
	// BetterCount counts pairs where B improved on A.
	BetterCount int `json:"b_better_count"`
}

func (res *Results) summarize() {
	res.Summary, res.Paired = nil, nil
	variants := []string{}
	clients := []int{}
	for _, r := range res.Runs {
		if !slices.Contains(variants, r.Variant) {
			variants = append(variants, r.Variant)
		}
		if !slices.Contains(clients, r.Clients) {
			clients = append(clients, r.Clients)
		}
	}
	slices.Sort(variants)
	for _, c := range clients {
		for _, v := range variants {
			for _, m := range metrics {
				var values []float64
				for _, r := range res.Runs {
					if x, ok := m.value(r); ok && r.Variant == v && r.Clients == c {
						values = append(values, x)
					}
				}
				if len(values) > 0 {
					res.Summary = append(res.Summary, Summary{Variant: v, Clients: c, Metric: m.name, Unit: m.unit,
						N: len(values), Median: median(values), Min: slices.Min(values), Max: slices.Max(values)})
				}
			}
		}
		if len(variants) == 2 {
			res.Paired = append(res.Paired, pairRuns(res.Runs, c)...)
		}
	}
}

func pairRuns(runs []Run, clients int) []Paired {
	byRepeat := map[int]map[string]Run{}
	for _, r := range runs {
		if r.Clients != clients {
			continue
		}
		if byRepeat[r.Repeat] == nil {
			byRepeat[r.Repeat] = map[string]Run{}
		}
		byRepeat[r.Repeat][r.Variant] = r
	}
	repeats := []int{}
	for repeat, pair := range byRepeat {
		if len(pair) == 2 {
			repeats = append(repeats, repeat)
		}
	}
	slices.Sort(repeats)
	var out []Paired
	for _, m := range metrics {
		p := Paired{Clients: clients, Metric: m.name, Unit: m.unit}
		var baseSum float64
		for _, repeat := range repeats {
			a, okA := m.value(byRepeat[repeat]["A"])
			b, okB := m.value(byRepeat[repeat]["B"])
			if !okA || !okB {
				continue
			}
			d := b - a
			p.Diffs = append(p.Diffs, d)
			baseSum += a
			if (m.lowerIsBetter && d < 0) || (!m.lowerIsBetter && d > 0) {
				p.BetterCount++
			}
		}
		if len(p.Diffs) == 0 {
			continue
		}
		p.Mean, p.Min, p.Max = mean(p.Diffs), slices.Min(p.Diffs), slices.Max(p.Diffs)
		if baseSum != 0 {
			p.MeanPercent = 100 * p.Mean / (baseSum / float64(len(p.Diffs)))
		}
		out = append(out, p)
	}
	return out
}

func mean(values []float64) float64 {
	var sum float64
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

func median(values []float64) float64 {
	sorted := slices.Clone(values)
	slices.Sort(sorted)
	n := len(sorted)
	if n%2 == 1 {
		return sorted[n/2]
	}
	return (sorted[n/2-1] + sorted[n/2]) / 2
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
	m := res.Manifest
	fmt.Fprintf(&b, "# pg-proxy pgbench results\n\n")
	fmt.Fprintf(&b, "%s, %s/%s, %d CPUs (%s), %s, %s, PostgreSQL %s.\n\n", m.Started.Format(time.RFC3339), m.Host.OS, m.Host.Arch, m.Host.CPUs, m.Host.CPUModel, m.Go, m.Pgbench, m.Backend.Version)
	s := m.Settings
	fmt.Fprintf(&b, "Settings: %s protocol, script `%s`, pool size %d, GOMAXPROCS=%s, %s measured after %s warmup, %d repeats, proxy CPUs %q, client CPUs %q.\n\n",
		s.Protocol, s.Script, s.PoolSize, orDefault(s.GOMAXPROCS, "default"), s.Duration, s.Warmup, s.Repeats, s.ProxyCPUs, s.ClientCPUs)
	b.WriteString("| Variant | Spec | Commit | Runtime metrics |\n| --- | --- | --- | --- |\n")
	for _, v := range m.Variants {
		fmt.Fprintf(&b, "| %s | `%s` | %s | %v |\n", v.Label, v.Spec, v.describe(), v.Instrumented)
	}
	b.WriteString("\n")
	if len(res.Summary) > 0 {
		b.WriteString("## Per-variant medians\n\nMedian [min, max] over repeats. Each run uses a fresh proxy process.\n\n")
		b.WriteString("| Clients | Variant | Metric | Unit | n | Median | Range |\n| ---: | --- | --- | --- | ---: | ---: | --- |\n")
		for _, s := range res.Summary {
			fmt.Fprintf(&b, "| %d | %s | %s | %s | %d | %s | [%s, %s] |\n", s.Clients, s.Variant, s.Metric, s.Unit, s.N, num(s.Median), num(s.Min), num(s.Max))
		}
		b.WriteString("\n")
	}
	if len(res.Paired) > 0 {
		b.WriteString("## Paired differences (B − A)\n\nEach pair shares a client count and repeat; order alternates between repeats. Negative CPU, latency, and allocation values favour B; positive TPS favours B.\n\n")
		b.WriteString("| Clients | Metric | Unit | Mean B−A | % of A | Min | Max | B better | Per repeat |\n| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |\n")
		for _, p := range res.Paired {
			diffs := make([]string, len(p.Diffs))
			for i, d := range p.Diffs {
				diffs[i] = num(d)
			}
			fmt.Fprintf(&b, "| %d | %s | %s | %s | %+.1f%% | %s | %s | %d/%d | %s |\n", p.Clients, p.Metric, p.Unit, num(p.Mean), p.MeanPercent, num(p.Min), num(p.Max), p.BetterCount, len(p.Diffs), strings.Join(diffs, ", "))
		}
		b.WriteString("\n")
	}
	b.WriteString("## Runs\n\n| Clients | Repeat | Variant | TPS | Avg ms | p99 ms | CPU µs/q | User | Sys | Alloc B/q | Objects/q | RSS MiB |\n| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, r := range res.Runs {
		fmt.Fprintf(&b, "| %d | %d | %s | %.0f | %.3f | %s | %.2f | %.2f | %.2f | %s | %s | %.1f |\n", r.Clients, r.Repeat, r.Variant, r.TPS, r.LatencyAvgMS,
			optNum(r.LatencyP99MS), r.Proxy.CPUUSPerQ, r.Proxy.UserUSPerQ, r.Proxy.SysUSPerQ, optNum(r.Proxy.AllocBytesPerQ), optNum(r.Proxy.AllocObjectsPerQ), float64(r.Proxy.RSSBytes)*mib)
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

func optNum(v *float64) string {
	if v == nil {
		return "n/a"
	}
	return num(*v)
}

func orDefault(s, d string) string {
	if s == "" {
		return d
	}
	return s
}

// Manifest records what is needed to judge whether two result files compare.
type Manifest struct {
	Started  time.Time    `json:"started"`
	Host     host         `json:"host"`
	Go       string       `json:"go"`
	Pgbench  string       `json:"pgbench"`
	Backend  *backendInfo `json:"backend"`
	Settings settings     `json:"settings"`
	Variants []*variant   `json:"variants"`
}

type host struct {
	OS       string `json:"os"`
	Arch     string `json:"arch"`
	Kernel   string `json:"kernel"`
	CPUs     int    `json:"cpus"`
	CPUModel string `json:"cpu_model"`
}

type settings struct {
	Clients    []int    `json:"clients"`
	Threads    int      `json:"threads"`
	PoolSize   int      `json:"pool_size"`
	Duration   string   `json:"duration"`
	Warmup     string   `json:"warmup"`
	Repeats    int      `json:"repeats"`
	Protocol   string   `json:"protocol"`
	Script     string   `json:"script"`
	ScriptText string   `json:"script_text"`
	GOMAXPROCS string   `json:"gomaxprocs"`
	ProxyCPUs  string   `json:"proxy_cpus"`
	ClientCPUs string   `json:"client_cpus"`
	LatencyLog bool     `json:"latency_log"`
	Profile    bool     `json:"pprof"`
	ProxyArgs  []string `json:"proxy_args"`
}

func newManifest(ctx context.Context, opts options, variants []*variant, b *backendInfo, script string) (Manifest, error) {
	text, err := os.ReadFile(script)
	if err != nil {
		return Manifest{}, err
	}
	if b.Version, err = backendVersion(ctx, b); err != nil {
		return Manifest{}, fmt.Errorf("query PostgreSQL version: %w", err)
	}
	return Manifest{
		Started: time.Now().UTC(),
		Host: host{OS: runtime.GOOS, Arch: runtime.GOARCH, Kernel: commandOutput("uname", "-sr"),
			CPUs: runtime.NumCPU(), CPUModel: cpuModel()},
		Go:      commandOutput("go", "version"),
		Pgbench: commandOutput(opts.Pgbench, "--version"),
		Backend: b,
		Settings: settings{Clients: opts.Clients, Threads: opts.Threads, PoolSize: opts.PoolSize,
			Duration: opts.Duration.String(), Warmup: opts.Warmup.String(), Repeats: opts.Repeats,
			Protocol: opts.Protocol, Script: filepath.Base(script), ScriptText: string(text),
			GOMAXPROCS: opts.GOMAXPROCS, ProxyCPUs: opts.ProxyCPUs, ClientCPUs: opts.ClientCPUs,
			LatencyLog: opts.LatencyLog, Profile: opts.Profile, ProxyArgs: opts.ProxyArgs},
		Variants: variants,
	}, nil
}

func backendVersion(ctx context.Context, b *backendInfo) (string, error) {
	u := url.URL{Scheme: "postgres", User: url.UserPassword(b.User, b.Password),
		Host: net.JoinHostPort(b.Host, strconv.Itoa(b.Port)), Path: "/" + b.DBName, RawQuery: "sslmode=disable"}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, u.String())
	if err != nil {
		return "", err
	}
	defer conn.Close(context.Background())
	var version string
	err = conn.QueryRow(ctx, "SHOW server_version").Scan(&version)
	return version, err
}

func cpuModel() string {
	if runtime.GOOS == "darwin" {
		return commandOutput("sysctl", "-n", "machdep.cpu.brand_string")
	}
	data, err := os.ReadFile("/proc/cpuinfo")
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(data), "\n") {
		if key, value, ok := strings.Cut(line, ":"); ok && strings.TrimSpace(key) == "model name" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func commandOutput(name string, args ...string) string {
	out, err := exec.Command(name, args...).Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}
