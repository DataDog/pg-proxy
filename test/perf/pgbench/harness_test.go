// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

//go:build integration

package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

const pgbenchOutput = `pgbench (17.5)
transaction type: select1.sql
query mode: simple
number of clients: 16
number of threads: 4
duration: 30 s
number of transactions actually processed: 412345
number of failed transactions: 0 (0.000%)
latency average = 1.164 ms
initial connection time = 12.345 ms
tps = 13745.123456 (without initial connection time)
`

func TestParsePgbenchOutput(t *testing.T) {
	s, err := parsePgbenchOutput(pgbenchOutput)
	require.NoError(t, err)
	require.Equal(t, uint64(412345), s.transactions)
	require.InDelta(t, 1.164, s.latencyAvgMS, 1e-9)
	require.InDelta(t, 13745.123456, s.tps, 1e-9)

	_, err = parsePgbenchOutput("number of transactions actually processed: 5\nnumber of failed transactions: 2 (40%)\nlatency average = 1 ms\ntps = 1\n")
	require.ErrorContains(t, err, "2 failed")
	_, err = parsePgbenchOutput("pgbench: error: connection failed\n")
	require.Error(t, err)
}

func TestLatencyLogsAndPercentile(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "pgbench.tx.1"), []byte("0 0 1000 0 1700000000 1\n0 1 3000 0 1700000000 2\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "pgbench.tx.1.1"), []byte("1 0 2000 0 1700000000 3\n1 1 100000 0 1700000000 4\n"), 0o644))
	lat, err := readLatencyLogs(dir, 4)
	require.NoError(t, err)
	require.Len(t, lat, 4)
	require.InDelta(t, 100.0, percentile(lat, 0.99), 1e-9)
	require.InDelta(t, 2.0, percentile(lat, 0.50), 1e-9)

	require.NoError(t, os.WriteFile(filepath.Join(dir, "pgbench.tx.2"), []byte("0 0 failed 0 1700000000 1\n"), 0o644))
	_, err = readLatencyLogs(dir, 5)
	require.Error(t, err)
}

func TestProxyMetricsPerQuery(t *testing.T) {
	before := map[string]uint64{"user_ns": 1_000_000, "sys_ns": 500_000, "heap_alloc_bytes": 100, "heap_alloc_objects": 10}
	after := map[string]uint64{"user_ns": 3_000_000, "sys_ns": 1_500_000, "heap_alloc_bytes": 100 + 64_000,
		"heap_alloc_objects": 10 + 2_000, "max_rss_bytes": 1 << 20}
	m := proxyMetrics(before, after, 1000)
	require.InDelta(t, 2.0, m.UserUSPerQ, 1e-9)
	require.InDelta(t, 1.0, m.SysUSPerQ, 1e-9)
	require.InDelta(t, 3.0, m.CPUUSPerQ, 1e-9)
	require.InDelta(t, 64.0, m.AllocBytesPerQ, 1e-9)
	require.InDelta(t, 2.0, m.AllocObjectsPerQ, 1e-9)
	require.Equal(t, uint64(1<<20), m.PeakRSSBytes)
}

func testResults(runs ...Run) *Results {
	return &Results{Manifest: Manifest{Variants: []*variant{{Spec: "origin/main", Commit: "0123456789abcdef"}, {Spec: ".", Commit: "fedcba9876543210"}}}, Runs: runs}
}

func pairRun(v string, repeat int, tps, cpu, alloc float64) Run {
	return Run{Variant: v, Clients: 16, Repeat: repeat, TPS: tps, LatencyAvgMS: 1, LatencyP99MS: 2,
		Proxy: ProxyMetrics{CPUUSPerQ: cpu, AllocBytesPerQ: alloc}}
}

func paired(res *Results) map[string]Paired {
	out := map[string]Paired{}
	for _, p := range res.Paired {
		out[p.Metric] = p
	}
	return out
}

func TestPairedDifferencesAndVerdict(t *testing.T) {
	// B allocates 10% more in every pair; CPU and TPS move within noise.
	res := testResults(
		pairRun("A", 1, 1000, 70, 2000), pairRun("B", 1, 1100, 67, 2200),
		pairRun("B", 2, 1050, 68, 2200), pairRun("A", 2, 1000, 71, 2000),
		pairRun("A", 3, 1000, 72, 2000), pairRun("B", 3, 990, 73, 2200),
	)
	res.summarize()
	p := paired(res)
	cpu := p["proxy CPU"]
	require.Equal(t, []float64{-3, -3, 1}, cpu.Diffs)
	require.InDelta(t, -5.0/3, cpu.Mean, 1e-9)
	require.Equal(t, 2, cpu.BetterCount)
	require.Equal(t, verdictNoChange, cpu.Verdict)
	require.Equal(t, verdictRegression, p["alloc bytes"].Verdict)
	require.Empty(t, p["latency avg"].Verdict, "metrics without a threshold get no verdict")
	require.Equal(t, verdictRegression, res.Verdict)
	require.Contains(t, res.markdown(), "**Verdict: REGRESSION**")
	require.Contains(t, res.markdown(), "regression: alloc bytes +10.0% at 16 clients (B better in 0/3 pairs)")
}

func TestVerdictIgnoresInconsistentOrSmallChanges(t *testing.T) {
	// CPU is worse in every pair but by under 3%; TPS improves by more than 3% in every pair.
	res := testResults(
		pairRun("A", 1, 1000, 100, 2000), pairRun("B", 1, 1100, 101, 2000),
		pairRun("B", 2, 1100, 101, 2000), pairRun("A", 2, 1000, 100, 2000),
		pairRun("A", 3, 1000, 100, 2000), pairRun("B", 3, 1100, 101, 2000),
	)
	res.summarize()
	p := paired(res)
	require.Equal(t, verdictNoChange, p["proxy CPU"].Verdict)
	require.Equal(t, verdictImprovement, p["TPS"].Verdict)
	require.Equal(t, verdictImprovement, res.Verdict)
}

func TestVerdictRequiresEveryPairWorse(t *testing.T) {
	// Two pairs tie and one is 12% worse: the 4% mean exceeds the threshold, but
	// B is not worse in every pair.
	res := testResults(
		pairRun("A", 1, 1000, 100, 2000), pairRun("B", 1, 1000, 100, 2000),
		pairRun("B", 2, 1000, 100, 2000), pairRun("A", 2, 1000, 100, 2000),
		pairRun("A", 3, 1000, 100, 2000), pairRun("B", 3, 1000, 112, 2000),
	)
	res.summarize()
	cpu := paired(res)["proxy CPU"]
	require.InDelta(t, 4.0, cpu.MeanPercent, 1e-9)
	require.Equal(t, 1, cpu.WorseCount)
	require.Equal(t, verdictNoChange, cpu.Verdict)
	require.Equal(t, verdictNoChange, res.Verdict)
}
