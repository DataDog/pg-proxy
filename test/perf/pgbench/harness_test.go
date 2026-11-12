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
scaling factor: 1
query mode: simple
number of clients: 16
number of threads: 4
maximum number of tries: 1
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

func TestLatencyLogsAndPercentiles(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "pgbench.tx.1"), []byte("0 0 1000 0 1700000000 1\n0 1 3000 0 1700000000 2\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "pgbench.tx.1.1"), []byte("1 0 2000 0 1700000000 3\n1 1 100000 0 1700000000 4\n"), 0o644))
	lat, err := readLatencyLogs(dir)
	require.NoError(t, err)
	require.Len(t, lat, 4)
	require.InDelta(t, 2.0, percentile(lat, 0.50), 1e-9)
	require.InDelta(t, 100.0, percentile(lat, 0.99), 1e-9)

	require.NoError(t, os.WriteFile(filepath.Join(dir, "pgbench.tx.2"), []byte("0 0 failed 0 1700000000 1\n"), 0o644))
	_, err = readLatencyLogs(dir)
	require.Error(t, err)
}

func TestParsePSTime(t *testing.T) {
	for in, want := range map[string]uint64{"0:01.50": 1_500_000_000, "2:03.25": 123_250_000_000, "1:00:00.00": 3600_000_000_000} {
		got, err := parsePSTime(in)
		require.NoError(t, err)
		require.Equal(t, want, got, in)
	}
}

func TestProxyMetricsPerQuery(t *testing.T) {
	before := snapshot{UserNS: 1_000_000, SysNS: 500_000, Runtime: map[string]uint64{"heap_alloc_bytes": 100, "heap_alloc_objects": 10, "user_ns": 1}}
	after := snapshot{UserNS: 3_000_000, SysNS: 1_500_000, RSSBytes: 1 << 20, CPUSource: "getrusage",
		Runtime: map[string]uint64{"heap_alloc_bytes": 100 + 64_000, "heap_alloc_objects": 10 + 2_000, "user_ns": 2}}
	m := proxyMetrics(before, after, 1000)
	require.InDelta(t, 2.0, m.UserUSPerQ, 1e-9)
	require.InDelta(t, 1.0, m.SysUSPerQ, 1e-9)
	require.InDelta(t, 3.0, m.CPUUSPerQ, 1e-9)
	require.InDelta(t, 64.0, *m.AllocBytesPerQ, 1e-9)
	require.InDelta(t, 2.0, *m.AllocObjectsPerQ, 1e-9)

	m = proxyMetrics(snapshot{}, snapshot{UserNS: 1000}, 1)
	require.False(t, m.RuntimeMetric)
	require.Nil(t, m.AllocBytesPerQ)
}

func TestRotateAlternatesOrder(t *testing.T) {
	a, b := &variant{Label: "A"}, &variant{Label: "B"}
	labels := func(vs []*variant) string { return vs[0].Label + vs[1].Label }
	require.Equal(t, "AB", labels(rotate([]*variant{a, b}, 0)))
	require.Equal(t, "BA", labels(rotate([]*variant{a, b}, 1)))
	require.Equal(t, "AB", labels(rotate([]*variant{a, b}, 2)))
}

func TestPairedDifferences(t *testing.T) {
	run := func(v string, repeat int, tps, cpu float64) Run {
		return Run{Variant: v, Clients: 16, Repeat: repeat, TPS: tps, LatencyAvgMS: 1, Proxy: ProxyMetrics{CPUUSPerQ: cpu}}
	}
	res := &Results{Manifest: Manifest{Backend: &backendInfo{}}, Runs: []Run{
		run("A", 1, 1000, 70), run("B", 1, 1100, 67),
		run("B", 2, 1050, 68), run("A", 2, 1000, 71),
		run("A", 3, 1000, 72), run("B", 3, 990, 73),
	}}
	res.summarize()
	byName := map[string]Paired{}
	for _, p := range res.Paired {
		byName[p.Metric] = p
	}
	cpu := byName["proxy CPU"]
	require.Equal(t, []float64{-3, -3, 1}, cpu.Diffs)
	require.InDelta(t, -5.0/3, cpu.Mean, 1e-9)
	require.Equal(t, 2, cpu.BetterCount)
	tps := byName["TPS"]
	require.Equal(t, 2, tps.BetterCount)
	require.InDelta(t, 4.666, tps.MeanPercent, 0.01)
	_, hasAlloc := byName["alloc bytes"]
	require.False(t, hasAlloc, "metrics missing from either variant are not paired")
	require.Contains(t, res.markdown(), "Paired differences")
}
