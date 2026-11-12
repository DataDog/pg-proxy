// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

//go:build integration

package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"
)

// measure runs one fresh proxy through warmup and one measured pgbench run.
func measure(ctx context.Context, opts options, v *variant, b *backendInfo, script, out string, clients, repeat int) (Run, error) {
	r := Run{Variant: v.Label, Clients: clients, Repeat: repeat + 1, Started: time.Now().UTC()}
	dir := filepath.Join(out, "runs", fmt.Sprintf("c%d-r%d-%s", clients, repeat+1, v.Label))
	r.Dir, _ = filepath.Rel(out, dir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return r, err
	}
	proxy, err := startProxy(ctx, opts, v, b, dir)
	if err != nil {
		return r, err
	}
	defer proxy.stop()

	pgbench := func(duration time.Duration, name string, logLatency bool) (string, error) {
		return runPgbench(ctx, opts, proxy.Addr, b, script, clients, duration, filepath.Join(dir, name), logLatency)
	}
	if opts.Warmup > 0 {
		if _, err := pgbench(opts.Warmup, "warmup", false); err != nil {
			return r, err
		}
	}
	before, err := proxy.snapshot(ctx)
	if err != nil {
		return r, err
	}
	profileDone := make(chan error, 1)
	if opts.Profile && before.Runtime != nil {
		go func() {
			seconds := int(opts.Duration.Seconds())
			body, err := proxy.get(ctx, fmt.Sprintf("/debug/pprof/profile?seconds=%d", seconds), opts.Duration+30*time.Second)
			if err == nil {
				err = os.WriteFile(filepath.Join(dir, "cpu.pprof"), body, 0o644)
			}
			profileDone <- err
		}()
	} else {
		profileDone <- nil
	}
	output, err := pgbench(opts.Duration, "pgbench", opts.LatencyLog)
	if err != nil {
		return r, err
	}
	after, err := proxy.snapshot(ctx)
	if err != nil {
		return r, err
	}
	if err := <-profileDone; err != nil {
		return r, fmt.Errorf("CPU profile: %w", err)
	}
	if opts.Profile && after.Runtime != nil {
		body, err := proxy.get(ctx, "/debug/pprof/allocs", 30*time.Second)
		if err == nil {
			err = os.WriteFile(filepath.Join(dir, "allocs.pprof"), body, 0o644)
		}
		if err != nil {
			return r, fmt.Errorf("allocs profile: %w", err)
		}
	}
	summary, err := parsePgbenchOutput(output)
	if err != nil {
		return r, fmt.Errorf("%w; see %s", err, filepath.Join(dir, "pgbench.txt"))
	}
	r.Transactions, r.TPS, r.LatencyAvgMS = summary.transactions, summary.tps, summary.latencyAvgMS
	if opts.LatencyLog {
		lat, err := readLatencyLogs(dir)
		if err != nil {
			return r, err
		}
		if uint64(len(lat)) != r.Transactions {
			return r, fmt.Errorf("latency logs hold %d transactions, pgbench reported %d", len(lat), r.Transactions)
		}
		p50, p99 := percentile(lat, 0.50), percentile(lat, 0.99)
		r.LatencyP50MS, r.LatencyP99MS = &p50, &p99
	}
	r.Proxy = proxyMetrics(before, after, r.Transactions)
	removeLatencyLogs(dir)
	return r, nil
}

func runPgbench(ctx context.Context, opts options, addr string, b *backendInfo, script string, clients int, duration time.Duration, prefix string, logLatency bool) (string, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", err
	}
	argv := []string{opts.Pgbench, "-h", host, "-p", port, "-U", b.User, "-n",
		"-c", strconv.Itoa(clients), "-j", strconv.Itoa(min(clients, opts.Threads)),
		"-M", opts.Protocol, "-T", strconv.Itoa(int(math.Ceil(duration.Seconds()))), "-f", script}
	if logLatency {
		argv = append(argv, "-l", "--log-prefix", prefix+".tx")
	}
	argv = append(argv, "bench")
	if opts.ClientCPUs != "" {
		argv = append([]string{"taskset", "-c", opts.ClientCPUs}, argv...)
	}
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Env = append(pgbenchEnv(), "PGPASSWORD="+b.Password, "PGSSLMODE=disable", "PGAPPNAME=pg-proxy-pgbench")
	output, err := cmd.CombinedOutput()
	if writeErr := os.WriteFile(prefix+".txt", output, 0o644); writeErr != nil {
		return "", writeErr
	}
	if err != nil {
		return "", fmt.Errorf("pgbench: %w; see %s.txt", err, prefix)
	}
	return string(output), nil
}

func pgbenchEnv() []string {
	var env []string
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "PG") {
			env = append(env, kv)
		}
	}
	return env
}

type pgbenchSummary struct {
	transactions uint64
	tps          float64
	latencyAvgMS float64
}

var (
	reProcessed = regexp.MustCompile(`(?m)^number of transactions actually processed: (\d+)`)
	reFailed    = regexp.MustCompile(`(?m)^number of failed transactions: (\d+)`)
	reLatency   = regexp.MustCompile(`(?m)^latency average = ([0-9.]+) ms`)
	reTPS       = regexp.MustCompile(`(?m)^tps = ([0-9.]+)`)
)

func parsePgbenchOutput(output string) (pgbenchSummary, error) {
	var s pgbenchSummary
	if m := reFailed.FindStringSubmatch(output); m != nil && m[1] != "0" {
		return s, fmt.Errorf("pgbench reported %s failed transactions", m[1])
	}
	processed, latency, tps := reProcessed.FindStringSubmatch(output), reLatency.FindStringSubmatch(output), reTPS.FindStringSubmatch(output)
	if processed == nil || latency == nil || tps == nil {
		return s, errors.New("pgbench output lacks transactions, latency average, or tps")
	}
	var err1, err2, err3 error
	s.transactions, err1 = strconv.ParseUint(processed[1], 10, 64)
	s.latencyAvgMS, err2 = strconv.ParseFloat(latency[1], 64)
	s.tps, err3 = strconv.ParseFloat(tps[1], 64)
	if err := errors.Join(err1, err2, err3); err != nil {
		return s, err
	}
	if s.transactions == 0 {
		return s, errors.New("pgbench processed no transactions")
	}
	return s, nil
}

// readLatencyLogs returns every transaction latency in microseconds from the
// measured run's pgbench -l files.
func readLatencyLogs(dir string) ([]float64, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "pgbench.tx.*"))
	if err != nil || len(paths) == 0 {
		return nil, fmt.Errorf("no pgbench latency logs in %s", dir)
	}
	var latencies []float64
	for _, path := range paths {
		if latencies, err = appendLatencies(latencies, path); err != nil {
			return nil, err
		}
	}
	return latencies, nil
}

// appendLatencies parses lines of the form
// "client_id transaction_no time script_no time_epoch time_us [schedule_lag]".
func appendLatencies(latencies []float64, path string) ([]float64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 6 {
			return nil, fmt.Errorf("%s: malformed line %q", path, scanner.Text())
		}
		us, err := strconv.ParseFloat(fields[2], 64)
		if err != nil {
			return nil, fmt.Errorf("%s: transaction %s", path, fields[2])
		}
		latencies = append(latencies, us)
	}
	return latencies, scanner.Err()
}

// percentile returns the nearest-rank quantile q of latencies, in milliseconds.
func percentile(latencies []float64, q float64) float64 {
	sorted := slices.Clone(latencies)
	slices.Sort(sorted)
	rank := max(0, int(math.Ceil(q*float64(len(sorted))))-1)
	return sorted[rank] / 1000
}

// removeLatencyLogs deletes raw logs, which reach hundreds of MB at full load.
func removeLatencyLogs(dir string) {
	paths, _ := filepath.Glob(filepath.Join(dir, "pgbench.tx.*"))
	for _, path := range paths {
		_ = os.Remove(path)
	}
}

// proxyMetrics turns counter deltas over the measured run into per-query costs.
func proxyMetrics(before, after snapshot, transactions uint64) ProxyMetrics {
	q := float64(transactions)
	perQueryUS := func(a, b uint64) float64 { return float64(b-a) / 1000 / q }
	m := ProxyMetrics{
		CPUSource:     after.CPUSource,
		UserUSPerQ:    perQueryUS(before.UserNS, after.UserNS),
		SysUSPerQ:     perQueryUS(before.SysNS, after.SysNS),
		RSSBytes:      after.RSSBytes,
		RuntimeMetric: after.Runtime != nil && before.Runtime != nil,
	}
	m.CPUUSPerQ = m.UserUSPerQ + m.SysUSPerQ
	if !m.RuntimeMetric {
		return m
	}
	delta := func(name string) float64 { return float64(after.Runtime[name] - before.Runtime[name]) }
	value := func(name string) *uint64 { v := after.Runtime[name]; return &v }
	allocBytes, allocObjects := delta("heap_alloc_bytes")/q, delta("heap_alloc_objects")/q
	gcs := delta("gc_cycles")
	m.AllocBytesPerQ, m.AllocObjectsPerQ, m.GCCycles = &allocBytes, &allocObjects, &gcs
	m.HeapLiveBytes, m.HeapObjectsBytes, m.GoTotalBytes = value("heap_live_bytes"), value("heap_objects_bytes"), value("go_total_bytes")
	m.GOMAXPROCS = value("gomaxprocs")
	maxRSS := after.Runtime["max_rss_raw"]
	if runtime.GOOS != "darwin" {
		maxRSS *= 1024 // Linux reports ru_maxrss in KiB, macOS in bytes.
	}
	m.MaxRSSBytes = &maxRSS
	return m
}
