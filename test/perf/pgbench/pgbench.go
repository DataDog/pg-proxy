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
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/DataDog/pg-proxy/internal/itest"
)

// measure runs one fresh proxy through warmup and one measured pgbench run.
func measure(ctx context.Context, opts options, v *variant, pg *itest.Postgres, script, out string, clients, repeat int) (Run, error) {
	r := Run{Variant: v.Label, Clients: clients, Repeat: repeat + 1}
	dir := filepath.Join(out, "runs", fmt.Sprintf("c%d-r%d-%s", clients, repeat+1, v.Label))
	r.Dir, _ = filepath.Rel(out, dir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return r, err
	}
	proxy, err := startProxy(ctx, opts, v, pg, dir)
	if err != nil {
		return r, err
	}
	defer proxy.stop()

	pgbench := func(duration time.Duration, name string, logLatency bool) (string, error) {
		return runPgbench(ctx, opts, proxy.Addr, script, clients, duration, filepath.Join(dir, name), logLatency)
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
	output, err := pgbench(opts.Duration, "pgbench", true)
	if err != nil {
		return r, err
	}
	after, err := proxy.snapshot(ctx)
	if err != nil {
		return r, err
	}
	summary, err := parsePgbenchOutput(output)
	if err != nil {
		return r, fmt.Errorf("%w; see %s", err, filepath.Join(dir, "pgbench.txt"))
	}
	r.Transactions, r.TPS, r.LatencyAvgMS = summary.transactions, summary.tps, summary.latencyAvgMS
	lat, err := readLatencyLogs(dir, r.Transactions)
	if err != nil {
		return r, err
	}
	if uint64(len(lat)) != r.Transactions {
		return r, fmt.Errorf("latency logs hold %d transactions, pgbench reported %d", len(lat), r.Transactions)
	}
	r.LatencyP99MS = percentile(lat, 0.99)
	r.Proxy = proxyMetrics(before, after, r.Transactions)
	removeLatencyLogs(dir)
	return r, nil
}

func runPgbench(ctx context.Context, opts options, addr, script string, clients int, duration time.Duration, prefix string, logLatency bool) (string, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", err
	}
	argv := []string{"pgbench", "-h", host, "-p", port, "-U", itest.PostgresUser, "-n",
		"-c", strconv.Itoa(clients), "-j", strconv.Itoa(min(clients, pgbenchThreads)),
		"-M", opts.Protocol, "-T", strconv.Itoa(int(math.Ceil(duration.Seconds()))), "-f", script}
	if logLatency {
		argv = append(argv, "-l", "--log-prefix", prefix+".tx")
	}
	argv = append(argv, "bench")
	if opts.ClientCPUs != "" {
		argv = append([]string{"taskset", "-c", opts.ClientCPUs}, argv...)
	}
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Env = append(baseEnv(), "PGPASSWORD="+itest.PostgresPassword, "PGSSLMODE=disable", "PGAPPNAME=pg-proxy-pgbench")
	output, err := cmd.CombinedOutput()
	if writeErr := os.WriteFile(prefix+".txt", output, 0o644); writeErr != nil {
		return "", writeErr
	}
	if err != nil {
		return "", fmt.Errorf("pgbench: %w; see %s.txt", err, prefix)
	}
	return string(output), nil
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
// measured run's pgbench -l files. Lines have the form
// "client_id transaction_no time script_no time_epoch time_us [schedule_lag]".
func readLatencyLogs(dir string, expected uint64) ([]float64, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "pgbench.tx.*"))
	if err != nil || len(paths) == 0 {
		return nil, fmt.Errorf("no pgbench latency logs in %s", dir)
	}
	latencies := make([]float64, 0, expected)
	for _, path := range paths {
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			fields := strings.Fields(scanner.Text())
			var us float64
			if len(fields) >= 6 {
				us, err = strconv.ParseFloat(fields[2], 64)
			}
			if len(fields) < 6 || err != nil {
				_ = f.Close()
				return nil, fmt.Errorf("%s: malformed or failed transaction %q", path, scanner.Text())
			}
			latencies = append(latencies, us)
		}
		err = errors.Join(scanner.Err(), f.Close())
		if err != nil {
			return nil, err
		}
	}
	return latencies, nil
}

// percentile sorts latencies in place and returns the nearest-rank quantile q
// in milliseconds.
func percentile(latencies []float64, q float64) float64 {
	slices.Sort(latencies)
	rank := max(0, int(math.Ceil(q*float64(len(latencies))))-1)
	return latencies[rank] / 1000
}

// removeLatencyLogs deletes raw logs, which reach hundreds of MB at full load.
func removeLatencyLogs(dir string) {
	paths, _ := filepath.Glob(filepath.Join(dir, "pgbench.tx.*"))
	for _, path := range paths {
		_ = os.Remove(path)
	}
}

// proxyMetrics turns counter deltas over the measured run into per-query costs.
func proxyMetrics(before, after map[string]uint64, transactions uint64) ProxyMetrics {
	perQuery := func(name string, scale float64) float64 {
		return float64(after[name]-before[name]) * scale / float64(transactions)
	}
	m := ProxyMetrics{
		UserUSPerQ:       perQuery("user_ns", 1e-3),
		SysUSPerQ:        perQuery("sys_ns", 1e-3),
		AllocBytesPerQ:   perQuery("heap_alloc_bytes", 1),
		AllocObjectsPerQ: perQuery("heap_alloc_objects", 1),
		PeakRSSBytes:     after["max_rss_bytes"],
		HeapLiveBytes:    after["heap_live_bytes"],
	}
	m.CPUUSPerQ = m.UserUSPerQ + m.SysUSPerQ
	return m
}
