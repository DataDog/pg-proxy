// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

//go:build integration

// Command pgbench runs pgbench through pg-proxy and records throughput, client
// latency, proxy CPU, memory, and allocations per query. With -b it compares two
// pg-proxy builds on the same settings, rotating their order between repeats.
//
//	go run -tags integration ./test/perf/pgbench -a main -b HEAD
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/DataDog/pg-proxy/internal/itest"
)

type options struct {
	A, B        string
	Clients     []int
	Threads     int
	PoolSize    int
	Duration    time.Duration
	Warmup      time.Duration
	Repeats     int
	Protocol    string
	Script      string
	GOMAXPROCS  string
	ProxyCPUs   string
	ClientCPUs  string
	PostgresURL string
	LatencyLog  bool
	Profile     bool
	Out         string
	Pgbench     string
	ProxyArgs   []string
}

func main() {
	opts, err := parseFlags(os.Args[1:])
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err == nil {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		err = run(ctx, opts)
		stop()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "pgbench harness:", err)
		os.Exit(1)
	}
}

func parseFlags(args []string) (options, error) {
	opts := options{}
	flags := flag.NewFlagSet("pgbench", flag.ContinueOnError)
	flags.StringVar(&opts.A, "a", ".", "variant A: git ref, \".\" for the working tree, or a pg-proxy binary path")
	flags.StringVar(&opts.B, "b", "", "variant B for A/B mode (same forms as -a); empty runs A only")
	clients := flags.String("clients", "16", "comma-separated pgbench client counts")
	flags.IntVar(&opts.Threads, "threads", 4, "maximum pgbench worker threads (-j), capped at the client count")
	flags.IntVar(&opts.PoolSize, "pool-size", 16, "backend connections (max_conns) for the proxy pool")
	flags.DurationVar(&opts.Duration, "duration", 30*time.Second, "measured pgbench duration per run")
	flags.DurationVar(&opts.Warmup, "warmup", 10*time.Second, "unmeasured pgbench run before each measured run")
	flags.IntVar(&opts.Repeats, "repeats", 3, "repeats per variant and client count; A/B order rotates between repeats")
	flags.StringVar(&opts.Protocol, "protocol", "simple", "pgbench query protocol: simple, extended, or prepared")
	flags.StringVar(&opts.Script, "script", "", "pgbench script file (default: SELECT 1)")
	flags.StringVar(&opts.GOMAXPROCS, "gomaxprocs", "1", "GOMAXPROCS for the proxy; empty inherits the Go default")
	flags.StringVar(&opts.ProxyCPUs, "proxy-cpus", "", "Linux only: pin the proxy with taskset -c to this CPU list")
	flags.StringVar(&opts.ClientCPUs, "client-cpus", "", "Linux only: pin pgbench with taskset -c to this CPU list")
	flags.StringVar(&opts.PostgresURL, "postgres", "", "existing backend as postgres://user:pass@host:port/db (default: start a Docker container)")
	flags.BoolVar(&opts.LatencyLog, "latency-log", true, "write pgbench per-transaction logs to compute p50/p99 latency")
	flags.BoolVar(&opts.Profile, "pprof", false, "capture a CPU profile over each measured run and an allocs profile after it")
	flags.StringVar(&opts.Out, "out", "", "results directory (default: ./pgbench-results/<UTC timestamp>)")
	flags.StringVar(&opts.Pgbench, "pgbench", "pgbench", "pgbench executable")
	proxyArgs := flags.String("proxy-args", "", "extra space-separated pg-proxy arguments, for example \"-log-level WARN\"")
	if err := flags.Parse(args); err != nil {
		return opts, err
	}
	if flags.NArg() != 0 {
		return opts, fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	for _, field := range strings.Split(*clients, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(field))
		if err != nil || n < 1 {
			return opts, fmt.Errorf("invalid -clients value %q", field)
		}
		opts.Clients = append(opts.Clients, n)
	}
	opts.ProxyArgs = strings.Fields(*proxyArgs)
	switch {
	case opts.Repeats < 1:
		return opts, errors.New("-repeats must be at least 1")
	case opts.B != "" && opts.Repeats < 3:
		return opts, errors.New("A/B mode needs -repeats of at least 3 for paired differences")
	case opts.Protocol != "simple" && opts.Protocol != "extended" && opts.Protocol != "prepared":
		return opts, fmt.Errorf("invalid -protocol %q", opts.Protocol)
	case opts.Duration < time.Second || opts.Warmup < 0:
		return opts, errors.New("-duration must be at least 1s and -warmup non-negative")
	case (opts.ProxyCPUs != "" || opts.ClientCPUs != "") && runtime.GOOS != "linux":
		return opts, errors.New("-proxy-cpus and -client-cpus need Linux taskset")
	}
	if opts.Out == "" {
		opts.Out = filepath.Join("pgbench-results", time.Now().UTC().Format("20060102T150405Z"))
	}
	return opts, nil
}

func run(ctx context.Context, opts options) error {
	out, err := filepath.Abs(opts.Out)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(out, "results.json")); err == nil {
		return fmt.Errorf("%s already contains results; choose another -out", out)
	}
	script := opts.Script
	if script == "" {
		script = filepath.Join(out, "select1.sql")
		if err := os.WriteFile(script, []byte("SELECT 1;\n"), 0o644); err != nil {
			return err
		}
	}
	if script, err = filepath.Abs(script); err != nil {
		return err
	}

	specs := []string{opts.A}
	if opts.B != "" {
		specs = append(specs, opts.B)
	}
	var variants []*variant
	defer func() {
		for _, v := range variants {
			v.cleanup()
		}
	}()
	for i, spec := range specs {
		v, err := prepareVariant(ctx, string(rune('A'+i)), spec, out)
		if v != nil {
			variants = append(variants, v)
		}
		if err != nil {
			return err
		}
		logf("variant %s: %s (%s), runtime metrics %v", v.Label, v.Spec, v.describe(), v.Instrumented)
	}

	backend, teardown, err := startBackend(ctx, opts.PostgresURL)
	if err != nil {
		return err
	}
	defer teardown()

	m, err := newManifest(ctx, opts, variants, backend, script)
	if err != nil {
		return err
	}
	results := &Results{Manifest: m}
	for _, clients := range opts.Clients {
		for repeat := range opts.Repeats {
			for _, v := range rotate(variants, repeat) {
				if err := ctx.Err(); err != nil {
					return err
				}
				logf("clients=%d repeat=%d variant=%s", clients, repeat+1, v.Label)
				r, err := measure(ctx, opts, v, backend, script, out, clients, repeat)
				if err != nil {
					return fmt.Errorf("variant %s clients=%d repeat=%d: %w", v.Label, clients, repeat+1, err)
				}
				v.Instrumented = v.Instrumented || r.Proxy.RuntimeMetric
				logf("  %s", r.oneLine())
				results.Runs = append(results.Runs, r)
				if err := results.write(out); err != nil {
					return err
				}
			}
		}
	}
	results.summarize()
	if err := results.write(out); err != nil {
		return err
	}
	logf("wrote %s and %s", filepath.Join(out, "results.json"), filepath.Join(out, "summary.md"))
	return nil
}

// rotate returns variants starting at repeat modulo their count, so each
// variant runs first equally often and slow drift does not favour one of them.
func rotate(variants []*variant, repeat int) []*variant {
	n := len(variants)
	out := make([]*variant, 0, n)
	for i := range n {
		out = append(out, variants[(repeat+i)%n])
	}
	return out
}

func logf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, time.Now().Format("15:04:05 ")+format+"\n", args...)
}

// startBackend returns the PostgreSQL server that the proxy pools.
func startBackend(ctx context.Context, rawURL string) (*backendInfo, func(), error) {
	if rawURL != "" {
		b, err := parseBackendURL(rawURL)
		return b, func() {}, err
	}
	logf("starting PostgreSQL container %s", itest.PostgresImage())
	startCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	pg, err := itest.StartPostgres(startCtx)
	if err != nil {
		return nil, nil, err
	}
	b := &backendInfo{
		Host: pg.Host, Port: int(pg.Port), DBName: "postgres",
		User: itest.PostgresUser, Password: itest.PostgresPassword, Source: itest.PostgresImage(),
	}
	return b, func() { _ = pg.Terminate(context.Background()) }, nil
}
