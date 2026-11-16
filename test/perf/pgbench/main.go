// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

//go:build integration

// Command pgbench compares the performance of two pg-proxy builds, by default
// origin/main and the working tree. It runs pgbench through a fresh proxy for
// each run, alternates the order of the builds between repeats, and reports
// paired differences in throughput, client latency, proxy CPU, memory, and
// allocations per query. It exits with status 2 when B regresses.
//
//	go run -tags integration ./test/perf/pgbench
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
	A          string        `json:"a"`
	B          string        `json:"b"`
	Clients    []int         `json:"clients"`
	PoolSize   int           `json:"pool_size"`
	Duration   time.Duration `json:"duration"`
	Warmup     time.Duration `json:"warmup"`
	Repeats    int           `json:"repeats"`
	Protocol   string        `json:"protocol"`
	GOMAXPROCS string        `json:"gomaxprocs"`
	ProxyCPUs  string        `json:"proxy_cpus,omitempty"`
	ClientCPUs string        `json:"client_cpus,omitempty"`
	Out        string        `json:"-"`
}

// pgbenchThreads caps pgbench worker threads (-j).
const pgbenchThreads = 4

func main() {
	opts, err := parseFlags(os.Args[1:])
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	var verdict string
	if err == nil {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		verdict, err = run(ctx, opts)
		stop()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "pgbench harness:", err)
		os.Exit(1)
	}
	if verdict == verdictRegression {
		os.Exit(2)
	}
}

func parseFlags(args []string) (options, error) {
	opts := options{}
	flags := flag.NewFlagSet("pgbench", flag.ContinueOnError)
	flags.StringVar(&opts.A, "a", "origin/main", "baseline: git ref, or \".\" for the working tree")
	flags.StringVar(&opts.B, "b", ".", "candidate: git ref, or \".\" for the working tree")
	clients := flags.String("clients", "16", "comma-separated pgbench client counts")
	flags.IntVar(&opts.PoolSize, "pool-size", 16, "backend connections (max_conns) for the proxy pool")
	flags.DurationVar(&opts.Duration, "duration", 30*time.Second, "measured pgbench duration per run")
	flags.DurationVar(&opts.Warmup, "warmup", 10*time.Second, "unmeasured pgbench run before each measured run")
	flags.IntVar(&opts.Repeats, "repeats", 3, "runs per build and client count, at least 3")
	flags.StringVar(&opts.Protocol, "protocol", "simple", "pgbench query protocol: simple, extended, or prepared")
	flags.StringVar(&opts.GOMAXPROCS, "gomaxprocs", "1", "GOMAXPROCS for the proxy; empty uses the Go default")
	flags.StringVar(&opts.ProxyCPUs, "proxy-cpus", "", "Linux only: pin the proxy with taskset -c to this CPU list")
	flags.StringVar(&opts.ClientCPUs, "client-cpus", "", "Linux only: pin pgbench with taskset -c to this CPU list")
	flags.StringVar(&opts.Out, "out", "", "results directory (default: ./pgbench-results/<UTC timestamp>)")
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
	switch {
	case opts.Repeats < 3:
		return opts, errors.New("-repeats must be at least 3 for paired differences")
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

func run(ctx context.Context, opts options) (string, error) {
	out, err := filepath.Abs(opts.Out)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		return "", err
	}
	if _, err := os.Stat(filepath.Join(out, "results.json")); err == nil {
		return "", fmt.Errorf("%s already contains results; choose another -out", out)
	}
	script := filepath.Join(out, "select1.sql")
	if err := os.WriteFile(script, []byte("SELECT 1;\n"), 0o644); err != nil {
		return "", err
	}

	var variants [2]*variant
	defer func() {
		for _, v := range variants {
			if v != nil {
				v.cleanup()
			}
		}
	}()
	for i, spec := range []string{opts.A, opts.B} {
		if variants[i], err = prepareVariant(ctx, string(rune('A'+i)), spec, out); err != nil {
			return "", err
		}
		logf("variant %s: %s (%s)", variants[i].Label, spec, variants[i].Commit[:12])
	}

	logf("starting PostgreSQL container %s", itest.PostgresImage())
	startCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	pg, err := itest.StartPostgres(startCtx)
	cancel()
	if err != nil {
		return "", err
	}
	defer func() { _ = pg.Terminate(context.Background()) }()

	results := &Results{Manifest: newManifest(opts, variants[:])}
	for _, clients := range opts.Clients {
		for repeat := range opts.Repeats {
			// Alternate which build runs first so slow drift does not favour one.
			order := variants
			if repeat%2 == 1 {
				order[0], order[1] = order[1], order[0]
			}
			for _, v := range order {
				if err := ctx.Err(); err != nil {
					return "", err
				}
				logf("clients=%d repeat=%d variant=%s", clients, repeat+1, v.Label)
				r, err := measure(ctx, opts, v, pg, script, out, clients, repeat)
				if err != nil {
					return "", fmt.Errorf("variant %s clients=%d repeat=%d: %w", v.Label, clients, repeat+1, err)
				}
				logf("  %s", r.oneLine())
				results.Runs = append(results.Runs, r)
				if err := results.write(out); err != nil {
					return "", err
				}
			}
		}
	}
	results.summarize()
	if err := results.write(out); err != nil {
		return "", err
	}
	logf("verdict: %s; see %s", results.Verdict, filepath.Join(out, "summary.md"))
	return results.Verdict, nil
}

func logf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, time.Now().Format("15:04:05 ")+format+"\n", args...)
}
