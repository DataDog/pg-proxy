// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

//go:build pgproxy_bench

// This file is not part of any package in this repository. The pgbench harness
// adds it to cmd/pg-proxy with go build -overlay, so binaries built from any git
// ref expose the same in-process counters. Building this directory directly
// fails because it has no main function.
package main

import (
	"encoding/json"
	"net"
	"net/http"
	_ "net/http/pprof" // Registers /debug/pprof on http.DefaultServeMux.
	"os"
	"runtime/metrics"
	"syscall"
)

// benchMetrics maps JSON fields to runtime/metrics names. Allocation counters
// are cumulative, so the harness reports their difference over a window.
var benchMetrics = map[string]string{
	"heap_alloc_bytes":   "/gc/heap/allocs:bytes",
	"heap_alloc_objects": "/gc/heap/allocs:objects",
	"heap_live_bytes":    "/gc/heap/live:bytes",
	"heap_objects_bytes": "/memory/classes/heap/objects:bytes",
	"go_total_bytes":     "/memory/classes/total:bytes",
	"gc_cycles":          "/gc/cycles/total:gc-cycles",
	"gomaxprocs":         "/sched/gomaxprocs:threads",
	"goroutines":         "/sched/goroutines:goroutines",
}

func init() {
	addr := os.Getenv("PG_PROXY_BENCH_ADDR")
	if addr == "" {
		return
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		panic("pgproxy_bench: listen " + addr + ": " + err.Error())
	}
	http.HandleFunc("/bench/metrics", serveBenchMetrics)
	go func() { _ = http.Serve(listener, nil) }()
}

func serveBenchMetrics(w http.ResponseWriter, _ *http.Request) {
	samples := make([]metrics.Sample, 0, len(benchMetrics))
	names := make([]string, 0, len(benchMetrics))
	for field, name := range benchMetrics {
		samples = append(samples, metrics.Sample{Name: name})
		names = append(names, field)
	}
	metrics.Read(samples)
	out := make(map[string]uint64, len(samples)+3)
	for i, sample := range samples {
		if sample.Value.Kind() == metrics.KindUint64 {
			out[names[i]] = sample.Value.Uint64()
		}
	}
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err == nil {
		out["user_ns"] = uint64(usage.Utime.Nano())
		out["sys_ns"] = uint64(usage.Stime.Nano())
		out["max_rss_raw"] = uint64(usage.Maxrss)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}
