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
	"os"
	"runtime"
	"runtime/metrics"
	"strconv"
	"strings"
	"syscall"
)

func init() {
	addr := os.Getenv("PG_PROXY_BENCH_ADDR")
	if addr == "" {
		return
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		panic("pgproxy_bench: listen " + addr + ": " + err.Error())
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/bench/metrics", serveBenchMetrics)
	go func() { _ = http.Serve(listener, mux) }()
}

// serveBenchMetrics reports cumulative CPU and allocation counters, which the
// harness differences over a measured run, and current memory sizes.
func serveBenchMetrics(w http.ResponseWriter, _ *http.Request) {
	samples := []metrics.Sample{
		{Name: "/gc/heap/allocs:bytes"},
		{Name: "/gc/heap/allocs:objects"},
		{Name: "/gc/heap/live:bytes"},
	}
	metrics.Read(samples)
	var usage syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &usage)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]uint64{
		"user_ns":            uint64(usage.Utime.Nano()),
		"sys_ns":             uint64(usage.Stime.Nano()),
		"max_rss_bytes":      peakRSS(usage),
		"heap_alloc_bytes":   samples[0].Value.Uint64(),
		"heap_alloc_objects": samples[1].Value.Uint64(),
		"heap_live_bytes":    samples[2].Value.Uint64(),
	})
}

// peakRSS returns the process's peak resident set size in bytes. Linux keeps
// ru_maxrss across exec, so a proxy started by a large harness process would
// report the harness's peak; VmHWM is reset by exec. macOS reports ru_maxrss
// in bytes and does not inherit it.
func peakRSS(usage syscall.Rusage) uint64 {
	if runtime.GOOS == "darwin" {
		return uint64(usage.Maxrss)
	}
	status, err := os.ReadFile("/proc/self/status")
	if err == nil {
		for _, line := range strings.Split(string(status), "\n") {
			if value, ok := strings.CutPrefix(line, "VmHWM:"); ok {
				kib, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimSpace(value), " kB"), 10, 64)
				if err == nil {
					return kib * 1024
				}
			}
		}
	}
	return uint64(usage.Maxrss) * 1024 // ru_maxrss is in KiB on Linux.
}
