// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

//go:build integration

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// backendInfo is the PostgreSQL server behind the proxy.
type backendInfo struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	DBName   string `json:"dbname"`
	User     string `json:"user"`
	Password string `json:"-"`
	Source   string `json:"source"`
	Version  string `json:"version,omitempty"`
}

func parseBackendURL(raw string) (*backendInfo, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "postgres" && u.Scheme != "postgresql" {
		return nil, fmt.Errorf("-postgres must be a postgres:// URL")
	}
	port := 5432
	if p := u.Port(); p != "" {
		if port, err = strconv.Atoi(p); err != nil {
			return nil, fmt.Errorf("-postgres port: %w", err)
		}
	}
	password, _ := u.User.Password()
	b := &backendInfo{Host: u.Hostname(), Port: port, DBName: strings.TrimPrefix(u.Path, "/"), User: u.User.Username(), Password: password, Source: "external"}
	if b.DBName == "" {
		b.DBName = "postgres"
	}
	return b, nil
}

// proxyConfig is a minimal configuration that every recent ref accepts.
func proxyConfig(b *backendInfo, listen string, poolSize int) string {
	return fmt.Sprintf(`databases:
  - name: primary
    host: %q
    port: %d
    dbname: %q
    sslmode: disable
    max_conns: %d
    username: %q
    password: %q
clusters:
  bench:
    primary: primary
logical_databases:
  bench:
    cluster: bench
proxy:
  listener_address: %q
`, b.Host, b.Port, b.DBName, poolSize, b.User, b.Password, listen)
}

type proxyProcess struct {
	Addr        string
	metricsAddr string
	cmd         *exec.Cmd
	done        chan struct{}
	log         *os.File
}

func startProxy(ctx context.Context, opts options, v *variant, b *backendInfo, dir string) (*proxyProcess, error) {
	listen, err := freeAddr()
	if err != nil {
		return nil, err
	}
	metricsAddr, err := freeAddr()
	if err != nil {
		return nil, err
	}
	configPath := filepath.Join(dir, "proxy.yaml")
	if err := os.WriteFile(configPath, []byte(proxyConfig(b, listen, opts.PoolSize)), 0o600); err != nil {
		return nil, err
	}
	log, err := os.Create(filepath.Join(dir, "proxy.log"))
	if err != nil {
		return nil, err
	}
	argv := append([]string{v.Binary, "-config", configPath}, opts.ProxyArgs...)
	if opts.ProxyCPUs != "" {
		// taskset execs the proxy, so the PID still identifies the proxy.
		argv = append([]string{"taskset", "-c", opts.ProxyCPUs}, argv...)
	}
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Env = proxyEnv(opts.GOMAXPROCS, metricsAddr)
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		_ = log.Close()
		return nil, err
	}
	p := &proxyProcess{Addr: listen, metricsAddr: metricsAddr, cmd: cmd, done: make(chan struct{}), log: log}
	go func() { _ = cmd.Wait(); close(p.done) }()
	deadline := time.Now().Add(30 * time.Second)
	for {
		select {
		case <-p.done:
			_ = log.Close()
			return nil, fmt.Errorf("pg-proxy exited during startup; see %s", log.Name())
		case <-ctx.Done():
			p.stop()
			return nil, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
		if conn, err := net.DialTimeout("tcp", listen, 100*time.Millisecond); err == nil {
			_ = conn.Close()
			return p, nil
		}
		if time.Now().After(deadline) {
			p.stop()
			return nil, fmt.Errorf("pg-proxy did not listen on %s; see %s", listen, log.Name())
		}
	}
}

// proxyEnv drops settings that change proxy cost between hosts or shells:
// telemetry exporters, Go runtime tuning, and libpq defaults.
func proxyEnv(gomaxprocs, metricsAddr string) []string {
	var env []string
	for _, kv := range os.Environ() {
		key, _, _ := strings.Cut(kv, "=")
		switch {
		case strings.HasPrefix(key, "OTEL_"), strings.HasPrefix(key, "DD_"), strings.HasPrefix(key, "PG"),
			key == "GOMAXPROCS", key == "GOGC", key == "GOMEMLIMIT", key == "GODEBUG":
			continue
		}
		env = append(env, kv)
	}
	if gomaxprocs != "" {
		env = append(env, "GOMAXPROCS="+gomaxprocs)
	}
	return append(env, "PG_PROXY_BENCH_ADDR="+metricsAddr)
}

func (p *proxyProcess) stop() {
	select {
	case <-p.done:
	default:
		_ = p.cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-p.done:
		case <-time.After(15 * time.Second):
			_ = p.cmd.Process.Kill()
			<-p.done
		}
	}
	_ = p.log.Close()
}

// snapshot holds cumulative proxy counters at one instant.
type snapshot struct {
	UserNS    uint64            `json:"user_ns"`
	SysNS     uint64            `json:"sys_ns"`
	CPUSource string            `json:"cpu_source"`
	RSSBytes  uint64            `json:"rss_bytes"`
	Runtime   map[string]uint64 `json:"runtime,omitempty"`
}

func (p *proxyProcess) snapshot(ctx context.Context) (snapshot, error) {
	var s snapshot
	runtimeMetrics, err := p.runtimeMetrics(ctx)
	if err == nil {
		s.Runtime = runtimeMetrics
		s.UserNS, s.SysNS, s.CPUSource = runtimeMetrics["user_ns"], runtimeMetrics["sys_ns"], "getrusage"
	} else if s.UserNS, s.SysNS, err = processCPU(p.cmd.Process.Pid); err == nil {
		s.CPUSource = "os"
	} else {
		return s, fmt.Errorf("read proxy CPU: %w", err)
	}
	s.RSSBytes, err = processRSS(p.cmd.Process.Pid)
	return s, err
}

func (p *proxyProcess) runtimeMetrics(ctx context.Context) (map[string]uint64, error) {
	var out map[string]uint64
	body, err := p.get(ctx, "/bench/metrics", 2*time.Second)
	if err != nil {
		return nil, err
	}
	return out, json.Unmarshal(body, &out)
}

// get fetches path from the overlay's HTTP server.
func (p *proxyProcess) get(ctx context.Context, path string, timeout time.Duration) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+p.metricsAddr+path, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: %s", path, resp.Status)
	}
	return io.ReadAll(resp.Body)
}

// processCPU reads user and system CPU time without the overlay.
func processCPU(pid int) (user, sys uint64, err error) {
	switch runtime.GOOS {
	case "linux":
		data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
		if err != nil {
			return 0, 0, err
		}
		// Fields after the parenthesised command: state is index 0, utime 11, stime 12.
		_, rest, ok := strings.Cut(string(data), ") ")
		fields := strings.Fields(rest)
		if !ok || len(fields) < 13 {
			return 0, 0, errors.New("unexpected /proc stat format")
		}
		// Linux reports clock ticks; USER_HZ is 100 on every supported architecture.
		u, err1 := strconv.ParseUint(fields[11], 10, 64)
		s, err2 := strconv.ParseUint(fields[12], 10, 64)
		return u * 10_000_000, s * 10_000_000, errors.Join(err1, err2)
	case "darwin":
		out, err := exec.Command("ps", "-o", "utime=,stime=", "-p", strconv.Itoa(pid)).Output()
		if err != nil {
			return 0, 0, err
		}
		fields := strings.Fields(string(out))
		if len(fields) != 2 {
			return 0, 0, fmt.Errorf("unexpected ps output %q", out)
		}
		u, err1 := parsePSTime(fields[0])
		s, err2 := parsePSTime(fields[1])
		return u, s, errors.Join(err1, err2)
	}
	return 0, 0, fmt.Errorf("process CPU is unsupported on %s", runtime.GOOS)
}

// parsePSTime parses BSD ps times such as "1:02.35" or "1:02:03.45" to nanoseconds.
func parsePSTime(s string) (uint64, error) {
	parts := strings.Split(s, ":")
	var seconds float64
	for _, part := range parts {
		v, err := strconv.ParseFloat(part, 64)
		if err != nil {
			return 0, fmt.Errorf("ps time %q: %w", s, err)
		}
		seconds = seconds*60 + v
	}
	return uint64(seconds * 1e9), nil
}

func processRSS(pid int) (uint64, error) {
	out, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return 0, fmt.Errorf("read proxy RSS: %w", err)
	}
	kb, err := strconv.ParseUint(strings.TrimSpace(string(out)), 10, 64)
	return kb * 1024, err
}

func freeAddr() (string, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	addr := l.Addr().String()
	return addr, l.Close()
}
