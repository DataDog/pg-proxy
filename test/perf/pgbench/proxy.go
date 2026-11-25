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
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	"github.com/DataDog/pg-proxy/internal/itest"
)

// proxyConfig is a minimal configuration that older refs also accept, so it is
// written by hand rather than encoded from the current pkg/config types. It sets
// client_disconnect_timer only when asked, because refs that predate the
// setting reject it.
func proxyConfig(pg *itest.Postgres, listen string, poolSize int, clientDisconnectTimer bool) string {
	config := fmt.Sprintf(`databases:
  - name: primary
    host: %q
    port: %d
    dbname: postgres
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
`, pg.Host, pg.Port, poolSize, itest.PostgresUser, itest.PostgresPassword, listen)
	if clientDisconnectTimer {
		config += "  client_disconnect_timer: true\n"
	}
	return config
}

type proxyProcess struct {
	Addr        string
	metricsAddr string
	cmd         *exec.Cmd
	done        chan struct{}
	log         *os.File
}

func startProxy(ctx context.Context, opts options, v *variant, pg *itest.Postgres, dir string) (*proxyProcess, error) {
	listen, err := freeAddr()
	if err != nil {
		return nil, err
	}
	metricsAddr, err := freeAddr()
	if err != nil {
		return nil, err
	}
	configPath := filepath.Join(dir, "proxy.yaml")
	timer := opts.BClientDisconnectTimer && v.Label == "B"
	if err := os.WriteFile(configPath, []byte(proxyConfig(pg, listen, opts.PoolSize, timer)), 0o600); err != nil {
		return nil, err
	}
	log, err := os.Create(filepath.Join(dir, "proxy.log"))
	if err != nil {
		return nil, err
	}
	argv := []string{v.Binary, "-config", configPath}
	if opts.ProxyCPUs != "" {
		argv = append([]string{"taskset", "-c", opts.ProxyCPUs}, argv...)
	}
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Env = append(baseEnv(), "PG_PROXY_BENCH_ADDR="+metricsAddr)
	if opts.GOMAXPROCS != "" {
		cmd.Env = append(cmd.Env, "GOMAXPROCS="+opts.GOMAXPROCS)
	}
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

// baseEnv passes only variables that do not change proxy or pgbench cost, so
// telemetry exporters, Go runtime tuning, and libpq defaults in the caller's
// shell cannot skew results.
func baseEnv() []string {
	var env []string
	for _, key := range []string{"PATH", "HOME", "TMPDIR", "USER", "LANG", "TZ"} {
		if value, ok := os.LookupEnv(key); ok {
			env = append(env, key+"="+value)
		}
	}
	return env
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

// snapshot returns the overlay's counters; see overlay/benchmetrics.go.
func (p *proxyProcess) snapshot(ctx context.Context) (map[string]uint64, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+p.metricsAddr+"/bench/metrics", nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("read proxy metrics: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("read proxy metrics: %s", resp.Status)
	}
	var out map[string]uint64
	return out, json.NewDecoder(resp.Body).Decode(&out)
}

func freeAddr() (string, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	addr := l.Addr().String()
	return addr, l.Close()
}
