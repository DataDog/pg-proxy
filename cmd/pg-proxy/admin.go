// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package main

import (
	"context"
	"errors"
	"maps"
	"net"
	"net/http"
	"net/netip"
	"slices"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"

	pgproxy "github.com/DataDog/pg-proxy"
)

const (
	backendProbeInterval = 10 * time.Second
	backendProbeTimeout  = 3 * time.Second
)

// healthState holds the signals that /livez and /readyz report.
// A nil *healthState ignores updates, so callers without an admin listener
// do not need checks.
type healthState struct {
	accepting atomic.Bool
	draining  atomic.Bool
	reachable atomic.Bool
}

func newHealthState() *healthState {
	h := &healthState{}
	// newProxy connects to every backend before the proxy starts.
	h.reachable.Store(true)
	return h
}

func (h *healthState) setAccepting(v bool) {
	if h != nil {
		h.accepting.Store(v)
	}
}

func (h *healthState) startDrain() {
	if h != nil {
		h.draining.Store(true)
	}
}

// Responses contain only a fixed status word. They never include backend
// names, addresses, users, or driver errors.
func (h *healthState) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /livez", func(w http.ResponseWriter, _ *http.Request) {
		if !h.accepting.Load() {
			writeStatus(w, http.StatusServiceUnavailable, "not accepting")
			return
		}
		writeStatus(w, http.StatusOK, "ok")
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		switch {
		case h.draining.Load():
			writeStatus(w, http.StatusServiceUnavailable, "draining")
		case !h.accepting.Load():
			writeStatus(w, http.StatusServiceUnavailable, "not accepting")
		case !h.reachable.Load():
			writeStatus(w, http.StatusServiceUnavailable, "backend unreachable")
		default:
			writeStatus(w, http.StatusOK, "ok")
		}
	})
	return mux
}

func writeStatus(w http.ResponseWriter, code int, body string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_, _ = w.Write([]byte(body + "\n"))
}

// probeBackends checks each cluster primary on an interval until ctx ends.
// Each check opens its own short-lived connection, so it never borrows from or
// changes the serving pools.
func (h *healthState) probeBackends(ctx context.Context, targets map[string]*pgx.ConnConfig, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	failing := map[string]bool{}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		healthy := true
		for _, name := range slices.Sorted(maps.Keys(targets)) {
			ok := pingBackend(ctx, targets[name])
			if ctx.Err() != nil {
				return
			}
			if ok == failing[name] {
				// Log only state changes, with the cluster name and no driver error.
				if ok {
					pgproxy.Logger().Info("backend health check recovered", "cluster", name)
				} else {
					pgproxy.Logger().Warn("backend health check failed", "cluster", name)
				}
				failing[name] = !ok
			}
			healthy = healthy && ok
		}
		h.reachable.Store(healthy)
	}
}

func pingBackend(ctx context.Context, settings *pgx.ConnConfig) bool {
	ctx, cancel := context.WithTimeout(ctx, backendProbeTimeout)
	defer cancel()
	conn, err := pgx.ConnectConfig(ctx, settings.Copy())
	if err != nil {
		return false
	}
	defer conn.Close(context.Background())
	return conn.Ping(ctx) == nil
}

func validateAdminAddress(address string) error {
	if address == "" {
		return nil
	}
	if _, err := netip.ParseAddrPort(address); err != nil {
		return errors.New("proxy.admin_address must be an IP address and port")
	}
	return nil
}

type adminServer struct {
	server   *http.Server
	listener net.Listener
	done     chan struct{}
}

// startAdmin binds the admin listener before the proxy starts, so a bad
// address fails startup instead of leaving the proxy without health checks.
func startAdmin(address string, health *healthState) (*adminServer, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, errors.New("could not listen on proxy.admin_address")
	}
	admin := &adminServer{
		server: &http.Server{
			Handler:           health.handler(),
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       5 * time.Second,
			WriteTimeout:      5 * time.Second,
			IdleTimeout:       30 * time.Second,
			MaxHeaderBytes:    8 << 10,
		},
		listener: listener,
		done:     make(chan struct{}),
	}
	pgproxy.Logger().Info("admin listener ready", "address", listener.Addr())
	go func() {
		defer close(admin.done)
		if err := admin.server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			pgproxy.Logger().Error("admin listener stopped", "error", err)
		}
	}()
	return admin, nil
}

func (a *adminServer) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := a.server.Shutdown(ctx); err != nil {
		_ = a.server.Close()
	}
	<-a.done
}
