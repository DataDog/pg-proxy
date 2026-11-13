// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package main

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestHealthEndpoints(t *testing.T) {
	health := newHealthState()
	handler := health.handler()
	get := func(path string) (int, string) {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		return recorder.Code, strings.TrimSpace(recorder.Body.String())
	}
	expect := func(path string, code int, body string) {
		t.Helper()
		gotCode, gotBody := get(path)
		require.Equal(t, code, gotCode, path)
		require.Equal(t, body, gotBody, path)
	}

	expect("/livez", http.StatusServiceUnavailable, "not accepting")
	expect("/readyz", http.StatusServiceUnavailable, "not accepting")

	health.setAccepting(true)
	expect("/livez", http.StatusOK, "ok")
	expect("/readyz", http.StatusOK, "ok")

	health.reachable.Store(false)
	expect("/livez", http.StatusOK, "ok")
	expect("/readyz", http.StatusServiceUnavailable, "backend unreachable")

	health.reachable.Store(true)
	health.startDrain()
	expect("/livez", http.StatusOK, "ok")
	expect("/readyz", http.StatusServiceUnavailable, "draining")

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/readyz", nil))
	require.Equal(t, http.StatusMethodNotAllowed, recorder.Code)
}

func TestNilHealthStateIgnoresUpdates(t *testing.T) {
	var health *healthState
	health.setAccepting(true)
	health.startDrain()
}

func TestAdminAddressConfig(t *testing.T) {
	shared := ossConfig()
	cfg, err := (config{}).withSharedConfig(shared)
	require.NoError(t, err)
	require.Empty(t, cfg.adminAddress)

	shared.Proxy.AdminAddress = "0.0.0.0:8080"
	cfg, err = (config{}).withSharedConfig(shared)
	require.NoError(t, err)
	require.Equal(t, "0.0.0.0:8080", cfg.adminAddress)

	shared.Proxy.AdminAddress = "localhost:8080"
	_, err = (config{}).withSharedConfig(shared)
	require.ErrorContains(t, err, "proxy.admin_address must be an IP address and port")
}

func TestProbeMarksUnreachableBackend(t *testing.T) {
	// A closed port refuses connections at once.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())
	settings, err := pgx.ParseConfig("postgres://postgres:secret-password@" + address + "/postgres?sslmode=disable&connect_timeout=1")
	require.NoError(t, err)

	health := newHealthState()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		health.probeBackends(ctx, map[string]*pgx.ConnConfig{"orders": settings}, 10*time.Millisecond)
	}()
	require.Eventually(t, func() bool { return !health.reachable.Load() }, 5*time.Second, 10*time.Millisecond)
	cancel()
	<-done
}

func TestAdminServerServesAndCloses(t *testing.T) {
	health := newHealthState()
	health.setAccepting(true)
	admin, err := startAdmin("127.0.0.1:0", health)
	require.NoError(t, err)
	address := admin.listener.Addr().String()
	response, err := http.Get("http://" + address + "/readyz")
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Equal(t, http.StatusOK, response.StatusCode)
	admin.Close()
	_, err = http.Get("http://" + address + "/readyz")
	require.Error(t, err)
}
