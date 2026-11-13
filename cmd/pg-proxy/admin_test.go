// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/DataDog/pg-proxy/pkg/health"
)

func TestAdminEndpoints(t *testing.T) {
	state := health.NewState([]string{"orders"})
	handler := adminHandler(state)
	expect := func(path string, code int, body string) {
		t.Helper()
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		require.Equal(t, code, recorder.Code, path)
		require.Equal(t, body, strings.TrimSpace(recorder.Body.String()), path)
		require.Equal(t, "no-store", recorder.Header().Get("Cache-Control"))
	}
	expect("/livez", http.StatusServiceUnavailable, "not accepting")
	expect("/readyz", http.StatusServiceUnavailable, "not accepting")
	state.SetAccepting(true)
	expect("/livez", http.StatusOK, "ok")
	expect("/readyz", http.StatusOK, "ok")
	state.StartDrain()
	expect("/livez", http.StatusOK, "ok")
	expect("/readyz", http.StatusServiceUnavailable, "draining")

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/readyz", nil))
	require.Equal(t, http.StatusMethodNotAllowed, recorder.Code)
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

func TestAdminServerServesAndCloses(t *testing.T) {
	state := health.NewState(nil)
	state.SetAccepting(true)
	admin, err := startAdmin("127.0.0.1:0", state)
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
