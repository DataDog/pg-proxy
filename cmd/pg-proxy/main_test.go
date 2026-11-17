// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package main

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	pgconfig "github.com/DataDog/pg-proxy/pkg/config"
	"github.com/DataDog/pg-proxy/pkg/limiter"
	"github.com/DataDog/pg-proxy/pkg/server"
	"github.com/DataDog/pg-proxy/pkg/topology"
)

func TestLoopbackOnly(t *testing.T) {
	for _, address := range []string{":15432", "0.0.0.0:15432", "[::]:15432", "192.0.2.1:15432", "localhost:15432"} {
		t.Run(address, func(t *testing.T) {
			cfg := backendConfig(t, "postgres://localhost/postgres")
			cfg.listenAddress = address
			require.ErrorContains(t, cfg.validate(), "loopback")
		})
	}
	for _, address := range []string{"127.0.0.1:0", "[::1]:15432"} {
		cfg := backendConfig(t, "postgres://localhost/postgres")
		cfg.listenAddress = address
		require.NoError(t, cfg.validate())
	}
}

func TestInvalidBackendSettingsDoNotExposePassword(t *testing.T) {
	shared := ossConfig()
	shared.Databases[0].SSLMode = "invalid-test-password"
	_, err := (config{}).withSharedConfig(shared)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "test-password")
}

func backendConfig(t *testing.T, url string) config {
	t.Helper()
	settings, err := pgxpool.ParseConfig(url)
	require.NoError(t, err)
	return config{
		listenAddress:    "127.0.0.1:0",
		databases:        map[string]*pgxpool.Config{"primary": settings},
		clusters:         map[string]pgconfig.Cluster{"local": {Primary: "primary"}},
		logicalDatabases: map[string]topology.LogicalDatabase{"local_db": {Cluster: "local"}},
	}
}

// A registry entry models a session still finishing a transaction. It does not
// need a backend, so both shutdown paths can be checked without PostgreSQL.
func TestServeShutdownGracePeriod(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		name := "drained"
		if timeout {
			name = "deadline exceeded"
		}
		t.Run(name, func(t *testing.T) {
			ctx, stop := context.WithCancel(t.Context())
			defer stop()
			sessions := newSessionRegistry()
			proxy, err := server.NewServer(ctx, server.Config{
				ListenAddress: "127.0.0.1:0",
				Admission:     limiter.New(limiter.Params{GlobalLimit: 100, DefaultServiceMax: 100}),
				Sessions:      sessions,
				Runtime:       server.RuntimeConfig{Inventory: &staticInventory{}},
			})
			require.NoError(t, err)
			defer proxy.Close()
			sessions.Register("pending", nil)
			defer sessions.Delete("pending")
			cleaned := make(chan struct{})
			done := make(chan error, 1)
			grace := time.Second
			if timeout {
				grace = 50 * time.Millisecond
			}
			go func() { done <- serve(ctx, stop, proxy, sessions, func() { close(cleaned) }, grace) }()
			stop()
			if !timeout {
				sessions.Delete("pending")
			}
			select {
			case err := <-done:
				if timeout {
					require.ErrorContains(t, err, "shutdown timed out waiting for client sessions")
					select {
					case <-cleaned:
						t.Fatal("backend cleanup ran before the session finished")
					default:
					}
				} else {
					require.NoError(t, err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("shutdown did not respect its grace period")
			}
			sessions.Delete("pending")
			select {
			case <-cleaned:
			case <-time.After(5 * time.Second):
				t.Fatal("shutdown cleanup did not finish")
			}
		})
	}
}
