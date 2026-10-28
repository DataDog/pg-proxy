package main

import (
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

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
		clusters:         map[string]string{"local": "primary"},
		logicalDatabases: map[string]topology.LogicalDatabase{"local_db": {Cluster: "local"}},
	}
}
