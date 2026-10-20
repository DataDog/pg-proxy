package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoopbackOnly(t *testing.T) {
	for _, address := range []string{":15432", "0.0.0.0:15432", "[::]:15432", "192.0.2.1:15432", "localhost:15432"} {
		t.Run(address, func(t *testing.T) {
			cfg := config{listenAddress: address, database: "local_db", backendURL: "postgres://localhost/postgres", maxSessions: 10}
			require.ErrorContains(t, cfg.validate(), "loopback")
		})
	}
	for _, address := range []string{"127.0.0.1:0", "[::1]:15432"} {
		cfg := config{listenAddress: address, database: "local_db", backendURL: "postgres://localhost/postgres", maxSessions: 10}
		require.NoError(t, cfg.validate())
	}
}

func TestInvalidBackendURLDoesNotExposePassword(t *testing.T) {
	_, err := newProxy(t.Context(), config{
		listenAddress: "127.0.0.1:0", database: "local_db", maxSessions: 10,
		backendURL: "postgres://postgres:secret-password@localhost:invalid/postgres",
	})
	require.EqualError(t, err, "invalid backend URL")
}
