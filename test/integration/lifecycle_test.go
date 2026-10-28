//go:build integration

package integration

import (
	"context"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/DataDog/pg-proxy/internal/itest"
	pgconfig "github.com/DataDog/pg-proxy/pkg/config"
)

// TestInvalidConfigFailsStartup verifies the binary refuses to start, with a
// reason, when the configuration is unsupported.
func TestInvalidConfigFailsStartup(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	_, err := itest.StartProxy(ctx, pgconfig.Config{
		Databases:        []pgconfig.Database{backend.Database("primary", "postgres")},
		Clusters:         map[string]pgconfig.Cluster{"local": {Primary: "primary"}},
		LogicalDatabases: map[string]pgconfig.LogicalDatabase{dbDefault: {Cluster: "local"}},
		Proxy:            pgconfig.Proxy{ListenerAddress: "0.0.0.0:0"},
	})
	require.ErrorContains(t, err, "loopback")
}

// TestShutdownWithIdleClients verifies SIGTERM closes idle client sessions and
// exits successfully.
func TestShutdownWithIdleClients(t *testing.T) {
	proxy := startProxy(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	conn, err := proxy.Connect(ctx, dbDefault, itest.PostgresUser)
	require.NoError(t, err)
	defer conn.Close(context.Background())
	_, err = conn.Exec(ctx, "SELECT 1")
	require.NoError(t, err)

	start := time.Now()
	require.NoError(t, proxy.Stop())
	require.Less(t, time.Since(start), 5*time.Second, "idle sessions should not delay shutdown")
	_, err = conn.Exec(ctx, "SELECT 1")
	require.Error(t, err, "the idle session should be closed by shutdown")
}

// TestShutdownWithOpenTransaction verifies an unfinished transaction holds
// shutdown until the five-second deadline, after which the process exits
// with an error.
func TestShutdownWithOpenTransaction(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	// StartProxy rather than MustStartProxy: the nonzero exit is expected here.
	proxy, err := itest.StartProxy(ctx, pgconfig.Config{
		Databases:        []pgconfig.Database{backend.Database("primary", "postgres")},
		Clusters:         map[string]pgconfig.Cluster{"local": {Primary: "primary"}},
		LogicalDatabases: map[string]pgconfig.LogicalDatabase{dbDefault: {Cluster: "local"}},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = proxy.Stop()
		if t.Failed() {
			t.Logf("pg-proxy output:\n%s", proxy.Logs())
		}
	})

	conn, err := proxy.Connect(ctx, dbDefault, itest.PostgresUser)
	require.NoError(t, err)
	defer conn.Close(context.Background())
	_, err = conn.Exec(ctx, "BEGIN")
	require.NoError(t, err)

	start := time.Now()
	err = proxy.Stop()
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr, "an open transaction should make shutdown exit nonzero")
	require.GreaterOrEqual(t, time.Since(start), 4*time.Second, "shutdown should wait for the session")
}
