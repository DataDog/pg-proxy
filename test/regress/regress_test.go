//go:build integration && regress

// Package regress runs PostgreSQL's own regression suite (installcheck) through
// pg-proxy to measure and enforce wire and behavioral compatibility.
//
// The image under image/ builds PostgreSQL from source with its regression
// tests. One container from it is the backend; another runs pg_regress against
// the proxy, which runs on the host through the internal/itest harness. Results
// are compared with expected_failures.txt: a new failure or a new pass fails the
// test, so the list only shrinks deliberately.
package regress

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"

	"github.com/DataDog/pg-proxy/internal/itest"
	pgconfig "github.com/DataDog/pg-proxy/pkg/config"
)

// regressDatabase is the database pg_regress and its expected output assume.
const regressDatabase = "regression"

// image is the regression image used by every test in the package.
var image string

func TestMain(m *testing.M) {
	os.Exit(itest.Main(m, func(ctx context.Context) (func(), error) {
		if prebuilt := os.Getenv("PG_PROXY_REGRESS_IMAGE"); prebuilt != "" {
			image = prebuilt
			return nil, nil
		}
		// Building from source takes several minutes the first time; later builds
		// reuse Docker's layer cache.
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Minute)
		defer cancel()
		var err error
		image, err = buildImage(ctx)
		return nil, err
	}))
}

// buildImage builds image/ with the Docker CLI. Testcontainers' builder is not
// used because it queries every configured credential helper, which fails on
// machines with helpers for unreachable registries.
func buildImage(ctx context.Context) (string, error) {
	const tag = "pg-proxy-regress:local"
	cmd := exec.CommandContext(ctx, "docker", "build", "--tag", tag, "image")
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	if os.Getenv("PG_PROXY_REGRESS_BUILD_LOG") != "" {
		cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	}
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("build regression image: %w\n%s", err, output.String())
	}
	return tag, nil
}

// TestInstallcheckDirect runs the suite against the backend without the proxy.
// Every test must pass; a failure here means the setup, not pg-proxy, is wrong.
func TestInstallcheckDirect(t *testing.T) {
	backend, volume := startBackend(t)
	results, diffs := runInstallcheck(t, backend.Port, volume, "direct")
	require.NotEmpty(t, results, "pg_regress reported no tests")
	logDiffs(t, results.failed(), diffs)
	require.Empty(t, results.failed(), "tests failed without the proxy; see the diffs above")
}

// TestInstallcheckThroughProxy runs the suite through pg-proxy and compares the
// failures with expected_failures.txt.
func TestInstallcheckThroughProxy(t *testing.T) {
	backend, volume := startBackend(t)
	database := backend.Database("regression-primary", regressDatabase)
	// One backend connection: consecutive sessions then share temporary tables,
	// prepared statements and other connection state, as they would in a busy
	// pool, while results stay deterministic.
	database.MaxConns = 1
	proxy := itest.MustStartProxy(t, pgconfig.Config{
		Databases:        []pgconfig.Database{database},
		Clusters:         map[string]pgconfig.Cluster{"regression": {Primary: database.Name}},
		LogicalDatabases: map[string]pgconfig.LogicalDatabase{regressDatabase: {Cluster: "regression"}},
	})
	_, portText, err := net.SplitHostPort(proxy.Addr)
	require.NoError(t, err)
	port, err := strconv.ParseUint(portText, 10, 16)
	require.NoError(t, err)

	results, diffs := runInstallcheck(t, uint16(port), volume, "proxy")
	require.NotEmpty(t, results, "pg_regress reported no tests")
	checkBaseline(t, "expected_failures.txt", results, diffs)
}

// startBackend starts a regression-image server with a regression database
// configured the way pg_regress configures one it creates. It also returns the
// volume mounted at the server's results directory, which the runner shares:
// some tests write a file with psql and read it with server-side COPY.
func startBackend(t *testing.T) (*itest.Postgres, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	volume := fmt.Sprintf("pg-proxy-regress-%d-%s", os.Getpid(), strings.ToLower(t.Name()))
	t.Cleanup(func() { _ = exec.Command("docker", "volume", "rm", "--force", volume).Run() })
	backend, err := itest.StartPostgres(ctx, itest.WithImage(image),
		itest.WithContainerOptions(testcontainers.WithMounts(resultsMount(volume))))
	require.NoError(t, err)
	t.Cleanup(func() { _ = backend.Terminate(context.Background()) })

	statements := []string{"CREATE DATABASE " + regressDatabase + " TEMPLATE template0"}
	// pg_regress applies these when it creates the database; --use-existing skips them.
	for _, setting := range []string{
		"lc_messages TO 'C'", "lc_monetary TO 'C'", "lc_numeric TO 'C'", "lc_time TO 'C'",
		"bytea_output TO 'hex'", "timezone_abbreviations TO 'Default'",
		// pg_regress sends these as PGTZ and PGDATESTYLE startup parameters, which
		// pg-proxy does not forward to backend connections.
		"timezone TO 'PST8PDT'", "datestyle TO 'Postgres, MDY'",
	} {
		statements = append(statements, "ALTER DATABASE "+regressDatabase+" SET "+setting)
	}
	require.NoError(t, backend.Exec(ctx, statements...))
	return backend, volume
}

func resultsMount(volume string) testcontainers.ContainerMount {
	return testcontainers.VolumeMount(volume, testcontainers.ContainerMountTarget(regressDir+"/results"))
}
