//go:build integration && regress

// Package regress runs PostgreSQL's own regression suite (installcheck) through
// pg-proxy to measure and enforce wire and behavioral compatibility, for every
// PostgreSQL major version in versions.json.
//
// The image under image/ builds a PostgreSQL release from source with its
// regression tests. One container from it is the backend; another runs
// pg_regress against the proxy, which runs on the host through the
// internal/itest harness. baseline/pg<major>.txt lists the tests skipped by
// design and the tests expected to fail: an unlisted failure or a listed test
// that passes fails the run, so the list only changes deliberately.
package regress

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"regexp"
	"slices"
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

// pgVersion is an entry of versions.json.
type pgVersion struct {
	Major   string `json:"major"`
	Version string `json:"version"`
	SHA256  string `json:"sha256"`
	image   string
}

// versions are the versions selected for this run, with their images.
var versions []*pgVersion

func TestMain(m *testing.M) {
	// testcontainers/sshd, which forwards the proxy's port on the host to the
	// pg_regress container, is not in registry.ddbuild.io's Docker Hub mirror, so
	// Testcontainers' own images come from Docker Hub here.
	if _, set := os.LookupEnv("TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX"); !set {
		_ = os.Setenv("TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX", "")
	}
	os.Exit(itest.Main(func() int { return m.Run() }, func(context.Context) (func(), error) {
		var err error
		if versions, err = selectVersions(os.Getenv("PG_PROXY_REGRESS_PG_MAJORS")); err != nil {
			return nil, err
		}
		// Building from source takes several minutes per version the first time;
		// later builds reuse Docker's layer cache.
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Minute)
		defer cancel()
		for _, v := range versions {
			if v.image, err = buildImage(ctx, v); err != nil {
				return nil, err
			}
		}
		return nil, nil
	}))
}

// selectVersions returns the versions in versions.json named by majors, a
// comma-separated list or "all". It defaults to the newest version.
func selectVersions(majors string) ([]*pgVersion, error) {
	data, err := os.ReadFile("versions.json")
	if err != nil {
		return nil, err
	}
	var all []*pgVersion
	if err := json.Unmarshal(data, &all); err != nil {
		return nil, fmt.Errorf("versions.json: %w", err)
	}
	if len(all) == 0 {
		return nil, fmt.Errorf("versions.json lists no versions")
	}
	switch majors {
	case "":
		return all[len(all)-1:], nil
	case "all":
		return all, nil
	}
	var selected []*pgVersion
	for _, major := range strings.Split(majors, ",") {
		index := slices.IndexFunc(all, func(v *pgVersion) bool { return v.Major == strings.TrimSpace(major) })
		if index < 0 {
			return nil, fmt.Errorf("PostgreSQL %s is not in versions.json", major)
		}
		selected = append(selected, all[index])
	}
	return selected, nil
}

// buildImage builds image/ for v with the Docker CLI. Testcontainers' builder is
// not used because it queries every configured credential helper, which fails
// on machines with helpers for unreachable registries. The tag names a registry
// host so that Testcontainers does not prefix it with the Docker Hub mirror; the
// image is never pushed.
func buildImage(ctx context.Context, v *pgVersion) (string, error) {
	tag := "pg-proxy.localhost/regress:" + v.Version
	cmd := exec.CommandContext(ctx, "docker", "build", "--tag", tag, "--build-arg", "DEBIAN_IMAGE="+itest.DebianImage(),
		"--build-arg", "PG_VERSION="+v.Version, "--build-arg", "PG_SHA256="+v.SHA256, "image")
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	if os.Getenv("PG_PROXY_REGRESS_BUILD_LOG") != "" {
		cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	}
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("build regression image for PostgreSQL %s: %w\n%s", v.Version, err, output.String())
	}
	return tag, nil
}

// TestInstallcheckDirect runs the full suite against the backend without the
// proxy. Every test must pass; a failure here means the setup, not pg-proxy, is
// wrong.
func TestInstallcheckDirect(t *testing.T) {
	for _, v := range versions {
		t.Run(v.Major, func(t *testing.T) {
			backend, volume := startBackend(t, v)
			schedule, _ := readSchedule(t, backend, nil)
			run := runInstallcheck(t, v, backend.Port, volume, schedule, "")
			logDiffs(t, run.results.failed(), run.diffs)
			require.Empty(t, run.results.failed(), "tests failed without the proxy; see the diffs above")
		})
	}
}

// TestInstallcheckThroughProxy runs the suite through pg-proxy, without the
// tests the baseline skips, and compares the failures with the baseline.
func TestInstallcheckThroughProxy(t *testing.T) {
	for _, v := range versions {
		t.Run(v.Major, func(t *testing.T) {
			path := "baseline/pg" + v.Major + ".txt"
			baseline := readBaseline(t, path)
			backend, volume := startBackend(t, v)
			schedule, scheduled := readSchedule(t, backend, baseline.skipped)
			var stale []string
			for name := range baseline.skipped {
				if !scheduled[name] {
					stale = append(stale, name)
				}
			}
			slices.Sort(stale)
			require.Empty(t, stale, "%s skips tests that PostgreSQL %s does not have", path, v.Version)

			ctx := t.Context()
			setup, rest := splitSetup(schedule)
			var run installcheck
			if setup != "" && hasSetting(t, ctx, backend, "allow_in_place_tablespaces") {
				// test_setup creates a tablespace after SET allow_in_place_tablespaces,
				// which the proxy resets after that transaction. Run it with the
				// setting as the database default, then run the rest on a new
				// backend connection without it, since the tablespace test checks
				// the default.
				require.NoError(t, backend.Exec(ctx, "ALTER DATABASE "+regressDatabase+" SET allow_in_place_tablespaces TO on"))
				setupProxy := startRegressProxy(t, backend)
				run = runInstallcheck(t, v, proxyPort(t, setupProxy), volume, setup, "setup")
				require.NoError(t, setupProxy.Stop())
				require.NoError(t, backend.Exec(ctx, "ALTER DATABASE "+regressDatabase+" RESET allow_in_place_tablespaces"))
				run.merge(runInstallcheck(t, v, proxyPort(t, startRegressProxy(t, backend)), volume, rest, "tests"))
			} else {
				run = runInstallcheck(t, v, proxyPort(t, startRegressProxy(t, backend)), volume, schedule, "")
			}
			checkBaseline(t, path, baseline, run)
		})
	}
}

// startRegressProxy starts a proxy serving the regression database. It has one
// backend connection: consecutive sessions then share temporary tables,
// prepared statements and other connection state, as they would in a busy pool,
// while results stay deterministic.
func startRegressProxy(t *testing.T, backend *itest.Postgres) *itest.Proxy {
	t.Helper()
	database := backend.Database("regression-primary", regressDatabase)
	database.MaxConns = 1
	return itest.MustStartProxy(t, pgconfig.Config{
		Databases:        []pgconfig.Database{database},
		Clusters:         map[string]pgconfig.Cluster{"regression": {Primary: database.Name}},
		LogicalDatabases: map[string]pgconfig.LogicalDatabase{regressDatabase: {Cluster: "regression"}},
	})
}

func proxyPort(t *testing.T, proxy *itest.Proxy) uint16 {
	t.Helper()
	_, portText, err := net.SplitHostPort(proxy.Addr)
	require.NoError(t, err)
	port, err := strconv.ParseUint(portText, 10, 16)
	require.NoError(t, err)
	return uint16(port)
}

// startBackend starts a server from v's image with a regression database set
// up the way pg_regress sets up one it creates. It also returns the volume
// mounted at the server's results directory, which the runner shares: some
// tests write a file with psql and read it with server-side COPY.
func startBackend(t *testing.T, v *pgVersion) (*itest.Postgres, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	volume := "pg-proxy-regress-" + strconv.Itoa(os.Getpid()) + "-" + strings.ToLower(strings.ReplaceAll(t.Name(), "/", "-"))
	t.Cleanup(func() { _ = exec.Command("docker", "volume", "rm", "--force", volume).Run() })
	backend, err := itest.StartPostgres(ctx, itest.WithImage(v.image),
		itest.WithContainerOptions(testcontainers.WithMounts(resultsMount(volume))))
	require.NoError(t, err)
	t.Cleanup(func() { _ = backend.Terminate(context.Background()) })

	settings := []string{
		// pg_regress applies these when it creates the database; --use-existing skips them.
		"lc_messages TO 'C'", "lc_monetary TO 'C'", "lc_numeric TO 'C'", "lc_time TO 'C'",
		"bytea_output TO 'hex'", "timezone_abbreviations TO 'Default'",
	}
	// pg_regress sends its time zone, date style and options as startup
	// parameters, which pg-proxy does not forward to backend connections.
	settings = append(settings, startupSettings(t, backend)...)
	statements := []string{"CREATE DATABASE " + regressDatabase + " TEMPLATE template0"}
	for _, setting := range settings {
		statements = append(statements, "ALTER DATABASE "+regressDatabase+" SET "+setting)
	}
	require.NoError(t, backend.Exec(ctx, statements...))
	return backend, volume
}

var (
	setenvPattern   = regexp.MustCompile(`setenv\("(PGTZ|PGDATESTYLE)", "([^"]+)"`)
	pgoptionPattern = regexp.MustCompile(`-c (\w+)=(\S+)`)
	myPgoptions     = regexp.MustCompile(`my_pgoptions = "([^"]+)"`)
)

// startupSettings returns the settings pg_regress sends at startup, read from
// the pg_regress source in the image so that they match its version.
func startupSettings(t *testing.T, backend *itest.Postgres) []string {
	t.Helper()
	source := copyFromContainer(t, backend.Container(), regressDir+"/pg_regress.c")
	parameters := map[string]string{"PGTZ": "timezone", "PGDATESTYLE": "datestyle"}
	var settings []string
	for _, match := range setenvPattern.FindAllStringSubmatch(source, -1) {
		settings = append(settings, parameters[match[1]]+" TO '"+match[2]+"'")
		delete(parameters, match[1])
	}
	require.Empty(t, parameters, "pg_regress.c does not set these environment variables")
	options := myPgoptions.FindStringSubmatch(source)
	require.NotNil(t, options, "pg_regress.c has no PGOPTIONS")
	for _, match := range pgoptionPattern.FindAllStringSubmatch(options[1], -1) {
		settings = append(settings, match[1]+" TO '"+match[2]+"'")
	}
	return settings
}

func hasSetting(t *testing.T, ctx context.Context, backend *itest.Postgres, name string) bool {
	t.Helper()
	conn, err := backend.Connect(ctx, "postgres")
	require.NoError(t, err)
	defer conn.Close(context.Background())
	var exists bool
	require.NoError(t, conn.QueryRow(ctx, "SELECT EXISTS (SELECT FROM pg_settings WHERE name = $1)", name).Scan(&exists))
	return exists
}

func resultsMount(volume string) testcontainers.ContainerMount {
	return testcontainers.VolumeMount(volume, testcontainers.ContainerMountTarget(regressDir+"/results"))
}

func copyFromContainer(t *testing.T, c testcontainers.Container, path string) string {
	t.Helper()
	reader, err := c.CopyFileFromContainer(context.Background(), path)
	require.NoError(t, err, "copy %s", path)
	defer reader.Close()
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	return string(data)
}
