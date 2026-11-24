// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

//go:build integration && regress

package regress

import (
	"context"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"

	"github.com/DataDog/pg-proxy/internal/itest"
)

// regressDir holds the built suite in the image; it is pg_regress's input,
// output and library directory on both the server and the runner.
const regressDir = "/usr/local/pgsql/regress"

// scheduleFile is where the runner receives the (filtered) schedule.
const scheduleFile = "/tmp/pg-proxy-schedule"

// installcheck is the outcome of one pg_regress run.
type installcheck struct {
	results results
	// diffs holds each failed test's section of regression.diffs.
	diffs map[string]string
}

// merge adds the outcome of a later run of other tests.
func (i *installcheck) merge(other installcheck) {
	maps.Copy(i.results, other.results)
	maps.Copy(i.diffs, other.diffs)
}

// splitSetup splits schedule into its test_setup group and the rest, or
// returns no setup if the schedule does not start with test_setup alone.
func splitSetup(schedule string) (setup, rest string) {
	var before strings.Builder
	for line := range strings.Lines(schedule) {
		if !strings.HasPrefix(line, "test:") {
			before.WriteString(line)
			continue
		}
		if strings.TrimSpace(line) != "test: test_setup" {
			return "", schedule
		}
		return before.String() + line, before.String() + strings.TrimPrefix(schedule[before.Len():], line)
	}
	return "", schedule
}

// readSchedule returns the backend image's parallel schedule without the
// skipped tests, and every test the full schedule names.
func readSchedule(t *testing.T, backend *itest.Postgres, skipped map[string]string) (string, map[string]bool) {
	t.Helper()
	scheduled := map[string]bool{}
	var out strings.Builder
	for _, line := range strings.Split(copyFromContainer(t, backend.Container(), regressDir+"/parallel_schedule"), "\n") {
		tests, isTestLine := strings.CutPrefix(line, "test:")
		if !isTestLine {
			out.WriteString(line + "\n")
			continue
		}
		var kept []string
		for _, name := range strings.Fields(tests) {
			scheduled[name] = true
			if _, skip := skipped[name]; !skip {
				kept = append(kept, name)
			}
		}
		if len(kept) > 0 {
			out.WriteString("test: " + strings.Join(kept, " ") + "\n")
		}
	}
	return out.String(), scheduled
}

// runInstallcheck runs schedule, one test at a time, against the server
// listening on port on the host, sharing the server's results volume. It saves
// pg_regress's output and diffs under the artifacts directory, in a label
// subdirectory if label is not empty.
func runInstallcheck(t *testing.T, v *pgVersion, port uint16, volume, schedule, label string) installcheck {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Minute)
	defer cancel()
	runner, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: v.image,
			Env: map[string]string{
				"PGHOST": testcontainers.HostInternal, "PGPORT": strconv.Itoa(int(port)),
				"PGUSER": itest.PostgresUser, "PGDATABASE": regressDatabase,
			},
			// The host port is forwarded only after the container starts, so wait
			// up to a minute for it before running pg_regress.
			Cmd: []string{
				"sh", "-c", `i=0; until pg_isready -q; do i=$((i+1)); [ $i -lt 600 ] || { echo "server unreachable"; exit 2; }; sleep 0.1; done; exec "$@"`, "sh",
				regressDir + "/pg_regress", "--use-existing", "--dbname=" + regressDatabase,
				"--inputdir=" + regressDir, "--outputdir=" + regressDir,
				"--dlpath=" + regressDir, "--bindir=/usr/local/pgsql/bin",
				"--schedule=" + scheduleFile, "--max-connections=1",
			},
			Files: []testcontainers.ContainerFile{{
				Reader: strings.NewReader(schedule), ContainerFilePath: scheduleFile, FileMode: 0o644,
			}},
			// No wait strategy: Testcontainers opens the host port tunnel once the
			// container is ready, so waiting for exit here would never connect.
			HostAccessPorts: []int{int(port)},
			Mounts:          testcontainers.ContainerMounts{resultsMount(volume)},
		},
		Started: true,
	})
	testcontainers.CleanupContainer(t, runner)
	require.NoError(t, err, "start pg_regress")
	var exitCode int
	for {
		state, err := runner.State(ctx)
		require.NoError(t, err, "pg_regress did not finish")
		if !state.Running {
			exitCode = state.ExitCode
			break
		}
		time.Sleep(time.Second)
	}

	dir := filepath.Join(artifactsDir(t), label)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	output := saveContainerFile(t, runner, "", filepath.Join(dir, "output.txt"))
	diffs := saveContainerFile(t, runner, regressDir+"/regression.diffs", filepath.Join(dir, "regression.diffs"))

	parsed := parseResults(output)
	t.Logf("PostgreSQL %s: %d tests, %d failed; output and diffs in %s", v.Version, len(parsed), len(parsed.failed()), dir)
	// pg_regress exits 0 when every test passes, 1 when some fail, and 2 on errors
	// such as a lost server connection. A run that stops early or prints a result
	// line the parsers do not recognize must not count its missing tests as
	// passing, nor let PG_PROXY_REGRESS_UPDATE drop their fail entries.
	if exitCode != 0 && exitCode != 1 {
		t.Fatalf("pg_regress exited with code %d; output in %s:\n%s", exitCode, dir, truncateLines(output, 40))
	}
	if missing := missingResults(schedule, parsed); len(missing) > 0 {
		t.Fatalf("pg_regress reported no result for %d scheduled tests: %s; output in %s",
			len(missing), strings.Join(missing, ", "), dir)
	}
	return installcheck{results: parsed, diffs: diffSections(diffs)}
}

// saveContainerFile saves path from the container to dest and returns its
// contents; an empty path reads the container's output. A missing file is
// empty: pg_regress writes no diffs when every test passes.
func saveContainerFile(t *testing.T, c testcontainers.Container, path, dest string) string {
	t.Helper()
	var reader io.ReadCloser
	var err error
	if path == "" {
		reader, err = c.Logs(context.Background())
		require.NoError(t, err, "read pg_regress output")
	} else if reader, err = c.CopyFileFromContainer(context.Background(), path); err != nil {
		return ""
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(dest, data, 0o644))
	return string(data)
}

// artifactsDir returns a directory for t that outlives the test, under
// PG_PROXY_REGRESS_ARTIFACTS if set, otherwise under a new temporary directory.
func artifactsDir(t *testing.T) string {
	t.Helper()
	root := os.Getenv("PG_PROXY_REGRESS_ARTIFACTS")
	if root == "" {
		var err error
		root, err = os.MkdirTemp("", "pg-proxy-regress-")
		require.NoError(t, err)
	}
	dir := filepath.Join(root, strings.ReplaceAll(t.Name(), "/", "-"))
	require.NoError(t, os.MkdirAll(dir, 0o755))
	return dir
}

// logDiffs logs the start of each named test's diff.
func logDiffs(t *testing.T, names []string, diffs map[string]string) {
	t.Helper()
	for _, name := range names {
		t.Logf("%s failed:\n%s", name, truncateLines(diffs[name], 30))
	}
}

func truncateLines(text string, limit int) string {
	lines := strings.Split(strings.TrimRight(text, "\n"), "\n")
	if len(lines) <= limit {
		return strings.Join(lines, "\n")
	}
	return strings.Join(lines[:limit], "\n") + fmt.Sprintf("\n... %d more lines", len(lines)-limit)
}

func readBaseline(t *testing.T, path string) baseline {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	b, err := parseBaseline(path, string(data))
	require.NoError(t, err)
	return b
}

// checkBaseline fails t when run differs from the baseline: a test failing that
// is not listed, a listed test that passes or no longer exists, or a listed
// test whose diff changed size. With PG_PROXY_REGRESS_UPDATE set it rewrites
// the fail entries of path from run instead, keeping the header, the skip and
// isolate entries and existing reasons.
func checkBaseline(t *testing.T, path string, b baseline, run installcheck) {
	t.Helper()
	var newFailures, newPasses, unknown []string
	for _, name := range run.results.failed() {
		if _, listed := b.failing[name]; !listed {
			newFailures = append(newFailures, name)
		}
	}
	for name := range b.failing {
		passed, ran := run.results[name]
		switch {
		case !ran:
			unknown = append(unknown, name)
		case passed:
			newPasses = append(newPasses, name)
		}
	}
	slices.Sort(newPasses)
	slices.Sort(unknown)
	worse, better := fingerprintChanges(b, run.results.failed(), run.diffs)

	if os.Getenv("PG_PROXY_REGRESS_UPDATE") != "" {
		require.NoError(t, os.WriteFile(path, []byte(formatBaseline(b, run.results.failed(), run.diffs)), 0o644))
		t.Logf("updated %s: %d new failures, %d new passes, %d removed, %d grew, %d shrank",
			path, len(newFailures), len(newPasses), len(unknown), len(worse), len(better))
		return
	}
	if len(newFailures) > 0 {
		logDiffs(t, newFailures, run.diffs)
		t.Errorf("tests failing through pg-proxy that %s does not list (fix them, or list them with a reason): %s",
			path, strings.Join(newFailures, ", "))
	}
	if len(newPasses) > 0 {
		t.Errorf("tests that %s lists as failing now pass through pg-proxy; remove them: %s", path, strings.Join(newPasses, ", "))
	}
	if len(unknown) > 0 {
		t.Errorf("tests that %s lists as failing did not run; remove them: %s", path, strings.Join(unknown, ", "))
	}
	if len(worse) > 0 {
		logDiffs(t, changedNames(worse), run.diffs)
		t.Errorf("tests that %s lists as failing now differ from PostgreSQL in more lines: %s", path, strings.Join(worse, ", "))
	}
	if len(better) > 0 {
		t.Errorf("tests that %s lists as failing now differ from PostgreSQL in fewer lines; lower their diff counts: %s",
			path, strings.Join(better, ", "))
	}
	if t.Failed() {
		t.Log("PG_PROXY_REGRESS_UPDATE=1 rewrites the fail entries and their diff counts from this run's results")
	}
}
