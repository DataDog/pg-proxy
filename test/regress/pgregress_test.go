//go:build integration && regress

package regress

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
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

// tapResult matches a pg_regress TAP result line, such as
// "not ok 12    + int8    25 ms".
var tapResult = regexp.MustCompile(`^(ok|not ok)\s+\d+\s+[-+]\s+(\S+)`)

// diffHeader starts the section of regression.diffs for one test.
var diffHeader = regexp.MustCompile(`^diff \S+ \S+ \S*/results/(\S+)\.out$`)

// results maps each test name to whether it passed.
type results map[string]bool

func (r results) failed() []string {
	var names []string
	for name, passed := range r {
		if !passed {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	return names
}

// runInstallcheck runs the parallel schedule, one test at a time, against the
// server listening on port on the host, sharing the server's results volume. It
// saves pg_regress's output and diffs under the artifacts directory and returns
// the per-test results with each failed test's diff.
func runInstallcheck(t *testing.T, port uint16, volume, label string) (results, map[string]string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Minute)
	defer cancel()
	runner, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: image,
			Env: map[string]string{
				"PGHOST": testcontainers.HostInternal, "PGPORT": strconv.Itoa(int(port)),
				"PGUSER": itest.PostgresUser, "PGDATABASE": regressDatabase,
			},
			// The host port is forwarded only after the container starts, so wait
			// up to a minute for it before running pg_regress.
			Cmd: []string{
				"sh", "-c", `i=0; until pg_isready -q; do i=$((i+1)); [ $i -lt 600 ] || { echo "server unreachable"; exit 2; }; sleep 0.1; done; exec "$@"`, "sh",
				regressDir + "/pg_regress", "--use-existing", "--dbname=" + regressDatabase,
				"--inputdir=" + regressDir, "--outputdir=" + regressDir, "--expecteddir=" + regressDir,
				"--dlpath=" + regressDir, "--bindir=/usr/local/pgsql/bin",
				"--schedule=" + regressDir + "/parallel_schedule", "--max-connections=1",
			},
			// No wait strategy: Testcontainers opens the host port tunnel once the
			// container is ready, so waiting for exit here would never connect.
			HostAccessPorts: []int{int(port)},
			Mounts:          testcontainers.ContainerMounts{resultsMount(volume)},
		},
		Started: true,
	})
	testcontainers.CleanupContainer(t, runner)
	require.NoError(t, err, "start pg_regress")
	for {
		state, err := runner.State(ctx)
		require.NoError(t, err, "pg_regress did not finish")
		if !state.Running {
			break
		}
		time.Sleep(time.Second)
	}

	dir := artifactsDir(t, label)
	output := readContainerFile(t, runner, "", filepath.Join(dir, "output.txt"))
	diffs := readContainerFile(t, runner, regressDir+"/regression.diffs", filepath.Join(dir, "regression.diffs"))

	parsed := results{}
	scanner := bufio.NewScanner(strings.NewReader(output))
	for scanner.Scan() {
		if match := tapResult.FindStringSubmatch(scanner.Text()); match != nil {
			parsed[match[2]] = match[1] == "ok"
		}
	}
	failed := parsed.failed()
	t.Logf("%s: %d tests, %d failed; output and diffs in %s", label, len(parsed), len(failed), dir)
	if len(parsed) == 0 {
		t.Logf("pg_regress output:\n%s", output)
	}
	return parsed, diffSections(diffs)
}

// logDiffs logs the start of each named test's diff.
func logDiffs(t *testing.T, names []string, diffs map[string]string) {
	t.Helper()
	for _, name := range names {
		t.Logf("%s failed:\n%s", name, truncateLines(diffs[name], 30))
	}
}

// readContainerFile saves path from the container to dest and returns its
// contents; an empty path reads the container's output. A missing file is
// empty: pg_regress writes no diffs when every test passes.
func readContainerFile(t *testing.T, c testcontainers.Container, path, dest string) string {
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

// artifactsDir returns a directory that outlives the test for pg_regress
// output: PG_PROXY_REGRESS_ARTIFACTS if set, otherwise a new temporary one.
func artifactsDir(t *testing.T, label string) string {
	t.Helper()
	root := os.Getenv("PG_PROXY_REGRESS_ARTIFACTS")
	if root == "" {
		var err error
		root, err = os.MkdirTemp("", "pg-proxy-regress-")
		require.NoError(t, err)
	}
	dir := filepath.Join(root, label)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	return dir
}

// diffSections splits regression.diffs into each test's section.
func diffSections(diffs string) map[string]string {
	sections := map[string]string{}
	var name string
	var section strings.Builder
	flush := func() {
		if name != "" {
			sections[name] = section.String()
		}
		section.Reset()
	}
	for _, line := range strings.Split(diffs, "\n") {
		if match := diffHeader.FindStringSubmatch(line); match != nil {
			flush()
			name = match[1]
		}
		section.WriteString(line)
		section.WriteByte('\n')
	}
	flush()
	return sections
}

func truncateLines(text string, limit int) string {
	lines := strings.Split(strings.TrimRight(text, "\n"), "\n")
	if len(lines) <= limit {
		return strings.Join(lines, "\n")
	}
	return strings.Join(lines[:limit], "\n") + fmt.Sprintf("\n... %d more lines", len(lines)-limit)
}

// checkBaseline fails t when results differ from the expected failures in path:
// a test failing that is not listed, or a listed test that passes or no longer
// exists. With PG_PROXY_REGRESS_UPDATE set it rewrites path instead, keeping the
// header and the reasons of tests that still fail.
func checkBaseline(t *testing.T, path string, got results, diffs map[string]string) {
	t.Helper()
	header, expected := readBaseline(t, path)

	var newFailures, newPasses, unknown []string
	for _, name := range got.failed() {
		if _, listed := expected[name]; !listed {
			newFailures = append(newFailures, name)
		}
	}
	for name := range expected {
		passed, ran := got[name]
		switch {
		case !ran:
			unknown = append(unknown, name)
		case passed:
			newPasses = append(newPasses, name)
		}
	}
	slices.Sort(newPasses)
	slices.Sort(unknown)

	if os.Getenv("PG_PROXY_REGRESS_UPDATE") != "" {
		var out strings.Builder
		out.WriteString(header)
		for _, name := range got.failed() {
			reason, listed := expected[name]
			if !listed || reason == "" {
				reason = "not triaged"
			}
			fmt.Fprintf(&out, "%-32s # %s\n", name, reason)
		}
		require.NoError(t, os.WriteFile(path, []byte(out.String()), 0o644))
		t.Logf("updated %s: %d new failures, %d new passes, %d removed", path, len(newFailures), len(newPasses), len(unknown))
		return
	}
	if len(newFailures) > 0 {
		logDiffs(t, newFailures, diffs)
		t.Errorf("tests failing through pg-proxy that are not in %s (fix them, or list them with a reason): %s",
			path, strings.Join(newFailures, ", "))
	}
	if len(newPasses) > 0 {
		t.Errorf("tests in %s now pass through pg-proxy; remove them: %s", path, strings.Join(newPasses, ", "))
	}
	if len(unknown) > 0 {
		t.Errorf("tests in %s did not run; remove them: %s", path, strings.Join(unknown, ", "))
	}
	if t.Failed() {
		t.Log("PG_PROXY_REGRESS_UPDATE=1 rewrites the file from this run's results")
	}
}

// readBaseline returns the leading comment block of path and its test names
// mapped to their reasons. Each entry is a test name with an optional
// "# reason"; blank lines and comment-only lines are ignored.
func readBaseline(t *testing.T, path string) (string, map[string]string) {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var header strings.Builder
	inHeader := true
	expected := map[string]string{}
	for _, line := range strings.Split(string(data), "\n") {
		entry, reason, _ := strings.Cut(line, "#")
		entry = strings.TrimSpace(entry)
		if entry == "" {
			if inHeader && strings.TrimSpace(line) != "" {
				header.WriteString(line + "\n")
			}
			continue
		}
		inHeader = false
		expected[entry] = strings.TrimSpace(reason)
	}
	return header.String(), expected
}
