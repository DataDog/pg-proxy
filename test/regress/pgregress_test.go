// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

//go:build integration && regress

package regress

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"maps"
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

// scheduleFile is where the runner receives the (filtered) schedule.
const scheduleFile = "/tmp/pg-proxy-schedule"

var (
	// tapResult matches a TAP result line (PostgreSQL 16 and later), such as
	// "not ok 12    + int8    25 ms".
	tapResult = regexp.MustCompile(`^(ok|not ok)\s+\d+\s+[-+]\s+(\S+)`)
	// plainResult matches an older result line, such as
	// "test int8    ... FAILED   25 ms" or "     int8    ... ok   25 ms".
	plainResult = regexp.MustCompile(`^(?:test\s+|\s+)(\S+)\s+\.\.\.\s+(ok|FAILED|failed \(ignored\))`)
	// diffHeader starts the section of regression.diffs for one test.
	diffHeader = regexp.MustCompile(`^diff .* \S*/results/(\S+)\.out$`)
)

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
	for {
		state, err := runner.State(ctx)
		require.NoError(t, err, "pg_regress did not finish")
		if !state.Running {
			break
		}
		time.Sleep(time.Second)
	}

	dir := filepath.Join(artifactsDir(t), label)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	output := saveContainerFile(t, runner, "", filepath.Join(dir, "output.txt"))
	diffs := saveContainerFile(t, runner, regressDir+"/regression.diffs", filepath.Join(dir, "regression.diffs"))

	parsed := results{}
	scanner := bufio.NewScanner(strings.NewReader(output))
	for scanner.Scan() {
		if match := tapResult.FindStringSubmatch(scanner.Text()); match != nil {
			parsed[match[2]] = match[1] == "ok"
		} else if match := plainResult.FindStringSubmatch(scanner.Text()); match != nil {
			parsed[match[1]] = match[2] != "FAILED"
		}
	}
	t.Logf("PostgreSQL %s: %d tests, %d failed; output and diffs in %s", v.Version, len(parsed), len(parsed.failed()), dir)
	if len(parsed) == 0 {
		t.Fatalf("pg_regress reported no tests:\n%s", output)
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

// baseline is a parsed baseline/pg<major>.txt. Each entry is
// "skip <test> # reason" for a test left out of the schedule because pg-proxy
// cannot pass it by design, or "fail <test> # reason" for a test that runs and
// is expected to fail. Comment lines at the top form the header.
type baseline struct {
	header  string
	skipped map[string]string
	failing map[string]string
}

func readBaseline(t *testing.T, path string) baseline {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	b := baseline{skipped: map[string]string{}, failing: map[string]string{}}
	inHeader := true
	for number, line := range strings.Split(string(data), "\n") {
		entry, reason, _ := strings.Cut(line, "#")
		fields := strings.Fields(entry)
		if len(fields) == 0 {
			if inHeader && strings.TrimSpace(line) != "" {
				b.header += line + "\n"
			}
			continue
		}
		inHeader = false
		require.Len(t, fields, 2, "%s:%d: want \"skip|fail <test> # reason\"", path, number+1)
		switch fields[0] {
		case "skip":
			b.skipped[fields[1]] = strings.TrimSpace(reason)
		case "fail":
			b.failing[fields[1]] = strings.TrimSpace(reason)
		default:
			t.Fatalf("%s:%d: unknown entry kind %q", path, number+1, fields[0])
		}
	}
	return b
}

// checkBaseline fails t when run differs from the expected failures: a test
// failing that is not listed, or a listed test that passes or no longer exists.
// With PG_PROXY_REGRESS_UPDATE set it rewrites the fail entries of path from
// run instead, keeping the header, the skip entries and existing reasons.
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

	if os.Getenv("PG_PROXY_REGRESS_UPDATE") != "" {
		var out strings.Builder
		out.WriteString(b.header)
		writeEntries := func(kind string, names []string, reasons map[string]string) {
			for _, name := range names {
				reason := reasons[name]
				if reason == "" {
					reason = "not triaged"
				}
				fmt.Fprintf(&out, "%s %-28s # %s\n", kind, name, reason)
			}
		}
		skipped := make([]string, 0, len(b.skipped))
		for name := range b.skipped {
			skipped = append(skipped, name)
		}
		slices.Sort(skipped)
		writeEntries("skip", skipped, b.skipped)
		writeEntries("fail", run.results.failed(), b.failing)
		require.NoError(t, os.WriteFile(path, []byte(out.String()), 0o644))
		t.Logf("updated %s: %d new failures, %d new passes, %d removed", path, len(newFailures), len(newPasses), len(unknown))
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
	if t.Failed() {
		t.Log("PG_PROXY_REGRESS_UPDATE=1 rewrites the fail entries from this run's results")
	}
}
