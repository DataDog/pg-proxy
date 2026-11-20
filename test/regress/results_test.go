// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package regress

// This file has no build tags, so `go test ./...` checks result parsing without
// Docker. The suite itself needs the integration and regress tags.

import (
	"bufio"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	// tapResult matches a TAP result line (PostgreSQL 16 and later), such as
	// "not ok 12    + int8    25 ms".
	tapResult = regexp.MustCompile(`^(ok|not ok)\s+\d+\s+[-+]\s+(\S+)`)
	// plainResult matches an older result line, such as
	// "test int8    ... FAILED   25 ms" or "     int8    ... ok   25 ms".
	plainResult = regexp.MustCompile(`^(?:test\s+|\s+)(\S+)\s+\.\.\.\s+(ok|FAILED|failed \(ignored\))`)
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

// parseResults reads the result lines of pg_regress output. A test that
// pg_regress ignores ("failed (ignored)") counts as passing.
func parseResults(output string) results {
	parsed := results{}
	scanner := bufio.NewScanner(strings.NewReader(output))
	for scanner.Scan() {
		if match := tapResult.FindStringSubmatch(scanner.Text()); match != nil {
			parsed[match[2]] = match[1] == "ok"
		} else if match := plainResult.FindStringSubmatch(scanner.Text()); match != nil {
			parsed[match[1]] = match[2] != "FAILED"
		}
	}
	return parsed
}

// scheduledTests returns the tests a pg_regress schedule names, in order.
func scheduledTests(schedule string) []string {
	var names []string
	for line := range strings.Lines(schedule) {
		if tests, ok := strings.CutPrefix(line, "test:"); ok {
			names = append(names, strings.Fields(tests)...)
		}
	}
	return names
}

// missingResults returns the scheduled tests that have no result.
func missingResults(schedule string, parsed results) []string {
	var missing []string
	for _, name := range scheduledTests(schedule) {
		if _, ok := parsed[name]; !ok {
			missing = append(missing, name)
		}
	}
	return missing
}

func TestParseResults(t *testing.T) {
	tap := `# using postmaster on localhost, port 5432
ok 1         - test_setup                               25 ms
not ok 2     + int8                                     31 ms
ok 3         - sequence                                 12 ms
1..3
# 1 of 3 tests failed.
`
	require.Equal(t, results{"test_setup": true, "int8": false, "sequence": true}, parseResults(tap))

	plain := `============== running regression test queries        ==============
test test_setup                   ... ok           25 ms
parallel group (2 tests):  int8 float8
     int8                         ... FAILED       31 ms
     float8                       ... failed (ignored) 12 ms
`
	require.Equal(t, results{"test_setup": true, "int8": false, "float8": true}, parseResults(plain))
}

func TestMissingResults(t *testing.T) {
	schedule := "# comment\ntest: test_setup\ntest: int8 float8\n\ntest: sequence\n"
	require.Equal(t, []string{"test_setup", "int8", "float8", "sequence"}, scheduledTests(schedule))

	// pg_regress stopped after int8: float8 and sequence have no result.
	parsed := parseResults("ok 1 - test_setup 1 ms\nnot ok 2 + int8 1 ms\n")
	require.Equal(t, []string{"float8", "sequence"}, missingResults(schedule, parsed))

	all := parseResults("ok 1 - test_setup 1 ms\nok 2 + int8 1 ms\nok 3 + float8 1 ms\nok 4 - sequence 1 ms\n")
	require.Empty(t, missingResults(schedule, all))
}
