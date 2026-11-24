// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package regress

// This file has no build tags, so `go test ./...` checks baseline parsing and
// fingerprinting without Docker.

import (
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// diffHeader starts the section of regression.diffs for one test.
var diffHeader = regexp.MustCompile(`^diff .* \S*/results/(\S+)\.out$`)

// baseline is a parsed baseline/pg<major>.txt. Each entry is one of:
//   - "skip <test> # reason": left out of the schedule because pg-proxy cannot
//     pass it by design.
//   - "isolate <test> # reason": left out of the schedule because its failure
//     would break later tests. Not established as by design; worth re-checking
//     as pg-proxy improves.
//   - "fail <test> diff=<lines> # reason": runs and is expected to fail, with
//     <lines> lines of its output differing from PostgreSQL's expected output.
//
// Comment lines at the top form the header.
type baseline struct {
	header   string
	skipped  map[string]string
	isolated map[string]string
	failing  map[string]failEntry
}

// failEntry is an expected failure and its fingerprint: the number of changed
// lines in its section of regression.diffs.
type failEntry struct {
	reason       string
	changedLines int
}

// excluded returns the tests left out of the schedule: skipped and isolated.
func (b baseline) excluded() map[string]string {
	excluded := maps.Clone(b.skipped)
	maps.Copy(excluded, b.isolated)
	return excluded
}

func parseBaseline(path, data string) (baseline, error) {
	b := baseline{skipped: map[string]string{}, isolated: map[string]string{}, failing: map[string]failEntry{}}
	inHeader := true
	for number, line := range strings.Split(data, "\n") {
		entry, reason, _ := strings.Cut(line, "#")
		reason = strings.TrimSpace(reason)
		fields := strings.Fields(entry)
		if len(fields) == 0 {
			if inHeader && strings.TrimSpace(line) != "" {
				b.header += line + "\n"
			}
			continue
		}
		inHeader = false
		at := fmt.Sprintf("%s:%d", path, number+1)
		if len(fields) < 2 {
			return baseline{}, fmt.Errorf(`%s: want "skip|isolate <test> # reason" or "fail <test> diff=<lines> # reason"`, at)
		}
		kind, name := fields[0], fields[1]
		_, excluded := b.excluded()[name]
		_, failing := b.failing[name]
		if excluded || failing {
			return baseline{}, fmt.Errorf("%s: %s is listed twice", at, name)
		}
		switch kind {
		case "skip", "isolate":
			if len(fields) != 2 {
				return baseline{}, fmt.Errorf(`%s: want "%s <test> # reason"`, at, kind)
			}
			if kind == "skip" {
				b.skipped[name] = reason
			} else {
				b.isolated[name] = reason
			}
		case "fail":
			count, found := "", false
			if len(fields) == 3 {
				count, found = strings.CutPrefix(fields[2], "diff=")
			}
			lines, err := strconv.Atoi(count)
			if !found || err != nil || lines < 0 {
				return baseline{}, fmt.Errorf(`%s: want "fail <test> diff=<lines> # reason"; PG_PROXY_REGRESS_UPDATE=1 writes the counts`, at)
			}
			b.failing[name] = failEntry{reason: reason, changedLines: lines}
		default:
			return baseline{}, fmt.Errorf("%s: unknown entry kind %q", at, kind)
		}
	}
	return b, nil
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

// changedLines counts the removed and added lines of a unified diff section.
// A section covers one file, so its "---" and "+++" file headers come before
// the first hunk. Inside hunks every line starts with " ", "-", "+" or "\",
// so a line starting with "@@" is always a hunk header. Removed or added lines
// that look like file headers, such as an echoed "-- comment", are counted.
func changedLines(section string) int {
	count := 0
	inHunks := false
	for _, line := range strings.Split(section, "\n") {
		if strings.HasPrefix(line, "@@") {
			inHunks = true
			continue
		}
		if inHunks && (strings.HasPrefix(line, "-") || strings.HasPrefix(line, "+")) {
			count++
		}
	}
	return count
}

// fingerprintChanges compares each listed failure's diff size with its baseline
// entry. worse lists tests that differ from PostgreSQL in more lines than
// recorded, better those that differ in fewer. Tests the baseline does not list
// are reported separately by the caller.
func fingerprintChanges(b baseline, failed []string, diffs map[string]string) (worse, better []string) {
	for _, name := range failed {
		entry, listed := b.failing[name]
		if !listed {
			continue
		}
		got := changedLines(diffs[name])
		change := fmt.Sprintf("%s (%d -> %d lines)", name, entry.changedLines, got)
		switch {
		case got > entry.changedLines:
			worse = append(worse, change)
		case got < entry.changedLines:
			better = append(better, change)
		}
	}
	return worse, better
}

// changedNames returns the test names of fingerprintChanges results.
func changedNames(changes []string) []string {
	names := make([]string, 0, len(changes))
	for _, change := range changes {
		name, _, _ := strings.Cut(change, " ")
		names = append(names, name)
	}
	return names
}

// formatBaseline writes b with fail entries for failed and their current diff
// sizes, keeping the header, skip and isolate entries and existing reasons.
func formatBaseline(b baseline, failed []string, diffs map[string]string) string {
	var out strings.Builder
	out.WriteString(b.header)
	for _, kind := range []struct {
		name    string
		reasons map[string]string
	}{{"skip", b.skipped}, {"isolate", b.isolated}} {
		for _, name := range slices.Sorted(maps.Keys(kind.reasons)) {
			fmt.Fprintf(&out, "%s %-28s # %s\n", kind.name, name, orNotTriaged(kind.reasons[name]))
		}
	}
	for _, name := range slices.Sorted(slices.Values(failed)) {
		fmt.Fprintf(&out, "fail %-28s diff=%-5d # %s\n", name, changedLines(diffs[name]), orNotTriaged(b.failing[name].reason))
	}
	return out.String()
}

func orNotTriaged(reason string) string {
	if reason == "" {
		return "not triaged"
	}
	return reason
}

const sampleDiffs = `diff -U3 /regress/expected/insert.out /regress/results/insert.out
--- /regress/expected/insert.out	2026-08-10 20:59:18 +0000
+++ /regress/results/insert.out	2026-11-18 12:18:41 +0000
@@ -1,3 +1,3 @@
 context
-ERROR:  permission denied for table t
+INSERT 0 1
 context
diff -U3 /regress/expected/lock.out /regress/results/lock.out
--- /regress/expected/lock.out	2026-08-10 20:59:18 +0000
+++ /regress/results/lock.out	2026-11-18 12:18:41 +0000
@@ -1,2 +1,1 @@
-ERROR:  permission denied for view v
 context
`

func TestParseBaseline(t *testing.T) {
	b, err := parseBaseline("pg18.txt", `# PostgreSQL 18 regression tests.
#   skip ...

skip largeobject                  # fastpath: large-object functions use FunctionCall
isolate aggregates                # backend-reuse: relation "t1" already exists
fail insert                       diff=2     # session-reset: role is reset
`)
	require.NoError(t, err)
	require.Equal(t, "# PostgreSQL 18 regression tests.\n#   skip ...\n", b.header)
	require.Equal(t, map[string]string{"largeobject": "fastpath: large-object functions use FunctionCall"}, b.skipped)
	require.Equal(t, map[string]string{"aggregates": `backend-reuse: relation "t1" already exists`}, b.isolated)
	require.Equal(t, map[string]failEntry{"insert": {reason: "session-reset: role is reset", changedLines: 2}}, b.failing)

	for name, data := range map[string]string{
		"missing count":    "fail insert # reason\n",
		"bad count":        "fail insert diff=two # reason\n",
		"count on skip":    "skip insert diff=2 # reason\n",
		"duplicate":        "skip insert # a\nfail insert diff=1 # b\n",
		"unknown kind":     "ignore insert # reason\n",
		"missing test":     "fail # reason\n",
		"negative count":   "fail insert diff=-1 # reason\n",
		"extra field":      "isolate insert extra # reason\n",
		"fail extra field": "fail insert diff=1 extra # reason\n",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := parseBaseline("pg18.txt", data)
			require.Error(t, err)
		})
	}
}

func TestFingerprintChanges(t *testing.T) {
	diffs := diffSections(sampleDiffs)
	require.Equal(t, 2, changedLines(diffs["insert"]))
	require.Equal(t, 1, changedLines(diffs["lock"]))

	b := baseline{failing: map[string]failEntry{"insert": {changedLines: 2}, "lock": {changedLines: 3}, "update": {changedLines: 1}}}
	worse, better := fingerprintChanges(b, []string{"insert", "lock"}, diffs)
	require.Empty(t, worse)
	require.Equal(t, []string{"lock (3 -> 1 lines)"}, better)

	b.failing["insert"] = failEntry{changedLines: 1}
	worse, _ = fingerprintChanges(b, []string{"insert"}, diffs)
	require.Equal(t, []string{"insert (1 -> 2 lines)"}, worse)
	require.Equal(t, []string{"insert"}, changedNames(worse))
}

// TestChangedLinesInsideHunks checks that removed and added lines that look
// like file headers are counted. Echoed SQL comments start with "--", so
// removing one produces a hunk line starting with "--- ".
func TestChangedLinesInsideHunks(t *testing.T) {
	const section = `diff -U3 /regress/expected/comments.out /regress/results/comments.out
--- /regress/expected/comments.out	2026-08-10 20:59:18 +0000
+++ /regress/results/comments.out	2026-11-18 12:18:41 +0000
@@ -1,3 +1,2 @@
 context
--- Test comment
-SELECT 1;
+++ added output
@@ -9,1 +8,1 @@
--- second hunk
++++ second hunk
`
	require.Equal(t, 5, changedLines(section))
}

func TestFormatBaselineRoundTrips(t *testing.T) {
	b := baseline{
		header:   "# header\n",
		skipped:  map[string]string{"largeobject": "fastpath"},
		isolated: map[string]string{"aggregates": "backend-reuse"},
		failing:  map[string]failEntry{"insert": {reason: "session-reset", changedLines: 9}},
	}
	formatted := formatBaseline(b, []string{"lock", "insert"}, diffSections(sampleDiffs))
	parsed, err := parseBaseline("pg18.txt", formatted)
	require.NoError(t, err)
	require.Equal(t, b.header, parsed.header)
	require.Equal(t, b.skipped, parsed.skipped)
	require.Equal(t, b.isolated, parsed.isolated)
	// The update records the current diff sizes and marks new entries for triage.
	require.Equal(t, map[string]failEntry{
		"insert": {reason: "session-reset", changedLines: 2},
		"lock":   {reason: "not triaged", changedLines: 1},
	}, parsed.failing)
}
