// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package setdetect

import (
	"bufio"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

var (
	// psqlInlineMeta ends a query with a psql command such as \gset.
	psqlInlineMeta = regexp.MustCompile(`\s\\(?:g|gx|gset|gexec|gdesc|crosstabview|bind|sendpipeline)\b[^\n]*$`)
	copyFromStdin  = regexp.MustCompile(`(?i)\bfrom\s+stdin\b[^;]*;\s*$`)
	// psqlVar matches :name, :'name' and :"name" interpolation, but not ::casts.
	psqlVar = regexp.MustCompile(`(?:^|[^:]):['"]?[A-Za-z_][A-Za-z0-9_]*['"]?`)
)

// RegressStatements splits a pg_regress script into the statements psql
// would send, dropping psql meta-commands, COPY FROM STDIN data, and
// statements that interpolate psql variables (their text is unknowable here).
// The result is in script order; indexes into it identify a statement.
func RegressStatements(script string) []string {
	var b strings.Builder
	inCopy := false
	sc := bufio.NewScanner(strings.NewReader(script))
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		line := sc.Text()
		if inCopy {
			if strings.TrimSpace(line) == `\.` {
				inCopy = false
			}
			continue
		}
		if strings.HasPrefix(strings.TrimSpace(line), `\`) {
			b.WriteString(";\n")
			continue
		}
		if psqlInlineMeta.MatchString(line) {
			line = psqlInlineMeta.ReplaceAllString(line, ";")
		}
		b.WriteString(line)
		b.WriteByte('\n')
		if copyFromStdin.MatchString(line) {
			inCopy = true
		}
	}
	parts, err := pg_query.SplitWithScanner(b.String(), true)
	if err != nil {
		return nil
	}
	out := parts[:0]
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" || p == ";" {
			continue
		}
		out = append(out, p)
	}
	return out
}

// UsesPsqlVar reports whether stmt interpolates a psql variable.
func UsesPsqlVar(stmt string) bool { return psqlVar.MatchString(stmt) }

// RegressSchedule returns the test file names in parallel_schedule order.
func RegressSchedule(regressDir string) ([]string, error) {
	data, err := os.ReadFile(filepath.Join(regressDir, "parallel_schedule"))
	if err != nil {
		return nil, err
	}
	var names []string
	for _, line := range strings.Split(string(data), "\n") {
		rest, ok := strings.CutPrefix(line, "test:")
		if !ok {
			continue
		}
		names = append(names, strings.Fields(rest)...)
	}
	return names, nil
}
