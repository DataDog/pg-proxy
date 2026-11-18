// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package pgproxy

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const bareMetricPrefix = "pg_proxy."

var metricMethods = map[string]bool{
	"Count": true, "Incr": true, "Gauge": true, "Distribution": true, "Timing": true,
}

// TestMetricNamesAreBare fails if any code passes a metric name that already
// carries a namespace. The namespace is applied exactly once, by the client
// Bootstrap installs, so a prefixed name would be namespaced twice.
//
// It checks two things across the module:
//   - In library code, every string literal containing "pg_proxy." starts with
//     it, which covers names held in constants and variables.
//   - In tests, a string literal passed as the first
//     argument to Count, Incr, Gauge, Distribution, or Timing (alone or at the
//     start of a concatenation) starts with "pg_proxy." when it names a
//     pg_proxy metric.
func TestMetricNamesAreBare(t *testing.T) {
	var violations []string
	fset := token.NewFileSet()
	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path != "." && strings.HasPrefix(d.Name(), ".") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		isTest := strings.HasSuffix(path, "_test.go")
		report := func(lit *ast.BasicLit, value string) {
			violations = append(violations, fset.Position(lit.Pos()).String()+": "+strconv.Quote(value))
		}
		ast.Inspect(file, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.BasicLit:
				if isTest || node.Kind != token.STRING {
					return true
				}
				if value, ok := stringValue(node); ok && prefixedMetricName(value) {
					report(node, value)
				}
			case *ast.CallExpr:
				// Library literals are already covered by the case above.
				sel, ok := node.Fun.(*ast.SelectorExpr)
				if !isTest || !ok || !metricMethods[sel.Sel.Name] || len(node.Args) == 0 {
					return true
				}
				if lit := leadingLiteral(node.Args[0]); lit != nil {
					if value, ok := stringValue(lit); ok && prefixedMetricName(value) {
						report(lit, value)
					}
				}
			}
			return true
		})
		return nil
	})
	require.NoError(t, err)
	require.Empty(t, violations,
		"metric names must be bare (%q…); the namespace is added once by the metrics client", bareMetricPrefix)
}

// prefixedMetricName reports whether value names a pg_proxy metric with
// something in front of "pg_proxy.".
func prefixedMetricName(value string) bool {
	i := strings.Index(value, bareMetricPrefix)
	return i > 0 && !strings.ContainsAny(value[:i], " \t\n\\")
}

func leadingLiteral(expr ast.Expr) *ast.BasicLit {
	for {
		switch e := expr.(type) {
		case *ast.BasicLit:
			if e.Kind == token.STRING {
				return e
			}
			return nil
		case *ast.BinaryExpr:
			if e.Op != token.ADD {
				return nil
			}
			expr = e.X
		case *ast.ParenExpr:
			expr = e.X
		default:
			return nil
		}
	}
}

func stringValue(lit *ast.BasicLit) (string, bool) {
	value, err := strconv.Unquote(lit.Value)
	return value, err == nil
}

func TestPrefixedMetricName(t *testing.T) {
	for value, want := range map[string]bool{
		"pg_proxy.sessions":           false,
		"datadog.pg_proxy.sessions":   true,
		"acme.db.pg_proxy.sessions":   true,
		"pg_pg.pg_proxy.sessions":     true,
		"-c pg_proxy.option":          false, // prose, not a metric name
		`-c pg_proxy\.(\w+)=([\w$]+)`: false,
		"unrelated":                   false,
	} {
		require.Equal(t, want, prefixedMetricName(value), value)
	}
}
