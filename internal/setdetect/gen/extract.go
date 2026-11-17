// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package main

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// SQL embedded in other projects' test code, recovered from string literals.

var (
	dqString     = regexp.MustCompile(`"((?:[^"\\\n]|\\.)*)"`)
	rustRaw      = regexp.MustCompile(`(?s)r(#*)"(.*?)"`)
	tripleDouble = regexp.MustCompile(`(?s)"""(.*?)"""`)
	tripleSingle = regexp.MustCompile(`(?s)'''(.*?)'''`)
	backtick     = regexp.MustCompile("(?s)`([^`]*)`")
	sqlStart     = regexp.MustCompile(`(?is)^\s*(?:(?:--[^\n]*\n|/\*.*?\*/)\s*)*(?:set|reset|select|show|begin|start|commit|rollback|do|update|insert|delete|with|discard|prepare|execute|explain|copy|call|create|alter|drop|values|listen|unlisten|notify|savepoint|release|declare|fetch|close|lock|truncate|vacuum|analyze|end|abort)\b`)
	printfVerb   = regexp.MustCompile(`%[-0-9.]*[sdiflu]|%lld|%zu|\{[a-z_0-9:]*\}`)
)

type literal struct {
	file string
	text string
}

// literalsIn returns SQL-looking literals that parse, from files under root
// whose names match one of the globs.
func literalsIn(root string, globs []string) ([]literal, error) {
	var files []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if n := d.Name(); n == "target" || n == "node_modules" || n == ".git" || n == "vendor" {
				return filepath.SkipDir
			}
			return nil
		}
		for _, g := range globs {
			if ok, _ := filepath.Match(g, d.Name()); ok {
				files = append(files, path)
				break
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	var out []literal
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}
		rel, _ := filepath.Rel(root, f)
		seen := map[string]bool{}
		for _, s := range rawLiterals(string(data), filepath.Ext(f)) {
			s = strings.TrimSpace(printfVerb.ReplaceAllString(s, "x"))
			if s == "" || seen[s] || !sqlStart.MatchString(s) || len(s) > 20000 {
				continue
			}
			if _, err := pg_query.Parse(s); err != nil {
				continue
			}
			seen[s] = true
			out = append(out, literal{file: rel, text: s})
		}
	}
	return out, nil
}

func rawLiterals(src, ext string) []string {
	var out []string
	add := func(re *regexp.Regexp, group int, unescape bool) {
		for _, m := range re.FindAllStringSubmatch(src, -1) {
			s := m[group]
			if unescape {
				s = cUnescape(s)
			}
			out = append(out, s)
		}
	}
	switch ext {
	case ".sql":
		// Whole SQL files are split instead.
		parts, err := pg_query.SplitWithScanner(src, true)
		if err == nil {
			out = append(out, parts...)
		}
		return out
	case ".rs":
		add(rustRaw, 2, false)
	case ".py":
		add(tripleDouble, 1, false)
		add(tripleSingle, 1, false)
	case ".go":
		add(backtick, 1, false)
	}
	add(dqString, 1, true)
	return out
}

func cUnescape(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c != '\\' || i+1 == len(s) {
			b.WriteByte(c)
			continue
		}
		i++
		switch s[i] {
		case 'n':
			b.WriteByte('\n')
		case 't':
			b.WriteByte('\t')
		case 'r':
			b.WriteByte('\r')
		default:
			b.WriteByte(s[i])
		}
	}
	return b.String()
}
