// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package pgproxy

import (
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type documentationYAMLBlock struct {
	text string
	line int
}

// Track all fenced languages so text inside other code examples is ignored.
// Preserve line offsets and remove the fence's indent for list-nested examples.
func documentationYAMLBlocks(markdown string) []documentationYAMLBlock {
	var blocks []documentationYAMLBlock
	var fence, indent string
	var body []string
	start, isYAML := 0, false
	for i, line := range strings.Split(markdown, "\n") {
		trimmed := strings.TrimSpace(line)
		if fence != "" {
			if strings.HasPrefix(trimmed, fence) && strings.Trim(trimmed, string(fence[0])) == "" {
				if isYAML {
					blocks = append(blocks, documentationYAMLBlock{strings.Join(body, "\n"), start})
				}
				fence, body = "", nil
			} else if isYAML {
				body = append(body, strings.TrimPrefix(line, indent))
			}
			continue
		}
		if !strings.HasPrefix(trimmed, "```") && !strings.HasPrefix(trimmed, "~~~") {
			continue
		}
		length := 0
		for length < len(trimmed) && trimmed[length] == trimmed[0] {
			length++
		}
		fence = trimmed[:length]
		indent = line[:len(line)-len(strings.TrimLeft(line, " \t"))]
		info := strings.Fields(trimmed[length:])
		isYAML = len(info) > 0 && (info[0] == "yaml" || info[0] == "yml")
		start = i + 2
	}
	return blocks
}

func inlineYAMLCollectionLines(text string) ([]int, error) {
	decoder := yaml.NewDecoder(strings.NewReader(text))
	var lines []int
	var visit func(*yaml.Node)
	visit = func(node *yaml.Node) {
		// Empty collections need {} or []; aliases point at nodes already visited
		// through their anchors and must not be expanded recursively.
		if node.Style&yaml.FlowStyle != 0 && len(node.Content) > 0 && (node.Kind == yaml.MappingNode || node.Kind == yaml.SequenceNode) {
			lines = append(lines, node.Line)
		}
		for _, child := range node.Content {
			visit(child)
		}
	}
	for {
		var document yaml.Node
		if err := decoder.Decode(&document); err != nil {
			if err == io.EOF {
				return lines, nil
			}
			return nil, err
		}
		visit(&document)
	}
}

func TestDocumentationYAMLStyle(t *testing.T) {
	var paths []string
	rootMarkdown, err := filepath.Glob("*.md")
	if err != nil {
		t.Fatal(err)
	}
	paths = append(paths, rootMarkdown...)
	for _, root := range []string{"docs", "examples", "pkg", "cmd"} {
		err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				if strings.HasPrefix(entry.Name(), ".") || entry.Name() == "node_modules" || entry.Name() == "dist" || entry.Name() == "dist-static" {
					return filepath.SkipDir
				}
				return nil
			}
			switch filepath.Ext(path) {
			case ".md", ".mdx", ".yaml", ".yml":
				paths = append(paths, path)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	checked := 0
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		blocks := []documentationYAMLBlock{{string(data), 1}}
		if ext := filepath.Ext(path); ext == ".md" || ext == ".mdx" {
			blocks = documentationYAMLBlocks(string(data))
		}
		for _, block := range blocks {
			checked++
			lines, err := inlineYAMLCollectionLines(block.text)
			if err != nil {
				t.Errorf("%s:%d: invalid YAML example: %v", path, block.line, err)
				continue
			}
			for _, line := range lines {
				t.Errorf("%s:%d: use block-style YAML mappings and lists; put each entry on its own line", path, block.line+line-1)
			}
		}
	}
	if checked == 0 {
		t.Fatal("no YAML documentation examples found")
	}
}

func TestDocumentationYAMLStyleDetection(t *testing.T) {
	for _, tc := range []struct {
		name, text string
		violations int
	}{
		{"block style", "clusters:\n  orders:\n    primary: orders-primary\nusers:\n  - alice\n", 0},
		{"inline mapping", "clusters:\n  orders: {primary: orders-primary}\n", 1},
		{"inline list", "databases: [orders, reports]\n", 1},
		{"nested inline mapping", "databases:\n  - connection_options: {statement_timeout: 5s}\n", 1},
		{"empty collections", "clusters: {}\nusers: []\n", 0},
		{"braces in strings", "endpoint: '${env:ENDPOINT}'\npattern: '[a-z]'\n", 0},
		{"multiple documents", "users: []\n---\nusers: [alice]\n", 1},
		{"recursive alias", "users: &users\n  - *users\n", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lines, err := inlineYAMLCollectionLines(tc.text)
			if err != nil {
				t.Fatal(err)
			}
			if len(lines) != tc.violations {
				t.Fatalf("got %v, want %d inline collections", lines, tc.violations)
			}
		})
	}
	blocks := documentationYAMLBlocks("```go\nvar x = 1\n```\n  ~~~yml\n  users: [alice]\n  ~~~\n```yaml title=example\nusers:\n  - alice\n```\n")
	if len(blocks) != 2 || blocks[0].line != 5 || blocks[0].text != "users: [alice]" || blocks[1].line != 8 {
		t.Fatalf("incorrect YAML fences: %+v", blocks)
	}
}
