// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package main

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const exampleWorkflow = `jobs:
  ci-gate:
    name: CI gate
    needs: [test, build]
  build:
    name: Build
  test:
    name: Tests
`

const successfulResults = `{"build":{"result":"success"},"test":{"result":"success"}}`

func TestCheckGate(t *testing.T) {
	type testCase struct {
		name    string
		source  string
		results string
		wantErr string
	}
	tests := []testCase{
		{name: "all jobs succeed", source: exampleWorkflow, results: successfulResults},
		{
			name: "new standalone job", source: exampleWorkflow + "  lint: {}\n",
			results: successfulResults, wantErr: "must list every other job",
		},
		{
			name: "unknown dependency", source: strings.Replace(exampleWorkflow, "[test, build]", "[test, build, missing]", 1),
			results: successfulResults, wantErr: "must list every other job",
		},
		{
			name: "duplicate dependency", source: strings.Replace(exampleWorkflow, "[test, build]", "[test, build, test]", 1),
			results: successfulResults, wantErr: "must list every other job",
		},
		{
			name: "missing result", source: exampleWorkflow,
			results: `{"build":{"result":"success"}}`, wantErr: "must contain every dependency",
		},
		{
			name: "unexpected result", source: exampleWorkflow,
			results: `{"build":{"result":"success"},"test":{"result":"success"},"extra":{"result":"success"}}`,
			wantErr: "must contain every dependency",
		},
		{name: "empty results", source: exampleWorkflow, results: "", wantErr: "parse CI_JOB_RESULTS"},
		{name: "null results", source: exampleWorkflow, results: "null", wantErr: "must contain every dependency"},
		{name: "malformed results", source: exampleWorkflow, results: "{", wantErr: "parse CI_JOB_RESULTS"},
		{name: "malformed workflow", source: "jobs: [", results: successfulResults, wantErr: "parse workflow"},
		{name: "missing gate", source: "jobs: {}", results: successfulResults, wantErr: "stable name"},
		{
			name: "renamed check", source: strings.Replace(exampleWorkflow, "name: CI gate", "name: Something else", 1),
			results: successfulResults, wantErr: "stable name",
		},
		{
			name: "no child jobs", source: "jobs:\n  ci-gate:\n    name: CI gate\n    needs: []\n",
			results: "{}", wantErr: "must list every other job",
		},
	}
	for _, result := range []string{"failure", "cancelled", "skipped", ""} {
		tests = append(tests, testCase{
			name: "reject result " + result, source: exampleWorkflow,
			results: strings.Replace(successfulResults, `"test":{"result":"success"}`, `"test":{"result":"`+result+`"}`, 1),
			wantErr: "job test did not succeed",
		})
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkGate([]byte(tt.source), []byte(tt.results))
			if tt.wantErr == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("want error containing %q, got %v", tt.wantErr, err)
			}
		})
	}
}

func TestCheckedInWorkflow(t *testing.T) {
	source, err := os.ReadFile("../workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
	var config workflow
	if err := yaml.Unmarshal(source, &config); err != nil {
		t.Fatal(err)
	}
	results := make(map[string]jobResult)
	for id := range config.Jobs {
		if id != "ci-gate" {
			results[id] = jobResult{Result: "success"}
		}
	}
	encoded, err := json.Marshal(results)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkGate(source, encoded); err != nil {
		t.Fatal(err)
	}
	if err := checkGate(append(source, []byte("\n  forgotten-job: {}\n")...), encoded); err == nil {
		t.Fatal("adding a job without updating ci-gate.needs must fail")
	}
}
