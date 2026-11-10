// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

// Command ci-gate verifies that every job in CI is required and has succeeded.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"slices"

	"gopkg.in/yaml.v3"
)

const workflowPath = ".github/workflows/ci.yml"

type workflow struct {
	Jobs map[string]struct {
		Name  string   `yaml:"name"`
		Needs []string `yaml:"needs"`
	} `yaml:"jobs"`
}

type jobResult struct {
	Result string `json:"result"`
}

func checkGate(source, results []byte) error {
	var config workflow
	if err := yaml.Unmarshal(source, &config); err != nil {
		return fmt.Errorf("parse workflow: %w", err)
	}
	gate, ok := config.Jobs["ci-gate"]
	if !ok || gate.Name != "CI gate" {
		return fmt.Errorf("workflow must define ci-gate with the stable name CI gate")
	}

	expected := make([]string, 0, len(config.Jobs)-1)
	for id := range config.Jobs {
		if id != "ci-gate" {
			expected = append(expected, id)
		}
	}
	slices.Sort(expected)
	slices.Sort(gate.Needs)
	if len(expected) == 0 || !slices.Equal(expected, gate.Needs) {
		return fmt.Errorf("ci-gate.needs must list every other job exactly once: want %v, got %v", expected, gate.Needs)
	}

	var completed map[string]jobResult
	if err := json.Unmarshal(results, &completed); err != nil {
		return fmt.Errorf("parse CI_JOB_RESULTS: %w", err)
	}
	actual := make([]string, 0, len(completed))
	for id := range completed {
		actual = append(actual, id)
	}
	slices.Sort(actual)
	if !slices.Equal(expected, actual) {
		return fmt.Errorf("CI_JOB_RESULTS must contain every dependency: want %v, got %v", expected, actual)
	}
	for _, id := range expected {
		// Skipped and cancelled jobs must not silently satisfy the required check.
		if completed[id].Result != "success" {
			return fmt.Errorf("job %s did not succeed: %q", id, completed[id].Result)
		}
	}
	return nil
}

func main() {
	source, err := os.ReadFile(workflowPath)
	if err == nil {
		err = checkGate(source, []byte(os.Getenv("CI_JOB_RESULTS")))
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "CI gate failed:", err)
		os.Exit(1)
	}
	fmt.Println("All CI jobs are dependencies of CI gate and succeeded.")
}
