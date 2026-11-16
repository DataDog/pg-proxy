// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// overlayPath is the instrumentation file, relative to the repository root.
const overlayPath = "test/perf/pgbench/overlay/benchmetrics.go"

// variant is one pg-proxy build under test.
type variant struct {
	Label  string `json:"label"`
	Spec   string `json:"spec"`
	Commit string `json:"commit"`
	Binary string `json:"-"`

	worktree string
	repo     string
}

func (v *variant) cleanup() {
	if v.worktree != "" {
		_ = exec.Command("git", "-C", v.repo, "worktree", "remove", "--force", v.worktree).Run()
		v.worktree = ""
	}
}

// prepareVariant builds spec, a git ref or "." for the working tree. Refs are
// checked out into a temporary worktree and built the same way as ".".
func prepareVariant(ctx context.Context, label, spec, out string) (*variant, error) {
	repo, err := gitOutput(ctx, ".", "rev-parse", "--show-toplevel")
	if err != nil {
		return nil, err
	}
	v := &variant{Label: label, Spec: spec, repo: repo, Binary: filepath.Join(out, "bin", "pg-proxy-"+label)}
	src := repo
	if spec == "." {
		v.Commit, err = gitOutput(ctx, repo, "rev-parse", "HEAD")
	} else {
		v.Commit, err = gitOutput(ctx, repo, "rev-parse", "--verify", spec+"^{commit}")
		if err == nil {
			src = filepath.Join(out, "src-"+label)
			_, err = gitOutput(ctx, repo, "worktree", "add", "--detach", src, v.Commit)
			v.worktree = src
		}
	}
	if err != nil {
		return v, fmt.Errorf("resolve %q: %w", spec, err)
	}
	return v, buildInstrumented(ctx, filepath.Join(repo, overlayPath), src, v.Binary, out, label)
}

// buildInstrumented builds src/cmd/pg-proxy with the overlay file added to it.
// The source tree is not modified, so any ref can be built the same way.
// -trimpath and -buildvcs=false make identical sources give identical binaries.
func buildInstrumented(ctx context.Context, overlay, src, binary, out, label string) error {
	target := filepath.Join(src, "cmd", "pg-proxy", "zz_pgproxy_bench.go")
	spec, err := json.Marshal(map[string]map[string]string{"Replace": {target: overlay}})
	if err != nil {
		return err
	}
	overlayJSON := filepath.Join(out, "overlay-"+label+".json")
	if err := os.WriteFile(overlayJSON, spec, 0o644); err != nil {
		return err
	}
	logf("building %s", binary)
	cmd := exec.CommandContext(ctx, "go", "build", "-trimpath", "-buildvcs=false", "-tags", "pgproxy_bench",
		"-overlay", overlayJSON, "-o", binary, "./cmd/pg-proxy")
	cmd.Dir = src
	cmd.Env = append(os.Environ(), "GOWORK=off", "GOFLAGS=-mod=readonly")
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("go build in %s: %w\n%s", src, err, output)
	}
	return nil
}

func gitOutput(ctx context.Context, dir string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", dir}, args...)...)
	output, err := cmd.Output()
	if err != nil {
		if exit, ok := err.(*exec.ExitError); ok {
			return "", fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(exit.Stderr)))
		}
		return "", err
	}
	return strings.TrimSpace(string(output)), nil
}
