// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

//go:build integration

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// overlayPath is the instrumentation file, relative to the repository root.
const overlayPath = "test/perf/pgbench/overlay/benchmetrics.go"

// variant is one pg-proxy binary under test.
type variant struct {
	Label string `json:"label"`
	Spec  string `json:"spec"`
	// Commit is the resolved commit for git refs and the working tree.
	Commit string `json:"commit,omitempty"`
	// Dirty reports uncommitted changes when Spec is ".".
	Dirty  bool   `json:"dirty,omitempty"`
	Binary string `json:"binary"`
	SHA256 string `json:"sha256"`
	// Instrumented reports a build with the runtime metrics overlay. Prebuilt
	// binaries are probed at runtime and may still expose it.
	Instrumented bool `json:"instrumented"`

	worktree string
	repo     string
}

func (v *variant) describe() string {
	if v.Commit == "" {
		return "prebuilt " + v.Binary
	}
	s := v.Commit[:min(12, len(v.Commit))]
	if v.Dirty {
		s += "+dirty"
	}
	return s
}

func (v *variant) cleanup() {
	if v.worktree != "" {
		_ = exec.Command("git", "-C", v.repo, "worktree", "remove", "--force", v.worktree).Run()
		v.worktree = ""
	}
}

// prepareVariant resolves spec to a binary. Git refs are checked out into a
// temporary worktree and built with the same toolchain and flags as ".".
func prepareVariant(ctx context.Context, label, spec, out string) (*variant, error) {
	v := &variant{Label: label, Spec: spec}
	if info, err := os.Stat(spec); err == nil && info.Mode().IsRegular() {
		if info.Mode()&0o111 == 0 {
			return nil, fmt.Errorf("%s is not executable", spec)
		}
		abs, err := filepath.Abs(spec)
		if err != nil {
			return nil, err
		}
		v.Binary = abs
		v.SHA256, err = fileSHA256(abs)
		return v, err
	}
	repo, err := gitOutput(ctx, ".", "rev-parse", "--show-toplevel")
	if err != nil {
		return nil, fmt.Errorf("%q is neither a binary nor resolvable in a git checkout: %w", spec, err)
	}
	v.repo = repo
	src := repo
	if spec == "." {
		if v.Commit, err = gitOutput(ctx, repo, "rev-parse", "HEAD"); err != nil {
			return nil, err
		}
		status, err := gitOutput(ctx, repo, "status", "--porcelain", "--untracked-files=no")
		if err != nil {
			return nil, err
		}
		v.Dirty = status != ""
	} else {
		if v.Commit, err = gitOutput(ctx, repo, "rev-parse", "--verify", spec+"^{commit}"); err != nil {
			return nil, fmt.Errorf("resolve git ref %q: %w", spec, err)
		}
		src = filepath.Join(out, "src-"+label)
		if _, err := gitOutput(ctx, repo, "worktree", "add", "--detach", src, v.Commit); err != nil {
			return nil, err
		}
		v.worktree = src
	}
	v.Binary = filepath.Join(out, "bin", "pg-proxy-"+label)
	if err := buildInstrumented(ctx, filepath.Join(repo, overlayPath), src, v.Binary, out, label); err != nil {
		return v, err
	}
	v.Instrumented = true
	v.SHA256, err = fileSHA256(v.Binary)
	return v, err
}

// buildInstrumented builds src/cmd/pg-proxy with the overlay file added to it.
// The source tree is not modified, so any ref can be built the same way.
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

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
