// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

//go:build linux

package limiter

import (
	"golang.org/x/sys/unix"
)

// isCgroup2Unified reports whether root is a cgroup v2 unified hierarchy.
// It stats the mount and compares the filesystem type to CGROUP2_SUPER_MAGIC,
// the same check runtimex/cgroup uses.
func isCgroup2Unified(root string) (bool, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(root, &st); err != nil {
		return false, err
	}
	return st.Type == unix.CGROUP2_SUPER_MAGIC, nil
}
