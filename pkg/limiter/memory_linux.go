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
