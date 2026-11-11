// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package limiter

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	defaultCgroupRoot = "/sys/fs/cgroup"
	defaultProcCgroup = "/proc/self/cgroup"

	// v1UnsetSentinel is the kernel's "unlimited" value for
	// memory.limit_in_bytes on cgroup v1.
	v1UnsetSentinel = 0x7FFFFFFFFFFFF000
)

// Memory is the process cgroup hard memory cap. Available is false when the
// cgroup has no hard limit or the limit could not be read.
type Memory struct {
	Bytes     int64
	Available bool
}

// ReadHardLimit returns the process cgroup hard memory cap.
// Hierarchy is detected with statfs (cgroup2 super magic vs v1 tmpfs layout);
// then the matching limit file is read: v2 memory.max (including a nested path
// from /proc/self/cgroup), or v1 memory.limit_in_bytes. An unavailable result
// is used both for an unlimited cgroup and for a read failure.
func ReadHardLimit() Memory {
	memory, _ := readDefaultHardLimit()
	return memory
}

func readDefaultHardLimit() (Memory, bool) {
	unifiedV2, err := isCgroup2Unified(defaultCgroupRoot)
	if err != nil {
		return Memory{}, false
	}
	return readHardLimit(defaultCgroupRoot, defaultProcCgroup, unifiedV2)
}

// readHardLimit reports ok for both a finite limit and a valid unlimited
// cgroup. A false result means the limit could not be read.
func readHardLimit(cgroupRoot, procCgroup string, unifiedV2 bool) (Memory, bool) {
	if unifiedV2 {
		v2Dir, ok := resolveV2Dir(cgroupRoot, procCgroup)
		if !ok {
			return Memory{}, false
		}
		memory, err := limitFromFile(filepath.Join(v2Dir, "memory.max"), true)
		if !errors.Is(err, fs.ErrNotExist) {
			return memory, err == nil
		}
		if v2Dir != cgroupRoot {
			memory, err = limitFromFile(filepath.Join(cgroupRoot, "memory.max"), true)
			if !errors.Is(err, fs.ErrNotExist) {
				return memory, err == nil
			}
		}
		return Memory{}, false
	}
	memory, err := limitFromFile(filepath.Join(cgroupRoot, "memory", "memory.limit_in_bytes"), false)
	return memory, err == nil
}

func resolveV2Dir(cgroupRoot, procCgroup string) (string, bool) {
	data, err := os.ReadFile(procCgroup)
	if err != nil {
		return "", false
	}
	for _, line := range strings.Split(string(data), "\n") {
		if p, found := strings.CutPrefix(line, "0::"); found {
			return filepath.Join(cgroupRoot, p), true
		}
	}
	return "", false
}

// limitFromFile returns an unavailable Memory with no error when the cgroup is
// explicitly unlimited. Missing files are returned as fs.ErrNotExist so the
// caller can try the next candidate; all other errors are terminal.
func limitFromFile(path string, v2 bool) (Memory, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Memory{}, err
	}
	s := strings.TrimSpace(string(b))
	if v2 && s == "max" {
		return Memory{}, nil
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return Memory{}, err
	}
	if n <= 0 {
		return Memory{}, strconv.ErrRange
	}
	if !v2 && n == v1UnsetSentinel {
		return Memory{}, nil
	}
	return Memory{Bytes: n, Available: true}, nil
}
