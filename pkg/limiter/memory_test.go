package limiter

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadHardLimitV2(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "memory.max"), "999999\n")
	proc := writeV2RootProcCgroup(t, root)

	mem, ok := readHardLimit(root, proc, true)
	assert.True(t, ok)
	assert.True(t, mem.Available)
	assert.Equal(t, int64(999999), mem.Bytes)
}

func TestReadHardLimitV2Unlimited(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "memory.max"), "max\n")
	writeFile(t, filepath.Join(root, "memory", "memory.limit_in_bytes"), "222\n")
	proc := writeV2RootProcCgroup(t, root)

	mem, ok := readHardLimit(root, proc, true)
	assert.True(t, ok, "v2 memory.max=max is a valid unlimited result")
	assert.False(t, mem.Available, "do not fall through to v1")
}

func TestReadHardLimitV2NestedPath(t *testing.T) {
	root := t.TempDir()
	nested := filepath.Join(root, "kubepods.slice")
	require.NoError(t, os.MkdirAll(nested, 0o755))
	writeFile(t, filepath.Join(nested, "memory.max"), "42\n")
	writeFile(t, filepath.Join(root, "memory.max"), "999\n")

	proc := filepath.Join(root, "proc-cgroup")
	writeFile(t, proc, "0::/kubepods.slice\n")

	mem, ok := readHardLimit(root, proc, true)
	assert.True(t, ok)
	assert.True(t, mem.Available)
	assert.Equal(t, int64(42), mem.Bytes)
}

func TestReadHardLimitV2NestedMissingFallsThroughToRoot(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "kubepods.slice"), 0o755))
	writeFile(t, filepath.Join(root, "memory.max"), "999\n")
	proc := filepath.Join(root, "proc-cgroup")
	writeFile(t, proc, "0::/kubepods.slice\n")

	mem, ok := readHardLimit(root, proc, true)
	assert.True(t, ok)
	assert.True(t, mem.Available)
	assert.Equal(t, int64(999), mem.Bytes)
}

func TestReadHardLimitV2NestedUnreadableDoesNotUseRoot(t *testing.T) {
	root := t.TempDir()
	nested := filepath.Join(root, "kubepods.slice")
	// A directory named memory.max makes ReadFile fail with an error other
	// than IsNotExist (typically EISDIR), without depending on chmod/root.
	require.NoError(t, os.MkdirAll(filepath.Join(nested, "memory.max"), 0o755))
	writeFile(t, filepath.Join(root, "memory.max"), "999\n")
	proc := filepath.Join(root, "proc-cgroup")
	writeFile(t, proc, "0::/kubepods.slice\n")

	_, ok := readHardLimit(root, proc, true)
	assert.False(t, ok, "a non-missing read error must not fall through to the parent cgroup limit")
}

func TestReadHardLimitV2UnreadableProcCgroupDoesNotUseRoot(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "memory.max"), "max\n")

	mem, ok := readHardLimit(root, filepath.Join(root, "missing-proc-cgroup"), true)
	assert.False(t, ok, "a failed process-cgroup lookup must not be treated as root unlimited")
	assert.False(t, mem.Available)
}

func TestReadHardLimitV1(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "memory", "memory.limit_in_bytes"), "888888\n")

	mem, ok := readHardLimit(root, filepath.Join(root, "missing-proc-cgroup"), false)
	assert.True(t, ok)
	assert.True(t, mem.Available)
	assert.Equal(t, int64(888888), mem.Bytes)
}

func TestReadHardLimitV1UnsetSentinel(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "memory", "memory.limit_in_bytes"), strconv.FormatInt(v1UnsetSentinel, 10)+"\n")

	mem, ok := readHardLimit(root, filepath.Join(root, "missing-proc-cgroup"), false)
	assert.True(t, ok)
	assert.False(t, mem.Available)
}

func TestReadHardLimitStatfsSelectsHierarchy(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "memory.max"), "111\n")
	writeFile(t, filepath.Join(root, "memory", "memory.limit_in_bytes"), "222\n")
	proc := writeV2RootProcCgroup(t, root)

	v2, ok := readHardLimit(root, proc, true)
	assert.True(t, ok)
	assert.True(t, v2.Available)
	assert.Equal(t, int64(111), v2.Bytes)

	v1, ok := readHardLimit(root, proc, false)
	assert.True(t, ok)
	assert.True(t, v1.Available)
	assert.Equal(t, int64(222), v1.Bytes, "v1 must ignore memory.max when statfs says the mount is not cgroup2")
}

func TestReadHardLimitMissing(t *testing.T) {
	root := t.TempDir()
	proc := writeV2RootProcCgroup(t, root)
	mem, ok := readHardLimit(root, proc, true)
	assert.False(t, ok)
	assert.Equal(t, int64(0), mem.Bytes)
}

func TestIsCgroup2UnifiedOrdinaryDir(t *testing.T) {
	unifiedV2, err := isCgroup2Unified(t.TempDir())
	assert.NoError(t, err)
	assert.False(t, unifiedV2, "a normal directory is not a cgroup2 mount")
}

func TestReadHardLimitSmoke(t *testing.T) {
	mem := ReadHardLimit()
	if mem.Available {
		assert.Greater(t, mem.Bytes, int64(0))
	}
}

func writeFile(t *testing.T, path, contents string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o644))
}

func writeV2RootProcCgroup(t *testing.T, root string) string {
	t.Helper()
	proc := filepath.Join(root, "proc-cgroup")
	writeFile(t, proc, "0::/\n")
	return proc
}
