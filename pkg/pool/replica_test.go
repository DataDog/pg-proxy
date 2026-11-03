package pool

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplicaPoolConsumesHealthUpdates(t *testing.T) {
	updates := make(chan *PoolHealth, 1)
	var mu sync.RWMutex
	snapshot := map[string]bool{"10.0.0.1": true}
	runtime, err := NewReplicaPool(ReplicaPoolConfig{
		NewBasePool:   func() (ConnectionPool, error) { return nil, nil },
		HealthUpdates: updates,
		Snapshot: func() map[string]bool {
			mu.RLock()
			defer mu.RUnlock()
			copy := make(map[string]bool, len(snapshot))
			for ip, healthy := range snapshot {
				copy[ip] = healthy
			}
			return copy
		},
		InitializeSnapshot:   true,
		StartSnapshotRefresh: true,
	})
	require.NoError(t, err)
	t.Cleanup(runtime.ClosePools)

	mu.Lock()
	snapshot = map[string]bool{"10.0.0.1": false, "10.0.0.2": true}
	mu.Unlock()
	updates <- &PoolHealth{}

	require.Eventually(t, func() bool {
		return !runtime.IsHealthy("10.0.0.1") && runtime.IsHealthy("10.0.0.2")
	}, time.Second, 10*time.Millisecond)
}

func TestReplicaPoolRefreshSnapshot(t *testing.T) {
	snapshot := map[string]bool{"10.0.0.1": true, "10.0.0.2": false}
	runtime, err := NewReplicaPool(ReplicaPoolConfig{
		NewBasePool: func() (ConnectionPool, error) { return nil, nil },
		Snapshot:    func() map[string]bool { return snapshot },
	})
	require.NoError(t, err)

	runtime.RefreshSnapshot()

	assert.True(t, runtime.IsHealthy("10.0.0.1"))
	assert.False(t, runtime.IsHealthy("10.0.0.2"))
	assert.False(t, runtime.IsHealthy("10.0.0.99"))
	assert.ElementsMatch(t, []string{"10.0.0.1", "10.0.0.2"}, runtime.Instances())
}

func TestReplicaPoolRefreshSnapshotSwapsOnNewInstance(t *testing.T) {
	snapshot := map[string]bool{"10.0.0.1": true}
	swapCount := 0
	runtime, err := NewReplicaPool(ReplicaPoolConfig{
		NewBasePool: func() (ConnectionPool, error) { return nil, nil },
		Snapshot:    func() map[string]bool { return snapshot },
		Hooks: ReplicaPoolHooks{
			SwapFinished: func(string) { swapCount++ },
		},
	})
	require.NoError(t, err)

	runtime.RefreshSnapshot()
	assert.Equal(t, 0, swapCount)

	snapshot = map[string]bool{"10.0.0.1": true, "10.0.0.2": true}
	runtime.RefreshSnapshot()
	assert.Equal(t, 1, swapCount)

	runtime.RefreshSnapshot()
	assert.Equal(t, 1, swapCount)
}

func TestReplicaPoolClosesReplacementCreatedDuringShutdown(t *testing.T) {
	current, next := &lifecycleTestPool{}, &lifecycleTestPool{}
	started, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	attempts := 0
	swapFinished := 0
	runtime, err := NewReplicaPool(ReplicaPoolConfig{
		NewBasePool: func() (ConnectionPool, error) {
			attempts++
			if attempts == 1 {
				return current, nil
			}
			close(started)
			<-release
			return next, nil
		},
		Hooks: ReplicaPoolHooks{SwapFinished: func(string) { swapFinished++ }},
	})
	require.NoError(t, err)
	done := make(chan struct{})
	go func() { runtime.Swap("test"); close(done) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("replacement did not start")
	}
	closed := make(chan struct{})
	go func() { runtime.ClosePools(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("shutdown waited for replacement creation")
	}
	assert.True(t, current.closed.Load())
	unblock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("replacement did not complete")
	}
	assert.True(t, next.closed.Load())
	assert.True(t, current == runtime.basePool.Load().pool, "late replacement must never be published")
	runtime.Swap("after-close")
	assert.Equal(t, 2, attempts)
	assert.Zero(t, swapFinished, "discarding a replacement must not report a successful swap")
	runtime.ClosePools()
	assert.EqualValues(t, 1, current.closeCount.Load())
	assert.EqualValues(t, 1, next.closeCount.Load())
}

func TestReplicaPoolCloseBeforeSwap(t *testing.T) {
	current := &lifecycleTestPool{}
	attempts := 0
	runtime, err := NewReplicaPool(ReplicaPoolConfig{
		NewBasePool: func() (ConnectionPool, error) {
			attempts++
			return current, nil
		},
	})
	require.NoError(t, err)
	runtime.ClosePools()
	runtime.Swap("after-close")
	runtime.ClosePools()
	assert.Equal(t, 1, attempts, "shutdown must prevent replacement creation")
	assert.EqualValues(t, 1, current.closeCount.Load())
}

func TestReplicaPoolCloseAfterSwap(t *testing.T) {
	current, next := &lifecycleTestPool{}, &lifecycleTestPool{}
	attempts, swapFinished := 0, 0
	runtime, err := NewReplicaPool(ReplicaPoolConfig{
		NewBasePool: func() (ConnectionPool, error) {
			attempts++
			if attempts == 1 {
				return current, nil
			}
			return next, nil
		},
		Hooks: ReplicaPoolHooks{SwapFinished: func(string) { swapFinished++ }},
	})
	require.NoError(t, err)
	t.Cleanup(runtime.ClosePools)
	runtime.Swap("test")
	assert.Same(t, next, runtime.basePool.Load().pool)
	require.Eventually(t, current.closed.Load, time.Second, time.Millisecond)
	assert.False(t, next.closed.Load())
	assert.Equal(t, 1, swapFinished)
	runtime.ClosePools()
	runtime.ClosePools()
	assert.EqualValues(t, 1, current.closeCount.Load())
	assert.EqualValues(t, 1, next.closeCount.Load())
}

func TestReplicaPoolResizeDuringReplacement(t *testing.T) {
	current, next := &instanceTestPool{}, &instanceTestPool{}
	started, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	attempts := 0
	runtime, err := NewReplicaPool(ReplicaPoolConfig{
		IsResizable:           true,
		ResizableVirtualLimit: 20, PhysicalMaxConns: 100,
		NewBasePool: func() (ConnectionPool, error) {
			attempts++
			if attempts == 1 {
				return current, nil
			}
			close(started)
			<-release
			return next, nil
		},
	})
	require.NoError(t, err)
	t.Cleanup(runtime.ClosePools)
	done := make(chan struct{})
	go func() { runtime.Swap("test"); close(done) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("replacement did not start")
	}
	resized := make(chan error, 1)
	go func() { resized <- runtime.ResizeMaxConns(30) }()
	select {
	case err := <-resized:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("resize blocked behind construction")
	}
	assert.EqualValues(t, 30, current.limit)
	unblock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("replacement did not complete")
	}
	assert.Same(t, next, runtime.basePool.Load().pool)
	assert.EqualValues(t, 30, next.limit)
	assert.EqualValues(t, 30, runtime.MaxConns())
	require.NoError(t, runtime.ResizeMaxConns(80))
	assert.EqualValues(t, 80, next.limit)
}

func TestReplicaPoolFailedResizeDoesNotChangeSavedLimit(t *testing.T) {
	current, next := &instanceTestPool{}, &instanceTestPool{}
	attempts := 0
	runtime, err := NewReplicaPool(ReplicaPoolConfig{
		IsResizable:           true,
		ResizableVirtualLimit: 20, PhysicalMaxConns: 100,
		NewBasePool: func() (ConnectionPool, error) {
			attempts++
			if attempts == 1 {
				return current, nil
			}
			return next, nil
		},
	})
	require.NoError(t, err)
	t.Cleanup(runtime.ClosePools)
	require.NoError(t, runtime.ResizeMaxConns(30))
	for _, limit := range []int32{-1, 0, 101} {
		require.Error(t, runtime.ResizeMaxConns(limit))
	}
	current.resizeErr = errors.New("failed")
	require.Error(t, runtime.ResizeMaxConns(40))
	assert.EqualValues(t, 30, runtime.MaxConns())
	runtime.Swap("test")
	assert.EqualValues(t, 30, next.limit)
}

func TestReplicaPoolRejectsFailedReplacement(t *testing.T) {
	current := &instanceTestPool{}
	next := &instanceTestPool{resizeErr: errors.New("failed")}
	attempts := 0
	var reported error
	runtime, err := NewReplicaPool(ReplicaPoolConfig{
		IsResizable:           true,
		ResizableVirtualLimit: 20, PhysicalMaxConns: 100,
		NewBasePool: func() (ConnectionPool, error) {
			attempts++
			if attempts == 1 {
				return current, nil
			}
			return next, nil
		},
		Hooks: ReplicaPoolHooks{SwapFailed: func(_ string, err error) { reported = err }},
	})
	require.NoError(t, err)
	t.Cleanup(runtime.ClosePools)
	runtime.Swap("test")
	assert.Same(t, current, runtime.basePool.Load().pool)
	assert.True(t, next.closed)
	assert.ErrorIs(t, reported, next.resizeErr)
}

func TestTraditionalReplicaPoolIgnoresResizableVirtualLimit(t *testing.T) {
	runtime, err := NewReplicaPool(ReplicaPoolConfig{
		ResizableVirtualLimit: 50,
		NewBasePool:           func() (ConnectionPool, error) { return nil, nil },
	})
	require.NoError(t, err)
	assert.False(t, runtime.isResizable)
	assert.Zero(t, runtime.resizableVirtualLimit)
	runtime.resizableVirtualLimit = 50
	assert.False(t, runtime.IsResizablePool())
	assert.Zero(t, runtime.MaxConns())
	assert.Error(t, runtime.ResizeMaxConns(10))
}
