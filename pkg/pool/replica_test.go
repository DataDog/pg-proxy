package pool

import (
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplicaPoolConsumesHealthUpdates(t *testing.T) {
	updates := make(chan *PoolHealth, 1)
	var mu sync.RWMutex
	snapshot := map[string]bool{"10.0.0.1": true}
	runtime, err := NewReplicaPool(ReplicaPoolConfig{
		NewBasePool:   func() (*pgxpool.Pool, error) { return nil, nil },
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
		NewBasePool: func() (*pgxpool.Pool, error) { return nil, nil },
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
		NewBasePool: func() (*pgxpool.Pool, error) { return nil, nil },
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
