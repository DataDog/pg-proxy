package pool

import (
	"context"
	"maps"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ReplicaPoolHooks adapt replica lifecycle events to host telemetry.
type ReplicaPoolHooks struct {
	SwapFailed   func(string, error)
	SwapFinished func(string)
	ExportStats  func(*pgxpool.Stat)
	Close        func()
}

// ReplicaPoolConfig configures a shared pgx pool across replica instances.
type ReplicaPoolConfig struct {
	Context              context.Context
	NewBasePool          func() (*pgxpool.Pool, error)
	Snapshot             func() map[string]bool
	HealthUpdates        HealthUpdates
	Hooks                ReplicaPoolHooks
	InitializeSnapshot   bool
	StartSnapshotRefresh bool
}

// ReplicaPool owns the portable shared-pool and health-snapshot lifecycle.
type ReplicaPool struct {
	basePool      atomic.Pointer[pgxpool.Pool]
	newBasePool   func() (*pgxpool.Pool, error)
	snapshot      func() map[string]bool
	healthUpdates HealthUpdates
	hooks         ReplicaPoolHooks
	cancel        context.CancelFunc
	closeOnce     sync.Once

	healthyMu sync.RWMutex
	healthy   map[string]bool
}

// NewReplicaPool creates a shared replica runtime and its initial pgx pool.
func NewReplicaPool(cfg ReplicaPoolConfig) (*ReplicaPool, error) {
	ctx := cfg.Context
	if ctx == nil {
		ctx = context.Background()
	}
	watcherCtx, cancel := context.WithCancel(ctx)
	pool := &ReplicaPool{
		newBasePool:   cfg.NewBasePool,
		snapshot:      cfg.Snapshot,
		healthUpdates: cfg.HealthUpdates,
		hooks:         cfg.Hooks,
		cancel:        cancel,
		healthy:       make(map[string]bool),
	}
	base, err := pool.newBasePool()
	if err != nil {
		cancel()
		return nil, err
	}
	pool.basePool.Store(base)
	if cfg.InitializeSnapshot {
		pool.RefreshSnapshot()
	}
	if cfg.StartSnapshotRefresh {
		go pool.runSnapshotRefresh(watcherCtx)
	}
	return pool, nil
}

// AcquireConnection checks out a connection from the current shared pool.
func (p *ReplicaPool) AcquireConnection(ctx context.Context) (*pgxpool.Conn, error) {
	return p.basePool.Load().Acquire(ctx)
}

// IsHealthy reports the latest health for an instance.
func (p *ReplicaPool) IsHealthy(instanceID string) bool {
	p.healthyMu.RLock()
	defer p.healthyMu.RUnlock()
	return p.healthy[instanceID]
}

// Instances returns all instances in the latest snapshot.
func (p *ReplicaPool) Instances() []string {
	p.healthyMu.RLock()
	defer p.healthyMu.RUnlock()
	return slices.Collect(maps.Keys(p.healthy))
}

// RefreshSnapshot replaces instance health and redistributes connections when
// a newly discovered instance appears.
func (p *ReplicaPool) RefreshSnapshot() {
	if p.snapshot == nil {
		return
	}
	next := p.snapshot()
	p.healthyMu.Lock()
	previous := p.healthy
	p.healthy = next
	p.healthyMu.Unlock()

	if len(previous) == 0 {
		return
	}
	for instanceID := range next {
		if _, ok := previous[instanceID]; !ok {
			p.Swap("worker")
			return
		}
	}
}

// Swap replaces the shared pgx pool with a freshly configured pool.
func (p *ReplicaPool) Swap(source string) {
	next, err := p.newBasePool()
	if err != nil {
		if p.hooks.SwapFailed != nil {
			p.hooks.SwapFailed(source, err)
		}
		return
	}
	previous := p.basePool.Swap(next)
	if previous != nil {
		go previous.Close()
	}
	if p.hooks.SwapFinished != nil {
		p.hooks.SwapFinished(source)
	}
}

// ExportDirectPoolStats exports stats from the current shared pool.
func (p *ReplicaPool) ExportDirectPoolStats() {
	if p.hooks.ExportStats != nil {
		p.hooks.ExportStats(p.basePool.Load().Stat())
	}
}

// Reset resets the current shared pool.
func (p *ReplicaPool) Reset() {
	p.basePool.Load().Reset()
}

// ClosePools closes the current pool and host adapters.
func (p *ReplicaPool) ClosePools() {
	p.closeOnce.Do(func() {
		p.cancel()
		if current := p.basePool.Load(); current != nil {
			current.Close()
		}
		if p.hooks.Close != nil {
			p.hooks.Close()
		}
	})
}

func (p *ReplicaPool) runSnapshotRefresh(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-p.healthUpdates:
			if !ok {
				return
			}
			p.RefreshSnapshot()
		}
	}
}
