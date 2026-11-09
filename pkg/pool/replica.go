// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package pool

import (
	"context"
	"fmt"
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
	IsResizable bool
	// ResizableVirtualLimit applies only when IsResizable is true.
	ResizableVirtualLimit int32
	PhysicalMaxConns      int32
	Context               context.Context
	NewBasePool           func() (ConnectionPool, error)
	Snapshot              func() map[string]bool
	HealthUpdates         HealthUpdates
	Hooks                 ReplicaPoolHooks
	InitializeSnapshot    bool
	StartSnapshotRefresh  bool
}

type physicalPoolRef struct{ pool ConnectionPool }

// ReplicaPool owns the portable shared-pool and health-snapshot lifecycle.
type ReplicaPool struct {
	basePool              atomic.Pointer[physicalPoolRef]
	newBasePool           func() (ConnectionPool, error)
	snapshot              func() map[string]bool
	healthUpdates         HealthUpdates
	hooks                 ReplicaPoolHooks
	cancel                context.CancelFunc
	closeOnce             sync.Once
	mu                    sync.Mutex
	closed                bool
	isResizable           bool
	resizableVirtualLimit int32
	physicalMaxConns      int32

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
		isResizable:      cfg.IsResizable,
		physicalMaxConns: cfg.PhysicalMaxConns,
		newBasePool:      cfg.NewBasePool,
		snapshot:         cfg.Snapshot,
		healthUpdates:    cfg.HealthUpdates,
		hooks:            cfg.Hooks,
		cancel:           cancel,
		healthy:          make(map[string]bool),
	}
	if cfg.IsResizable {
		pool.resizableVirtualLimit = cfg.ResizableVirtualLimit
	}
	base, err := pool.newBasePool()
	if err != nil {
		cancel()
		return nil, err
	}
	if pool.isResizable {
		if err := resizeConnectionPool(base, pool.resizableVirtualLimit); err != nil {
			base.Close()
			cancel()
			return nil, err
		}
	}
	pool.basePool.Store(&physicalPoolRef{pool: base})
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
	return p.basePool.Load().pool.Acquire(ctx)
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
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		return
	}
	next, err := p.newBasePool()
	if err != nil {
		if p.hooks.SwapFailed != nil {
			p.hooks.SwapFailed(source, err)
		}
		return
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		if next != nil {
			next.Close()
		}
		return
	}
	if p.isResizable {
		if err := resizeConnectionPool(next, p.resizableVirtualLimit); err != nil {
			p.mu.Unlock()
			next.Close()
			if p.hooks.SwapFailed != nil {
				p.hooks.SwapFailed(source, err)
			}
			return
		}
	}
	previous := p.basePool.Swap(&physicalPoolRef{pool: next})
	p.mu.Unlock()
	if previous != nil && previous.pool != nil {
		go previous.pool.Close()
	}
	if p.hooks.SwapFinished != nil {
		p.hooks.SwapFinished(source)
	}
}

// ExportDirectPoolStats exports stats from the current shared pool.
func (p *ReplicaPool) ExportDirectPoolStats() {
	if p.hooks.ExportStats != nil {
		p.hooks.ExportStats(p.basePool.Load().pool.Stat())
	}
}

// Reset resets the current shared pool.
func (p *ReplicaPool) Reset() {
	p.basePool.Load().pool.Reset()
}

// ClosePools closes the current pool and host adapters.
func (p *ReplicaPool) ClosePools() {
	p.closeOnce.Do(func() {
		p.cancel()
		p.mu.Lock()
		p.closed = true
		current := p.basePool.Load()
		p.mu.Unlock()
		if current != nil && current.pool != nil {
			current.pool.Close()
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

// IsResizablePool reports whether the shared pool was configured for resizing.
func (p *ReplicaPool) IsResizablePool() bool { return p.isResizable }

// MaxConns returns the current resize limit, or zero when resizing is disabled.
func (p *ReplicaPool) MaxConns() int32 {
	if !p.isResizable {
		return 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.resizableVirtualLimit
}

// PhysicalMaxConns returns the fixed physical ceiling selected at startup.
func (p *ReplicaPool) PhysicalMaxConns() int32 { return p.physicalMaxConns }

// ResizeMaxConns updates both the current pool and the limit applied to replacements.
func (p *ReplicaPool) ResizeMaxConns(limit int32) error {
	if !p.isResizable {
		return fmt.Errorf("dynamic resizing is disabled")
	}
	if limit <= 0 || limit > p.physicalMaxConns {
		return fmt.Errorf("connection limit %d must be between 1 and %d", limit, p.physicalMaxConns)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return fmt.Errorf("replica pool is closed")
	}
	if err := resizeConnectionPool(p.basePool.Load().pool, limit); err != nil {
		return err
	}
	p.resizableVirtualLimit = limit
	return nil
}
