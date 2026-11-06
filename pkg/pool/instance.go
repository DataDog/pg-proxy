// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package pool

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ConnectionPool is the pool surface shared by traditional and resizable connection pools.
type ConnectionPool interface {
	Acquire(context.Context) (*pgxpool.Conn, error)
	Ping(context.Context) error
	Close()
	Reset()
	Stat() *pgxpool.Stat
	Config() *pgxpool.Config
}

// PostgresInstance is a directly addressable PostgreSQL server.
type PostgresInstance interface {
	AcquireConnection(context.Context) (*pgxpool.Conn, error)
	HealthCheck()
	IsHealthy() bool
	Close()
	Stat() *pgxpool.Stat
	Reset()
}

// InstanceHooks adapt portable instance events to host telemetry.
type InstanceHooks struct {
	HealthObserved  func(address string, healthy bool, tags []string)
	PoolCreateError func(address, database string, err error)
}

// InstanceConfig contains the portable connection inputs for one server.
type InstanceConfig struct {
	Address          string
	Database         string
	ConnectionString string
	Tags             []string
	PoolFactory      func(context.Context, string) (ConnectionPool, error)
	IsResizable      bool
	// ResizableVirtualLimit applies only when IsResizable is true.
	ResizableVirtualLimit int32
	// PhysicalMaxConns records the fixed physical ceiling.
	PhysicalMaxConns int32
	Hooks            InstanceHooks
}

// Instance owns the connection pool and health state for one server.
type Instance struct {
	address          string
	database         string
	connectionString string
	tags             []string
	pool             ConnectionPool
	healthy          bool
	closed           bool
	poolFactory      func(context.Context, string) (ConnectionPool, error)
	hooks            InstanceHooks
	// initMu serializes pool construction; mu protects pool publication and
	// shutdown without being held during connection establishment.
	initMu                sync.Mutex
	mu                    sync.RWMutex
	isResizable           bool
	resizableVirtualLimit int32
	physicalMaxConns      int32
}

// NewInstance creates an uninitialized PostgreSQL instance.
func NewInstance(cfg InstanceConfig) *Instance {
	poolFactory := cfg.PoolFactory
	if poolFactory == nil {
		poolFactory = func(ctx context.Context, connectionString string) (ConnectionPool, error) {
			return NewPGX(ctx, connectionString)
		}
	}
	instance := &Instance{
		address:          cfg.Address,
		database:         cfg.Database,
		connectionString: cfg.ConnectionString,
		tags:             cfg.Tags,
		poolFactory:      poolFactory,
		hooks:            cfg.Hooks,
		isResizable:      cfg.IsResizable,
		physicalMaxConns: cfg.PhysicalMaxConns,
	}
	if cfg.IsResizable {
		instance.resizableVirtualLimit = cfg.ResizableVirtualLimit
	}
	return instance
}

// Address returns the server address.
func (i *Instance) Address() string { return i.address }

// Database returns the PostgreSQL database name.
func (i *Instance) Database() string { return i.database }

// Tags returns the host-provided telemetry tags.
func (i *Instance) Tags() []string { return i.tags }

// Pool returns the underlying pool, or nil before initialization.
func (i *Instance) Pool() ConnectionPool {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.pool
}

// SetHealthy updates the health snapshot.
func (i *Instance) SetHealthy(healthy bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.healthy = healthy
}

// IsHealthy reports the current health snapshot.
func (i *Instance) IsHealthy() bool {
	i.mu.RLock()
	healthy := i.healthy
	i.mu.RUnlock()
	if i.hooks.HealthObserved != nil {
		i.hooks.HealthObserved(i.address, healthy, i.tags)
	}
	return healthy
}

// AcquireConnection checks out a connection from the initialized pool.
func (i *Instance) AcquireConnection(ctx context.Context) (*pgxpool.Conn, error) {
	pool := i.Pool()
	if pool == nil {
		return nil, fmt.Errorf("pool unexpectedly not initialized when attempting to acquire connection")
	}
	return pool.Acquire(ctx)
}

// Close prevents further initialization and closes the underlying pool asynchronously.
func (i *Instance) Close() {
	i.mu.Lock()
	if i.closed {
		i.mu.Unlock()
		return
	}
	i.closed = true
	pool := i.pool
	i.mu.Unlock()
	if pool != nil {
		go pool.Close()
	}
}

// Stat returns nil until the underlying pool is initialized.
func (i *Instance) Stat() *pgxpool.Stat {
	pool := i.Pool()
	if pool == nil {
		return nil
	}
	return pool.Stat()
}

// Reset closes all connections while leaving the pool open.
func (i *Instance) Reset() {
	if pool := i.Pool(); pool != nil {
		pool.Reset()
	}
}

// IsResizablePool reports whether this instance was configured for resizing.
func (i *Instance) IsResizablePool() bool { return i.isResizable }

// ResizeMaxConns updates an existing pool or saves the limit for initialization.
func (i *Instance) ResizeMaxConns(limit int32) error {
	if !i.isResizable {
		return fmt.Errorf("dynamic resizing is disabled")
	}
	if limit <= 0 {
		return fmt.Errorf("connection limit must be positive: %d", limit)
	}
	if limit > i.physicalMaxConns {
		return fmt.Errorf("connection limit %d exceeds physical ceiling %d", limit, i.physicalMaxConns)
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.pool != nil {
		if err := resizeConnectionPool(i.pool, limit); err != nil {
			return err
		}
	}
	i.resizableVirtualLimit = limit
	return nil
}

func resizeConnectionPool(pool ConnectionPool, limit int32) error {
	resizable, ok := pool.(interface{ Resize(int32) error })
	if !ok {
		return fmt.Errorf("connection pool does not support dynamic resizing")
	}
	return resizable.Resize(limit)
}

// EnsurePool initializes the pool if necessary, unless the instance is closed.
func (i *Instance) EnsurePool(ctx context.Context) bool {
	i.initMu.Lock()
	defer i.initMu.Unlock()
	i.mu.RLock()
	closed, initialized := i.closed, i.pool != nil
	i.mu.RUnlock()
	if closed {
		return false
	}
	if initialized {
		return true
	}
	connectionString := strings.TrimSpace(strings.ReplaceAll(i.connectionString, "target_session_attrs=primary", ""))
	pool, err := i.poolFactory(ctx, connectionString)

	if err != nil {
		if i.hooks.PoolCreateError != nil {
			i.hooks.PoolCreateError(i.address, i.database, err)
		}
		return false
	}
	i.mu.Lock()
	if i.closed {
		i.mu.Unlock()
		pool.Close()
		return false
	}
	// Apply the latest limit before publishing, including resizes received during construction.
	if i.isResizable {
		if err := resizeConnectionPool(pool, i.resizableVirtualLimit); err != nil {
			i.mu.Unlock()
			pool.Close()
			if i.hooks.PoolCreateError != nil {
				i.hooks.PoolCreateError(i.address, i.database, err)
			}
			return false
		}
	}
	i.pool = pool
	i.mu.Unlock()
	return true
}
