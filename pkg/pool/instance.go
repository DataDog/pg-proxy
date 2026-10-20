package pool

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

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
	PoolFactory      func(context.Context, string) (*pgxpool.Pool, error)
	Hooks            InstanceHooks
}

// Instance owns the connection pool and health state for one server.
type Instance struct {
	address          string
	database         string
	connectionString string
	tags             []string
	pool             *pgxpool.Pool
	healthy          bool
	poolFactory      func(context.Context, string) (*pgxpool.Pool, error)
	hooks            InstanceHooks
}

// NewInstance creates an uninitialized PostgreSQL instance.
func NewInstance(cfg InstanceConfig) *Instance {
	poolFactory := cfg.PoolFactory
	if poolFactory == nil {
		poolFactory = NewPGX
	}
	return &Instance{
		address:          cfg.Address,
		database:         cfg.Database,
		connectionString: cfg.ConnectionString,
		tags:             cfg.Tags,
		poolFactory:      poolFactory,
		hooks:            cfg.Hooks,
	}
}

// Address returns the server address.
func (i *Instance) Address() string { return i.address }

// Database returns the PostgreSQL database name.
func (i *Instance) Database() string { return i.database }

// Tags returns the host-provided telemetry tags.
func (i *Instance) Tags() []string { return i.tags }

// Pool returns the underlying pool, or nil before initialization.
func (i *Instance) Pool() *pgxpool.Pool { return i.pool }

// SetHealthy updates the health snapshot.
func (i *Instance) SetHealthy(healthy bool) { i.healthy = healthy }

// IsHealthy reports the current health snapshot.
func (i *Instance) IsHealthy() bool {
	if i.hooks.HealthObserved != nil {
		i.hooks.HealthObserved(i.address, i.healthy, i.tags)
	}
	return i.healthy
}

// AcquireConnection checks out a connection from the initialized pool.
func (i *Instance) AcquireConnection(ctx context.Context) (*pgxpool.Conn, error) {
	if i.pool == nil {
		return nil, fmt.Errorf("pool unexpectedly not initialized when attempting to acquire connection")
	}
	return i.pool.Acquire(ctx)
}

// Close closes the underlying pool asynchronously.
func (i *Instance) Close() {
	if i.pool != nil {
		go i.pool.Close()
	}
}

// Stat returns nil until the underlying pool is initialized.
func (i *Instance) Stat() *pgxpool.Stat {
	if i.pool == nil {
		return nil
	}
	return i.pool.Stat()
}

// Reset closes all connections while leaving the pool open.
func (i *Instance) Reset() {
	if i.pool != nil {
		i.pool.Reset()
	}
}

// EnsurePool initializes the pool if necessary.
func (i *Instance) EnsurePool(ctx context.Context) bool {
	if i.pool != nil {
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
	i.pool = pool
	return true
}
