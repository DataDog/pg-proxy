package pool

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PGXHooks adapt single-pool events to host telemetry.
type PGXHooks struct {
	Acquired func(error)
}

// PGXPool adapts pgxpool.Pool to the portable Pool contract.
type PGXPool struct {
	pool  ConnectionPool
	hooks PGXHooks
}

// NewPGXPool wraps an initialized pgx pool.
func NewPGXPool(pool ConnectionPool, hooks PGXHooks) *PGXPool {
	return &PGXPool{pool: pool, hooks: hooks}
}

// Acquire checks out a connection.
func (p *PGXPool) Acquire(ctx context.Context) (conn *pgxpool.Conn, err error) {
	// pgxpool.Pool.Acquire can panic when the pool is not initialized, such as
	// the zero-value pools currently used by tests to avoid a real connection.
	// TODO: Replace direct pgxpool.Pool usage with a mockable interface, then
	// remove this production panic recovery instead of hiding unexpected panics.
	defer func() {
		if recover() != nil {
			conn = nil
			err = fmt.Errorf("recovered from panicking: failed to acquire connection from pgxpool.Pool")
		}
	}()
	conn, err = p.pool.Acquire(ctx)
	if p.hooks.Acquired != nil {
		p.hooks.Acquired(err)
	}
	return conn, err
}

// ExportStats exports the current pgx statistics.
func (p *PGXPool) ExportStats(tags []string) {
	ExportPoolStats(p.pool.Stat(), tags, false)
}

// Close closes the pgx pool.
func (p *PGXPool) Close() {
	if p.pool != nil {
		p.pool.Close()
	}
}

// Reset resets the pgx pool.
func (p *PGXPool) Reset() {
	if p.pool != nil {
		p.pool.Reset()
	}
}

// FallbackHooks adapt direct-pool fallback events to the host.
type FallbackHooks struct {
	DirectAcquireFailed func(error)
}

// FallbackPool prefers a direct multi-instance pool and falls back to a primary pool.
type FallbackPool struct {
	Primary Pool
	Direct  DirectPool
	Hooks   FallbackHooks
}

// Acquire tries the direct pool before the primary pool.
func (p *FallbackPool) Acquire(ctx context.Context) (*pgxpool.Conn, error) {
	if p.Direct != nil {
		conn, err := p.Direct.AcquireConnection(ctx)
		if err == nil {
			return conn, nil
		}
		if p.Hooks.DirectAcquireFailed != nil {
			p.Hooks.DirectAcquireFailed(err)
		}
	}
	return p.Primary.Acquire(ctx)
}

// ExportStats exports both underlying pools.
func (p *FallbackPool) ExportStats(tags []string) {
	p.Primary.ExportStats(tags)
	if p.Direct != nil {
		p.Direct.ExportDirectPoolStats()
	}
}

// Close closes both underlying pools.
func (p *FallbackPool) Close() {
	p.Primary.Close()
	if p.Direct != nil {
		p.Direct.ClosePools()
	}
}

// Reset resets both underlying pools.
func (p *FallbackPool) Reset() {
	p.Primary.Reset()
	if p.Direct != nil {
		p.Direct.Reset()
	}
}

// DirectOnlyPool adapts a direct pool to Pool.
type DirectOnlyPool struct {
	Direct DirectPool
}

func (p *DirectOnlyPool) Acquire(ctx context.Context) (*pgxpool.Conn, error) {
	return p.Direct.AcquireConnection(ctx)
}
func (p *DirectOnlyPool) ExportStats([]string) { p.Direct.ExportDirectPoolStats() }
func (p *DirectOnlyPool) Close() {
	if p.Direct != nil {
		p.Direct.ClosePools()
	}
}
func (p *DirectOnlyPool) Reset() { p.Direct.Reset() }

// ConnectionPool returns the underlying physical pool.
func (p *PGXPool) ConnectionPool() ConnectionPool { return p.pool }
