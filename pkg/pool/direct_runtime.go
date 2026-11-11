// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package pool

import (
	"context"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DirectAcquireRunner applies host-specific retry policy around one direct
// connection attempt.
type DirectAcquireRunner func(
	context.Context,
	func() (*pgxpool.Conn, int, error),
) (*pgxpool.Conn, int, error)

// DirectRuntimeHooks adapt direct-pool events to host telemetry and policy.
type DirectRuntimeHooks struct {
	AcquireFinished func(error, int)
	HealthyUpdated  func([]string)
	ExportStats     func(string, *pgxpool.Stat)
}

// DirectRuntimeConfig configures a multi-instance direct pool.
type DirectRuntimeConfig struct {
	DatabaseHost string
	State        *DirectState
	Acquire      DirectAcquireRunner
	Hooks        DirectRuntimeHooks
}

// DirectRuntime owns the portable lifecycle and selection logic for a
// multi-instance PostgreSQL pool.
type DirectRuntime struct {
	databaseHost string
	state        *DirectState
	acquire      DirectAcquireRunner
	hooks        DirectRuntimeHooks
	done         chan struct{}
	closeOnce    sync.Once
}

// NewDirectRuntime creates an empty direct-pool runtime.
func NewDirectRuntime(cfg DirectRuntimeConfig) *DirectRuntime {
	state := cfg.State
	if state == nil {
		state = NewDirectState()
	}
	return &DirectRuntime{
		databaseHost: cfg.DatabaseHost,
		state:        state,
		acquire:      cfg.Acquire,
		hooks:        cfg.Hooks,
		done:         make(chan struct{}),
	}
}

// State returns the managed instance inventory.
func (r *DirectRuntime) State() *DirectState { return r.state }

// Done closes when ClosePools completes.
func (r *DirectRuntime) Done() <-chan struct{} { return r.done }

// AcquireConnection selects a healthy instance and checks out a connection.
func (r *DirectRuntime) AcquireConnection(ctx context.Context) (*pgxpool.Conn, error) {
	var conn *pgxpool.Conn
	var index int
	var err error
	if r.acquire == nil {
		conn, index, err = r.AcquireOnce(ctx)
	} else {
		conn, index, err = r.acquire(ctx, func() (*pgxpool.Conn, int, error) {
			return r.AcquireOnce(ctx)
		})
	}
	if r.hooks.AcquireFinished != nil {
		r.hooks.AcquireFinished(err, index)
	}
	return conn, err
}

// AcquireOnce performs one selection and checkout attempt without retry policy.
func (r *DirectRuntime) AcquireOnce(ctx context.Context) (*pgxpool.Conn, int, error) {
	address, index, err := r.state.NextHealthyAddress(r.databaseHost)
	if err != nil {
		return nil, index, err
	}
	instance, ok := r.state.Load(address)
	if !ok {
		return nil, index, &InstanceNotFoundError{Address: address, DatabaseHost: r.databaseHost}
	}
	conn, err := instance.AcquireConnection(ctx)
	return conn, index, err
}

// CheckHealth updates all instance health in parallel.
func (r *DirectRuntime) CheckHealth() {
	r.state.CheckAllHealth()
}

// RefreshHealthy updates selection state and notifies the host.
func (r *DirectRuntime) RefreshHealthy() []string {
	healthy := r.state.RefreshHealthy()
	if r.hooks.HealthyUpdated != nil {
		r.hooks.HealthyUpdated(healthy)
	}
	return healthy
}

// ExportDirectPoolStats exports stats for every initialized instance.
func (r *DirectRuntime) ExportDirectPoolStats() {
	if r.hooks.ExportStats == nil {
		return
	}
	r.state.Range(func(address string, instance PostgresInstance) bool {
		if stats := instance.Stat(); stats != nil {
			r.hooks.ExportStats(address, stats)
		}
		return true
	})
}

// ClosePools closes every instance and stops background workers.
func (r *DirectRuntime) ClosePools() {
	r.closeOnce.Do(func() {
		r.state.CloseAll()
		close(r.done)
	})
}

// Reset resets every managed instance.
func (r *DirectRuntime) Reset() {
	r.state.ResetAll()
}

// InstanceNotFoundError reports inconsistent direct-pool selection state.
type InstanceNotFoundError struct {
	Address      string
	DatabaseHost string
}

func (e *InstanceNotFoundError) Error() string {
	return "healthy pool " + e.Address + " not found in pool map for db " + e.DatabaseHost
}
