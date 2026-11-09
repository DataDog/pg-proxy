// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package pool

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// MockInstance is a configurable PostgresInstance for pool tests.
type MockInstance struct {
	address string
	pool    *pgxpool.Pool
	healthy bool

	acquire func(*pgxpool.Pool) (*pgxpool.Conn, error)
	health  func(string) bool
}

// NewMockInstance creates a configurable instance test double.
func NewMockInstance(
	address string,
	pool *pgxpool.Pool,
	acquire func(*pgxpool.Pool) (*pgxpool.Conn, error),
	health func(string) bool,
) PostgresInstance {
	return &MockInstance{
		address: address,
		pool:    pool,
		acquire: acquire,
		health:  health,
	}
}

func (m *MockInstance) AcquireConnection(context.Context) (*pgxpool.Conn, error) {
	return m.acquire(m.pool)
}

func (m *MockInstance) HealthCheck() {
	m.healthy = m.health(m.address)
}

func (m *MockInstance) IsHealthy() bool { return m.healthy }
func (m *MockInstance) Close()          {}

func (m *MockInstance) Stat() *pgxpool.Stat {
	// This is intentionally a distinct zero-value stat on every call.
	return &pgxpool.Stat{}
}

func (m *MockInstance) Reset() {}
