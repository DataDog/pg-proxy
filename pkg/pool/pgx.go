// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package pool

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// ReplicaMaxConnLifetime limits connection lifetime so connections are
	// periodically recycled and redistributed across all healthy replicas via
	// LookupFunc. Without this, connections persist for the default lifetime
	// (1h), and during a rolling restart replicas that come up earlier accumulate
	// a disproportionate share of connections since new connections are only
	// spread across the instances available at creation time.
	ReplicaMaxConnLifetime = 15 * time.Minute
	// ReplicaMaxConnLifetimeJitter spreads out connection expiry to avoid
	// thundering-herd reconnections. The default is 0 (no jitter).
	ReplicaMaxConnLifetimeJitter = 5 * time.Minute
)

// NewPGX creates and verifies a PostgreSQL connection pool.
func NewPGX(ctx context.Context, connectionString string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(connectionString)
	if err != nil {
		return nil, err
	}
	return NewPGXWithConfig(ctx, cfg)
}

// NewPGXWithConfig creates and verifies a pool from already parsed settings.
func NewPGXWithConfig(ctx context.Context, cfg *pgxpool.Config) (*pgxpool.Pool, error) {
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("error pinging pool: %w", err)
	}
	return pool, nil
}

// NewReplicaPGX creates and verifies a pool with replica connection lifetimes.
func NewReplicaPGX(ctx context.Context, connectionString string) (*pgxpool.Pool, error) {
	cfg, err := ReplicaPGXConfig(connectionString)
	if err != nil {
		return nil, err
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping after setup: %w", err)
	}
	return pool, nil
}

// ReplicaPGXConfig applies the connection lifetime used by replica pools.
func ReplicaPGXConfig(connectionString string) (*pgxpool.Config, error) {
	cfg, err := pgxpool.ParseConfig(connectionString)
	if err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	cfg.MaxConnLifetime = ReplicaMaxConnLifetime
	cfg.MaxConnLifetimeJitter = ReplicaMaxConnLifetimeJitter
	return cfg, nil
}
