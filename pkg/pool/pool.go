// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package pool

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Pool is the consumer-facing backend pool: acquire a connection, export
// gauges, and shut down. Release is (*pgxpool.Conn).Release on the returned
// connection — txn-pooling reset policy lives with the session/link, not here.
type Pool interface {
	Acquire(ctx context.Context) (*pgxpool.Conn, error)
	ExportStats(tags []string)
	Close()
	Reset()
}

// DirectPool is a multi-instance pool that connects to Postgres hosts
// directly (rather than through a single load-balanced endpoint).
type DirectPool interface {
	AcquireConnection(ctx context.Context) (*pgxpool.Conn, error)
	ClosePools()
	ExportDirectPoolStats()
	Reset()
}
