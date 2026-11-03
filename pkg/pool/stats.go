package pool

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	pgproxy "github.com/DataDog/pg-proxy"
)

// ExportPoolStats emits the standard connection-pool gauges for a pgxpool.Stat.
func ExportPoolStats(stats *pgxpool.Stat, tags []string, isDirectPool bool) {
	tags = append(tags, fmt.Sprintf("direct_pool:%t", isDirectPool))
	metrics := pgproxy.Metrics()

	_ = metrics.Gauge("dd.orgstore.pg_proxy.connections.pool_total", float64(stats.TotalConns()), tags, 1)
	_ = metrics.Gauge("dd.orgstore.pg_proxy.connections.pool_acquired", float64(stats.AcquiredConns()), tags, 1)
	_ = metrics.Gauge("dd.orgstore.pg_proxy.connections.pool_acquired_total", float64(stats.AcquireCount()), tags, 1)
	_ = metrics.Gauge("dd.orgstore.pg_proxy.connections.pool_idle", float64(stats.IdleConns()), tags, 1)
	_ = metrics.Gauge("dd.orgstore.pg_proxy.connections.pool_constructing", float64(stats.ConstructingConns()), tags, 1)
	_ = metrics.Gauge("dd.orgstore.pg_proxy.connections.pool_max", float64(stats.MaxConns()), tags, 1)
}
