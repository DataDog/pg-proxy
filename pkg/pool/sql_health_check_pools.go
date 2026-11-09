// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package pool

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// SQLHealthCheckConnectTimeout bounds creation of a cold health-check connection.
	SQLHealthCheckConnectTimeout = 400 * time.Millisecond
	// SQLHealthCheckAcquireTimeout has to cover a cold connection: the first check against a newly
	// promoted primary pays a TCP, TLS, and authentication round trip. It intentionally matches the
	// connection timeout so a cold connection can use the full acquisition budget.
	SQLHealthCheckAcquireTimeout = SQLHealthCheckConnectTimeout
	// SQLHealthCheckQueryTimeout only covers a trivial read. It stays above the statement timeout
	// so PostgreSQL normally cancels first and leaves the connection reusable.
	SQLHealthCheckQueryTimeout = 300 * time.Millisecond
	// SQLHealthCheckStatementTimeout is shorter than the client-side query timeout so PostgreSQL
	// normally cancels a slow query without forcing pgx to discard the connection.
	SQLHealthCheckStatementTimeout = 200 * time.Millisecond
	// SQLHealthCheckQuery reports whether the connected PostgreSQL instance is a replica.
	SQLHealthCheckQuery = "SELECT pg_is_in_recovery();"

	sqlHealthCheckMaxConns = 1
	sqlHealthCheckMinConns = 1

	// DefaultSQLHealthCheckApplicationName identifies connections created by the standalone
	// library when the embedding application does not provide a more specific name.
	DefaultSQLHealthCheckApplicationName = "pg-proxy-sql-prober"
)

// SQLHealthCheckPoolsConfig configures the dedicated SQL health-check pool registry.
type SQLHealthCheckPoolsConfig struct {
	ApplicationName string
}

// SQLHealthCheckPools owns one SQL health-check pool per PostgreSQL instance IP. A checker serves
// one subscription identity, so a changed connection string replaces the pool for that IP. This
// closes connections authenticated with stale credentials after a secret rotation.
type SQLHealthCheckPools struct {
	ctx             context.Context
	applicationName string

	mu    sync.Mutex
	pools map[string]sqlHealthCheckPoolEntry
}

type sqlHealthCheckPoolEntry struct {
	connString string
	pool       *pgxpool.Pool
}

func NewSQLHealthCheckPools(ctx context.Context, cfg SQLHealthCheckPoolsConfig) *SQLHealthCheckPools {
	applicationName := cfg.ApplicationName
	if applicationName == "" {
		applicationName = DefaultSQLHealthCheckApplicationName
	}
	return &SQLHealthCheckPools{
		ctx:             ctx,
		applicationName: applicationName,
		pools:           make(map[string]sqlHealthCheckPoolEntry),
	}
}

// GetOrAdd returns the pool for the IP and connection string. The IP identifies the concrete
// PostgreSQL instance, not the Envoy proxy host. An exact connection-string match reuses the
// existing pool. A changed connection string builds a replacement first, then closes the stale
// pool. Callers must not concurrently use different connection strings for the same IP.
func (p *SQLHealthCheckPools) GetOrAdd(ip, connString string) (*pgxpool.Pool, error) {
	cfg, err := sqlHealthCheckPoolConfig(connString, p.applicationName)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	current, ok := p.pools[ip]
	if ok && current.connString == connString {
		return current.pool, nil
	}

	replacement, err := newSQLHealthCheckPool(p.ctx, cfg)
	if err != nil {
		return nil, err
	}
	p.pools[ip] = sqlHealthCheckPoolEntry{
		connString: connString,
		pool:       replacement,
	}
	if ok {
		current.pool.Close()
	}
	return replacement, nil
}

// NewSQLHealthCheckPool constructs one configured pool. The caller owns its lifecycle.
func NewSQLHealthCheckPool(ctx context.Context, connString string, cfg SQLHealthCheckPoolsConfig) (*pgxpool.Pool, error) {
	applicationName := cfg.ApplicationName
	if applicationName == "" {
		applicationName = DefaultSQLHealthCheckApplicationName
	}
	poolCfg, err := sqlHealthCheckPoolConfig(connString, applicationName)
	if err != nil {
		return nil, err
	}
	return newSQLHealthCheckPool(ctx, poolCfg)
}

func sqlHealthCheckPoolConfig(connString, applicationName string) (*pgxpool.Config, error) {
	cfg, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("parse SQL health-check pool config: %w", err)
	}

	return SQLHealthCheckConfig(cfg, SQLHealthCheckPoolsConfig{ApplicationName: applicationName}), nil
}

// SQLHealthCheckConfig copies connection settings and applies the probe limits.
func SQLHealthCheckConfig(source *pgxpool.Config, options SQLHealthCheckPoolsConfig) *pgxpool.Config {
	cfg := source.Copy()
	applicationName := options.ApplicationName
	if applicationName == "" {
		applicationName = DefaultSQLHealthCheckApplicationName
	}
	// Apply health-check settings after parsing so connection-string parameters cannot override
	// them. MinConns keeps the connection warm between checks.
	cfg.MaxConns = sqlHealthCheckMaxConns
	cfg.MinConns = sqlHealthCheckMinConns
	cfg.ConnConfig.ConnectTimeout = SQLHealthCheckConnectTimeout
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = make(map[string]string)
	}
	cfg.ConnConfig.RuntimeParams["application_name"] = applicationName
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = strconv.FormatInt(SQLHealthCheckStatementTimeout.Milliseconds(), 10)
	return cfg
}

func newSQLHealthCheckPool(ctx context.Context, cfg *pgxpool.Config) (*pgxpool.Pool, error) {
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create SQL health-check pool: %w", err)
	}
	return pool, nil
}

func (p *SQLHealthCheckPools) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.pools)
}

// RemoveIP closes and removes the pool for ip. The caller must ensure no health check is still
// using the pool being removed.
func (p *SQLHealthCheckPools) RemoveIP(ip string) {
	p.mu.Lock()
	entry, ok := p.pools[ip]
	delete(p.pools, ip)
	p.mu.Unlock()

	if ok {
		entry.pool.Close()
	}
}

// RetainIPs closes and removes pools whose addresses are not in ips. The caller must ensure no
// health check is still using a pool being removed.
func (p *SQLHealthCheckPools) RetainIPs(ips []string) {
	retained := make(map[string]struct{}, len(ips))
	for _, ip := range ips {
		retained[ip] = struct{}{}
	}

	p.mu.Lock()
	removed := make([]*pgxpool.Pool, 0)
	for ip, entry := range p.pools {
		if _, ok := retained[ip]; ok {
			continue
		}
		delete(p.pools, ip)
		removed = append(removed, entry.pool)
	}
	p.mu.Unlock()

	for _, pool := range removed {
		pool.Close()
	}
}

// Close closes all pools and empties the registry. The registry can be reused afterward.
func (p *SQLHealthCheckPools) Close() {
	p.mu.Lock()
	pools := p.pools
	p.pools = make(map[string]sqlHealthCheckPoolEntry)
	p.mu.Unlock()

	for _, entry := range pools {
		entry.pool.Close()
	}
}
