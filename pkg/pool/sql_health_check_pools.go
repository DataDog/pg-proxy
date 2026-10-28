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

// SQLHealthCheckPools owns SQL health-check pools grouped by PostgreSQL instance IP and keyed by
// complete connection string within each IP. This permits separate tenants, roles, or databases
// on the same PostgreSQL instance to use independent pools.
type SQLHealthCheckPools struct {
	ctx             context.Context
	applicationName string

	mu    sync.Mutex
	pools map[string]map[string]*pgxpool.Pool
}

func NewSQLHealthCheckPools(ctx context.Context, cfg SQLHealthCheckPoolsConfig) *SQLHealthCheckPools {
	applicationName := cfg.ApplicationName
	if applicationName == "" {
		applicationName = DefaultSQLHealthCheckApplicationName
	}
	return &SQLHealthCheckPools{
		ctx:             ctx,
		applicationName: applicationName,
		pools:           make(map[string]map[string]*pgxpool.Pool),
	}
}

// GetOrAdd returns the pool for the IP and connection string. The IP identifies the concrete
// PostgreSQL instance, not the Envoy proxy host. Exact IP and connection-string matches reuse the
// existing pool; a different connection string for the same IP gets a separate pool.
func (p *SQLHealthCheckPools) GetOrAdd(ip, connString string) (*pgxpool.Pool, error) {
	cfg, err := sqlHealthCheckPoolConfig(connString, p.applicationName)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	poolsForIP := p.pools[ip]
	if pool, ok := poolsForIP[connString]; ok {
		return pool, nil
	}

	pool, err := newSQLHealthCheckPool(p.ctx, cfg)
	if err != nil {
		return nil, err
	}
	if poolsForIP == nil {
		poolsForIP = make(map[string]*pgxpool.Pool)
		p.pools[ip] = poolsForIP
	}
	poolsForIP[connString] = pool
	return pool, nil
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

	poolCount := 0
	for _, poolsForIP := range p.pools {
		poolCount += len(poolsForIP)
	}
	return poolCount
}

// RemoveIP closes and removes every pool for ip. The caller must ensure no health check is still
// using one of the pools being removed.
func (p *SQLHealthCheckPools) RemoveIP(ip string) {
	p.mu.Lock()
	poolsForIP := p.pools[ip]
	delete(p.pools, ip)
	p.mu.Unlock()

	for _, pool := range poolsForIP {
		pool.Close()
	}
}

// Close closes all pools and empties the registry. The registry can be reused afterward.
func (p *SQLHealthCheckPools) Close() {
	p.mu.Lock()
	pools := p.pools
	p.pools = make(map[string]map[string]*pgxpool.Pool)
	p.mu.Unlock()

	for _, poolsForIP := range pools {
		for _, pool := range poolsForIP {
			pool.Close()
		}
	}
}
