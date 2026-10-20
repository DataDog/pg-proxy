package pool

import (
	"context"
	"errors"
	"slices"
	"sync"
	"time"
)

const (
	// SQLHealthCheckInterval is the default interval between centralized SQL snapshots.
	SQLHealthCheckInterval = time.Second

	SQLHealthCheckPoolLookupStage SQLHealthCheckStage = "pool_lookup"
	SQLHealthCheckAcquireStage    SQLHealthCheckStage = "acquire"
	SQLHealthCheckQueryStage      SQLHealthCheckStage = "query"

	sqlHealthCheckCrashShutdown    = "57P02"
	sqlHealthCheckCannotConnectNow = "57P03"
)

// SQLHealthProber reports pg_is_in_recovery() for one concrete PostgreSQL instance.
type SQLHealthProber interface {
	Probe(ctx context.Context, ip, connString string, tags []string) (bool, error)
}

// SQLHealthCheckStage identifies one stage of a SQL health probe.
type SQLHealthCheckStage string

// SQLHealthCheckResult is one instance's SQL health result.
type SQLHealthCheckResult struct {
	IP         string
	InRecovery bool
	Err        error
	Stage      SQLHealthCheckStage
}

// HealthyPrimary reports whether the probe conclusively observed a writable primary.
func (r SQLHealthCheckResult) HealthyPrimary() bool {
	return r.Err == nil && !r.InRecovery
}

// SQLHealthCheckOutcome is one complete concurrent cluster snapshot.
type SQLHealthCheckOutcome struct {
	Results    []SQLHealthCheckResult
	PrimaryIPs []string
}

// SQLHealthCheckRunner applies embedding-application retry policy around one instance probe.
// metricTags already include the concrete instance IP.
type SQLHealthCheckRunner func(context.Context, []string, func() error) error

// SQLHealthCheckHooks adapt portable checker events to host telemetry.
type SQLHealthCheckHooks struct {
	StageFinished     func(SQLHealthCheckStage, time.Duration, []string, error)
	ProbeFinished     func(SQLHealthCheckResult, bool, time.Duration, []string)
	PoolCount         func(int, []string)
	MultiplePrimaries func([]string, []string)
}

// SQLHealthCheckerConfig configures the portable SQL checker.
type SQLHealthCheckerConfig struct {
	Pools        *SQLHealthCheckPools
	Prober       SQLHealthProber
	RunWithRetry SQLHealthCheckRunner
	Hooks        SQLHealthCheckHooks
}

type sqlHealthProbeError struct {
	stage SQLHealthCheckStage
	err   error
}

func (e *sqlHealthProbeError) Error() string { return e.err.Error() }
func (e *sqlHealthProbeError) Unwrap() error { return e.err }

type registrySQLHealthProber struct {
	pools *SQLHealthCheckPools
	hooks SQLHealthCheckHooks
}

func (p registrySQLHealthProber) Probe(ctx context.Context, ip, connString string, tags []string) (bool, error) {
	poolLookupStart := time.Now()
	pool, err := p.pools.GetOrAdd(ip, connString)
	p.reportStage(SQLHealthCheckPoolLookupStage, time.Since(poolLookupStart), tags, err)
	if err != nil {
		return false, &sqlHealthProbeError{stage: SQLHealthCheckPoolLookupStage, err: err}
	}

	acquireCtx, cancelAcquire := context.WithTimeout(ctx, SQLHealthCheckAcquireTimeout)
	acquireStart := time.Now()
	conn, err := pool.Acquire(acquireCtx)
	p.reportStage(SQLHealthCheckAcquireStage, time.Since(acquireStart), tags, err)
	if err != nil {
		cancelAcquire()
		return false, &sqlHealthProbeError{stage: SQLHealthCheckAcquireStage, err: err}
	}
	cancelAcquire()
	defer conn.Release()

	queryCtx, cancelQuery := context.WithTimeout(ctx, SQLHealthCheckQueryTimeout)
	defer cancelQuery()
	var inRecovery bool
	queryStart := time.Now()
	err = conn.QueryRow(queryCtx, SQLHealthCheckQuery).Scan(&inRecovery)
	p.reportStage(SQLHealthCheckQueryStage, time.Since(queryStart), tags, err)
	if err != nil {
		return false, &sqlHealthProbeError{stage: SQLHealthCheckQueryStage, err: err}
	}
	return inRecovery, nil
}

func (p registrySQLHealthProber) reportStage(stage SQLHealthCheckStage, elapsed time.Duration, tags []string, err error) {
	if p.hooks.StageFinished != nil {
		p.hooks.StageFinished(stage, elapsed, tags, err)
	}
}

// SQLHealthChecker owns dedicated probe pools and gathers concurrent SQL snapshots.
type SQLHealthChecker struct {
	pools        *SQLHealthCheckPools
	prober       SQLHealthProber
	runWithRetry SQLHealthCheckRunner
	hooks        SQLHealthCheckHooks
}

// NewSQLHealthChecker constructs a checker. When Prober is nil it uses the dedicated pool
// registry. When RunWithRetry is nil each instance is probed once.
func NewSQLHealthChecker(ctx context.Context, poolCfg SQLHealthCheckPoolsConfig, cfg SQLHealthCheckerConfig) *SQLHealthChecker {
	pools := cfg.Pools
	if pools == nil {
		pools = NewSQLHealthCheckPools(ctx, poolCfg)
	}
	prober := cfg.Prober
	if prober == nil {
		prober = registrySQLHealthProber{pools: pools, hooks: cfg.Hooks}
	}
	return &SQLHealthChecker{
		pools:        pools,
		prober:       prober,
		runWithRetry: cfg.RunWithRetry,
		hooks:        cfg.Hooks,
	}
}

// Close closes all dedicated probe pools.
func (c *SQLHealthChecker) Close() { c.pools.Close() }

// RemoveIP closes all dedicated pools for one concrete instance.
func (c *SQLHealthChecker) RemoveIP(ip string) { c.pools.RemoveIP(ip) }

// Check probes every IP concurrently. patroniHealth must be a stable comparison snapshot.
func (c *SQLHealthChecker) Check(
	ctx context.Context,
	ips []string,
	connStringForIP func(string) string,
	patroniHealth map[string]bool,
	tags []string,
) SQLHealthCheckOutcome {
	tags = append(slices.Clone(tags), "pool_role:primary")
	results := make([]SQLHealthCheckResult, len(ips))
	var wg sync.WaitGroup
	for index, ip := range ips {
		wg.Add(1)
		go func(index int, ip string) {
			defer wg.Done()
			results[index] = c.checkIP(ctx, ip, connStringForIP(ip), patroniHealth[ip], tags)
		}(index, ip)
	}
	wg.Wait()

	primaryIPs := primaryIPsFromSQLResults(results)
	if c.hooks.PoolCount != nil {
		c.hooks.PoolCount(c.pools.Len(), tags)
	}
	if len(primaryIPs) > 1 && c.hooks.MultiplePrimaries != nil {
		c.hooks.MultiplePrimaries(primaryIPs, tags)
	}
	return SQLHealthCheckOutcome{Results: results, PrimaryIPs: primaryIPs}
}

func (c *SQLHealthChecker) checkIP(
	ctx context.Context,
	ip, connString string,
	patroniHealthy bool,
	tags []string,
) SQLHealthCheckResult {
	start := time.Now()
	metricTags := append(slices.Clone(tags), "ip:"+ip)
	var inRecovery bool
	attempt := func() error {
		var err error
		inRecovery, err = c.prober.Probe(ctx, ip, connString, metricTags)
		return err
	}
	var err error
	if c.runWithRetry == nil {
		err = attempt()
	} else {
		err = c.runWithRetry(ctx, metricTags, attempt)
	}
	result := SQLHealthCheckResult{
		IP:         ip,
		InRecovery: inRecovery,
		Err:        err,
		Stage:      sqlHealthCheckResultStage(err),
	}
	if c.hooks.ProbeFinished != nil {
		c.hooks.ProbeFinished(result, patroniHealthy, time.Since(start), metricTags)
	}
	return result
}

func primaryIPsFromSQLResults(results []SQLHealthCheckResult) []string {
	primaryIPs := make([]string, 0, len(results))
	for _, result := range results {
		if result.HealthyPrimary() {
			primaryIPs = append(primaryIPs, result.IP)
		}
	}
	slices.Sort(primaryIPs)
	return primaryIPs
}

func sqlHealthCheckResultStage(err error) SQLHealthCheckStage {
	var probeErr *sqlHealthProbeError
	if errors.As(err, &probeErr) {
		return probeErr.stage
	}
	// Successful checks and injected-prober errors reached the query stage.
	return SQLHealthCheckQueryStage
}

type sqlHealthCheckSQLStateError interface {
	SQLState() string
}

// IsSQLHealthCheckRetriable reports whether retrying an error can provide new information.
// PostgreSQL crash-shutdown and cannot-connect-now responses are conclusive for the iteration.
func IsSQLHealthCheckRetriable(err error) bool {
	var sqlStateErr sqlHealthCheckSQLStateError
	if errors.As(err, &sqlStateErr) {
		switch sqlStateErr.SQLState() {
		case sqlHealthCheckCrashShutdown, sqlHealthCheckCannotConnectNow:
			return false
		}
	}
	return true
}
