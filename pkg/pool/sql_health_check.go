// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package pool

import (
	"context"
	"errors"
	"maps"
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

	sqlHealthCheckAdminShutdown    = "57P01"
	sqlHealthCheckCrashShutdown    = "57P02"
	sqlHealthCheckCannotConnectNow = "57P03"
	sqlHealthCheckInvalidPassword  = "28P01"
)

// SQLHealthProber reports pg_is_in_recovery() for one concrete PostgreSQL instance.
type SQLHealthProber interface {
	Probe(ctx context.Context, ip, connString string, tags []string) (bool, error)
}

// SQLHealthCheckStage identifies one stage of a SQL health probe.
type SQLHealthCheckStage string

// SQLHealthCheckResult is one instance's SQL health result.
type SQLHealthCheckResult struct {
	// The interval covers this IP's probe, including retries. It is not the
	// credential round's completion time, which may wait for other IPs.
	StartedAt   time.Time
	CompletedAt time.Time
	IP          string
	InRecovery  bool
	Err         error
	Stage       SQLHealthCheckStage
}

// HealthyPrimary reports whether the probe conclusively observed a writable primary.
func (r SQLHealthCheckResult) HealthyPrimary() bool {
	return r.Err == nil && !r.InRecovery
}

// SQLHealthCheckOutcome is one complete concurrent cluster snapshot.
type SQLHealthCheckOutcome struct {
	// The worker stamps the routing period reserved before discovery/probing. Raw
	// comparison results remain useful even if routing changes before completion.
	routingGeneration uint64
	// StartedAt is captured before probing. When combining credential snapshots,
	// retain the earliest start time of all included evidence. A zero value cannot
	// establish freshness after Patroni restores a hybrid route.
	StartedAt  time.Time
	Results    []SQLHealthCheckResult
	PrimaryIPs []string
}

// sqlHealthCheckAggregation summarizes completed SQL evidence across credential targets.
// Proven older successful roles are removed first. Health is then optimistic per
// address; Outcome keeps errors and overlapping/unknown-time roles so routing can
// reject unresolved conflicts. Each credential's SQLHealthResultsSnapshot retains
// all raw results for diagnosis.
type sqlHealthCheckAggregation struct {
	Outcome SQLHealthCheckOutcome
	Health  map[string]bool
}

func aggregateSQLHealthCheckOutcomes(outcomes []SQLHealthCheckOutcome) sqlHealthCheckAggregation {
	results := make([]SQLHealthCheckResult, 0)
	var startedAt time.Time
	for i, outcome := range outcomes {
		results = append(results, outcome.Results...)
		// Unknown start times must remain unknown; later evidence cannot make
		// an older credential snapshot fresh enough to supersede recovery.
		if i == 0 || outcome.StartedAt.Before(startedAt) {
			startedAt = outcome.StartedAt
		}
	}
	aggregation := aggregateSQLHealthCheckResults(results)
	aggregation.Outcome.StartedAt = startedAt
	return aggregation
}

func aggregateSQLHealthCheckResults(results []SQLHealthCheckResult) sqlHealthCheckAggregation {
	results = filterSupersededSQLRoles(results)
	aggregation := sqlHealthCheckAggregation{
		Outcome: SQLHealthCheckOutcome{Results: slices.Clone(results)},
		Health:  make(map[string]bool),
	}
	primaryIPs := make(map[string]struct{})
	for _, result := range results {
		if result.HealthyPrimary() {
			aggregation.Health[result.IP] = true
			primaryIPs[result.IP] = struct{}{}
		} else if result.Err == nil || isConclusiveSQLUnroutableError(result.Err) {
			// A completed determinate result is enough to classify the address as non-primary.
			// Indeterminate results remain unknown unless another credential supplies
			// determinate evidence for the same address.
			if _, recorded := aggregation.Health[result.IP]; recorded {
				continue
			}
			aggregation.Health[result.IP] = false
		}
	}
	aggregation.Outcome.PrimaryIPs = slices.Collect(maps.Keys(primaryIPs))
	slices.Sort(aggregation.Outcome.PrimaryIPs)
	return aggregation
}

// filterSupersededSQLRoles drops a successful role only when another successful
// probe of the same IP started strictly after it finished. Arrival order and
// whole-credential timestamps cannot establish that ordering. Keep all errors,
// and keep observations with missing or invalid timing conservatively.
// The input is never modified; raw credential snapshots remain unchanged.
func filterSupersededSQLRoles(results []SQLHealthCheckResult) []SQLHealthCheckResult {
	latestStarts := make(map[string]time.Time)
	for _, result := range results {
		if result.Err == nil && result.hasProbeInterval() && result.StartedAt.After(latestStarts[result.IP]) {
			latestStarts[result.IP] = result.StartedAt
		}
	}
	var filtered []SQLHealthCheckResult
	for i, result := range results {
		if result.Err == nil && result.hasProbeInterval() && latestStarts[result.IP].After(result.CompletedAt) {
			if filtered == nil {
				filtered = make([]SQLHealthCheckResult, 0, len(results)-1)
				filtered = append(filtered, results[:i]...)
			}
			continue
		}
		if filtered != nil {
			filtered = append(filtered, result)
		}
	}
	if filtered == nil {
		return results
	}
	return filtered
}

func (r SQLHealthCheckResult) hasProbeInterval() bool {
	return !r.StartedAt.IsZero() && !r.CompletedAt.IsZero() && !r.CompletedAt.Before(r.StartedAt)
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

// ReleasePools closes all dedicated probe pools. A later check recreates them as needed.
func (c *SQLHealthChecker) ReleasePools() { c.pools.Close() }

// Close closes all dedicated probe pools.
func (c *SQLHealthChecker) Close() { c.ReleasePools() }

// RemoveIP closes all dedicated pools for one concrete instance.
func (c *SQLHealthChecker) RemoveIP(ip string) { c.pools.RemoveIP(ip) }

// RetainIPs closes dedicated pools for addresses outside the current retained SQL snapshot.
// The caller must ensure no health check is still using a pool that may be removed.
func (c *SQLHealthChecker) RetainIPs(ips []string) { c.pools.RetainIPs(ips) }

// Check probes every IP concurrently and waits for all results. patroniHealth must be a stable
// comparison snapshot. onResult is called as each IP's post-retry result completes and must not
// block.
func (c *SQLHealthChecker) Check(
	ctx context.Context,
	ips []string,
	connStringForIP func(string) string,
	patroniHealth map[string]bool,
	tags []string,
	onResult func(SQLHealthCheckResult),
) SQLHealthCheckOutcome {
	startedAt := time.Now()
	tags = append(slices.Clone(tags), "pool_role:primary")
	results := make([]SQLHealthCheckResult, len(ips))
	var wg sync.WaitGroup
	for index, ip := range ips {
		wg.Add(1)
		go func(index int, ip string) {
			defer wg.Done()
			result := c.checkIP(ctx, ip, connStringForIP(ip), patroniHealth[ip], tags)
			results[index] = result
			if onResult != nil {
				onResult(result)
			}
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
	return SQLHealthCheckOutcome{StartedAt: startedAt, Results: results, PrimaryIPs: primaryIPs}
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
		StartedAt:   start,
		CompletedAt: time.Now(),
		IP:          ip,
		InRecovery:  inRecovery,
		Err:         err,
		Stage:       sqlHealthCheckResultStage(err),
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

// IsSQLHealthCheckPasswordAuthenticationError reports whether PostgreSQL rejected the supplied
// password. This is deterministic for a connection string and instance.
func IsSQLHealthCheckPasswordAuthenticationError(err error) bool {
	var sqlStateErr sqlHealthCheckSQLStateError
	return errors.As(err, &sqlStateErr) && sqlStateErr.SQLState() == sqlHealthCheckInvalidPassword
}

// IsSQLHealthCheckRetriable reports whether retrying an error can provide new information.
// PostgreSQL administrative/crash-shutdown, cannot-connect-now, and invalid-password responses
// are conclusive for the iteration. Retrying shutdown evidence could replace it with a timeout
// before the routing banlist sees it.
func IsSQLHealthCheckRetriable(err error) bool {
	var sqlStateErr sqlHealthCheckSQLStateError
	if errors.As(err, &sqlStateErr) {
		switch sqlStateErr.SQLState() {
		case sqlHealthCheckAdminShutdown, sqlHealthCheckCrashShutdown, sqlHealthCheckCannotConnectNow, sqlHealthCheckInvalidPassword:
			return false
		}
	}
	return true
}
