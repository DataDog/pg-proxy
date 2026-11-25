// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

// Package backend owns portable within-cluster backend routing and connection checkout.
package backend

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/time/rate"

	pgproxy "github.com/DataDog/pg-proxy"
	"github.com/DataDog/pg-proxy/pkg/link"
	"github.com/DataDog/pg-proxy/pkg/pool"
	"github.com/DataDog/pg-proxy/pkg/session"
)

const (
	// Route a read to primary if it's within 100ms of the last write
	readAfterWriteThreshold         = time.Millisecond * 100
	acquireConnectionTimeout        = time.Second * 1
	replicaAcquireConnectionTimeout = time.Millisecond * 50
	// Route 10% of reads to the primary
	percentageReadsToPrimary = 10
	// Rate limiting for fallback to primary
	replicaFallbackRateLimit = 100.0
	replicaFallbackRateBurst = 100
)

var parameterStatusKeys = []string{
	"server_version", "server_encoding", "client_encoding",
	"application_name", "default_transaction_read_only", "in_hot_standby", "is_superuser",
	"session_authorization", "DateStyle", "IntervalStyle", "TimeZone", "integer_datetimes",
	"standard_conforming_strings",
}

// RoleSessionFactory constructs the embedder's role-management implementation.
// Returning nil disables role management for that checkout.
type RoleSessionFactory func(roleName string) link.RoleSession

// LinkFactory wraps an acquired connection as a session link.
type LinkFactory func(*pgxpool.Conn, link.Options) (link.Link, error)

// LinkDecorator wraps a link after checkout with the pool that supplied the
// connection. Callers that later need another connection to the same backend
// (for example DBM replay) use that pool instead of routing again.
// Nil leaves the link unchanged. The backend calls this only when
// request.DecorateLink is true, independently of session notification hooks.
type LinkDecorator func(link.Link, pool.Pool, *pgxpool.Conn, session.LinkRequest) link.Link

// Config configures one logical cluster backend.
type Config struct {
	ClusterName         string
	PrimaryDatabaseName string
	Primary             pool.Pool
	Replicas            []pool.Pool
	ParameterStatuses   map[string]string
	LoadShedder         session.LoadShedder
	RoleSessionFactory  RoleSessionFactory
	LinkFactory         LinkFactory
	LinkDecorator       LinkDecorator
}

// Runtime owns routing and connection checkout for one logical database cluster.
type Runtime struct {
	selector            *PoolSelector
	parameterStatuses   map[string]string
	clusterName         string
	clusterMetricTag    string
	primaryDatabaseName string
	sessionsWaiting     atomic.Int32
	stopExportStats     chan struct{}
	startStatsOnce      sync.Once
	closeOnce           sync.Once
	fallbackRateLimiter *rate.Limiter
	loadShedder         session.LoadShedder
	roleSessionFactory  RoleSessionFactory
	linkFactory         LinkFactory
	linkDecorator       LinkDecorator
}

var _ session.Backend = (*Runtime)(nil)

// PoolSelector selects the primary or a replica pool for a checkout.
type PoolSelector struct {
	primary     pool.Pool
	replicas    []pool.Pool
	roundRobin  *pool.RoundRobinCounter
	randomIntFn func(int) int
}

// New creates a backend runtime. The caller retains responsibility for closing it.
func New(cfg Config) *Runtime {
	loadShedder := cfg.LoadShedder
	if loadShedder == nil {
		loadShedder = noopLoadShedder{}
	}
	linkFactory := cfg.LinkFactory
	if linkFactory == nil {
		linkFactory = link.New
	}
	return &Runtime{
		selector: &PoolSelector{
			primary:     cfg.Primary,
			replicas:    append([]pool.Pool(nil), cfg.Replicas...),
			roundRobin:  &pool.RoundRobinCounter{},
			randomIntFn: rand.Intn,
		},
		parameterStatuses:   cfg.ParameterStatuses,
		clusterName:         cfg.ClusterName,
		clusterMetricTag:    "database_cluster:" + cfg.ClusterName,
		primaryDatabaseName: cfg.PrimaryDatabaseName,
		stopExportStats:     make(chan struct{}),
		fallbackRateLimiter: rate.NewLimiter(rate.Limit(replicaFallbackRateLimit), replicaFallbackRateBurst),
		loadShedder:         loadShedder,
		roleSessionFactory:  cfg.RoleSessionFactory,
		linkFactory:         linkFactory,
		linkDecorator:       cfg.LinkDecorator,
	}
}

// PrimaryPool returns the primary pool for app-owned lifecycle integrations such as probers.
func (r *Runtime) PrimaryPool() pool.Pool { return r.selector.primary }

// ParameterStatuses returns the cached primary backend parameter statuses.
func (r *Runtime) ParameterStatuses() map[string]string { return r.parameterStatuses }

// LoadShedder returns the checkout-scoped load shedder.
func (r *Runtime) LoadShedder() session.LoadShedder { return r.loadShedder }

// SessionsWaiting returns the number of sessions currently waiting for a connection.
func (r *Runtime) SessionsWaiting() int32 { return r.sessionsWaiting.Load() }

// CanPerformDynamicRouting reports whether dynamic routing has a replica to target.
func (r *Runtime) CanPerformDynamicRouting(dynamicRoutingEnabled bool) bool {
	if dynamicRoutingEnabled {
		if len(r.selector.replicas) > 0 {
			return true
		}
		pgproxy.Logger().Warn(fmt.Sprintf("dynamic routing is enabled, but no replica pools were found for cluster %s", r.clusterName))
	}
	return false
}

// Acquire implements session.Backend by acquiring a pool connection and constructing a link.
func (r *Runtime) Acquire(ctx context.Context, request session.LinkRequest) (link.Link, error) {
	start := time.Now()

	// Own the tags: link setup appends cluster/routing information, while the
	// portable session keeps its base metric tags for the rest of its lifetime.
	sessionLinkTags := r.buildSessionLinkMetricTags(request.MetricTags, request.ForReadQuery)

	// Note that if dynamic routing acquires a replica conn for a qualified parse,
	// then all subsequent queries in the tx will be forwarded to the replica
	conn, selectedPool, rerouted, err := r.acquireConnection(ctx, request.DynamicRoutingEnabled && request.ForReadQuery, request.SessionLastWrite)
	// Emitting the metric before the error check to maintain existing behaviour
	// and to have observability on the number of connections that are rerouted
	// even if they fail
	if rerouted {
		_ = pgproxy.Metrics().Count("pg_proxy.sessions.connections.rerouted", 1, sessionLinkTags, 1)
	}
	// Same for the tag, as we want the trace to be marked as rerouted whether it failed or not
	if request.TraceState != nil {
		request.TraceState.SetTag("rerouted", rerouted)
	}
	if err != nil {
		outcome := "error"
		if ctx.Err() != nil {
			// Distinguish context-cancelled acquires (e.g. client disconnect
			// monitor) from genuine pool/PG failures. We don't suppress the
			// metric entirely because the context could be cancelled for other
			// reasons in the future that we'd want visibility into.
			outcome = "ctx_cancelled"
		}
		_ = pgproxy.Metrics().Count("pg_proxy.sessions.connections", 1, append(sessionLinkTags, "outcome:"+outcome), 1)
		return nil, err
	}

	_ = pgproxy.Metrics().Count("pg_proxy.sessions.connections", 1, append(sessionLinkTags, "outcome:successful"), 1)
	_ = pgproxy.Metrics().Distribution(pgproxy.MetricConnectWait, float64(time.Since(start).Microseconds()), sessionLinkTags, 1)

	opts := link.Options{
		MaxPreparedStmts: request.MaxPreparedStmts,
		MetricTags:       sessionLinkTags,
	}
	if request.PassthroughRoleName != "" && r.roleSessionFactory != nil {
		opts.Role = r.roleSessionFactory(request.PassthroughRoleName)
	}
	backendLink, err := r.linkFactory(conn, opts)
	if err != nil {
		return nil, err
	}
	if r.linkDecorator != nil && request.DecorateLink {
		backendLink = r.linkDecorator(backendLink, selectedPool, conn, request)
	}
	return backendLink, nil
}

func (r *Runtime) buildSessionLinkMetricTags(baseTags []string, forRead bool) []string {
	readTag := "read:false"
	if forRead {
		readTag = "read:true"
	}
	tags := make([]string, 0, len(baseTags)+2)
	tags = append(tags, baseTags...)
	return append(tags, r.clusterMetricTag, readTag)
}

// AcquireConnection returns a connection and whether it came from a replica pool.
func (r *Runtime) AcquireConnection(ctx context.Context, replicaReroutingEligible bool, sessionLastWrite time.Time) (*pgxpool.Conn, bool, error) {
	conn, _, rerouted, err := r.acquireConnection(ctx, replicaReroutingEligible, sessionLastWrite)
	return conn, rerouted, err
}

func (r *Runtime) acquireConnection(ctx context.Context, replicaReroutingEligible bool, sessionLastWrite time.Time) (*pgxpool.Conn, pool.Pool, bool, error) {
	r.sessionsWaiting.Add(1)
	defer r.decrementSessionsWaiting()

	if replicaReroutingEligible {
		selected, rerouted := r.selector.selectPool(sessionLastWrite)
		if rerouted {
			replicaCtx, cancel := context.WithTimeout(ctx, replicaAcquireConnectionTimeout)
			defer cancel()
			conn, err := selected.Acquire(replicaCtx)
			if err != nil {
				pgproxy.Logger().Error(fmt.Sprintf("failed to acquire connection from replica pool: %v", err))
				conn, err = r.fallbackToPrimary(ctx)
				return conn, r.selector.primary, false, err
			}
			return conn, selected, true, nil
		}
		conn, err := selected.Acquire(ctx)
		return conn, selected, rerouted, err
	}

	conn, err := r.selector.primary.Acquire(ctx)
	return conn, r.selector.primary, false, err
}

func (r *Runtime) decrementSessionsWaiting() {
	value := r.sessionsWaiting.Load()
	if value <= 0 {
		pgproxy.Logger().Error(fmt.Sprintf("attempting to decrement sessionsWaiting counter to below 0: %v", value))
		r.sessionsWaiting.Store(0)
		return
	}
	r.sessionsWaiting.Add(-1)
}

func (r *Runtime) fallbackToPrimary(ctx context.Context) (*pgxpool.Conn, error) {
	// Check rate limiter before falling back to primary
	if !r.fallbackRateLimiter.Allow() {
		_ = pgproxy.Metrics().Incr("pg_proxy.replica_acquire_fallback",
			[]string{
				fmt.Sprintf("database:%s", r.primaryDatabaseName),
				fmt.Sprintf("database_cluster:%s", r.clusterName),
				"outcome:rate_limited",
			}, 1)
		return nil, fmt.Errorf("primary database fallback rate limit exceeded for cluster:%s", r.clusterName)
	}

	_ = pgproxy.Metrics().Incr("pg_proxy.replica_acquire_fallback",
		[]string{
			fmt.Sprintf("database:%s", r.primaryDatabaseName),
			fmt.Sprintf("database_cluster:%s", r.clusterName),
			"outcome:allowed",
		}, 1)
	return r.selector.primary.Acquire(ctx)
}

// shouldReroute returns true if we should re-route a query to a replica pool
// only re-route when the query is deemed not to be a read-after-write and isn't part of the 10%
// of reads we want to send to the primary
func (p *PoolSelector) shouldReroute(sessionLastWrite time.Time) bool {
	return len(p.replicas) > 0 && p.randomIntFn(100) >= percentageReadsToPrimary
	//	&&	!isReadAfterWrite(sessionLastWrite)
	// Commented until we enable a config flag to enable read-after-write consistency
}

// isReadAfterWrite determines if a read query sent from a session was within the readAfterWriteThreshold of the
// last write query sent from that same session
func isReadAfterWrite(sessionLastWrite time.Time) bool {
	return !sessionLastWrite.IsZero() &&
		time.Since(sessionLastWrite) < readAfterWriteThreshold
}

// roundRobinSelect returns the next replica pool to query
// replicaPools is guaranteed to be non-empty by the caller
func (p *PoolSelector) roundRobinSelect(replicaPools []pool.Pool) pool.Pool {
	if len(replicaPools) == 1 {
		return replicaPools[0]
	}
	idx := p.roundRobin.Next(len(replicaPools))
	// This should never happen since we check the length of the list before calling Next,
	// but it's good to have the check here to ensure that we don't panic
	if idx == pool.InvalidIndex {
		pgproxy.Logger().Warn("round robin counter returned -1, returning first replica pool")
		return replicaPools[0]
	}
	return replicaPools[idx]
}

// selectPool returns a primary or replica pool to query and if the query will be rerouted to a replica pool
func (p *PoolSelector) selectPool(sessionLastWrite time.Time) (pool.Pool, bool) {
	if p.shouldReroute(sessionLastWrite) {
		return p.roundRobinSelect(p.replicas), true
	}
	return p.primary, false
}

// StartStats starts exporting sessions-waiting and primary pool stats every two seconds.
// Later calls are no-ops, and Close stops the export.
func (r *Runtime) StartStats() {
	r.startStatsOnce.Do(func() { go r.exportStats() })
}

func (r *Runtime) exportStats() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	tags := []string{
		fmt.Sprintf("database:%s", r.primaryDatabaseName),
		fmt.Sprintf("database_cluster:%s", r.clusterName),
	}
	for {
		select {
		case <-ticker.C:
			_ = pgproxy.Metrics().Gauge("pg_proxy.sessions.waiting", float64(r.SessionsWaiting()), tags, 1)
			r.selector.primary.ExportStats(tags)
		case <-r.stopExportStats:
			return
		}
	}
}

// Close stops stats export and closes all pools.
func (r *Runtime) Close() {
	r.closeOnce.Do(func() {
		close(r.stopExportStats)
		for _, replica := range r.selector.replicas {
			replica.Close()
		}
		r.selector.primary.Close()
	})
}

// RetrieveParameterStatuses checks a pool connection and caches startup parameter statuses.
func RetrieveParameterStatuses(p pool.Pool, poolName string) (map[string]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), acquireConnectionTimeout)
	defer cancel()
	// TODO replace this test conn with a readiness check
	testConn, err := p.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to open test conn to postgres for %s: %w", poolName, err)
	}
	defer testConn.Release()
	if err = testConn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping test conn to postgres for %s: %w", poolName, err)
	}

	// this is a lower level pgconn.PgConn object
	rawConn := testConn.Conn().PgConn()
	statuses := make(map[string]string)
	for _, key := range parameterStatusKeys {
		statuses[key] = rawConn.ParameterStatus(key)
	}
	return statuses, nil
}

type noopLoadShedder struct{}

func (noopLoadShedder) Run(_ context.Context, fn func() error, _ session.Priority) (error, error) {
	return fn(), nil
}
