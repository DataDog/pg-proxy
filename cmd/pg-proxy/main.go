// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

// Command pg-proxy runs the OSS PostgreSQL transaction pooler.
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	pgproxy "github.com/DataDog/pg-proxy"
	"github.com/DataDog/pg-proxy/pkg/backend"
	pgconfig "github.com/DataDog/pg-proxy/pkg/config"
	"github.com/DataDog/pg-proxy/pkg/limiter"
	"github.com/DataDog/pg-proxy/pkg/listener"
	"github.com/DataDog/pg-proxy/pkg/pool"
	"github.com/DataDog/pg-proxy/pkg/protocol"
	"github.com/DataDog/pg-proxy/pkg/server"
	"github.com/DataDog/pg-proxy/pkg/session"
	"github.com/DataDog/pg-proxy/pkg/topology"
)

const (
	defaultShutdownGracePeriod       = 30 * time.Second
	defaultMaxSessions         int32 = 100_000
	defaultMaxUserSessions     int32 = 10_000
)

// config holds validated startup settings for the OSS binary.
type config struct {
	telemetry           telemetryConfig
	telemetryWarnings   []string
	listenAddress       string
	messageBufferSize   int
	shutdownGracePeriod time.Duration
	sessionLimits       limiter.Params
	tlsConfig           *tls.Config
	requireTLS          bool
	authenticate        authenticationHook
	databases           map[string]*pgxpool.Config
	clusters            map[string]pgconfig.Cluster // Primary and replica database names.
	logicalDatabases    map[string]topology.LogicalDatabase
}

func main() {
	cfg, err := parseConfig(os.Args[1:])
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		slog.Error("invalid configuration", "error", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runWithTelemetry(ctx, cfg); err != nil {
		os.Exit(1)
	}
}

func (c config) validate() error {
	if err := c.validateListener(c.authenticate != nil); err != nil {
		return err
	}
	if c.sessionLimits.GlobalLimit < 0 {
		return errors.New("proxy.max_sessions must not be negative")
	}
	if c.sessionLimits.DefaultServiceMax < 0 {
		return errors.New("proxy.default_max_user_sessions must not be negative")
	}
	for _, quota := range c.sessionLimits.Quotas {
		if quota.Max < 0 {
			return errors.New("users[].max_sessions must not be negative")
		}
	}
	if c.shutdownGracePeriod < 0 {
		return errors.New("proxy.shutdown_grace_period must be positive")
	}
	if c.messageBufferSize < 0 {
		return errors.New("postgres_msg_buffer_size must not be negative")
	}
	if len(c.databases) == 0 || len(c.clusters) == 0 || len(c.logicalDatabases) == 0 {
		return errors.New("databases, clusters, and logical_databases must not be empty")
	}
	usedDatabases := make(map[string]bool, len(c.databases))
	for name, cluster := range c.clusters {
		if name == "" || c.databases[cluster.Primary] == nil {
			return errors.New("each cluster must have a name and refer to a configured primary database")
		}
		usedDatabases[cluster.Primary] = true
		seen := make(map[string]bool, len(cluster.Replicas))
		for _, replica := range cluster.Replicas {
			if c.databases[replica] == nil {
				return errors.New("each cluster replica must refer to a configured database")
			}
			if replica == cluster.Primary || seen[replica] {
				return errors.New("cluster replicas must be unique and differ from the cluster primary")
			}
			seen[replica] = true
			usedDatabases[replica] = true
		}
	}
	for name, logical := range c.logicalDatabases {
		if _, exists := c.clusters[logical.Cluster]; name == "" || !exists {
			return errors.New("each logical database must have a name and refer to a configured cluster")
		}
		for cluster := range logical.ReadClusters {
			if _, exists := c.clusters[cluster]; !exists {
				return errors.New("each read cluster must refer to a configured cluster")
			}
		}
	}
	for name := range c.databases {
		if !usedDatabases[name] {
			return errors.New("each database must be used by a cluster")
		}
	}
	return nil
}

// routingWarningsFor reports valid but likely wrong routing settings. Replicas
// and read clusters can intentionally use other databases or roles, so these
// are warnings, not errors. Messages contain only names from the file.
func routingWarningsFor(c config) []string {
	var warnings []string
	for _, name := range slices.Sorted(maps.Keys(c.clusters)) {
		cluster := c.clusters[name]
		for _, replica := range cluster.Replicas {
			subject := fmt.Sprintf("cluster %q: replica %q", name, replica)
			warnings = append(warnings, connectionWarnings(subject, c.databases[replica], cluster.Primary, c.databases[cluster.Primary])...)
		}
	}
	for _, name := range slices.Sorted(maps.Keys(c.logicalDatabases)) {
		logical := c.logicalDatabases[name]
		mainPrimary := c.clusters[logical.Cluster].Primary
		for _, cluster := range slices.Sorted(maps.Keys(logical.ReadClusters)) {
			if cluster != logical.Cluster {
				primary := c.clusters[cluster].Primary
				subject := fmt.Sprintf("logical database %q: read cluster %q primary %q", name, cluster, primary)
				warnings = append(warnings, connectionWarnings(subject, c.databases[primary], mainPrimary, c.databases[mainPrimary])...)
			}
		}
		switch {
		case len(logical.ReadClusters) == 0:
		case !readsCanLeavePrimary(c, logical):
			warnings = append(warnings, fmt.Sprintf("logical database %q: read_clusters cannot send reads away from the primary of cluster %q", name, logical.Cluster))
		case topology.TotalWeight(logical.ReadClusters) == 0:
			warnings = append(warnings, fmt.Sprintf("logical database %q: all read_clusters weights are 0, so reads use cluster %q and its replicas", name, logical.Cluster))
		}
	}
	return warnings
}

// connectionWarnings compares the dbname and username of got, which subject
// describes, with want, the settings of database reference.
func connectionWarnings(subject string, got *pgxpool.Config, reference string, want *pgxpool.Config) []string {
	var warnings []string
	if got.ConnConfig.Database != want.ConnConfig.Database {
		warnings = append(warnings, fmt.Sprintf("%s uses dbname %q, but database %q uses %q", subject, got.ConnConfig.Database, reference, want.ConnConfig.Database))
	}
	if got.ConnConfig.User != want.ConnConfig.User {
		warnings = append(warnings, fmt.Sprintf("%s uses a different username than database %q", subject, reference))
	}
	return warnings
}

// readsCanLeavePrimary reports whether read routing can send a read away from
// the primary of the logical database's cluster. With all weights at 0, reads
// use that cluster, so only its replicas count.
func readsCanLeavePrimary(c config, logical topology.LogicalDatabase) bool {
	hasReplicas := len(c.clusters[logical.Cluster].Replicas) > 0
	if topology.TotalWeight(logical.ReadClusters) == 0 {
		return hasReplicas
	}
	for cluster, read := range logical.ReadClusters {
		if topology.Weight(read) > 0 && (cluster != logical.Cluster || hasReplicas) {
			return true
		}
	}
	return false
}

func run(ctx context.Context, cfg config) error {
	ctx, stop := context.WithCancel(ctx)
	defer stop()
	proxy, err := newProxy(ctx, cfg)
	if err != nil {
		return err
	}
	pgproxy.Logger().Info("proxy ready", "address", proxy.server.Addr(), "clusters", len(cfg.clusters), "client_authentication", cfg.authenticate != nil)
	grace := cfg.shutdownGracePeriod
	if grace == 0 {
		grace = defaultShutdownGracePeriod
	}
	return serve(ctx, stop, proxy.server, proxy.sessions, proxy.inventory.Close, grace)
}

type localProxy struct {
	server    *server.Server
	sessions  *sessionRegistry
	inventory *staticInventory
}

func newProxy(ctx context.Context, cfg config) (*localProxy, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	for _, warning := range routingWarningsFor(cfg) {
		pgproxy.Logger().Warn("check routing configuration", "reason", warning)
	}
	if cfg.messageBufferSize == 0 {
		cfg.messageBufferSize = topology.DefaultPostgresMessageBufferSize
	}
	// TODO: support configuration refresh through the library Warden.
	// File changes currently require a restart.
	inventory := &staticInventory{
		logicalDatabases: cfg.logicalDatabases,
		backends:         make(map[string]*clusterBackend, len(cfg.clusters)),
	}
	ready := false
	defer func() {
		if !ready {
			inventory.Close()
		}
	}()
	// Open clusters concurrently so startup waits for the slowest check, not
	// the sum of all checks.
	names := slices.Sorted(maps.Keys(cfg.clusters))
	runtimes := make([]*backend.Runtime, len(names))
	errs := make([]error, len(names))
	var opening sync.WaitGroup
	for i, name := range names {
		opening.Go(func() { runtimes[i], errs[i] = openBackend(ctx, cfg, name) })
	}
	opening.Wait()
	for i, runtime := range runtimes {
		if runtime != nil {
			inventory.backends[names[i]] = &clusterBackend{runtime}
		}
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	users := make(map[string]string, len(cfg.logicalDatabases))
	for name, logical := range cfg.logicalDatabases {
		users[name] = cfg.databases[cfg.clusters[logical.Cluster].Primary].ConnConfig.User
	}
	sessions := newSessionRegistry()
	authenticate := cfg.authenticate
	if authenticate == nil {
		authenticate = localUser(users)
	}
	proxy, err := server.NewServer(ctx, server.Config{
		ListenAddress:              cfg.listenAddress,
		TLSConfig:                  cfg.tlsConfig,
		RequireTLS:                 cfg.requireTLS,
		AuthenticationMessageLimit: 16 << 10,
		StartupTimeout:             10 * time.Second,
		MaxPendingConnections:      100,
		Admission:                  limiter.New(cfg.sessionLimitParams()),
		Authenticate:               authenticate,
		Sessions:                   sessions,
		Runtime: server.RuntimeConfig{
			Inventory:             inventory,
			EnableTraceParameters: cfg.telemetry.CaptureBindParameters,
			Settings: topology.RuntimeConfig{
				PostgresMessageBufferSize: cfg.messageBufferSize,
			},
		},
	})
	if err != nil {
		return nil, err
	}
	if cfg.telemetry.Metrics != metricNone {
		inventory.StartStats()
	}
	ready = true
	return &localProxy{server: proxy, sessions: sessions, inventory: inventory}, nil
}

// openBackend opens the primary and replica pools of cluster name concurrently.
// Replicas are static: there is no health worker. A failed replica checkout
// falls back to the cluster primary at runtime, subject to the backend's
// fallback rate limit, but each replica must accept a connection at startup so
// that a configuration error stops the proxy. Clients receive the primary's
// parameter statuses, so the replicas' statuses are not kept.
func openBackend(ctx context.Context, cfg config, name string) (*backend.Runtime, error) {
	cluster := cfg.clusters[name]
	databases := append([]string{cluster.Primary}, cluster.Replicas...)
	pools := make([]pool.Pool, len(databases))
	statuses := make([]map[string]string, len(databases))
	errs := make([]error, len(databases))
	var opening sync.WaitGroup
	for i, database := range databases {
		role := "primary"
		if i > 0 {
			role = "replica"
		}
		opening.Go(func() {
			pools[i], statuses[i], errs[i] = openPool(ctx, cfg.databases[database], name, role, database)
		})
	}
	opening.Wait()
	if err := errors.Join(errs...); err != nil {
		for _, opened := range pools {
			if opened != nil {
				opened.Close()
			}
		}
		return nil, err
	}
	return backend.New(backend.Config{
		ClusterName: name, PrimaryDatabaseName: cfg.databases[cluster.Primary].ConnConfig.Database,
		Primary: pools[0], Replicas: pools[1:], ParameterStatuses: statuses[0],
	}), nil
}

// openPool creates the pgx pool for database, the primary or a replica (role)
// of cluster, and checks that it accepts a connection. It returns the backend
// parameter statuses from that check. Errors omit the pgx cause of a failed
// pool creation because it can contain connection settings.
func openPool(ctx context.Context, settings *pgxpool.Config, cluster, role, database string) (pool.Pool, map[string]string, error) {
	pgxPool, err := pgxpool.NewWithConfig(ctx, settings)
	if err != nil {
		return nil, nil, fmt.Errorf("could not create pool for backend %q %s %q", cluster, role, database)
	}
	opened := pool.NewPGXPool(pgxPool, pool.PGXHooks{})
	parameters, err := backend.RetrieveParameterStatuses(opened, database)
	if err != nil {
		opened.Close()
		return nil, nil, fmt.Errorf("connect to backend %q %s: %w", cluster, role, err)
	}
	return opened, parameters, nil
}

// sessionLimitParams also applies defaults for callers constructing config in Go.
func (c config) sessionLimitParams() limiter.Params {
	params := c.sessionLimits
	if params.GlobalLimit == 0 {
		params.GlobalLimit = defaultMaxSessions
	}
	if params.DefaultServiceMax == 0 {
		params.DefaultServiceMax = defaultMaxUserSessions
	}
	return params
}

// Explicitly close the socket to wake Accept, then drain sessions before pools.
// Bound the wait so a client with an unfinished transaction cannot prevent exit.
func serve(ctx context.Context, stop context.CancelFunc, proxy *server.Server, sessions *sessionRegistry, closeBackend func(), timeout time.Duration) error {
	runDone := make(chan error, 1)
	go func() { runDone <- proxy.Run() }()
	var runErr error
	finished := false
	select {
	case <-ctx.Done():
	case runErr = <-runDone:
		finished = true
	}
	stop()
	_ = proxy.Close()
	closed := make(chan struct{})
	go func() {
		if !finished {
			runErr = <-runDone
		}
		sessions.Wait()
		closeBackend()
		close(closed)
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-closed:
	case <-timer.C:
		return errors.New("shutdown timed out waiting for client sessions")
	}
	if errors.Is(runErr, net.ErrClosed) {
		return nil
	}
	return runErr
}

// Local trust requires the username configured for the selected database.
func localUser(users map[string]string) func(context.Context, listener.StartupRequest, listener.StartupResult[server.ResolvedStartup], protocol.Exchange) (listener.AuthenticationResult, error) {
	return func(_ context.Context, request listener.StartupRequest, _ listener.StartupResult[server.ResolvedStartup], _ protocol.Exchange) (listener.AuthenticationResult, error) {
		user, exists := users[request.Database]
		if !exists || request.User != user {
			return listener.AuthenticationResult{}, errors.New("client user must match the configured backend user")
		}
		if request.Options.PassthroughUser {
			return listener.AuthenticationResult{}, listener.ErrUnsupportedAuthOption
		}
		return listener.AuthenticationResult{Service: user}, nil
	}
}

type clusterBackend struct{ *backend.Runtime }

func (b *clusterBackend) SessionBackend() session.Backend { return b.Runtime }

type staticInventory struct {
	logicalDatabases map[string]topology.LogicalDatabase
	backends         map[string]*clusterBackend
}

// GetLogicalDatabase returns the static routing view. Read clusters were
// validated against the configured clusters at startup.
func (i *staticInventory) GetLogicalDatabase(name string) (topology.LogicalDatabase, bool) {
	database, exists := i.logicalDatabases[name]
	return database, exists
}

func (i *staticInventory) GetBackend(name string) (server.Backend, bool) {
	backend, exists := i.backends[name]
	return backend, exists
}

// StartStats starts periodic pool and waiting-session gauges; Close stops them.
func (i *staticInventory) StartStats() {
	for _, backend := range i.backends {
		backend.StartStats()
	}
}

func (i *staticInventory) Close() {
	for _, backend := range i.backends {
		backend.Close()
	}
}

type sessionRegistry struct {
	mu       sync.Mutex
	changed  *sync.Cond
	sessions map[string]session.SessionHandle
}

func newSessionRegistry() *sessionRegistry {
	r := &sessionRegistry{sessions: make(map[string]session.SessionHandle)}
	r.changed = sync.NewCond(&r.mu)
	return r
}

func (r *sessionRegistry) Register(id string, active session.SessionHandle) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sessions[id] = active
}

func (r *sessionRegistry) Delete(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.sessions, id)
	r.changed.Broadcast()
}

func (r *sessionRegistry) Get(id string) (session.SessionHandle, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	active, ok := r.sessions[id]
	return active, ok
}

func (r *sessionRegistry) Wait() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for len(r.sessions) != 0 {
		r.changed.Wait()
	}
}
