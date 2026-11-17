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
	routingWarnings     []string
	listenAddress       string
	messageBufferSize   int
	shutdownGracePeriod time.Duration
	sessionLimits       limiter.Params
	tlsConfig           *tls.Config
	requireTLS          bool
	authenticate        authenticationHook
	databases           map[string]*pgxpool.Config
	clusters            map[string]string   // Cluster name to primary database name.
	replicas            map[string][]string // Cluster name to replica database names.
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
	for name, primary := range c.clusters {
		if name == "" || c.databases[primary] == nil {
			return errors.New("each cluster must have a name and refer to a configured primary database")
		}
		usedDatabases[primary] = true
	}
	for name, replicas := range c.replicas {
		seen := make(map[string]bool, len(replicas))
		for _, replica := range replicas {
			if c.databases[replica] == nil {
				return errors.New("each cluster replica must refer to a configured database")
			}
			if replica == c.clusters[name] || seen[replica] {
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
		for cluster, read := range logical.ReadClusters {
			if _, exists := c.clusters[cluster]; !exists {
				return errors.New("each read cluster must refer to a configured cluster")
			}
			if read == nil {
				return errors.New("each read cluster must have routing settings")
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
	// mismatch compares database with reference. subject describes database.
	mismatch := func(subject, database, reference string) {
		got, want := c.databases[database].ConnConfig, c.databases[reference].ConnConfig
		if got.Database != want.Database {
			warnings = append(warnings, fmt.Sprintf("%s uses dbname %q, but database %q uses %q", subject, got.Database, reference, want.Database))
		}
		if got.User != want.User {
			warnings = append(warnings, fmt.Sprintf("%s uses a different username than database %q", subject, reference))
		}
	}
	for _, cluster := range slices.Sorted(maps.Keys(c.replicas)) {
		for _, replica := range c.replicas[cluster] {
			mismatch(fmt.Sprintf("replica %q of cluster %q", replica, cluster), replica, c.clusters[cluster])
		}
	}
	for _, name := range slices.Sorted(maps.Keys(c.logicalDatabases)) {
		logical := c.logicalDatabases[name]
		if len(logical.ReadClusters) == 0 {
			continue
		}
		for _, cluster := range slices.Sorted(maps.Keys(logical.ReadClusters)) {
			if cluster != logical.Cluster {
				primary := c.clusters[cluster]
				mismatch(fmt.Sprintf("database %q, the primary of read cluster %q for logical database %q,", primary, cluster, name), primary, c.clusters[logical.Cluster])
			}
		}
		if topology.TotalWeight(logical.ReadClusters) == 0 {
			warnings = append(warnings, fmt.Sprintf("logical database %q: all read_clusters weights are 0, so reads use cluster %q, including its replicas", name, logical.Cluster))
		}
	}
	return warnings
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
	for _, name := range slices.Sorted(maps.Keys(cfg.clusters)) {
		settings := cfg.databases[cfg.clusters[name]]
		primary, parameters, err := openPool(ctx, settings, fmt.Sprintf("backend %q", name))
		if err != nil {
			return nil, err
		}
		replicas, err := newReplicaPools(ctx, cfg, name)
		if err != nil {
			primary.Close()
			return nil, err
		}
		inventory.backends[name] = &clusterBackend{backend.New(backend.Config{
			ClusterName: name, PrimaryDatabaseName: settings.ConnConfig.Database,
			Primary: primary, Replicas: replicas, ParameterStatuses: parameters,
		})}
	}
	users := make(map[string]string, len(cfg.logicalDatabases))
	for name, logical := range cfg.logicalDatabases {
		users[name] = cfg.databases[cfg.clusters[logical.Cluster]].ConnConfig.User
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

// openPool creates a pgx pool and checks that it accepts a connection. It
// returns the backend parameter statuses from that check. The label names the
// pool in errors. Errors omit the pgx cause of a failed pool creation because
// it can contain connection settings.
func openPool(ctx context.Context, settings *pgxpool.Config, label string) (*pool.PGXPool, map[string]string, error) {
	pgxPool, err := pgxpool.NewWithConfig(ctx, settings)
	if err != nil {
		return nil, nil, fmt.Errorf("could not create pool for %s", label)
	}
	opened := pool.NewPGXPool(pgxPool, pool.PGXHooks{})
	parameters, err := backend.RetrieveParameterStatuses(opened, label)
	if err != nil {
		opened.Close()
		return nil, nil, fmt.Errorf("connect to %s: %w", label, err)
	}
	return opened, parameters, nil
}

// newReplicaPools opens one pgx pool per replica database of cluster. Replicas
// are static: there is no health worker. A failed replica checkout falls back to
// the cluster primary at runtime, but each replica must accept a connection at
// startup so that a configuration error stops the proxy. Clients receive the
// primary's parameter statuses, so the replica's statuses are not kept.
func newReplicaPools(ctx context.Context, cfg config, cluster string) ([]pool.Pool, error) {
	var replicas []pool.Pool
	for _, name := range cfg.replicas[cluster] {
		replica, _, err := openPool(ctx, cfg.databases[name], fmt.Sprintf("replica %q of backend %q", name, cluster))
		if err != nil {
			for _, opened := range replicas {
				opened.Close()
			}
			return nil, err
		}
		replicas = append(replicas, replica)
	}
	return replicas, nil
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
