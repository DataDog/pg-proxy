// Command pg-proxy runs the OSS PostgreSQL transaction pooler.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"net/netip"
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

// config holds validated startup settings for the OSS binary.
type config struct {
	listenAddress     string
	messageBufferSize int
	databases         map[string]*pgxpool.Config
	clusters          map[string]string
	logicalDatabases  map[string]topology.LogicalDatabase
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
	pgproxy.Bootstrap(pgproxy.Dependencies{LogHandler: slog.NewTextHandler(os.Stderr, nil)})
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg); err != nil {
		slog.Error("pg-proxy stopped", "error", err)
		os.Exit(1)
	}
}

func (c config) validate() error {
	address, err := netip.ParseAddrPort(c.listenAddress)
	if err != nil || !address.Addr().IsLoopback() {
		return errors.New("proxy.listener_address must use a loopback IP: the OSS binary does not authenticate clients")
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
	for name, logical := range c.logicalDatabases {
		if _, exists := c.clusters[logical.Cluster]; name == "" || !exists {
			return errors.New("each logical database must have a name and refer to a configured cluster")
		}
	}
	for name := range c.databases {
		if !usedDatabases[name] {
			return errors.New("each database must be used by a cluster")
		}
	}
	return nil
}

func run(ctx context.Context, cfg config) error {
	ctx, stop := context.WithCancel(ctx)
	defer stop()
	proxy, err := newProxy(ctx, cfg)
	if err != nil {
		return err
	}
	slog.Info("local proxy ready; clients are trusted", "address", proxy.server.Addr(), "clusters", len(cfg.clusters))
	return serve(ctx, stop, proxy.server, proxy.sessions, proxy.inventory.Close, 5*time.Second)
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
	// TODO(OSQQUERY-714): support configuration refresh through the library Warden.
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
		pgxPool, err := pgxpool.NewWithConfig(ctx, settings)
		if err != nil {
			return nil, errors.New("could not create backend pool")
		}
		primary := pool.NewPGXPool(pgxPool, pool.PGXHooks{})
		parameters, err := backend.RetrieveParameterStatuses(primary, name)
		if err != nil {
			primary.Close()
			return nil, fmt.Errorf("connect to backend %q: %w", name, err)
		}
		inventory.backends[name] = &clusterBackend{backend.New(backend.Config{
			ClusterName: name, PrimaryDatabaseName: settings.ConnConfig.Database,
			Primary: primary, ParameterStatuses: parameters,
		})}
	}
	users := make(map[string]string, len(cfg.logicalDatabases))
	for name, logical := range cfg.logicalDatabases {
		users[name] = cfg.databases[cfg.clusters[logical.Cluster]].ConnConfig.User
	}
	sessions := newSessionRegistry()
	proxy, err := server.NewServer(ctx, server.Config{
		ListenAddress: cfg.listenAddress,
		// TODO(OSQQUERY-713): configure session admission limits independently of backend pool sizes.
		Admission: limiter.New(limiter.Params{
			GlobalLimit: 100, DefaultServiceMax: 100,
		}),
		Authenticate: localUser(users),
		Sessions:     sessions,
		Runtime: server.RuntimeConfig{
			Inventory: inventory,
			Settings: topology.RuntimeConfig{
				PostgresMessageBufferSize: cfg.messageBufferSize,
			},
		},
	})
	if err != nil {
		return nil, err
	}
	ready = true
	return &localProxy{server: proxy, sessions: sessions, inventory: inventory}, nil
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
			return listener.AuthenticationResult{}, errors.New("passthrough_user is not supported by the OSS binary")
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

func (i *staticInventory) GetLogicalDatabase(name string) (topology.LogicalDatabase, bool) {
	database, exists := i.logicalDatabases[name]
	return database, exists
}

func (i *staticInventory) GetBackend(name string) (server.Backend, bool) {
	backend, exists := i.backends[name]
	return backend, exists
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
