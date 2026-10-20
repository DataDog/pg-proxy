// Command pg-proxy runs a local, single-backend PostgreSQL transaction pooler.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"os/signal"
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

// config is the minimal starting point for building and running the standalone
// binary. It is not the final configuration design for multiple backends and users.
type config struct {
	listenAddress string
	database      string
	backendURL    string
	maxSessions   int
}

func main() {
	var cfg config
	flag.StringVar(&cfg.backendURL, "backend-url", "", "PostgreSQL backend URL (defaults to PG_PROXY_BACKEND_URL)")
	flag.StringVar(&cfg.listenAddress, "listen", "127.0.0.1:15432", "loopback IP address and port")
	flag.StringVar(&cfg.database, "database", "local_db", "logical database exposed to clients")
	flag.IntVar(&cfg.maxSessions, "max-sessions", 100, "maximum concurrent client sessions")
	flag.Parse()
	if cfg.backendURL == "" {
		cfg.backendURL = os.Getenv("PG_PROXY_BACKEND_URL")
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
		return errors.New("-listen must use a loopback IP: this local reference binary does not authenticate clients")
	}
	if c.database == "" {
		return errors.New("-database must not be empty")
	}
	if c.backendURL == "" {
		return errors.New("-backend-url or PG_PROXY_BACKEND_URL is required")
	}
	if c.maxSessions < 1 || c.maxSessions > 1<<31-1 {
		return errors.New("-max-sessions must be between 1 and 2147483647")
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
	slog.Info("local proxy ready; clients are trusted", "address", proxy.server.Addr(), "database", cfg.database)
	return serve(ctx, stop, proxy.server, proxy.sessions, proxy.backend.Close, 5*time.Second)
}

type localProxy struct {
	server   *server.Server
	sessions *sessionRegistry
	backend  *singleBackend
}

func newProxy(ctx context.Context, cfg config) (*localProxy, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	// Parse errors can contain the connection string, including its password.
	poolConfig, err := pgxpool.ParseConfig(cfg.backendURL)
	if err != nil {
		return nil, errors.New("invalid backend URL")
	}
	pgxPool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, errors.New("could not create backend pool")
	}
	primary := pool.NewPGXPool(pgxPool, pool.PGXHooks{})
	parameters, err := backend.RetrieveParameterStatuses(primary, cfg.database)
	if err != nil {
		primary.Close()
		return nil, fmt.Errorf("connect to backend: %w", err)
	}
	upstream := &singleBackend{backend.New(backend.Config{
		ClusterName: cfg.database, PrimaryDatabaseName: poolConfig.ConnConfig.Database,
		Primary: primary, ParameterStatuses: parameters,
	})}
	sessions := newSessionRegistry()
	proxy, err := server.NewServer(ctx, server.Config{
		ListenAddress: cfg.listenAddress,
		Admission: limiter.New(limiter.Params{
			GlobalLimit: int32(cfg.maxSessions), DefaultServiceMax: int32(cfg.maxSessions),
		}),
		Authenticate: localUser(poolConfig.ConnConfig.User),
		Sessions:     sessions,
		Runtime: server.RuntimeConfig{
			Inventory: staticInventory{database: cfg.database, backend: upstream},
			Settings: topology.RuntimeConfig{
				PostgresMessageBufferSize: topology.DefaultPostgresMessageBufferSize,
			},
		},
	})
	if err != nil {
		upstream.Close()
		return nil, err
	}
	return &localProxy{server: proxy, sessions: sessions, backend: upstream}, nil
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

// This command deliberately supports only local trust and one backend identity.
func localUser(user string) func(context.Context, listener.StartupRequest, listener.StartupResult[server.ResolvedStartup], protocol.Exchange) (listener.AuthenticationResult, error) {
	return func(_ context.Context, request listener.StartupRequest, _ listener.StartupResult[server.ResolvedStartup], _ protocol.Exchange) (listener.AuthenticationResult, error) {
		if request.User != user {
			return listener.AuthenticationResult{}, errors.New("client user must match the configured backend user")
		}
		if request.Options.PassthroughUser {
			return listener.AuthenticationResult{}, errors.New("passthrough_user is not supported by the local reference binary")
		}
		return listener.AuthenticationResult{Service: user}, nil
	}
}

type singleBackend struct{ *backend.Runtime }

func (b *singleBackend) SessionBackend() session.Backend { return b.Runtime }

type staticInventory struct {
	database string
	backend  server.Backend
}

func (i staticInventory) GetLogicalDatabase(name string) (topology.LogicalDatabase, bool) {
	return topology.LogicalDatabase{Cluster: i.database}, name == i.database
}

func (i staticInventory) GetBackend(name string) (server.Backend, bool) {
	return i.backend, name == i.database
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
