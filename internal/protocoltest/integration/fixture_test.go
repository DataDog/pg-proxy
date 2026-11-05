//go:build integration

package integration

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/DataDog/pg-proxy/internal/protocoltest"
	"github.com/DataDog/pg-proxy/pkg/backend"
	"github.com/DataDog/pg-proxy/pkg/limiter"
	"github.com/DataDog/pg-proxy/pkg/link"
	"github.com/DataDog/pg-proxy/pkg/listener"
	"github.com/DataDog/pg-proxy/pkg/pool"
	"github.com/DataDog/pg-proxy/pkg/protocol"
	"github.com/DataDog/pg-proxy/pkg/server"
	"github.com/DataDog/pg-proxy/pkg/session"
	"github.com/DataDog/pg-proxy/pkg/topology"
)

const postgresImage = "postgres:14.20@sha256:062b02051873a101837543675b5b4601cba06b1972d22f4b7969adbde5a280e4"
const fixtureTimeout = 10 * time.Second

type postgresFixture struct {
	Admin    *pgx.Conn
	DSN      string
	Metadata map[string]any
	logs     func(context.Context) (string, error)
}

func (f *postgresFixture) Logs(ctx context.Context) (string, error) {
	if f.logs == nil {
		return "PostgreSQL logs unavailable for an externally managed fixture", nil
	}
	return f.logs(ctx)
}

// startPostgres owns a disposable public PostgreSQL container unless the caller
// explicitly opts into database creation on a supplied loopback trust server.
func startPostgres(t *testing.T) *postgresFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	f := &postgresFixture{DSN: os.Getenv("PGPROXY_TEST_ADMIN_DSN"), Metadata: make(map[string]any)}
	managedHost := ""
	if f.DSN == "" {
		provider, err := testcontainers.NewDockerProvider()
		if err != nil {
			t.Fatalf("resolve Docker provider: %v", err)
		}
		defer provider.Close()
		managedHost, err = provider.DaemonHost(ctx)
		if err != nil {
			t.Fatalf("resolve Docker endpoint: %v", err)
		}
		// Local fixtures stay on loopback. A separate Docker daemon (as in CI)
		// must publish ports on the interface reachable from this test process.
		bindIP := netip.IPv4Unspecified()
		if loopbackHost(managedHost) {
			bindIP, err = netip.ParseAddr(managedHost)
			if err != nil { // localhost
				bindIP = netip.MustParseAddr("127.0.0.1")
			}
		}
		image := os.Getenv("PGPROXY_TEST_POSTGRES_IMAGE")
		if image == "" {
			image = postgresImage
		}
		pg, err := postgres.Run(ctx, image,
			postgres.WithDatabase("postgres"), postgres.WithUsername("postgres"), postgres.WithPassword("protocol-test-only"),
			testcontainers.WithCmd("postgres", "-c", "fsync=on"),
			testcontainers.WithEnv(map[string]string{"POSTGRES_HOST_AUTH_METHOD": "trust", "POSTGRES_INITDB_ARGS": "--encoding=UTF8 --locale=C"}),
			testcontainers.WithHostConfigModifier(func(host *container.HostConfig) {
				host.PortBindings = network.PortMap{network.MustParsePort("5432/tcp"): {{HostIP: bindIP}}}
			}),
			testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(time.Minute)),
		)
		if pg != nil {
			t.Cleanup(func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), fixtureTimeout)
				defer cleanupCancel()
				if err := pg.Terminate(cleanupCtx); err != nil {
					t.Errorf("terminate PostgreSQL fixture: %v", err)
				}
			})
		}
		if err != nil {
			t.Fatalf("start required PostgreSQL fixture: %v", err)
		}
		f.DSN, err = pg.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			t.Fatalf("resolve PostgreSQL fixture endpoint: %v", err)
		}
		f.Metadata["postgres_image"] = image
		inspect, err := pg.Inspect(ctx)
		if err != nil {
			t.Fatalf("inspect PostgreSQL fixture image: %v", err)
		}
		f.Metadata["postgres_image_id"] = inspect.Image
		f.Metadata["postgres_image_resolved"] = inspect.Config.Image
		f.Metadata["docker_host"] = managedHost
		f.logs = func(ctx context.Context) (string, error) {
			reader, err := pg.Logs(ctx)
			if err != nil {
				return "", err
			}
			defer reader.Close()
			const limit = 2 << 20
			data, err := io.ReadAll(io.LimitReader(reader, limit+1))
			if len(data) > limit {
				return string(data[:limit]) + "\n[fixture log truncated at 2 MiB]", err
			}
			return string(data), err
		}
	} else {
		if os.Getenv("PGPROXY_TEST_ALLOW_DATABASE_CREATION") != "1" {
			t.Fatal("PGPROXY_TEST_ADMIN_DSN requires PGPROXY_TEST_ALLOW_DATABASE_CREATION=1 and a disposable loopback trust server")
		}
		f.Metadata["postgres_image"] = "external disposable server"
	}
	cfg, err := parseFixtureConfig(f.DSN, managedHost)
	if err != nil {
		t.Fatal(err)
	}
	f.Admin, err = pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect to PostgreSQL fixture: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), fixtureTimeout)
		defer cleanupCancel()
		if err := f.Admin.Close(cleanupCtx); err != nil {
			t.Errorf("close fixture admin: %v", err)
		}
	})
	var mayCreate bool
	if err := f.Admin.QueryRow(ctx, "SELECT rolsuper OR rolcreatedb FROM pg_roles WHERE rolname = current_user").Scan(&mayCreate); err != nil || !mayCreate {
		t.Fatalf("fixture role must create disposable databases: allowed=%v error=%v", mayCreate, err)
	}
	var version string
	if err := f.Admin.QueryRow(ctx, "SELECT version()").Scan(&version); err != nil {
		t.Fatalf("read PostgreSQL version: %v", err)
	}
	f.Metadata["postgres_version"] = version
	return f
}

// Only a container owned by this fixture may use a non-loopback Docker endpoint.
func parseFixtureConfig(dsn, managedHost string) (*pgx.ConnConfig, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, errors.New("invalid PostgreSQL fixture connection configuration")
	}
	allowedHost := loopbackHost(cfg.Host) || managedHost != "" && cfg.Host == managedHost
	if !allowedHost || cfg.TLSConfig != nil || len(cfg.Fallbacks) != 0 {
		return nil, errors.New("PostgreSQL fixture requires one loopback or owned Docker endpoint with sslmode=disable and no fallbacks")
	}
	// The raw client does not inherit PGOPTIONS/PGAPPNAME. Use the same
	// explicit baseline for control and pooled connections.
	cfg.RuntimeParams = map[string]string{"client_encoding": "UTF8"}
	cfg.ConnectTimeout = fixtureTimeout
	return cfg, nil
}

func TestFixtureConfig(t *testing.T) {
	for _, test := range []struct {
		name, dsn, managedHost string
		valid                  bool
	}{
		{"loopback", "host=127.0.0.1 sslmode=disable", "", true},
		{"localhost", "host=localhost sslmode=disable", "", true},
		{"ipv6", "host=::1 sslmode=disable", "", true},
		{"owned_docker", "host=docker sslmode=disable", "docker", true},
		{"other_docker", "host=other sslmode=disable", "docker", false},
		{"owned_docker_fallback", "host=docker,other sslmode=disable", "docker", false},
		{"remote", "host=db.example sslmode=disable", "", false},
		{"remote_fallback", "host=127.0.0.1,db.example port=1,5432 sslmode=disable", "", false},
		{"loopback_fallback", "host=127.0.0.1,127.0.0.2 sslmode=disable", "", false},
		{"tls_fallback", "host=127.0.0.1 sslmode=allow", "", false},
		{"tls", "host=127.0.0.1 sslmode=require", "", false},
		{"unix_socket", "host=/tmp sslmode=disable", "", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := parseFixtureConfig(test.dsn, test.managedHost); (err == nil) != test.valid {
				t.Fatalf("valid=%v, error=%v", test.valid, err)
			}
		})
	}
}

func loopbackHost(host string) bool {
	return host == "localhost" || net.ParseIP(host) != nil && net.ParseIP(host).IsLoopback()
}

type lane struct {
	Endpoint  protocoltest.Endpoint
	Admin     *pgx.Conn
	Relations map[uint32]string

	adminConfig *pgx.ConnConfig
	database    string
	pool        *pgxpool.Pool
	backend     *backend.Runtime
	server      *server.Server
	registry    *fixtureRegistry
	cancel      context.CancelFunc
	runDone     chan error
	closeOnce   sync.Once
	closeErr    error

	mu              sync.Mutex
	checkoutPIDs    []uint32
	evictedPID      uint32
	reassigned      bool
	reassignmentErr error
}

// The caller owns every non-nil lane, including one returned with a setup error.
func newLane(ctx context.Context, adminConfig *pgx.ConnConfig, setup []string, proxied bool, bufferSize int) (*lane, error) {
	ctx, cancel := context.WithTimeout(ctx, fixtureTimeout)
	defer cancel()
	// Reuse the validated endpoint, including its owned Docker host in CI.
	cfg := adminConfig.Copy()
	control, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect database creator: %w", err)
	}
	defer control.Close(ctx)
	var suffix [8]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return nil, fmt.Errorf("name fixture database: %w", err)
	}
	l := &lane{adminConfig: cfg.Copy(), database: "pgproxy_protocol_" + hex.EncodeToString(suffix[:]), Relations: make(map[uint32]string)}
	quoted := pgx.Identifier{l.database}.Sanitize()
	if _, err := control.Exec(ctx, "CREATE DATABASE "+quoted+" TEMPLATE template0 ENCODING 'UTF8' LC_COLLATE 'C' LC_CTYPE 'C'"); err != nil {
		return l, fmt.Errorf("create isolated database: %w", err)
	}
	// Defaults apply equally to raw clients and PgProxy's pgx backend connections.
	for _, setting := range []string{"TimeZone = 'UTC'", "DateStyle = 'ISO, MDY'", "IntervalStyle = 'postgres'", "extra_float_digits = 3", "standard_conforming_strings = on"} {
		if _, err := control.Exec(ctx, "ALTER DATABASE "+quoted+" SET "+setting); err != nil {
			return l, fmt.Errorf("configure isolated database: %w", err)
		}
	}
	laneConfig := cfg.Copy()
	laneConfig.Database = l.database
	l.Admin, err = pgx.ConnectConfig(ctx, laneConfig)
	if err != nil {
		return l, fmt.Errorf("connect isolated database: %w", err)
	}
	for index, sql := range setup {
		if _, err := l.Admin.Exec(ctx, sql, pgx.QueryExecModeSimpleProtocol); err != nil {
			return l, fmt.Errorf("fixture setup statement %d: %w", index, err)
		}
	}
	rows, err := l.Admin.Query(ctx, `SELECT c.oid, quote_ident(n.nspname) || '.' || quote_ident(c.relname)
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema') AND n.nspname NOT LIKE 'pg_toast%'`)
	if err != nil {
		return l, fmt.Errorf("read relation identities: %w", err)
	}
	for rows.Next() {
		var oid uint32
		var name string
		if err := rows.Scan(&oid, &name); err != nil {
			rows.Close()
			return l, fmt.Errorf("read relation identity: %w", err)
		}
		l.Relations[oid] = name
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return l, fmt.Errorf("read relation identities: %w", err)
	}
	l.Endpoint = protocoltest.Endpoint{Address: net.JoinHostPort(laneConfig.Host, strconv.Itoa(int(laneConfig.Port))), User: laneConfig.User, Database: l.database}
	if !proxied {
		return l, nil
	}

	// ParseConfig must construct the pgxpool config; replacing ConnConfig retains
	// pgx's parsed-configuration invariant without inheriting pool options.
	poolConfig, err := pgxpool.ParseConfig(cfg.ConnString())
	if err != nil {
		return l, errors.New("invalid fixture pool configuration")
	}
	poolConfig.ConnConfig = laneConfig.Copy()
	poolConfig.MaxConns = 1
	poolConfig.MinConns = 0
	poolConfig.MinIdleConns = 0
	poolConfig.HealthCheckPeriod = time.Hour
	poolConfig.MaxConnLifetime = time.Hour
	poolConfig.MaxConnIdleTime = time.Hour
	l.pool, err = pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return l, fmt.Errorf("create fixture backend pool: %w", err)
	}
	primary := pool.NewPGXPool(l.pool, pool.PGXHooks{})
	parameters, err := backend.RetrieveParameterStatuses(primary, "protocol")
	if err != nil {
		return l, fmt.Errorf("retrieve fixture backend parameters: %w", err)
	}
	l.backend = backend.New(backend.Config{
		ClusterName: "protocol", PrimaryDatabaseName: l.database, Primary: primary, ParameterStatuses: parameters,
		LinkFactory: func(conn *pgxpool.Conn, options link.Options) (link.Link, error) {
			l.observeCheckout(conn.Conn().PgConn().PID())
			return link.New(conn, options)
		},
	})
	l.registry = newFixtureRegistry()
	serverCtx, stop := context.WithCancel(context.Background())
	l.cancel = stop
	l.server, err = server.NewServer(serverCtx, server.Config{
		ListenAddress: "127.0.0.1:0",
		Admission:     limiter.New(limiter.Params{GlobalLimit: 10, DefaultServiceMax: 10}),
		Authenticate: func(_ context.Context, request listener.StartupRequest, _ listener.StartupResult[server.ResolvedStartup], _ protocol.Exchange) (listener.AuthenticationResult, error) {
			if request.User != laneConfig.User || request.Options.PassthroughUser {
				return listener.AuthenticationResult{}, errors.New("fixture client identity rejected")
			}
			return listener.AuthenticationResult{Service: laneConfig.User}, nil
		},
		Sessions: l.registry,
		Runtime:  server.RuntimeConfig{Inventory: fixtureInventory{database: l.database, runtime: l.backend}, Settings: topology.RuntimeConfig{PostgresMessageBufferSize: bufferSize}},
	})
	if err != nil {
		return l, fmt.Errorf("create fixture proxy: %w", err)
	}
	l.runDone = make(chan error, 1)
	go func() { l.runDone <- l.server.Run() }()
	l.Endpoint.Address = l.server.Addr().String()
	return l, nil
}

func (l *lane) observeCheckout(pid uint32) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.checkoutPIDs = append(l.checkoutPIDs, pid)
	if l.evictedPID != 0 && !l.reassigned {
		if pid == l.evictedPID {
			l.reassignmentErr = fmt.Errorf("backend reassignment reused evicted PID %d", pid)
		} else {
			l.reassigned = true
		}
	}
}

// Reassign evicts the exact previously observed backend at an idle boundary.
// Acquiring the sole pool slot also waits for PgProxy's release/reset to finish.
func (l *lane) Reassign(ctx context.Context) error {
	if l.pool == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, fixtureTimeout)
	defer cancel()
	conn, err := l.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire idle backend for reassignment: %w", err)
	}
	defer conn.Release()
	pid := conn.Conn().PgConn().PID()
	l.mu.Lock()
	if len(l.checkoutPIDs) == 0 || l.checkoutPIDs[len(l.checkoutPIDs)-1] != pid {
		l.mu.Unlock()
		return errors.New("reassignment did not acquire the last observed backend")
	}
	l.evictedPID, l.reassigned = pid, false
	l.mu.Unlock()
	if err := conn.Conn().Close(ctx); err != nil {
		return fmt.Errorf("evict idle backend: %w", err)
	}
	return nil
}

func (l *lane) CheckReassignment() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.reassignmentErr != nil {
		return l.reassignmentErr
	}
	if l.evictedPID != 0 && !l.reassigned {
		return errors.New("scenario did not demonstrate a new backend after eviction")
	}
	return nil
}

func (l *lane) CheckoutPIDs() []uint32 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]uint32(nil), l.checkoutPIDs...)
}

func (l *lane) Close() error {
	l.closeOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), fixtureTimeout)
		defer cancel()
		if l.cancel != nil {
			l.cancel()
		}
		if l.server != nil {
			if err := l.server.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				l.closeErr = errors.Join(l.closeErr, err)
			}
		}
		if l.Admin != nil {
			if err := l.Admin.Close(ctx); err != nil {
				l.closeErr = errors.Join(l.closeErr, err)
			}
		}
		// The independent admin connection can terminate stranded queries even
		// when the proxy has not returned its sole pool connection.
		control, err := pgx.ConnectConfig(ctx, l.adminConfig.Copy())
		if err != nil {
			l.closeErr = errors.Join(l.closeErr, fmt.Errorf("connect database cleanup: %w", err))
		} else {
			defer func() {
				closeCtx, closeCancel := context.WithTimeout(context.Background(), fixtureTimeout)
				defer closeCancel()
				_ = control.Close(closeCtx)
			}()
			if _, err := control.Exec(ctx, "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1", l.database); err != nil {
				l.closeErr = errors.Join(l.closeErr, fmt.Errorf("terminate fixture database sessions: %w", err))
			}
		}
		if l.runDone != nil {
			select {
			case err := <-l.runDone:
				if err != nil && !errors.Is(err, net.ErrClosed) {
					l.closeErr = errors.Join(l.closeErr, err)
				}
			case <-ctx.Done():
				l.closeErr = errors.Join(l.closeErr, errors.New("proxy listener cleanup deadline"))
			}
		}
		if l.registry != nil {
			if err := l.registry.waitContext(ctx); err != nil {
				l.closeErr = errors.Join(l.closeErr, err)
			}
		}
		if l.pool != nil {
			done := make(chan struct{})
			go func() {
				if l.backend != nil {
					l.backend.Close()
				} else {
					l.pool.Close()
				}
				close(done)
			}()
			select {
			case <-done:
			case <-ctx.Done():
				l.closeErr = errors.Join(l.closeErr, errors.New("backend pool cleanup deadline"))
			}
		}
		// A preceding cleanup deadline must not prevent an independent drop
		// attempt. Only this randomly named fixture database is ever removed.
		dropCtx, dropCancel := context.WithTimeout(context.Background(), fixtureTimeout)
		defer dropCancel()
		if control == nil || control.IsClosed() {
			control, err = pgx.ConnectConfig(dropCtx, l.adminConfig.Copy())
			if err != nil {
				l.closeErr = errors.Join(l.closeErr, fmt.Errorf("reconnect database cleanup: %w", err))
				return
			}
			defer control.Close(dropCtx)
		}
		if _, err := control.Exec(dropCtx, "DROP DATABASE IF EXISTS "+pgx.Identifier{l.database}.Sanitize()+" WITH (FORCE)"); err != nil {
			l.closeErr = errors.Join(l.closeErr, fmt.Errorf("drop fixture database: %w", err))
		}
	})
	return l.closeErr
}

type fixtureBackend struct{ *backend.Runtime }

func (b fixtureBackend) SessionBackend() session.Backend { return b.Runtime }

type fixtureInventory struct {
	database string
	runtime  *backend.Runtime
}

func (i fixtureInventory) GetLogicalDatabase(name string) (topology.LogicalDatabase, bool) {
	return topology.LogicalDatabase{Cluster: "protocol"}, name == i.database
}

func (i fixtureInventory) GetBackend(name string) (server.Backend, bool) {
	return fixtureBackend{i.runtime}, name == "protocol"
}

type fixtureRegistry struct {
	mu       sync.Mutex
	changed  chan struct{}
	sessions map[string]session.SessionHandle
}

func newFixtureRegistry() *fixtureRegistry {
	return &fixtureRegistry{changed: make(chan struct{}), sessions: make(map[string]session.SessionHandle)}
}

func (r *fixtureRegistry) Register(id string, s session.SessionHandle) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sessions[id] = s
}

func (r *fixtureRegistry) Delete(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.sessions, id)
	close(r.changed)
	r.changed = make(chan struct{})
}

func (r *fixtureRegistry) Get(id string) (session.SessionHandle, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.sessions[id]
	return s, ok
}

func (r *fixtureRegistry) Wait() { _ = r.waitContext(context.Background()) }

func (r *fixtureRegistry) waitContext(ctx context.Context) error {
	for {
		r.mu.Lock()
		empty, changed := len(r.sessions) == 0, r.changed
		r.mu.Unlock()
		if empty {
			return nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return errors.New("proxy sessions cleanup deadline")
		}
	}
}
