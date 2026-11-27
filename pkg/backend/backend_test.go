// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package backend

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"

	pgproxy "github.com/DataDog/pg-proxy"
	"github.com/DataDog/pg-proxy/internal/fakepg"
	"github.com/DataDog/pg-proxy/pkg/config"
	"github.com/DataDog/pg-proxy/pkg/link"
	"github.com/DataDog/pg-proxy/pkg/pool"
	"github.com/DataDog/pg-proxy/pkg/session"
)

type fakePool struct {
	acquire func(context.Context) (*pgxpool.Conn, error)
	closed  bool
	exports atomic.Int32
}

func (p *fakePool) Acquire(ctx context.Context) (*pgxpool.Conn, error) {
	if p.acquire != nil {
		return p.acquire(ctx)
	}
	return nil, nil
}
func (p *fakePool) ExportStats([]string) { p.exports.Add(1) }
func (p *fakePool) Close()               { p.closed = true }
func (*fakePool) Reset()                 {}

func newTestRuntime(primary *fakePool, replicas ...*fakePool) *Runtime {
	replicaPools := make([]interfacePool, 0, len(replicas))
	for _, replica := range replicas {
		replicaPools = append(replicaPools, replica)
	}
	return newTestRuntimeWithPools(primary, replicaPools)
}

type interfacePool = interface {
	Acquire(context.Context) (*pgxpool.Conn, error)
	ExportStats([]string)
	Close()
	Reset()
}

func newTestRuntimeWithPools(primary interfacePool, replicas []interfacePool) *Runtime {
	cfg := Config{ClusterName: "test-cluster", PrimaryDatabaseName: "primary", Primary: primary}
	for _, replica := range replicas {
		cfg.Replicas = append(cfg.Replicas, replica)
	}
	return New(cfg)
}

func TestSelectPool(t *testing.T) {
	primary := &fakePool{}
	replica := &fakePool{}
	runtime := newTestRuntime(primary, replica)

	runtime.selector.randomIntFn = func(int) int { return 5 }
	index := runtime.selector.selectReplica(time.Time{})
	assert.Equal(t, -1, index)
	assert.Same(t, primary, runtime.selector.pool(index))

	runtime.selector.randomIntFn = func(int) int { return 25 }
	index = runtime.selector.selectReplica(time.Time{})
	assert.Equal(t, 0, index)
	assert.Same(t, replica, runtime.selector.pool(index))

	runtime = newTestRuntime(primary)
	runtime.selector.randomIntFn = func(int) int { return 25 }
	index = runtime.selector.selectReplica(time.Time{})
	assert.Equal(t, -1, index)
	assert.Same(t, primary, runtime.selector.pool(index))
}

func TestSelectPoolRoundRobin(t *testing.T) {
	primary := &fakePool{}
	replicas := []*fakePool{{}, {}}
	runtime := newTestRuntime(primary, replicas...)
	runtime.selector.randomIntFn = func(int) int { return 25 }
	for _, expected := range []int{1, 0, 1, 0} {
		index := runtime.selector.selectReplica(time.Time{})
		assert.Equal(t, expected, index)
		assert.Same(t, replicas[expected], runtime.selector.pool(index))
	}
}

func TestAcquireConnectionReplica(t *testing.T) {
	tests := map[string]struct {
		acquire        func(context.Context) (*pgxpool.Conn, error)
		expectRerouted bool
	}{
		"timeout falls back to primary": {
			acquire: func(ctx context.Context) (*pgxpool.Conn, error) {
				<-ctx.Done()
				return nil, ctx.Err()
			},
		},
		"failure falls back to primary": {
			acquire: func(context.Context) (*pgxpool.Conn, error) { return nil, errors.New("failed to open test conn") },
		},
		"success uses replica": {
			acquire:        func(context.Context) (*pgxpool.Conn, error) { return nil, nil },
			expectRerouted: true,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			runtime := newTestRuntime(&fakePool{}, &fakePool{acquire: test.acquire})
			runtime.selector.randomIntFn = func(int) int { return 25 }
			conn, rerouted, err := runtime.AcquireConnection(context.Background(), true, time.Time{})
			require.NoError(t, err)
			assert.Nil(t, conn)
			assert.Equal(t, test.expectRerouted, rerouted)
			assert.Zero(t, runtime.SessionsWaiting())
		})
	}
}

func TestAcquireConnectionFallbackRateLimit(t *testing.T) {
	tests := []struct {
		name        string
		replica     *fakePool
		rateLimiter *rate.Limiter
		testFunc    func(t *testing.T, runtime *Runtime)
	}{
		{
			name: "replica fallback exceeds rate limit",
			replica: &fakePool{acquire: func(context.Context) (*pgxpool.Conn, error) {
				return nil, errors.New("replica unavailable")
			}},
			rateLimiter: rate.NewLimiter(1, 2),
			testFunc: func(t *testing.T, runtime *Runtime) {
				for range 2 {
					_, rerouted, err := runtime.AcquireConnection(context.Background(), true, time.Time{})
					require.NoError(t, err)
					assert.False(t, rerouted, "should fall back to primary")
				}
				_, rerouted, err := runtime.AcquireConnection(context.Background(), true, time.Time{})
				assert.EqualError(t, err, "primary database fallback rate limit exceeded for cluster:test-cluster")
				assert.False(t, rerouted)
			},
		},
		{
			name: "replica fallback within rate limit",
			replica: &fakePool{acquire: func(context.Context) (*pgxpool.Conn, error) {
				return nil, errors.New("replica unavailable")
			}},
			rateLimiter: rate.NewLimiter(rate.Limit(replicaFallbackRateLimit), replicaFallbackRateBurst),
			testFunc: func(t *testing.T, runtime *Runtime) {
				for i := 0; i < 10; i++ {
					_, rerouted, err := runtime.AcquireConnection(context.Background(), true, time.Time{})
					assert.NoError(t, err, "attempt %d should succeed", i)
					assert.False(t, rerouted, "attempt %d should fall back to primary", i)
				}
			},
		},
		{
			name:        "direct primary not rate limited",
			rateLimiter: rate.NewLimiter(1, 1),
			testFunc: func(t *testing.T, runtime *Runtime) {
				for i := 0; i < 10; i++ {
					_, rerouted, err := runtime.AcquireConnection(context.Background(), false, time.Time{})
					assert.NoError(t, err, "attempt %d should succeed", i)
					assert.False(t, rerouted, "attempt %d should not be rerouted", i)
				}
			},
		},
		{
			name:        "successful replica not rate limited",
			replica:     &fakePool{},
			rateLimiter: rate.NewLimiter(1, 1),
			testFunc: func(t *testing.T, runtime *Runtime) {
				for i := 0; i < 10; i++ {
					_, rerouted, err := runtime.AcquireConnection(context.Background(), true, time.Time{})
					assert.NoError(t, err, "attempt %d should succeed", i)
					assert.True(t, rerouted, "attempt %d should be rerouted to replica", i)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var runtime *Runtime
			if tt.replica != nil {
				runtime = newTestRuntime(&fakePool{}, tt.replica)
			} else {
				runtime = newTestRuntime(&fakePool{})
			}
			runtime.selector.randomIntFn = func(int) int { return 25 }
			runtime.fallbackRateLimiter = tt.rateLimiter
			tt.testFunc(t, runtime)
		})
	}
}

func TestRetrieveParameterStatusesAcquireError(t *testing.T) {
	p := &fakePool{acquire: func(context.Context) (*pgxpool.Conn, error) {
		return nil, errors.New("failed to open test conn")
	}}
	statuses, err := RetrieveParameterStatuses(p, "broken-pool")
	assert.Nil(t, statuses)
	assert.ErrorContains(t, err, "failed to open test conn to postgres for broken-pool: failed to open test conn")
}

type recordingMetrics struct {
	pgproxy.NoopMetrics
	mu     sync.Mutex
	counts []metricCall
	gauges []metricCall
}

type metricCall struct {
	name string
	tags []string
}

func (m *recordingMetrics) Count(name string, _ int64, tags []string, _ float64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counts = append(m.counts, metricCall{name: name, tags: append([]string(nil), tags...)})
	return nil
}

func (m *recordingMetrics) Gauge(name string, _ float64, tags []string, _ float64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gauges = append(m.gauges, metricCall{name: name, tags: append([]string(nil), tags...)})
	return nil
}

func (m *recordingMetrics) gaugeCalls() []metricCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]metricCall(nil), m.gauges...)
}

func TestStartStatsExportsOneLoopUntilClose(t *testing.T) {
	metrics := &recordingMetrics{}
	t.Cleanup(pgproxy.Bootstrap(pgproxy.Dependencies{Metrics: metrics}))
	primary := &fakePool{}
	runtime := newTestRuntime(primary)
	runtime.StartStats()
	runtime.StartStats()

	// Ticks fire at two and four seconds; check between them, then close before the second.
	time.Sleep(3 * time.Second)
	want := []metricCall{{
		name: pgproxy.MetricName("pg_proxy.sessions.waiting"),
		tags: []string{"database:primary", "database_cluster:test-cluster"},
	}}
	assert.Equal(t, want, metrics.gaugeCalls())
	assert.EqualValues(t, 1, primary.exports.Load())

	runtime.Close()
	time.Sleep(2 * time.Second)
	assert.Equal(t, want, metrics.gaugeCalls())
	assert.EqualValues(t, 1, primary.exports.Load())
}

type fakeRole struct{}

func (fakeRole) Set(context.Context, *pgxpool.Conn, []string) (string, error) { return "", nil }
func (fakeRole) Reset(context.Context, *pgxpool.Conn, string, []string) error { return nil }

type failingRole struct{ fakeRole }

func (failingRole) Set(context.Context, *pgxpool.Conn, []string) (string, error) {
	return "", errors.New("permission denied to set role")
}

// When link.New fails, no link holds the conn, so the default factory must destroy
// it: released alone it would return to the pool in an unknown state, and kept it
// would hold a pool slot forever. A custom factory owns its conn on error, so
// Checkout must leave it alone, including one the factory has already released.
func TestLinkFactoryOwnsConnOnError(t *testing.T) {
	// checkout runs one Checkout against a single-conn fake Postgres and returns its
	// pool and Checkout's error.
	checkout := func(t *testing.T, cfg Config) (*pgxpool.Pool, error) {
		pgPool := fakepg.NewPool(t, fakepg.NewServer(fakepg.Answer))
		var acquired *pgxpool.Conn
		t.Cleanup(func() {
			if acquired != nil {
				acquired.Release() // a no-op once released; keeps a leak from hanging the pool's cleanup
			}
		})
		cfg.ClusterName, cfg.PrimaryDatabaseName = "cluster-a", "primary-a"
		cfg.Primary = &fakePool{acquire: func(ctx context.Context) (*pgxpool.Conn, error) {
			conn, err := pgPool.Acquire(ctx)
			acquired = conn
			return conn, err
		}}
		acquiredLink, err := New(cfg).Checkout(context.Background(), session.LinkRequest{PassthroughRoleName: "app_user"})
		require.Error(t, err)
		require.Nil(t, acquiredLink.Link)
		return pgPool, err
	}

	t.Run("default factory destroys the conn", func(t *testing.T) {
		pgPool, _ := checkout(t, Config{RoleSessionFactory: func(string) link.RoleSession { return failingRole{} }})
		// pgxpool destroys a closed conn in the background, so wait for it to go.
		require.Eventually(t, func() bool { return pgPool.Stat().TotalConns() == 0 }, 5*time.Second, time.Millisecond,
			"the conn must be destroyed: not left checked out, and not pooled in an unknown state")
	})
	t.Run("custom factory releases its conn", func(t *testing.T) {
		sentinel := errors.New("link factory sentinel")
		pgPool, err := checkout(t, Config{LinkFactory: func(conn *pgxpool.Conn, _ link.Options) (link.Link, error) {
			conn.Release()
			return nil, sentinel
		}})
		require.ErrorIs(t, err, sentinel)
		require.Equal(t, int32(1), pgPool.Stat().IdleConns(), "a custom factory's released conn stays pooled")
	})
}

func TestAcquireBuildsLinkWithInjectedDependencies(t *testing.T) {
	metrics := &recordingMetrics{}
	restore := pgproxy.Bootstrap(pgproxy.Dependencies{Metrics: metrics})
	t.Cleanup(restore)

	var gotOptions link.Options
	var gotRoleName string
	sentinel := errors.New("link factory sentinel")
	runtime := New(Config{
		ClusterName:         "cluster-a",
		PrimaryDatabaseName: "primary-a",
		Primary:             &fakePool{},
		RoleSessionFactory: func(roleName string) link.RoleSession {
			gotRoleName = roleName
			return fakeRole{}
		},
		LinkFactory: func(_ *pgxpool.Conn, options link.Options) (link.Link, error) {
			gotOptions = options
			return nil, sentinel
		},
	})
	baseTags := []string{"service:test"}
	_, err := runtime.Checkout(context.Background(), session.LinkRequest{
		ForReadQuery:        true,
		PassthroughRoleName: "role-a",
		MaxPreparedStmts:    12,
		MetricTags:          baseTags,
	})
	require.ErrorIs(t, err, sentinel)
	assert.Equal(t, "role-a", gotRoleName)
	assert.Equal(t, 12, gotOptions.MaxPreparedStmts)
	assert.Equal(t, []string{"service:test", "database_cluster:cluster-a", "read:true"}, gotOptions.MetricTags)
	assert.Equal(t, []string{"service:test"}, baseTags, "base tags must not be mutated")
	assert.IsType(t, fakeRole{}, gotOptions.Role)
	require.NotEmpty(t, metrics.counts)
	assert.Equal(t, "datadog.pg_proxy.sessions.connections", metrics.counts[0].name)
	assert.Contains(t, metrics.counts[0].tags, "outcome:successful")
}

func TestCanPerformDynamicRouting(t *testing.T) {
	assert.False(t, newTestRuntime(&fakePool{}).CanPerformDynamicRouting(true))
	assert.True(t, newTestRuntime(&fakePool{}, &fakePool{}).CanPerformDynamicRouting(true))
	assert.False(t, newTestRuntime(&fakePool{}, &fakePool{}).CanPerformDynamicRouting(false))
}

func TestCloseClosesAllPools(t *testing.T) {
	primary := &fakePool{}
	replica := &fakePool{}
	runtime := newTestRuntime(primary, replica)
	runtime.Close()
	assert.True(t, primary.closed)
	assert.True(t, replica.closed)
	assert.NotPanics(t, runtime.Close)
}

func TestReadAfterWritePlaceholder(t *testing.T) {
	assert.False(t, isReadAfterWrite(time.Time{}))
	assert.True(t, isReadAfterWrite(time.Now()))
	assert.False(t, isReadAfterWrite(time.Now().Add(-readAfterWriteThreshold-time.Millisecond)))
}

func TestSessionsWaitingTracksConcurrentAcquire(t *testing.T) {
	block := make(chan struct{})
	started := make(chan struct{})
	runtime := newTestRuntime(&fakePool{acquire: func(context.Context) (*pgxpool.Conn, error) {
		close(started)
		<-block
		return nil, fmt.Errorf("done")
	}})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _, _ = runtime.AcquireConnection(context.Background(), false, time.Time{})
	}()
	<-started
	assert.Equal(t, int32(1), runtime.SessionsWaiting())
	close(block)
	<-done
	assert.Zero(t, runtime.SessionsWaiting())
}

func (m *recordingMetrics) Incr(name string, tags []string, rate float64) error {
	return m.Count(name, 1, tags, rate)
}

func TestFallbackMetricIdentity(t *testing.T) {
	metrics := &recordingMetrics{}
	t.Cleanup(pgproxy.Bootstrap(pgproxy.Dependencies{Metrics: metrics}))
	runtime := newTestRuntime(&fakePool{})
	runtime.fallbackRateLimiter = rate.NewLimiter(0, 1)
	_, err := runtime.fallbackToPrimary(context.Background())
	require.NoError(t, err)
	_, err = runtime.fallbackToPrimary(context.Background())
	require.Error(t, err)
	require.Len(t, metrics.counts, 2)
	for i, outcome := range []string{"allowed", "rate_limited"} {
		assert.Equal(t, "datadog.pg_proxy.replica_acquire_fallback", metrics.counts[i].name)
		assert.ElementsMatch(t, []string{"database:primary", "database_cluster:test-cluster", "outcome:" + outcome}, metrics.counts[i].tags)
	}
}

func TestCheckoutPopulatesCheckoutMeta(t *testing.T) {
	tests := []struct {
		name           string
		routeToReplica bool
		replicaAcquire func(context.Context) (*pgxpool.Conn, error)
		expectReplica  bool
		expectRerouted bool
	}{
		{name: "direct primary"},
		{
			name:           "successful replica",
			routeToReplica: true,
			replicaAcquire: func(context.Context) (*pgxpool.Conn, error) { return nil, nil },
			expectReplica:  true,
			expectRerouted: true,
		},
		{
			name:           "replica failure falls back to primary",
			routeToReplica: true,
			replicaAcquire: func(context.Context) (*pgxpool.Conn, error) {
				return nil, errors.New("replica unavailable")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			primary := &fakePool{}
			replica := &fakePool{acquire: test.replicaAcquire}
			var annotated session.CheckoutMeta
			runtime := New(Config{
				ClusterName:         "cluster-a",
				PrimaryDatabaseName: "primary-a",
				Primary:             primary,
				Replicas:            []pool.Pool{replica},
				LinkFactory: func(*pgxpool.Conn, link.Options) (link.Link, error) {
					return nil, nil
				},
				CheckoutAnnotator: func(meta session.CheckoutMeta) any {
					annotated = meta
					return "route-annotation"
				},
			})
			runtime.selector.randomIntFn = func(int) int { return 25 }

			acquired, err := runtime.Checkout(context.Background(), session.LinkRequest{
				DynamicRoutingEnabled: test.routeToReplica,
				ForReadQuery:          test.routeToReplica,
				PassthroughRoleName:   "role-a",
			})
			require.NoError(t, err)
			if test.expectReplica {
				assert.Same(t, replica, acquired.Meta.Pool)
				assert.Same(t, replica, annotated.Pool)
			} else {
				assert.Same(t, primary, acquired.Meta.Pool)
				assert.Same(t, primary, annotated.Pool)
			}
			assert.Equal(t, "cluster-a", acquired.Meta.ClusterName)
			assert.Equal(t, "role-a", acquired.Meta.EffectiveRole)
			assert.Equal(t, test.expectRerouted, acquired.Meta.Rerouted)
			assert.Equal(t, test.routeToReplica, acquired.Meta.ForReadQuery)
			assert.Equal(t, "route-annotation", acquired.Meta.Annotation)
		})
	}
}

func TestCheckoutAnnotatorPanicIsRecovered(t *testing.T) {
	metrics := &recordingMetrics{}
	t.Cleanup(pgproxy.Bootstrap(pgproxy.Dependencies{Metrics: metrics}))
	primary := &fakePool{}
	runtime := New(Config{
		ClusterName:         "cluster-a",
		PrimaryDatabaseName: "primary-a",
		Primary:             primary,
		LinkFactory: func(*pgxpool.Conn, link.Options) (link.Link, error) {
			return nil, nil
		},
		CheckoutAnnotator: func(session.CheckoutMeta) any {
			panic("annotator boom")
		},
	})

	acquired, err := runtime.Checkout(context.Background(), session.LinkRequest{})
	require.NoError(t, err)
	assert.Same(t, primary, acquired.Meta.Pool)
	assert.Nil(t, acquired.Meta.Annotation)
	found := false
	for _, entry := range metrics.counts {
		if entry.name == "datadog.pg_proxy.backend.checkout_annotator_panic" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected checkout_annotator_panic metric")
}

type scriptedHandshakeConn struct {
	in []byte
}

func (c *scriptedHandshakeConn) Read(p []byte) (int, error) {
	if len(c.in) == 0 {
		return 0, errors.New("script exhausted")
	}
	n := copy(p, c.in)
	c.in = c.in[n:]
	return n, nil
}
func (*scriptedHandshakeConn) Write(p []byte) (int, error)      { return len(p), nil }
func (*scriptedHandshakeConn) Close() error                     { return nil }
func (*scriptedHandshakeConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (*scriptedHandshakeConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (*scriptedHandshakeConn) SetDeadline(time.Time) error      { return nil }
func (*scriptedHandshakeConn) SetReadDeadline(time.Time) error  { return nil }
func (*scriptedHandshakeConn) SetWriteDeadline(time.Time) error { return nil }

// TestCheckoutCachesConfiguredUserWithoutConfigCopy verifies that steady-state
// Runtime.Checkout reads the pool's configured user without calling
// pgx.Conn.Config() (which deep-copies ConnConfig, RuntimeParams, TLSConfig,
// and Fallbacks on every call).
func TestCheckoutCachesConfiguredUserWithoutConfigCopy(t *testing.T) {
	var handshake []byte
	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.AuthenticationOk{},
		&pgproto3.BackendKeyData{ProcessID: 1, SecretKey: []byte{0, 0, 0, 2}},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	} {
		var err error
		handshake, err = msg.Encode(handshake)
		require.NoError(t, err)
	}
	cfg, err := pgxpool.ParseConfig("postgres://app_user@127.0.0.1/app_db?sslmode=disable")
	require.NoError(t, err)
	cfg.MaxConns = 1
	cfg.HealthCheckPeriod = time.Hour
	cfg.ShouldPing = func(context.Context, pgxpool.ShouldPingParams) bool { return false }
	cfg.ConnConfig.DialFunc = func(context.Context, string, string) (net.Conn, error) {
		return &scriptedHandshakeConn{in: append([]byte(nil), handshake...)}, nil
	}
	pgxPool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(pgxPool.Close)

	wrappedPool := pool.NewPGXPool(pgxPool, pool.PGXHooks{})
	runtime := New(Config{
		ClusterName:         "cluster-a",
		PrimaryDatabaseName: "app_db",
		Primary:             wrappedPool,
		LinkFactory: func(conn *pgxpool.Conn, _ link.Options) (link.Link, error) {
			return &releaseOnlyLink{conn: conn}, nil
		},
	})

	// Warm the single pooled connection so CustomData caches the configured user.
	first, err := runtime.Checkout(context.Background(), session.LinkRequest{})
	require.NoError(t, err)
	assert.Equal(t, "app_user", first.Meta.EffectiveRole)
	require.NoError(t, first.Link.Release())

	// Steady-state effectiveRoleForCheckout must allocate 0 times (no Config().Copy()).
	conn, err := pgxPool.Acquire(context.Background())
	require.NoError(t, err)
	roleAllocs := testing.AllocsPerRun(50, func() {
		if role := effectiveRoleForCheckout("", conn); role != "app_user" {
			t.Fatalf("effectiveRole=%q, want app_user", role)
		}
	})
	conn.Release()
	if !raceEnabled {
		assert.Zero(t, roleAllocs, "steady-state effectiveRoleForCheckout must not deep-copy pgx.ConnConfig")

		// Steady-state Runtime.Checkout with empty PassthroughRoleName must not
		// allocate more than a passthrough checkout. Run 128 iterations (one
		// full pgxpool Conn wrapper batch) so both measurements see the exact
		// same pool amortization.
		nonPassthrough := testing.AllocsPerRun(128, func() {
			acq, err := runtime.Checkout(context.Background(), session.LinkRequest{})
			if err != nil {
				t.Fatal(err)
			}
			if acq.Meta.EffectiveRole != "app_user" {
				t.Fatalf("effectiveRole=%q, want app_user", acq.Meta.EffectiveRole)
			}
			_ = acq.Link.Release()
		})
		passthrough := testing.AllocsPerRun(128, func() {
			acq, err := runtime.Checkout(context.Background(), session.LinkRequest{PassthroughRoleName: "role-a"})
			if err != nil {
				t.Fatal(err)
			}
			if acq.Meta.EffectiveRole != "role-a" {
				t.Fatalf("effectiveRole=%q, want role-a", acq.Meta.EffectiveRole)
			}
			_ = acq.Link.Release()
		})
		assert.LessOrEqual(t, nonPassthrough, passthrough,
			"non-passthrough Runtime.Checkout (%v allocs) must not allocate more than passthrough (%v allocs)",
			nonPassthrough, passthrough)
	}
}

type releaseOnlyLink struct {
	link.Link
	conn *pgxpool.Conn
}

func (l *releaseOnlyLink) Release() error {
	l.conn.Release()
	return nil
}

func TestCheckoutUsesSelectedPoolsResetPolicy(t *testing.T) {
	policy := func(mode config.ResetMode) link.ResetPolicy {
		p, err := link.NewResetPolicy(config.DatabaseSettings{ResetMode: mode})
		require.NoError(t, err)
		return p
	}
	tests := []struct {
		name           string
		replicaResets  []link.ResetPolicy
		routeToReplica bool
		replicaAcquire func(context.Context) (*pgxpool.Conn, error)
		want           config.ResetMode
	}{
		{name: "primary", replicaResets: []link.ResetPolicy{policy(config.ResetDynamic)}, want: config.ResetAudit},
		{
			name: "replica", replicaResets: []link.ResetPolicy{policy(config.ResetDynamic)}, routeToReplica: true,
			replicaAcquire: func(context.Context) (*pgxpool.Conn, error) { return nil, nil },
			want:           config.ResetDynamic,
		},
		{
			name: "replica without a policy resets always", routeToReplica: true,
			replicaAcquire: func(context.Context) (*pgxpool.Conn, error) { return nil, nil },
			want:           config.ResetAlways,
		},
		{
			name: "replica failure falls back to the primary's policy", replicaResets: []link.ResetPolicy{policy(config.ResetDynamic)},
			routeToReplica: true,
			replicaAcquire: func(context.Context) (*pgxpool.Conn, error) { return nil, errors.New("replica unavailable") },
			want:           config.ResetAudit,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var got link.Options
			runtime := New(Config{
				ClusterName:         "cluster-a",
				PrimaryDatabaseName: "primary-a",
				Primary:             &fakePool{},
				Replicas:            []pool.Pool{&fakePool{acquire: test.replicaAcquire}},
				PrimaryReset:        policy(config.ResetAudit),
				ReplicaResets:       test.replicaResets,
				LinkFactory: func(_ *pgxpool.Conn, options link.Options) (link.Link, error) {
					got = options
					return nil, nil
				},
			})
			runtime.selector.randomIntFn = func(int) int { return 25 }

			_, err := runtime.Checkout(context.Background(), session.LinkRequest{
				DynamicRoutingEnabled: test.routeToReplica,
				ForReadQuery:          test.routeToReplica,
			})
			require.NoError(t, err)
			assert.Equal(t, test.want, got.Reset.Mode())
		})
	}
}
