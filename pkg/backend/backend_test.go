// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package backend

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

// When the default factory, link.New, fails, no link holds the conn, so Acquire must
// destroy it: released alone it would return to the pool in an unknown state, and kept
// it would hold a pool slot forever. A custom factory owns its conn on error, so
// Acquire must leave it alone, including one the factory has already released.
func TestAcquireDiscardsConnOnlyForDefaultFactory(t *testing.T) {
	sentinel := errors.New("link factory sentinel")
	for _, tc := range []struct {
		name          string
		factory       LinkFactory // nil = link.New
		role          link.RoleSession
		wantDestroyed bool
	}{
		{name: "default factory, role set fails", role: failingRole{}, wantDestroyed: true},
		{name: "custom factory releases its conn", factory: func(conn *pgxpool.Conn, _ link.Options) (link.Link, error) {
			conn.Release()
			return nil, sentinel
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pgPool := fakepg.NewPool(t, fakepg.Answer)
			var acquired *pgxpool.Conn
			t.Cleanup(func() {
				if acquired != nil {
					acquired.Release() // a no-op once released; keeps a leak from hanging the pool's cleanup
				}
			})
			cfg := Config{
				ClusterName:         "cluster-a",
				PrimaryDatabaseName: "primary-a",
				Primary: &fakePool{acquire: func(ctx context.Context) (*pgxpool.Conn, error) {
					conn, err := pgPool.Acquire(ctx)
					acquired = conn
					return conn, err
				}},
				LinkFactory: tc.factory,
			}
			if tc.role != nil {
				cfg.RoleSessionFactory = func(string) link.RoleSession { return tc.role }
			}

			_, err := New(cfg).Acquire(context.Background(), session.LinkRequest{PassthroughRoleName: "app_user"})
			require.Error(t, err)
			if !tc.wantDestroyed {
				require.ErrorIs(t, err, sentinel)
				require.Equal(t, int32(1), pgPool.Stat().IdleConns(), "a custom factory's released conn stays pooled")
				return
			}
			// pgxpool destroys a closed conn in the background, so wait for it to go.
			require.Eventually(t, func() bool { return pgPool.Stat().TotalConns() == 0 }, 5*time.Second, time.Millisecond,
				"the conn must be destroyed: not left checked out, and not pooled in an unknown state")
		})
	}
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
	_, err := runtime.Acquire(context.Background(), session.LinkRequest{
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

func TestAcquireDecoratesLinkOnlyWhenRequested(t *testing.T) {
	primary := &fakePool{}
	decorated := 0
	var selected pool.Pool
	runtime := New(Config{
		ClusterName:         "cluster-a",
		PrimaryDatabaseName: "primary-a",
		Primary:             primary,
		LinkFactory: func(*pgxpool.Conn, link.Options) (link.Link, error) {
			return nil, nil
		},
		LinkDecorator: func(backendLink link.Link, selectedPool pool.Pool, _ *pgxpool.Conn, _ session.LinkRequest) link.Link {
			decorated++
			selected = selectedPool
			return backendLink
		},
	})

	_, err := runtime.Acquire(context.Background(), session.LinkRequest{})
	require.NoError(t, err)
	assert.Zero(t, decorated)

	_, err = runtime.Acquire(context.Background(), session.LinkRequest{DecorateLink: true})
	require.NoError(t, err)
	assert.Equal(t, 1, decorated)
	assert.Same(t, primary, selected)
}

func TestAcquireDecoratorReceivesExactSelectedPool(t *testing.T) {
	tests := []struct {
		name           string
		routeToReplica bool
		replicaAcquire func(context.Context) (*pgxpool.Conn, error)
		expectReplica  bool
	}{
		{name: "direct primary"},
		{
			name:           "successful replica",
			routeToReplica: true,
			replicaAcquire: func(context.Context) (*pgxpool.Conn, error) { return nil, nil },
			expectReplica:  true,
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
			var selected pool.Pool
			runtime := New(Config{
				ClusterName:         "cluster-a",
				PrimaryDatabaseName: "primary-a",
				Primary:             primary,
				Replicas:            []pool.Pool{replica},
				LinkFactory: func(*pgxpool.Conn, link.Options) (link.Link, error) {
					return nil, nil
				},
				LinkDecorator: func(backendLink link.Link, selectedPool pool.Pool, _ *pgxpool.Conn, _ session.LinkRequest) link.Link {
					selected = selectedPool
					return backendLink
				},
			})
			runtime.selector.randomIntFn = func(int) int { return 25 }

			_, err := runtime.Acquire(context.Background(), session.LinkRequest{
				DynamicRoutingEnabled: test.routeToReplica,
				ForReadQuery:          test.routeToReplica,
				DecorateLink:          true,
			})
			require.NoError(t, err)
			if test.expectReplica {
				assert.Same(t, replica, selected)
			} else {
				assert.Same(t, primary, selected)
			}
		})
	}
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

func TestAcquireUsesSelectedPoolsResetPolicy(t *testing.T) {
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

			_, err := runtime.Acquire(context.Background(), session.LinkRequest{
				DynamicRoutingEnabled: test.routeToReplica,
				ForReadQuery:          test.routeToReplica,
			})
			require.NoError(t, err)
			assert.Equal(t, test.want, got.Reset.Mode())
		})
	}
}
