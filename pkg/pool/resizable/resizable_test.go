package resizable

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pgproxy "github.com/DataDog/pg-proxy"
)

type testPingConnection struct {
	pingErr  error
	released bool
}

type resizeMetricCall struct {
	name  string
	value float64
	tags  []string
}

type resizeRecordingMetrics struct {
	pgproxy.NoopMetrics
	counts []resizeMetricCall
	gauges []resizeMetricCall
}

func (m *resizeRecordingMetrics) Count(name string, value int64, tags []string, _ float64) error {
	m.counts = append(m.counts, resizeMetricCall{name: name, value: float64(value), tags: append([]string(nil), tags...)})
	return nil
}

func (m *resizeRecordingMetrics) Gauge(name string, value float64, tags []string, _ float64) error {
	m.gauges = append(m.gauges, resizeMetricCall{name: name, value: value, tags: append([]string(nil), tags...)})
	return nil
}

func (c *testPingConnection) Ping(context.Context) error { return c.pingErr }
func (c *testPingConnection) Release()                   { c.released = true }

func TestNewValidatesLimits(t *testing.T) {
	_, err := New(context.Background(), nil, 1)
	require.EqualError(t, err, "pgxpool config must not be nil")

	config := testConfig(t, 10)
	_, err = New(context.Background(), config, 0)
	require.EqualError(t, err, "connection limit must be positive: 0")
	_, err = New(context.Background(), config, 11)
	require.EqualError(t, err, "connection limit 11 exceeds physical ceiling 10")
}

func TestNewPreservesConfiguration(t *testing.T) {
	config := testConfig(t, 10)
	config.MinConns = 0
	config.MinIdleConns = 0
	originalBeforeConnect := config.BeforeConnect
	resizable, err := New(context.Background(), config, 2)
	require.NoError(t, err)
	t.Cleanup(resizable.Close)

	assert.EqualValues(t, 10, resizable.GetPhysicalCeiling())
	assert.EqualValues(t, 2, resizable.Limit())
	assert.EqualValues(t, 0, resizable.Config().MinConns)
	assert.EqualValues(t, 0, resizable.Config().MinIdleConns)
	assert.Equal(t, originalBeforeConnect == nil, config.BeforeConnect == nil)
}

func TestPgxPoolMinimumConnectionDefaultsRemainDisabled(t *testing.T) {
	config, err := pgxpool.ParseConfig("postgres://localhost/test")
	require.NoError(t, err)
	require.Zero(t, config.MinConns, "non-zero MinConns requires generation-aware cleanup for background connections")
	require.Zero(t, config.MinIdleConns, "non-zero MinIdleConns requires generation-aware cleanup for background connections")
}

func TestAcquireWithGateRetriesAfterVirtualLimit(t *testing.T) {
	gate, err := newResizableGate(1)
	require.NoError(t, err)
	acquisitions := 0
	acquire := func(context.Context) (*pgxpool.Conn, error) {
		acquisitions++
		if acquisitions == 1 {
			return nil, errVirtualConnectionLimit
		}
		return nil, nil
	}

	conn, err := acquireWithGate(context.Background(), gate, acquire)
	require.NoError(t, err)
	assert.Nil(t, conn)
	assert.Equal(t, 2, acquisitions)
}

func TestAcquireWithGateRetriesAfterEarlyReleaseNotification(t *testing.T) {
	gate, err := newResizableGate(1)
	require.NoError(t, err)
	acquisitions := 0
	acquire := func(context.Context) (*pgxpool.Conn, error) {
		acquisitions++
		switch acquisitions {
		case 1:
			gate.notifyIdleConnAvailability()
			return nil, errVirtualConnectionLimit
		case 2:
			// The release notification ran before pgxpool returned the physical
			// connection to its idle queue, so the immediate retry still fails.
			return nil, errVirtualConnectionLimit
		default:
			return nil, nil
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, err := acquireWithGate(ctx, gate, acquire)
	require.NoError(t, err)
	assert.Nil(t, conn)
	assert.Equal(t, 3, acquisitions)
}

func TestAcquireWithGateDoesNotBlockIdleAcquireBehindConstruction(t *testing.T) {
	gate, err := newResizableGate(1)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	constructionStarted := make(chan struct{})
	finishConstruction := make(chan struct{})
	firstResult := make(chan error, 1)
	firstAcquisitions := 0
	go func() {
		_, err := acquireWithGate(ctx, gate, func(context.Context) (*pgxpool.Conn, error) {
			firstAcquisitions++
			if firstAcquisitions == 1 {
				return nil, errVirtualConnectionLimit
			}
			close(constructionStarted)
			<-finishConstruction
			return nil, nil
		})
		firstResult <- err
	}()
	<-constructionStarted

	secondResult := make(chan error, 1)
	secondAcquisitions := 0
	go func() {
		_, err := acquireWithGate(ctx, gate, func(context.Context) (*pgxpool.Conn, error) {
			secondAcquisitions++
			if secondAcquisitions == 1 {
				return nil, errVirtualConnectionLimit
			}
			return nil, nil
		})
		secondResult <- err
	}()

	select {
	case err := <-secondResult:
		require.NoError(t, err)
	case <-time.After(time.Second):
		close(finishConstruction)
		t.Fatal("idle connection acquisition was blocked behind connection construction")
	}
	close(finishConstruction)
	require.NoError(t, <-firstResult)
	assert.Equal(t, 2, firstAcquisitions)
	assert.Equal(t, 2, secondAcquisitions)
}

func TestAcquireWithGateHonorsCancellation(t *testing.T) {
	gate, err := newResizableGate(1)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = acquireWithGate(ctx, gate, func(context.Context) (*pgxpool.Conn, error) {
		return nil, errVirtualConnectionLimit
	})
	require.ErrorIs(t, err, context.Canceled)
}

func TestAcquireWithGateReturnsOtherErrors(t *testing.T) {
	gate, err := newResizableGate(1)
	require.NoError(t, err)
	expected := errors.New("database unavailable")

	_, err = acquireWithGate(context.Background(), gate, func(context.Context) (*pgxpool.Conn, error) {
		return nil, expected
	})
	require.ErrorIs(t, err, expected)
}

func TestResize(t *testing.T) {
	resizable, err := New(context.Background(), testConfig(t, 10), 5)
	require.NoError(t, err)
	t.Cleanup(resizable.Close)

	require.NoError(t, resizable.Resize(8))
	assert.EqualValues(t, 8, resizable.Limit())
	require.NoError(t, resizable.Resize(3))
	assert.EqualValues(t, 3, resizable.Limit())
	require.EqualError(t, resizable.Resize(11), "connection limit 11 exceeds physical ceiling 10")
	assert.EqualValues(t, 3, resizable.Limit())
	require.EqualError(t, resizable.Resize(0), "connection limit must be positive: 0")
	assert.EqualValues(t, 3, resizable.Limit())
}

func TestResizeLogsLimitChangeAndReset(t *testing.T) {
	var output bytes.Buffer
	metrics := &resizeRecordingMetrics{}
	restore := pgproxy.Bootstrap(pgproxy.Dependencies{
		LogHandler: slog.NewTextHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug}),
		Metrics:    metrics,
	})
	t.Cleanup(restore)

	resizable, err := New(context.Background(), testConfig(t, 10), 2)
	require.NoError(t, err)
	t.Cleanup(resizable.Close)
	require.True(t, resizable.gate.tryReserve())
	require.True(t, resizable.gate.tryReserve())

	require.NoError(t, resizable.Resize(1))
	logs := output.String()
	assert.Contains(t, logs, "resizable connection pool limit changed")
	assert.Contains(t, logs, "old_limit=2")
	assert.Contains(t, logs, "new_limit=1")
	assert.Contains(t, logs, "total_open_connections=2")
	assert.Contains(t, logs, "retirement_required=true")
	require.Len(t, metrics.counts, 1)
	assert.Equal(t, resizeMetricCall{
		name:  poolResizeMetric,
		value: 1,
		tags:  []string{"outcome:success"},
	}, metrics.counts[0])
	require.Len(t, metrics.gauges, 1)
	assert.Equal(t, resizeMetricCall{name: poolLimitMetric, value: 1}, metrics.gauges[0])
}

func TestResizeLogsAndCountsFailures(t *testing.T) {
	var output bytes.Buffer
	metrics := &resizeRecordingMetrics{}
	restore := pgproxy.Bootstrap(pgproxy.Dependencies{
		LogHandler: slog.NewTextHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug}),
		Metrics:    metrics,
	})
	t.Cleanup(restore)

	resizable, err := New(context.Background(), testConfig(t, 10), 5)
	require.NoError(t, err)
	t.Cleanup(resizable.Close)

	require.EqualError(t, resizable.Resize(11), "connection limit 11 exceeds physical ceiling 10")
	require.EqualError(t, resizable.Resize(0), "connection limit must be positive: 0")
	logs := output.String()
	assert.Contains(t, logs, "resizable connection pool resize failed")
	assert.Contains(t, logs, "reason=physical_ceiling")
	assert.Contains(t, logs, "new_limit=11")
	assert.Contains(t, logs, "physical_ceiling=10")
	assert.Contains(t, logs, "reason=invalid_limit")
	assert.Contains(t, logs, "new_limit=0")
	require.Len(t, metrics.counts, 2)
	assert.Equal(t, []string{"outcome:error", "reason:physical_ceiling"}, metrics.counts[0].tags)
	assert.Equal(t, []string{"outcome:error", "reason:invalid_limit"}, metrics.counts[1].tags)
	assert.Empty(t, metrics.gauges)
}

func TestPingWithAcquireReleasesConnection(t *testing.T) {
	expected := errors.New("ping failed")
	conn := &testPingConnection{pingErr: expected}

	err := pingWithAcquire(context.Background(), func(context.Context) (pingConnection, error) {
		return conn, nil
	})

	require.ErrorIs(t, err, expected)
	assert.True(t, conn.released)
}

func TestPingWithAcquireReturnsAcquireError(t *testing.T) {
	expected := errors.New("acquire failed")
	called := false

	err := pingWithAcquire(context.Background(), func(context.Context) (pingConnection, error) {
		called = true
		return nil, expected
	})

	require.ErrorIs(t, err, expected)
	assert.True(t, called)
}

func TestResetLeavesPoolOpen(t *testing.T) {
	resizable, err := New(context.Background(), testConfig(t, 10), 5)
	require.NoError(t, err)
	t.Cleanup(resizable.Close)

	resizable.Reset()
	assert.NotNil(t, resizable.Stat())
}

func testConfig(t *testing.T, maxConns int32) *pgxpool.Config {
	t.Helper()
	config, err := pgxpool.ParseConfig("postgres://localhost/test")
	require.NoError(t, err)
	config.MaxConns = maxConns
	config.MinConns = 0
	config.MinIdleConns = 0
	return config
}

func TestReconciliationRetiresLateIdleConnections(t *testing.T) {
	for _, publication := range []string{"after_release", "canceled_constructor"} {
		t.Run(publication, func(t *testing.T) {
			gate, err := newResizableGate(2)
			require.NoError(t, err)
			require.True(t, gate.tryReserve())
			require.True(t, gate.tryReserve())
			config := testConfig(t, 2)
			installHooks(config, gate)
			if publication == "after_release" {
				// Both hooks decided to keep under the old limit. Puddle has
				// not published either resource idle yet.
				require.True(t, config.AfterRelease(nil))
				require.True(t, config.AfterRelease(nil))
			}
			require.NoError(t, gate.resize(1))

			var idle atomic.Bool
			swept := make(chan struct{}, 1)
			retired := make(chan struct{}, 2)
			r := startTestReconciler(t, gate, func() {
				if !idle.CompareAndSwap(true, false) {
					select {
					case swept <- struct{}{}:
					default:
					}
					return
				}
				// Releasing the newly idle resources reruns the actual hook.
				for i := 0; i < 2; i++ {
					if !config.AfterRelease(nil) {
						retired <- struct{}{}
					}
				}
			})
			r.requestReconciliation()
			select {
			case <-swept:
			case <-time.After(time.Second):
				t.Fatal("initial sweep did not run")
			}
			// Publish after the first sweep, with no further resize, client
			// acquisition, or notification. Canceled constructors bypass the
			// release hook entirely when publishing idle.
			idle.Store(true)
			select {
			case <-retired:
			case <-time.After(time.Second):
				t.Fatal("late idle connection was not retired")
			}
			assert.EqualValues(t, 1, gate.getNumConnsRetiring())
			assert.EqualValues(t, 2, gate.getTotalOpenConns())
			assert.Never(t, func() bool { return len(retired) > 0 },
				3*retirementRetryInterval, time.Millisecond)
			gate.retiringConnClosed()
			assert.EqualValues(t, 1, gate.getTotalOpenConns())
		})
	}
}

func TestReconciliationWaitsForAlreadySelectedClosures(t *testing.T) {
	gate, err := newResizableGate(4)
	require.NoError(t, err)
	for i := 0; i < 4; i++ {
		require.True(t, gate.tryReserve())
	}
	require.NoError(t, gate.resize(2))
	require.True(t, gate.tryRetireExcessConnection())
	require.True(t, gate.tryRetireExcessConnection())

	var sweeps atomic.Int32
	r := startTestReconciler(t, gate, func() {
		sweeps.Add(1)
		for i := 0; i < 4; i++ {
			gate.tryRetireExcessConnection()
		}
	})
	r.requestReconciliation()
	assert.Never(t, func() bool { return sweeps.Load() > 0 },
		3*retirementRetryInterval, time.Millisecond)
	assert.EqualValues(t, 4, gate.getTotalOpenConns())

	// A further downsize needs exactly one additional retirement even while
	// the previous two connections are still physically open.
	require.NoError(t, gate.resize(1))
	r.requestReconciliation()
	assert.Eventually(t, func() bool { return gate.getNumConnsRetiring() == 3 },
		time.Second, time.Millisecond)
	require.False(t, gate.tryRetireExcessConnection())
	assert.EqualValues(t, 4, gate.getTotalOpenConns())
}

func TestReconciliationUsesLatestLimit(t *testing.T) {
	gate, err := newResizableGate(2)
	require.NoError(t, err)
	require.True(t, gate.tryReserve())
	require.True(t, gate.tryReserve())
	require.NoError(t, gate.resize(1))
	var sweeps atomic.Int32
	firstSweep := make(chan struct{})
	resumeSweep := make(chan struct{})
	r := startTestReconciler(t, gate, func() {
		if sweeps.Add(1) == 1 {
			close(firstSweep)
			<-resumeSweep
		}
		gate.tryRetireExcessConnection()
	})
	// Unblock the sweep even if the test fails.
	t.Cleanup(func() {
		if !channelClosed(resumeSweep) {
			close(resumeSweep)
		}
	})
	r.requestReconciliation()
	select {
	case <-firstSweep:
	case <-time.After(time.Second):
		t.Fatal("initial sweep did not run")
	}
	require.NoError(t, gate.resize(2))
	r.requestReconciliation()
	close(resumeSweep)
	assert.Never(t, func() bool { return sweeps.Load() > 1 },
		3*retirementRetryInterval, time.Millisecond)
	assert.Zero(t, gate.getNumConnsRetiring())

	require.NoError(t, gate.resize(1))
	r.requestReconciliation()
	assert.Eventually(t, func() bool { return gate.getNumConnsRetiring() == 1 },
		time.Second, time.Millisecond)
}

func TestCloseStopsReconciliationWithExcessConnections(t *testing.T) {
	r, err := New(context.Background(), testConfig(t, 2), 2)
	require.NoError(t, err)
	t.Cleanup(r.Close)
	// Simulate outstanding constructors with no idle resources to sweep.
	require.True(t, r.gate.tryReserve())
	require.True(t, r.gate.tryReserve())
	require.NoError(t, r.Resize(1))
	require.NoError(t, r.Resize(1)) // Same-limit requests are also safe.
	closed := make(chan struct{})
	go func() {
		r.Close()
		r.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close waited for reconciliation to reach the target")
	}
	assert.True(t, channelClosed(r.reconcileDone))
}

func startTestReconciler(t *testing.T, gate *resizableGate, sweep func()) *Resizable {
	t.Helper()
	r := &Resizable{
		gate:          gate,
		reconcileWake: make(chan struct{}, 1),
		reconcileStop: make(chan struct{}),
		reconcileDone: make(chan struct{}),
	}
	go r.reconcileConnections(sweep)
	t.Cleanup(func() {
		close(r.reconcileStop)
		select {
		case <-r.reconcileDone:
		case <-time.After(time.Second):
			t.Error("reconciliation worker did not stop")
		}
	})
	return r
}
