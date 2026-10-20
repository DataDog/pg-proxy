package limiter

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/stretchr/testify/assert"

	pgproxy "github.com/DataDog/pg-proxy"
)

const (
	miB = 1024 * 1024
	giB = 1024 * miB

	testServiceMax  = int32(10_000)
	testGlobalLimit = int32(100_000)
)

func newTestLimiter(global int32) *Limiter {
	return New(Params{
		GlobalLimit:                global,
		DefaultServiceMax:          testServiceMax,
		MemDerivedGlobalLimitFloor: testServiceMax,
	})
}

type limiterGaugeCall struct {
	name  string
	value float64
	tags  []string
	rate  float64
}

type limiterRecordingMetrics struct {
	statsd.NoOpClient
	mu     sync.Mutex
	gauges []limiterGaugeCall
}

func (m *limiterRecordingMetrics) Gauge(name string, value float64, tags []string, rate float64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var recordedTags []string
	if tags != nil {
		recordedTags = make([]string, len(tags))
		copy(recordedTags, tags)
	}
	m.gauges = append(m.gauges, limiterGaugeCall{name: name, value: value, tags: recordedTags, rate: rate})
	return nil
}

func (m *limiterRecordingMetrics) snapshot() []limiterGaugeCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]limiterGaugeCall(nil), m.gauges...)
}

func TestExportGaugesUsesBootstrappedMetricsAndStops(t *testing.T) {
	metrics := &limiterRecordingMetrics{}
	restore := pgproxy.Bootstrap(pgproxy.Dependencies{Metrics: metrics})
	t.Cleanup(restore)
	sl := New(Params{
		GlobalLimit:       10,
		DefaultServiceMax: 10,
		MemoryFallback:    42,
		GaugeInterval:     time.Millisecond,
	})
	assert.NoError(t, sl.RegisterSession("service-a"))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		sl.ExportGauges(ctx)
		close(done)
	}()
	assert.Eventually(t, func() bool { return len(metrics.snapshot()) >= 3 }, time.Second, time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("ExportGauges did not stop after context cancellation")
	}

	calls := metrics.snapshot()
	assert.Equal(t, []limiterGaugeCall{
		{name: "dd.orgstore.pg_proxy.listener.service_connections_open", value: 1, tags: []string{"client_service_account:service-a"}, rate: 1},
		{name: "dd.orgstore.pg_proxy.listener.connections_open", value: 1, tags: []string{}, rate: 1},
		{name: "dd.orgstore.pg_proxy.session_limit.global", value: 42, tags: []string{"mode:mem_derived"}, rate: 1},
	}, calls[:3])
}

// TestMemDerivedLimit validates the formula across representative memory hard-limit values.
func TestMemDerivedLimit(t *testing.T) {
	tests := []struct {
		name       string
		memBytes   int64
		wantApprox int32
	}{
		{
			name:       "400 MiB (hits floor)",
			memBytes:   400 * miB,
			wantApprox: testServiceMax,
		},
		{
			name:       "800 MiB",
			memBytes:   800 * miB,
			wantApprox: 10_922,
		},
		{
			name:       "1.6 GiB",
			memBytes:   1638 * miB,
			wantApprox: 22_364,
		},
		{
			name:       "3.2 GiB",
			memBytes:   3276 * miB,
			wantApprox: 44_728,
		},
		{
			name:       "1 GiB",
			memBytes:   1 * giB,
			wantApprox: 13_981,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := MemDerivedLimit(tc.memBytes, testServiceMax)
			delta := float64(tc.wantApprox) * 0.05
			assert.InDelta(t, tc.wantApprox, got, delta, "got %d, want ~%d", got, tc.wantApprox)
		})
	}
}

// TestMemDerivedLimitFloor validates that very small or invalid memory values return the floor.
func TestMemDerivedLimitFloor(t *testing.T) {
	tests := []struct {
		name     string
		memBytes int64
	}{
		{"100 MiB", 100 * miB},
		{"500 MiB (just below floor)", 500 * miB},
		{"zero", 0},
		{"negative", -1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := MemDerivedLimit(tc.memBytes, testServiceMax)
			assert.Equal(t, testServiceMax, got)
		})
	}
}

// TestRegisterSessionLimit validates that the limiter enforces the per-service limit.
func TestRegisterSessionLimit(t *testing.T) {
	service := "service_foo"
	sl := newTestLimiter(5)
	sl.RegisterService(service, 2)

	assert.NoError(t, sl.RegisterSession(service))
	assert.NoError(t, sl.RegisterSession(service))

	err := sl.RegisterSession(service)
	assert.Error(t, err)
	assert.Equal(t, string(ServiceLimitExceeded), sl.GetErrorType(err))
}

// TestRegisterSessionGlobalLimit validates that the limiter enforces the global limit,
// which is checked before the service limit.
func TestRegisterSessionGlobalLimit(t *testing.T) {
	service := "service_foo"
	sl := newTestLimiter(2)
	sl.RegisterService(service, 2)

	assert.NoError(t, sl.RegisterSession(service))
	assert.NoError(t, sl.RegisterSession(service))

	err := sl.RegisterSession(service)
	assert.Error(t, err)
	assert.Equal(t, string(GlobalLimitExceeded), sl.GetErrorType(err))
}

// TestRegisterSessionEnforceMemDerived validates that when enforceMemDerived returns true,
// RegisterSession enforces min(globalLimit, memDerivedGlobalLimit) and tags the rejection
// with the cap mode that actually bound.
//
// The test stores memDerivedGlobalLimit directly to bypass the formula's floor;
// the floor is covered by TestMemDerivedLimitFloor.
func TestRegisterSessionEnforceMemDerived(t *testing.T) {
	tests := []struct {
		name            string
		globalLimit     int32
		memDerivedLimit int32
		flagEnabled     bool
		expectedCapMode string
	}{
		{
			name:            "flag on, mem-derived smaller -> mem-derived binds",
			globalLimit:     1000,
			memDerivedLimit: 2,
			flagEnabled:     true,
			expectedCapMode: CapModeMemDerived,
		},
		{
			name:            "flag on, static smaller -> clamp keeps static cap",
			globalLimit:     2,
			memDerivedLimit: 1000,
			flagEnabled:     true,
			expectedCapMode: CapModeDefault,
		},
		{
			name:            "flag off -> mem-derived ignored",
			globalLimit:     2,
			memDerivedLimit: 1,
			flagEnabled:     false,
			expectedCapMode: CapModeDefault,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			service := "service_foo"
			sl := newTestLimiter(tc.globalLimit)
			sl.RegisterService(service, 1000)
			sl.memDerivedGlobalLimit.Store(tc.memDerivedLimit)
			sl.SetEnforceMemDerived(func() bool { return tc.flagEnabled })

			assert.NoError(t, sl.RegisterSession(service))
			assert.NoError(t, sl.RegisterSession(service))
			err := sl.RegisterSession(service)
			assert.Error(t, err)
			assert.Equal(t, string(GlobalLimitExceeded), sl.GetErrorType(err))
			assert.Equal(t, tc.expectedCapMode, sl.GetCapMode(err))
		})
	}
}

// TestEffectiveGlobalLimit validates that EffectiveGlobalLimit reports the same binding cap
// and mode that RegisterSession enforces, across the static/memory-derived combinations
// covered by TestRegisterSessionEnforceMemDerived.
func TestEffectiveGlobalLimit(t *testing.T) {
	tests := []struct {
		name            string
		globalLimit     int32
		memDerivedLimit int32
		flagEnabled     bool
		wantLimit       int32
		wantCapMode     string
	}{
		{
			name:            "flag on, mem-derived smaller -> mem-derived binds",
			globalLimit:     1000,
			memDerivedLimit: 2,
			flagEnabled:     true,
			wantLimit:       2,
			wantCapMode:     CapModeMemDerived,
		},
		{
			name:            "flag on, static smaller -> clamp keeps static cap",
			globalLimit:     2,
			memDerivedLimit: 1000,
			flagEnabled:     true,
			wantLimit:       2,
			wantCapMode:     CapModeDefault,
		},
		{
			name:            "flag off -> mem-derived ignored",
			globalLimit:     2,
			memDerivedLimit: 1,
			flagEnabled:     false,
			wantLimit:       2,
			wantCapMode:     CapModeDefault,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sl := newTestLimiter(tc.globalLimit)
			sl.memDerivedGlobalLimit.Store(tc.memDerivedLimit)
			sl.SetEnforceMemDerived(func() bool { return tc.flagEnabled })

			gotLimit, gotCapMode := sl.EffectiveGlobalLimit()
			assert.Equal(t, tc.wantLimit, gotLimit)
			assert.Equal(t, tc.wantCapMode, gotCapMode)
		})
	}
}

// TestDeregisterSessionUnderflow validates safe deregistration behavior and underflow protection.
func TestDeregisterSessionUnderflow(t *testing.T) {
	service := "service_foo"
	sl := newTestLimiter(2)
	sl.RegisterService(service, 2)

	assert.NoError(t, sl.RegisterSession(service))
	assert.NoError(t, sl.DeregisterSession(service))
	assert.NoError(t, sl.DeregisterSession(service))

	assert.Equal(t, int32(0), sl.GetGlobalCount())
	assert.Equal(t, int32(0), sl.GetCurrentServiceCount(service))
}

// TestRegisterAndDeregisterConcurrent ensures sessions can be registered and safely
// deregistered concurrently. Only successful registrations are deregistered, so counts
// return to zero. This test is deterministic and non-flaky.
func TestRegisterAndDeregisterConcurrent(t *testing.T) {
	service := "service_foo"
	limit := int32(50)
	sl := newTestLimiter(limit)
	sl.RegisterService(service, limit)

	var wg sync.WaitGroup
	var start sync.WaitGroup
	start.Add(1)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start.Wait()
			if err := sl.RegisterSession(service); err == nil {
				assert.NoError(t, sl.DeregisterSession(service))
			}
		}()
	}

	start.Done()
	wg.Wait()

	assert.Equal(t, int32(0), sl.GetGlobalCount())
	assert.Equal(t, int32(0), sl.GetCurrentServiceCount(service))
}

// TestLimitEnforcementConcurrent verifies that the session limit is strictly enforced
// under high concurrency. The success count may be slightly under the limit due to
// over-strict atomic enforcement. This test is deterministic and non-flaky.
func TestLimitEnforcementConcurrent(t *testing.T) {
	service := "service_foo"
	limit := int32(50)
	sl := newTestLimiter(limit)
	sl.RegisterService(service, limit)

	var wg sync.WaitGroup
	var start sync.WaitGroup
	start.Add(1)

	var success int32

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start.Wait()
			if err := sl.RegisterSession(service); err == nil {
				atomic.AddInt32(&success, 1)
			}
		}()
	}

	start.Done()
	wg.Wait()

	assert.LessOrEqual(t, success, limit)
	assert.Equal(t, limit, sl.GetGlobalCount())
	assert.Equal(t, limit, sl.GetCurrentServiceCount(service))
}

// TestRegisterSessionDoesNotPanicConcurrent ensures RegisterSession is safe when the
// same new service name is used concurrently. It guards against unsynchronized map writes
// and is deterministic and non-flaky.
func TestRegisterSessionDoesNotPanicConcurrent(t *testing.T) {
	sl := newTestLimiter(1000)

	var wg sync.WaitGroup
	var start sync.WaitGroup
	start.Add(1)

	var didPanic atomic.Bool

	numGoroutines := 100
	services := make([]string, numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		services[i] = fmt.Sprintf("service_%d", i)
	}

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(serviceIdx int) {
			defer wg.Done()
			start.Wait()
			defer func() {
				if r := recover(); r != nil {
					didPanic.Store(true)
				}
			}()
			_ = sl.RegisterSession(services[serviceIdx])
		}(i)
	}

	start.Done()
	wg.Wait()

	assert.False(t, didPanic.Load(), "expected no panic from concurrent RegisterSession calls")
	assert.Equal(t, int32(numGoroutines), sl.GetGlobalCount())
}

func TestNewRegistersQuotas(t *testing.T) {
	sl := New(Params{
		GlobalLimit:                1000,
		DefaultServiceMax:          testServiceMax,
		MemDerivedGlobalLimitFloor: testServiceMax,
		Quotas: []ServiceQuota{
			{Name: "svc-a", Max: 2},
			{Name: "svc-b", Max: 3},
		},
	})

	quotas := []ServiceQuota{
		{Name: "svc-a", Max: 2},
		{Name: "svc-b", Max: 3},
	}
	for _, quota := range quotas {
		limit, ok := sl.ServiceLimit(quota.Name)
		assert.True(t, ok)
		assert.Equal(t, quota.Max, limit)
		for i := int32(0); i < quota.Max; i++ {
			assert.NoError(t, sl.RegisterSession(quota.Name))
		}
		err := sl.RegisterSession(quota.Name)
		assert.Error(t, err)
		assert.Equal(t, string(ServiceLimitExceeded), sl.GetErrorType(err))
	}
}

func TestNewUsesMemoryBudget(t *testing.T) {
	sl := New(Params{
		GlobalLimit:                100_000,
		DefaultServiceMax:          testServiceMax,
		MemDerivedGlobalLimitFloor: testServiceMax,
		Memory:                     Memory{Bytes: 1 * giB, Available: true},
	})
	assert.Equal(t, MemDerivedLimit(1*giB, testServiceMax), sl.GetMemDerivedGlobalLimit())
}

func TestNewFallsBackWithoutMemoryLimit(t *testing.T) {
	sl := New(Params{
		GlobalLimit:                5,
		DefaultServiceMax:          testServiceMax,
		MemDerivedGlobalLimitFloor: testServiceMax,
		Memory:                     Memory{},
	})
	assert.Equal(t, int32(5), sl.GetMemDerivedGlobalLimit())
}

func TestRefreshTracksResize(t *testing.T) {
	sl := New(Params{
		GlobalLimit:                testGlobalLimit,
		DefaultServiceMax:          testServiceMax,
		MemDerivedGlobalLimitFloor: testServiceMax,
		Memory:                     Memory{Bytes: 1 * giB, Available: true},
	})
	atOneGiB := sl.GetMemDerivedGlobalLimit()
	assert.Equal(t, MemDerivedLimit(1*giB, testServiceMax), atOneGiB)

	sl.setMemDerivedGlobalLimit(Memory{Bytes: 2 * giB, Available: true}, testGlobalLimit)
	atTwoGiB := sl.GetMemDerivedGlobalLimit()
	assert.Equal(t, MemDerivedLimit(2*giB, testServiceMax), atTwoGiB)
	assert.Greater(t, atTwoGiB, atOneGiB)

	sl.setMemDerivedGlobalLimit(Memory{Bytes: 800 * miB, Available: true}, testGlobalLimit)
	atEightHundredMiB := sl.GetMemDerivedGlobalLimit()
	assert.Equal(t, MemDerivedLimit(800*miB, testServiceMax), atEightHundredMiB)
	assert.Less(t, atEightHundredMiB, atOneGiB)
}

func TestReadFailureLeavesMemDerivedLimitUnchanged(t *testing.T) {
	sl := New(Params{
		GlobalLimit:                testGlobalLimit,
		DefaultServiceMax:          testServiceMax,
		MemDerivedGlobalLimitFloor: testServiceMax,
		Memory:                     Memory{Bytes: 1 * giB, Available: true},
	})
	want := sl.GetMemDerivedGlobalLimit()

	assert.False(t, sl.applyCgroupRefresh(Memory{}, false))
	assert.Equal(t, want, sl.GetMemDerivedGlobalLimit())

	assert.True(t, sl.applyCgroupRefresh(Memory{Bytes: 2 * giB, Available: true}, true))
	assert.Equal(t, MemDerivedLimit(2*giB, testServiceMax), sl.GetMemDerivedGlobalLimit())
}

func TestRefreshFlowsIntoEffectiveGlobalLimit(t *testing.T) {
	sl := New(Params{
		GlobalLimit:                testGlobalLimit,
		DefaultServiceMax:          testServiceMax,
		MemDerivedGlobalLimitFloor: testServiceMax,
		Memory:                     Memory{Bytes: 2 * giB, Available: true},
	})
	sl.SetEnforceMemDerived(func() bool { return true })
	before, mode := sl.EffectiveGlobalLimit()
	assert.Equal(t, CapModeMemDerived, mode)

	sl.setMemDerivedGlobalLimit(Memory{Bytes: 800 * miB, Available: true}, testGlobalLimit)

	after, mode := sl.EffectiveGlobalLimit()
	assert.Equal(t, CapModeMemDerived, mode)
	assert.Less(t, after, before)
	assert.Equal(t, MemDerivedLimit(800*miB, testServiceMax), after)
}
