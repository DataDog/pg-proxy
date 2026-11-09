// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package limiter

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	pgproxy "github.com/DataDog/pg-proxy"
)

const testServiceMax = int32(10_000)

func newTestLimiter(global int32) *Limiter {
	return New(Params{
		GlobalLimit:       global,
		DefaultServiceMax: testServiceMax,
	})
}

type limiterGaugeCall struct {
	name  string
	value float64
	tags  []string
	rate  float64
}

type limiterRecordingMetrics struct {
	pgproxy.NoopMetrics
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
		{name: "datadog.pg_proxy.listener.service_connections_open", value: 1, tags: []string{"client_service_account:service-a"}, rate: 1},
		{name: "datadog.pg_proxy.listener.connections_open", value: 1, tags: []string{}, rate: 1},
		{name: "datadog.pg_proxy.session_limit.global", value: 10, tags: []string{}, rate: 1},
	}, calls[:3])
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
		GlobalLimit:       1000,
		DefaultServiceMax: testServiceMax,
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
