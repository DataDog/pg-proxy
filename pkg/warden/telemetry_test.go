// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package warden

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pgproxy "github.com/DataDog/pg-proxy"
	"github.com/DataDog/pg-proxy/pkg/topology"
)

type logEvent struct {
	level   slog.Level
	message string
}

type metricEvent struct {
	name     string
	count    int64
	duration time.Duration
	tags     []string
	rate     float64
}

type recordingTelemetry struct {
	pgproxy.NoopMetrics
	mu      sync.Mutex
	records []logEvent
	calls   []metricEvent
}

func (r *recordingTelemetry) Enabled(context.Context, slog.Level) bool { return true }
func (r *recordingTelemetry) WithAttrs([]slog.Attr) slog.Handler       { return r }
func (r *recordingTelemetry) WithGroup(string) slog.Handler            { return r }
func (r *recordingTelemetry) Handle(_ context.Context, record slog.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, logEvent{record.Level, record.Message})
	return nil
}

func (r *recordingTelemetry) Count(name string, value int64, tags []string, rate float64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, metricEvent{name: name, count: value, tags: slices.Clone(tags), rate: rate})
	return nil
}

func (r *recordingTelemetry) Timing(name string, value time.Duration, tags []string, rate float64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, metricEvent{name: name, duration: value, tags: slices.Clone(tags), rate: rate})
	return nil
}

func (r *recordingTelemetry) logs() []logEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.records)
}

func (r *recordingTelemetry) metrics() []metricEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.calls)
}

func captureTelemetry(t *testing.T) *recordingTelemetry {
	t.Helper()
	telemetry := &recordingTelemetry{}
	t.Cleanup(pgproxy.Bootstrap(pgproxy.Dependencies{LogHandler: telemetry, Metrics: telemetry}))
	return telemetry
}

type telemetryConfig struct{ Host string }

func TestControllerTelemetry(t *testing.T) {
	telemetry := captureTelemetry(t)
	version := uint64(1)
	reject := false
	controller := NewController(ControllerConfig[telemetryConfig, string]{
		Interval: time.Hour,
		Source: func(context.Context) (telemetryConfig, uint64, error) {
			return telemetryConfig{Host: "host"}, version, nil
		},
		Normalize: func(cfg telemetryConfig) (Snapshot[string], error) {
			if reject {
				return Snapshot[string]{}, errors.New("rejected")
			}
			return Snapshot[string]{
				Databases:        map[Identifier]string{"primary": cfg.Host},
				Clusters:         map[Identifier]Cluster{"cluster": {Primary: "primary"}},
				LogicalDatabases: map[Identifier]topology.LogicalDatabase{"logical": {Cluster: "cluster"}},
			}, nil
		},
		Factory: func(context.Context, string, string, []string) (ManagedBackend, error) {
			return &testManagedBackend{}, nil
		},
		RefreshInterval: func(telemetryConfig) (time.Duration, bool) { return time.Minute, true },
	})
	t.Cleanup(controller.refresher.ticker.Stop)

	require.NoError(t, controller.Refresh())
	assert.Equal(t, []logEvent{
		{slog.LevelInfo, "applying configuration using source change counter: source_revision=1 configuration=&warden.telemetryConfig{Host:\"host\"}"},
		{slog.LevelInfo, "Updated databases:\n{\n  \"primary\": \"host\"\n}"},
		{slog.LevelInfo, "Updated clusters:\n{\n  \"cluster\": {\n    \"primary\": \"primary\",\n    \"replicas\": null,\n    \"Name\": \"cluster\"\n  }\n}"},
		{slog.LevelInfo, "States to create: [cluster]"},
		{slog.LevelInfo, "creating new proxy state for cluster \"cluster\""},
		{slog.LevelInfo, "Updated logical databases:\n{\n  \"logical\": {\n    \"Cluster\": \"cluster\",\n    \"ReadClusters\": null,\n    \"Name\": \"logical\"\n  }\n}"},
		{slog.LevelInfo, "Updated refresh period: 1m0s"},
	}, telemetry.logs())

	require.NoError(t, controller.Refresh())
	reject = true
	version++
	require.EqualError(t, controller.Refresh(), "failed to accept configuration: normalize configuration: rejected")

	metrics := telemetry.metrics()
	require.Len(t, metrics, 4)
	assert.Equal(t, "datadog.pg_proxy.warden.accept.duration", metrics[0].name)
	assert.GreaterOrEqual(t, metrics[0].duration, time.Duration(0))
	assert.Nil(t, metrics[0].tags)
	assert.Equal(t, float64(1), metrics[0].rate)
	assert.Equal(t, []metricEvent{
		{name: "datadog.pg_proxy.warden.accept.count", count: 1, tags: []string{"outcome:success"}, rate: 1},
		{name: "datadog.pg_proxy.warden.accept.count", count: 1, tags: []string{"outcome:skipped"}, rate: 1},
		{name: "datadog.pg_proxy.warden.accept.count", count: 1, tags: []string{"outcome:error"}, rate: 1},
	}, metrics[1:])
}

func TestReconcilerFailureTelemetry(t *testing.T) {
	telemetry := captureTelemetry(t)
	reconciler := NewReconciler(ReconcilerConfig[string]{
		Factory: func(context.Context, string, string, []string) (ManagedBackend, error) {
			return nil, errors.New("unavailable")
		},
		Retry: func(context.Context, func() (ManagedBackend, error)) (ManagedBackend, error) {
			return nil, errors.New("exhausted")
		},
	})
	require.NoError(t, reconciler.Accept(Snapshot[string]{
		Databases: map[Identifier]string{"primary": "host"},
		Clusters:  map[Identifier]Cluster{"cluster": {Primary: "primary"}},
	}))
	pending, _ := reconciler.backends.Get("cluster")
	select {
	case <-pending.done:
	case <-time.After(time.Second):
		t.Fatal("backend retry did not finish")
	}
	assert.Contains(t, telemetry.logs(), logEvent{slog.LevelError, "failed to create state for cluster \"cluster\" (will retry in the background): unavailable"})
	assert.Contains(t, telemetry.logs(), logEvent{slog.LevelError, "Future computation failed after exhausting retries: exhausted"})
	require.NoError(t, reconciler.Accept(Snapshot[string]{}))
	assert.Contains(t, telemetry.logs(), logEvent{slog.LevelInfo, "States to remove: [cluster]"})
}

func TestRefresherFailureTelemetry(t *testing.T) {
	telemetry := captureTelemetry(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	refresher := NewRefresher(RefresherConfig[string]{
		Context:  ctx,
		Interval: time.Millisecond,
		Source: func(context.Context) (string, uint64, error) {
			cancel()
			return "", 0, errors.New("source failed")
		},
	})
	done := make(chan struct{})
	go func() {
		refresher.run()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("refresh loop did not finish")
	}
	assert.Contains(t, telemetry.logs(), logEvent{slog.LevelError, "configuration refresh failed: failed to fetch configuration: source failed"})
	assert.Empty(t, telemetry.metrics(), "source failures must not count as rejected configurations")
}
