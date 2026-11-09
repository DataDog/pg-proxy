// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package pgproxy

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests replace one process-wide value, so none of them may call
// t.Parallel(): concurrent Bootstraps would race over which one wins.

// TestAccessorsNeverNil pins the invariant the accessors' callers rely on to
// skip nil checks. It matters most for Logger: methods on a nil *slog.Logger
// dereference the handler and panic, so returning nil would turn a missing
// Bootstrap into a crash on the first diagnostic.
func TestAccessorsNeverNil(t *testing.T) {
	require.NotNil(t, Logger())
	require.NotNil(t, Metrics())
	require.NotNil(t, Tracing())

	// The no-op substitutes must be usable, not merely non-nil.
	Logger().Debug("must not panic")
	assert.NoError(t, Metrics().Count("test.count", 1, nil, 1))
}

func TestBootstrapInstallsDependencies(t *testing.T) {
	handler := slog.NewTextHandler(io.Discard, nil)
	metrics := &NoopMetrics{}

	restore := Bootstrap(Dependencies{LogHandler: handler, Metrics: metrics})
	t.Cleanup(restore)

	assert.Same(t, handler, Logger().Handler(), "Logger must wrap the supplied handler")
	assert.Same(t, metrics, Deps().Metrics, "Deps must report the supplied client")
	assert.Equal(t, namespacedMetrics{namespace: DefaultMetricNamespace, inner: metrics}, Metrics(),
		"Metrics must forward to the supplied client with the namespace")
	assert.Same(t, handler, Deps().LogHandler, "Deps must report the raw value as supplied")
}

func TestRestoreRevertsToPreviousDependencies(t *testing.T) {
	before := Logger()

	restore := Bootstrap(Dependencies{LogHandler: slog.NewTextHandler(io.Discard, nil)})
	require.NotSame(t, before, Logger(), "Bootstrap must have taken effect")
	restore()

	assert.Same(t, before, Logger(), "restore must reinstate the prior logger")
}

// TestPartialDependenciesFallBack covers the mixed case: one field supplied,
// the other left unset, which must not disturb the unset one's no-op.
func TestPartialDependenciesFallBack(t *testing.T) {
	metrics := &NoopMetrics{}

	restore := Bootstrap(Dependencies{Metrics: metrics})
	t.Cleanup(restore)

	assert.Same(t, metrics, Deps().Metrics)
	require.NotNil(t, Logger(), "an unset LogHandler must still yield a usable logger")
	Logger().Debug("must not panic")
}

func TestBootstrapTracerRestore(t *testing.T) {
	before := Tracing()
	tracer := &NoopTracer{}
	restore := Bootstrap(Dependencies{Tracer: tracer})
	require.Same(t, tracer, Tracing())
	restore()
	require.Equal(t, before, Tracing())
}

type recordedMetric struct {
	kind, name string
}

type recordingMetrics struct{ got []recordedMetric }

func (r *recordingMetrics) Count(name string, _ int64, _ []string, _ float64) error {
	r.got = append(r.got, recordedMetric{"count", name})
	return nil
}

func (r *recordingMetrics) Incr(name string, _ []string, _ float64) error {
	r.got = append(r.got, recordedMetric{"incr", name})
	return nil
}

func (r *recordingMetrics) Gauge(name string, _ float64, _ []string, _ float64) error {
	r.got = append(r.got, recordedMetric{"gauge", name})
	return nil
}

func (r *recordingMetrics) Distribution(name string, _ float64, _ []string, _ float64) error {
	r.got = append(r.got, recordedMetric{"distribution", name})
	return nil
}

func (r *recordingMetrics) Timing(name string, _ time.Duration, _ []string, _ float64) error {
	r.got = append(r.got, recordedMetric{"timing", name})
	return nil
}

func emitAll(t *testing.T) {
	t.Helper()
	m := Metrics()
	require.NoError(t, m.Count("pg_proxy.a", 1, nil, 1))
	require.NoError(t, m.Incr("pg_proxy.b", nil, 1))
	require.NoError(t, m.Gauge("pg_proxy.c", 1, nil, 1))
	require.NoError(t, m.Distribution("pg_proxy.d", 1, nil, 1))
	require.NoError(t, m.Timing("pg_proxy.e", time.Millisecond, nil, 1))
}

func TestMetricNamespace(t *testing.T) {
	for _, tc := range []struct {
		name, namespace, want string
	}{
		{name: "default", namespace: "", want: "datadog."},
		{name: "custom", namespace: "dd.orgstore.", want: "dd.orgstore."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sink := &recordingMetrics{}
			restore := Bootstrap(Dependencies{Metrics: sink, MetricNamespace: tc.namespace})
			t.Cleanup(restore)

			emitAll(t)
			assert.Equal(t, []recordedMetric{
				{"count", tc.want + "pg_proxy.a"},
				{"incr", tc.want + "pg_proxy.b"},
				{"gauge", tc.want + "pg_proxy.c"},
				{"distribution", tc.want + "pg_proxy.d"},
				{"timing", tc.want + "pg_proxy.e"},
			}, sink.got)
			assert.Equal(t, tc.want, MetricNamespace())
			assert.Equal(t, tc.want+"pg_proxy.x", MetricName("pg_proxy.x"))
		})
	}
}

// A sink that forwards an already-namespaced name back through Metrics() must
// not see the namespace twice.
func TestMetricNamespaceIsIdempotent(t *testing.T) {
	sink := &recordingMetrics{}
	restore := Bootstrap(Dependencies{Metrics: sink})
	t.Cleanup(restore)

	require.NoError(t, Metrics().Count(MetricName("pg_proxy.a"), 1, nil, 1))
	assert.Equal(t, []recordedMetric{{"count", "datadog.pg_proxy.a"}}, sink.got)
}
