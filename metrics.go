// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package pgproxy

import (
	"strings"
	"time"
)

// DefaultMetricNamespace prefixes every metric name when
// Dependencies.MetricNamespace is empty.
const DefaultMetricNamespace = "datadog."

// Latency metric names are shared by emitters and telemetry adapters.
// Values are measured in microseconds. Like every name the library emits, they
// exclude the namespace, which is added when the metric reaches the client.
const (
	MetricMessageLatency       = "pg_proxy.messages.latency_us"
	MetricQueryParseLatency    = "pg_proxy.messages.parse.latency_us"
	MetricFlushPostgresLatency = "pg_proxy.flush_postgres.latency_us"
	MetricConnectWait          = "pg_proxy.sessions.connect_wait_us"
)

// MetricsClient is the metric subset used by the proxy. Existing StatsD clients
// implement it directly. Names, tags and sampling hints retain their meaning.
type MetricsClient interface {
	Count(name string, value int64, tags []string, rate float64) error
	Incr(name string, tags []string, rate float64) error
	Gauge(name string, value float64, tags []string, rate float64) error
	Distribution(name string, value float64, tags []string, rate float64) error
	Timing(name string, value time.Duration, tags []string, rate float64) error
}

// NoopMetrics discards all measurements. It can also be embedded in test sinks.
type NoopMetrics struct{}

func (NoopMetrics) Count(string, int64, []string, float64) error          { return nil }
func (NoopMetrics) Incr(string, []string, float64) error                  { return nil }
func (NoopMetrics) Gauge(string, float64, []string, float64) error        { return nil }
func (NoopMetrics) Distribution(string, float64, []string, float64) error { return nil }
func (NoopMetrics) Timing(string, time.Duration, []string, float64) error { return nil }

// MetricName returns name with the configured metric namespace prepended. A
// name that already carries the namespace is returned unchanged, so a sink that
// forwards to Metrics() does not prefix twice.
func MetricName(name string) string {
	return withNamespace(current.Load().namespace, name)
}

// MetricNamespace returns the namespace prepended to emitted metric names.
func MetricNamespace() string { return current.Load().namespace }

func withNamespace(namespace, name string) string {
	if strings.HasPrefix(name, namespace) {
		return name
	}
	return namespace + name
}

// namespacedMetrics prepends the configured namespace before forwarding.
type namespacedMetrics struct {
	namespace string
	inner     MetricsClient
}

func (m namespacedMetrics) Count(name string, value int64, tags []string, rate float64) error {
	return m.inner.Count(withNamespace(m.namespace, name), value, tags, rate)
}

func (m namespacedMetrics) Incr(name string, tags []string, rate float64) error {
	return m.inner.Incr(withNamespace(m.namespace, name), tags, rate)
}

func (m namespacedMetrics) Gauge(name string, value float64, tags []string, rate float64) error {
	return m.inner.Gauge(withNamespace(m.namespace, name), value, tags, rate)
}

func (m namespacedMetrics) Distribution(name string, value float64, tags []string, rate float64) error {
	return m.inner.Distribution(withNamespace(m.namespace, name), value, tags, rate)
}

func (m namespacedMetrics) Timing(name string, value time.Duration, tags []string, rate float64) error {
	return m.inner.Timing(withNamespace(m.namespace, name), value, tags, rate)
}
