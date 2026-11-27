// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package pgproxy

import (
	"fmt"
	"regexp"
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
	MetricResetLatency         = "pg_proxy.sessions.connections.reset.latency_us"
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

// metricNamespacePattern accepts a letter followed by letters, digits,
// underscores, and dots, ending in a dot.
var metricNamespacePattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_.]*\.$`)

// NormalizeMetricNamespace returns the namespace Bootstrap applies for ns.
// Empty selects DefaultMetricNamespace. A missing trailing dot is added, so
// "acme" becomes "acme.". It returns an error unless ns starts with a letter
// and contains only letters, digits, underscores, and dots.
func NormalizeMetricNamespace(ns string) (string, error) {
	if ns == "" {
		return DefaultMetricNamespace, nil
	}
	if !strings.HasSuffix(ns, ".") {
		ns += "."
	}
	if !metricNamespacePattern.MatchString(ns) {
		return "", fmt.Errorf("pgproxy: invalid MetricNamespace %q: must start with a letter and contain only letters, digits, '_' and '.'", strings.TrimSuffix(ns, "."))
	}
	return ns, nil
}

// MetricName returns name with the configured metric namespace prepended.
// Library code emits bare names (pg_proxy.…), and the client installed by
// Bootstrap adds the namespace exactly once. Use MetricName only when sending
// a library metric to a client other than Metrics(), for example from a
// custom session.Metrics sink with its own StatsD client.
func MetricName(name string) string {
	return current.Load().namespace + name
}

// MetricNamespace returns the namespace prepended to emitted metric names.
func MetricNamespace() string { return current.Load().namespace }

// namespacedMetrics prepends the configured namespace before forwarding.
type namespacedMetrics struct {
	namespace string
	inner     MetricsClient
}

func (m namespacedMetrics) Count(name string, value int64, tags []string, rate float64) error {
	return m.inner.Count(m.namespace+name, value, tags, rate)
}

func (m namespacedMetrics) Incr(name string, tags []string, rate float64) error {
	return m.inner.Incr(m.namespace+name, tags, rate)
}

func (m namespacedMetrics) Gauge(name string, value float64, tags []string, rate float64) error {
	return m.inner.Gauge(m.namespace+name, value, tags, rate)
}

func (m namespacedMetrics) Distribution(name string, value float64, tags []string, rate float64) error {
	return m.inner.Distribution(m.namespace+name, value, tags, rate)
}

func (m namespacedMetrics) Timing(name string, value time.Duration, tags []string, rate float64) error {
	return m.inner.Timing(m.namespace+name, value, tags, rate)
}
