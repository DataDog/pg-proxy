// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package pgproxy

import "time"

// Latency metric names are shared by emitters and telemetry adapters.
// Values are measured in microseconds.
const (
	MetricMessageLatency       = "dd.orgstore.pg_proxy.messages.latency_us"
	MetricQueryParseLatency    = "dd.orgstore.pg_proxy.messages.parse.latency_us"
	MetricFlushPostgresLatency = "dd.orgstore.pg_proxy.flush_postgres.latency_us"
	MetricConnectWait          = "dd.orgstore.pg_proxy.sessions.connect_wait_us"
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
