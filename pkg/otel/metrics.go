// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package otel

import (
	"context"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	pgproxy "github.com/DataDog/pg-proxy"
)

// Metrics retains the proxy's metric names and tags. Counts are counters,
// distributions are histograms, and timings are histograms in milliseconds.
// The proxy's known latency distributions use microsecond units and buckets
// through 60 seconds. Caller-owned SDK views can override the bucket boundaries.
// StatsD sampling hints are ignored: every OTel measurement is recorded.
// Instruments are cached by name and kind, not by tag values.
type Metrics struct {
	meter      metric.Meter
	mu         sync.Mutex
	counters   map[string]metric.Int64Counter
	gauges     map[string]metric.Float64Gauge
	histograms map[string]metric.Float64Histogram
	timings    map[string]metric.Float64Histogram
}

// NewMetrics wraps a caller-owned meter. A nil meter discards measurements.
func NewMetrics(meter metric.Meter) *Metrics {
	if meter == nil {
		meter = noop.NewMeterProvider().Meter("pg-proxy")
	}
	return &Metrics{meter: meter, counters: map[string]metric.Int64Counter{}, gauges: map[string]metric.Float64Gauge{}, histograms: map[string]metric.Float64Histogram{}, timings: map[string]metric.Float64Histogram{}}
}

func attributes(tags []string) attribute.Set {
	attrs := make([]attribute.KeyValue, 0, len(tags))
	for _, tag := range tags {
		key, value, _ := strings.Cut(tag, ":")
		attrs = append(attrs, attribute.String(key, value))
	}
	return attribute.NewSet(attrs...)
}
func (m *Metrics) Count(name string, value int64, tags []string, _ float64) error {
	// TODO: Validate Datadog dashboards and monitors with mixed
	// OTLP/DogStatsD submissions. docs/otel-collector-datadog.yaml maps our delta
	// counters to RATE; these generic counters keep their OTel semantics.
	m.mu.Lock()
	instrument, ok := m.counters[name]
	var err error
	if !ok {
		instrument, err = m.meter.Int64Counter(name)
		if err == nil {
			m.counters[name] = instrument
		}
	}
	m.mu.Unlock()
	if err == nil {
		instrument.Add(context.Background(), value, metric.WithAttributeSet(attributes(tags)))
	}
	return err
}
func (m *Metrics) Incr(name string, tags []string, rate float64) error {
	return m.Count(name, 1, tags, rate)
}
func (m *Metrics) Gauge(name string, value float64, tags []string, _ float64) error {
	m.mu.Lock()
	instrument, ok := m.gauges[name]
	var err error
	if !ok {
		instrument, err = m.meter.Float64Gauge(name)
		if err == nil {
			m.gauges[name] = instrument
		}
	}
	m.mu.Unlock()
	if err == nil {
		instrument.Record(context.Background(), value, metric.WithAttributeSet(attributes(tags)))
	}
	return err
}
func (m *Metrics) histogram(name string, value float64, tags []string, timing bool) error {
	m.mu.Lock()
	instruments := m.histograms
	if timing {
		instruments = m.timings
	}
	instrument, ok := instruments[name]
	var err error
	if !ok {
		var opts []metric.Float64HistogramOption
		if timing {
			opts = append(opts, metric.WithUnit("ms"))
		} else {
			switch name {
			case pgproxy.MetricMessageLatency, pgproxy.MetricQueryParseLatency,
				pgproxy.MetricFlushPostgresLatency, pgproxy.MetricConnectWait:
				// Keep the SDK's fine buckets for fast operations, then extend them
				// beyond 10,000 us so slow queries do not all share the overflow bucket.
				opts = append(opts, metric.WithUnit("us"), metric.WithExplicitBucketBoundaries(
					0, 5, 10, 25, 50, 75, 100, 250, 500, 750,
					1_000, 2_500, 5_000, 7_500, 10_000,
					25_000, 50_000, 75_000, 100_000, 250_000, 500_000, 750_000,
					1_000_000, 2_500_000, 5_000_000, 7_500_000, 10_000_000,
					30_000_000, 60_000_000,
				))
			}
		}
		instrument, err = m.meter.Float64Histogram(name, opts...)
		if err == nil {
			instruments[name] = instrument
		}
	}
	m.mu.Unlock()
	if err == nil {
		instrument.Record(context.Background(), value, metric.WithAttributeSet(attributes(tags)))
	}
	return err
}
func (m *Metrics) Distribution(name string, value float64, tags []string, _ float64) error {
	return m.histogram(name, value, tags, false)
}
func (m *Metrics) Timing(name string, value time.Duration, tags []string, _ float64) error {
	return m.histogram(name, float64(value)/float64(time.Millisecond), tags, true)
}
