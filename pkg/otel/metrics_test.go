// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package otel_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	pgproxy "github.com/DataDog/pg-proxy"
	proxyotel "github.com/DataDog/pg-proxy/pkg/otel"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestMetricSDKCollection(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
	t.Cleanup(pgproxy.Bootstrap(pgproxy.Dependencies{Metrics: proxyotel.NewMetrics(provider.Meter("pg-proxy"))}))
	tags := []string{"database:orders", "endpoint:localhost:5432"}
	require.NoError(t, pgproxy.Metrics().Count("queries", 2, tags, 0.1))
	require.NoError(t, pgproxy.Metrics().Incr("queries", tags, 1))
	require.NoError(t, pgproxy.Metrics().Gauge("connections", 7, tags, 1))
	require.NoError(t, pgproxy.Metrics().Distribution("rows", 10, tags, 1))
	// Similar names must not silently acquire latency units or buckets.
	const customMetric = "pg_proxy.custom_us"
	require.NoError(t, pgproxy.Metrics().Distribution(customMetric, 20_000, tags, 1))
	require.NoError(t, pgproxy.Metrics().Timing("latency", 1500*time.Microsecond, tags, 1))
	var result metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &result))
	require.Len(t, result.ScopeMetrics, 1)
	metrics := map[string]metricdata.Metrics{}
	for _, m := range result.ScopeMetrics[0].Metrics {
		metrics[strings.TrimPrefix(m.Name, pgproxy.DefaultMetricNamespace)] = m
	}
	count := metrics["queries"].Data.(metricdata.Sum[int64]).DataPoints[0]
	require.Equal(t, int64(3), count.Value)
	value, ok := count.Attributes.Value("endpoint")
	require.True(t, ok)
	require.Equal(t, "localhost:5432", value.AsString())
	require.Equal(t, float64(7), metrics["connections"].Data.(metricdata.Gauge[float64]).DataPoints[0].Value)
	require.Equal(t, float64(10), metrics["rows"].Data.(metricdata.Histogram[float64]).DataPoints[0].Sum)
	require.Empty(t, metrics["rows"].Unit)
	require.Empty(t, metrics[customMetric].Unit)
	for _, name := range []string{"rows", "latency", customMetric} {
		bounds := metrics[name].Data.(metricdata.Histogram[float64]).DataPoints[0].Bounds
		require.Equal(t, float64(10_000), bounds[len(bounds)-1], "other histograms retain the SDK defaults")
	}
	require.Equal(t, "ms", metrics["latency"].Unit)
	require.Equal(t, 1.5, metrics["latency"].Data.(metricdata.Histogram[float64]).DataPoints[0].Sum)
}

func TestConcurrentMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
	metrics := proxyotel.NewMetrics(provider.Meter("pg-proxy"))
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			for range 100 {
				_ = metrics.Count("queries", 1, nil, 1)
				_ = metrics.Gauge("connections", 1, nil, 1)
				_ = metrics.Distribution("rows", 1, nil, 1)
			}
		})
	}
	wg.Wait()
	var result metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &result))
	for _, m := range result.ScopeMetrics[0].Metrics {
		if m.Name == "queries" {
			require.Equal(t, int64(2000), m.Data.(metricdata.Sum[int64]).DataPoints[0].Value)
		}
	}
}

func TestMicrosecondLatencyHistograms(t *testing.T) {
	for _, name := range []string{
		pgproxy.MetricMessageLatency,
		pgproxy.MetricQueryParseLatency,
		pgproxy.MetricFlushPostgresLatency,
		pgproxy.MetricConnectWait,
	} {
		t.Run(name, func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
			metrics := proxyotel.NewMetrics(provider.Meter("pg-proxy"))
			// Preserve detail for fast operations and distinguish slow queries
			// that previously all landed in the >10 ms overflow bucket.
			values := []float64{1, 50, 20_000, 100_000, 1_000_000, 30_000_000, 60_000_000}
			var sum float64
			for _, value := range values {
				require.NoError(t, metrics.Distribution(name, value, nil, 1))
				sum += value
			}
			var result metricdata.ResourceMetrics
			require.NoError(t, reader.Collect(context.Background(), &result))
			require.Len(t, result.ScopeMetrics, 1)
			require.Len(t, result.ScopeMetrics[0].Metrics, 1)
			collected := result.ScopeMetrics[0].Metrics[0]
			histogram := collected.Data.(metricdata.Histogram[float64]).DataPoints[0]
			require.Zero(t, histogram.BucketCounts[len(histogram.BucketCounts)-1], "latencies through 60 seconds must not overflow")
			var occupied int
			for _, count := range histogram.BucketCounts {
				if count != 0 {
					require.Equal(t, uint64(1), count, "each latency must have a distinct bucket")
					occupied++
				}
			}
			require.Equal(t, len(values), occupied)
			require.Equal(t, uint64(len(values)), histogram.Count)
			require.Equal(t, sum, histogram.Sum, "measurements must stay in microseconds")
			require.Equal(t, name, collected.Name)
			require.Equal(t, "us", collected.Unit)
		})
	}
}

func TestLatencyHistogramViewOverride(t *testing.T) {
	const name = pgproxy.MetricMessageLatency
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
		sdkmetric.WithView(sdkmetric.NewView(
			sdkmetric.Instrument{Name: name},
			sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{Boundaries: []float64{100_000}}},
		)),
	)
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
	metrics := proxyotel.NewMetrics(provider.Meter("pg-proxy"))
	require.NoError(t, metrics.Distribution(name, 20_000, nil, 1))
	require.NoError(t, metrics.Distribution(name, 200_000, nil, 1))
	var result metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &result))
	require.Len(t, result.ScopeMetrics, 1)
	require.Len(t, result.ScopeMetrics[0].Metrics, 1)
	histogram := result.ScopeMetrics[0].Metrics[0].Data.(metricdata.Histogram[float64]).DataPoints[0]
	require.Equal(t, []float64{100_000}, histogram.Bounds)
	require.Equal(t, []uint64{1, 1}, histogram.BucketCounts)
}
