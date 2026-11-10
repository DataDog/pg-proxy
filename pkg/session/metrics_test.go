// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package session

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pgproxy "github.com/DataDog/pg-proxy"
	"github.com/DataDog/pg-proxy/pkg/actions"
	"github.com/DataDog/pg-proxy/pkg/classifier"
)

type metricCall struct {
	name  string
	value float64
	tags  []string
}

type recordingMetrics struct {
	pgproxy.NoopMetrics
	mu            sync.Mutex
	counts        []metricCall
	distributions []metricCall
}

func (m *recordingMetrics) Count(name string, value int64, tags []string, _ float64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counts = append(m.counts, metricCall{name: name, value: float64(value), tags: append([]string(nil), tags...)})
	return nil
}

func (m *recordingMetrics) Distribution(name string, value float64, tags []string, _ float64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.distributions = append(m.distributions, metricCall{name: name, value: value, tags: append([]string(nil), tags...)})
	return nil
}

type recordingSessionMetrics struct {
	counts        []metricCall
	distributions []metricCall
}

func (m *recordingSessionMetrics) Count(name string, value int64, tags MetricTags) {
	m.counts = append(m.counts, metricCall{name: name, value: float64(value), tags: append([]string(nil), tags.Tags()...)})
}

func (m *recordingSessionMetrics) Distribution(name string, value float64, tags MetricTags) {
	m.distributions = append(m.distributions, metricCall{name: name, value: value, tags: append([]string(nil), tags.Tags()...)})
}

func newMetricTestSession(tags []string) *Session {
	return New(Config{
		Context:    context.Background(),
		User:       "user",
		Database:   "database",
		Service:    "service",
		Client:     &testClient{},
		MetricTags: tags,
	})
}

func TestSessionTelemetryUsesBootstrappedMetrics(t *testing.T) {
	metrics := &recordingMetrics{}
	restore := pgproxy.Bootstrap(pgproxy.Dependencies{Metrics: metrics})
	t.Cleanup(restore)

	baseTags := []string{"user:foo", "database:bar"}
	s := newMetricTestSession(baseTags)
	s.recordMessageStarted("query")
	s.recordMessageFinished("query", 150*time.Microsecond)
	s.recordQueryObserved()
	s.recordQueryClassified(
		"SELECT 1; INSERT INTO dogs VALUES (1)",
		actions.SimpleQueryProtocol,
		classifier.Classification{StatementCount: 2},
	)
	s.recordQueryParseFinished(75 * time.Microsecond)
	s.recordFlushPostgresFinished(25 * time.Microsecond)
	s.recordBufferFlushFinished(3)
	s.recordLoadSheddingRejected()
	s.recordDrained()
	s.recordBackendDestroyed("force_close")
	s.recordCloseError(assert.AnError)

	require.Equal(t, []metricCall{
		{name: "datadog.pg_proxy.messages", value: 1, tags: []string{"user:foo", "database:bar", "message:query"}},
		{name: "datadog.pg_proxy.messages.queries", value: 1, tags: baseTags},
		{name: "datadog.pg_proxy.messages.queries.multiple_statements", value: 1, tags: baseTags},
		{name: "datadog.pg_proxy.messages.load_shedding_rejected", value: 1, tags: baseTags},
		{name: "datadog.pg_proxy.sessions.drained_connections", value: 1, tags: baseTags},
		{name: "datadog.pg_proxy.sessions.connections.destroyed", value: 1, tags: []string{"user:foo", "database:bar", "reason:client_disconnect", "path:force_close"}},
		{name: "datadog.pg_proxy.sessions.close_errors", value: 1, tags: baseTags},
	}, metrics.counts)
	require.Equal(t, []metricCall{
		{name: "datadog.pg_proxy.messages.latency_us", value: 150, tags: []string{"user:foo", "database:bar", "message:query"}},
		{name: "datadog.pg_proxy.messages.parse.latency_us", value: 75, tags: baseTags},
		{name: "datadog.pg_proxy.flush_postgres.latency_us", value: 25, tags: baseTags},
		{name: "datadog.pg_proxy.sessions.buffer_flush.count", value: 3, tags: baseTags},
	}, metrics.distributions)
	require.Equal(t, []string{"user:foo", "database:bar"}, baseTags)
}

func TestSessionTelemetryUsesConfiguredMetrics(t *testing.T) {
	metrics := &recordingSessionMetrics{}
	s := New(Config{
		Context:    context.Background(),
		Client:     &testClient{},
		MetricTags: []string{"user:foo"},
		Metrics:    metrics,
	})

	s.recordMessageStarted("query")
	s.recordMessageFinished("query", 150*time.Microsecond)

	require.Equal(t, []metricCall{
		{name: MetricMessages, value: 1, tags: []string{"user:foo", "message:query"}},
	}, metrics.counts)
	require.Equal(t, []metricCall{
		{name: MetricMessageLatency, value: 150, tags: []string{"user:foo", "message:query"}},
	}, metrics.distributions)
}

func TestSessionMetricTagCaches(t *testing.T) {
	baseTags := make([]string, 2, 8)
	copy(baseTags, []string{"user:foo", "database:bar"})
	s := newMetricTestSession(baseTags)

	messageTags := s.obsVals.messageMetricTags("query")
	require.Same(t, &messageTags.Tags()[0], &s.obsVals.messageMetricTags("query").Tags()[0])

	baseTags[0] = "mutated"
	require.Equal(t, []string{"user:foo", "database:bar"}, s.obsVals.baseMetricTags().Tags())
}
