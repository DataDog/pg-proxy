// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package session

import (
	"fmt"
	"strings"
	"time"

	pgproxy "github.com/DataDog/pg-proxy"
	"github.com/DataDog/pg-proxy/pkg/actions"
	"github.com/DataDog/pg-proxy/pkg/classifier"
	"github.com/DataDog/pg-proxy/pkg/query"
)

const (
	pgSeverityFatal = "FATAL"

	MetricBufferFlushCount        = "pg_proxy.sessions.buffer_flush.count"
	MetricFlushPostgresLatency    = pgproxy.MetricFlushPostgresLatency
	MetricClientMessageBodyLength = "pg_proxy.client_messages.body_length_bytes"
	MetricMessages                = "pg_proxy.messages"
	MetricMessageLatency          = pgproxy.MetricMessageLatency
	MetricQueries                 = "pg_proxy.messages.queries"
	MetricQueryParseLatency       = pgproxy.MetricQueryParseLatency
	MetricLoadSheddingRejected    = "pg_proxy.messages.load_shedding_rejected"
	// MetricQuerySheddingSessions counts sessions by capability (capable:true|false).
	// Capable sessions are candidates, not proof that backend enforcement is on.
	MetricQuerySheddingSessions = "pg_proxy.sessions.query_shedding"
	// MetricQuerySheddingEnforcedSessions counts each session at its first rejection.
	MetricQuerySheddingEnforcedSessions = "pg_proxy.sessions.query_shedding_enforced"
)

// MetricTags carries both the tags sent to DogStatsD and a stable lookup key.
// The key lets embedders use buffered metric vectors without rebuilding and
// hashing a tag string on every frontend message.
type MetricTags struct {
	tags []string
	key  string
}

// Tags returns the DogStatsD tags.
func (t MetricTags) Tags() []string {
	return t.tags
}

// Key returns a stable key for this tag combination.
func (t MetricTags) Key() string {
	return t.key
}

// Metrics is the session metric sink. Embedders may provide a buffered
// implementation for high-throughput production traffic. Sinks receive bare
// names (pg_proxy.…). Forward them through pgproxy.Metrics(), which applies
// the namespace once, or wrap them with pgproxy.MetricName when sending to
// another client.
type Metrics interface {
	Count(name string, value int64, tags MetricTags)
	Distribution(name string, value float64, tags MetricTags)
}

type directMetrics struct{}

func (directMetrics) Count(name string, value int64, tags MetricTags) {
	_ = pgproxy.Metrics().Count(name, value, tags.Tags(), 1)
}

func (directMetrics) Distribution(name string, value float64, tags MetricTags) {
	_ = pgproxy.Metrics().Distribution(name, value, tags.Tags(), 1)
}

func resolveMetrics(metrics Metrics) Metrics {
	if metrics == nil {
		return directMetrics{}
	}
	return metrics
}

func newMetricTags(tags []string) MetricTags {
	return MetricTags{tags: tags, key: strings.Join(tags, ",")}
}

func (v *SessionObservabilityValues) setMetricTags(tags []string) {
	v.metricTags = append([]string(nil), tags...)
	v.baseMetricTagsCache = newMetricTags(v.metricTags)
	v.messageMetricTagCache = nil
}

func (v *SessionObservabilityValues) baseMetricTags() MetricTags {
	return v.baseMetricTagsCache
}

func (v *SessionObservabilityValues) messageMetricTags(message string) MetricTags {
	if v.messageMetricTagCache == nil {
		v.messageMetricTagCache = make(map[string]MetricTags, 8)
	}
	if tags, ok := v.messageMetricTagCache[message]; ok {
		return tags
	}
	tags := newMetricTags(appendTags(v.metricTags, "message:"+message))
	v.messageMetricTagCache[message] = tags
	return tags
}

func appendTags(base []string, extra ...string) []string {
	tags := make([]string, 0, len(base)+len(extra))
	tags = append(tags, base...)
	return append(tags, extra...)
}

func (s *Session) recordMessageStarted(message string) {
	s.metrics.Count(
		MetricMessages,
		1,
		s.obsVals.messageMetricTags(message),
	)
}

func (s *Session) recordMessageFinished(message string, elapsed time.Duration) {
	s.metrics.Distribution(
		MetricMessageLatency,
		float64(elapsed.Microseconds()),
		s.obsVals.messageMetricTags(message),
	)
}

func (s *Session) recordQueryObserved() {
	s.metrics.Count(
		MetricQueries,
		1,
		s.obsVals.baseMetricTags(),
	)
}

// pgx doesn't support multiple statements in a single query
// if the extended query protocol is being used
// see: https://github.com/jackc/pgx/issues/1090#issuecomment-932747206
func (s *Session) recordQueryClassified(sql string, protocol actions.PGQueryProtocolType, classification classifier.Classification) {
	if classification.StatementCount <= 1 {
		return
	}
	s.metrics.Count(
		"pg_proxy.messages.queries.multiple_statements",
		1,
		s.obsVals.baseMetricTags(),
	)
	if protocol == actions.ExtendedQueryProtocol {
		pgproxy.Logger().Warn(fmt.Sprintf("pgx does not support multiple statements in extended protocol: '%s'", sql))
	}
}

func recordQueryClassificationFailed(sql string, err error) {
	pgproxy.Logger().Warn(fmt.Sprintf("error parsing query: %s\nactual parsed query: %q", err, sql))
}

func (s *Session) recordQueryParseFinished(elapsed time.Duration) {
	s.metrics.Distribution(
		MetricQueryParseLatency,
		float64(elapsed.Microseconds()),
		s.obsVals.baseMetricTags(),
	)
}

// Warn the user that they are using unique queries as a result of injecting span ids into their queries.
// If this is the same query as the root trace, we know we already checked it for span information.
// However, if it is a subsequent message, we must check for span ids here.
// This assumes that the same query would not be used in both a named and unnamed prepared statement.
func recordNamedPreparedStatement(name, sql string, traceState *query.TraceState, service string) {
	usingUniqueTraceData := traceState.UsingFullPropagation()
	if !usingUniqueTraceData && traceState.RootQuery() != sql {
		if pgproxy.Tracing().HasQueryContext(sql) {
			usingUniqueTraceData = true
		}
	}
	if usingUniqueTraceData {
		pgproxy.Tracing().WarnNamedPreparedStatement(name, sql, service)
	}
}

func (s *Session) recordFlushPostgresFinished(elapsed time.Duration) {
	s.metrics.Distribution(
		MetricFlushPostgresLatency,
		float64(elapsed.Microseconds()),
		s.obsVals.baseMetricTags(),
	)
}

func (s *Session) recordBufferFlushFinished(count int) {
	s.metrics.Distribution(
		MetricBufferFlushCount,
		float64(count),
		s.obsVals.baseMetricTags(),
	)
}

func (s *Session) recordLoadSheddingRejected() {
	s.metrics.Count(
		MetricLoadSheddingRejected,
		1,
		s.obsVals.baseMetricTags(),
	)
}

func (s *Session) recordDrained() {
	s.metrics.Count(
		"pg_proxy.sessions.drained_connections",
		1,
		s.obsVals.baseMetricTags(),
	)
}

func (s *Session) recordCloseError(err error) {
	message := fmt.Sprintf("Closing session with err: %v. user: %s, database: %s, service_account: %s", err, s.user, s.database, s.service)
	tags := s.obsVals.metricTags
	if isFatalPostgresError(err) {
		pgproxy.Logger().Error(message)
		tags = appendTags(tags, "pg_severity:fatal")
	} else {
		pgproxy.Logger().Warn(message)
	}
	s.metrics.Count("pg_proxy.sessions.close_errors", 1, newMetricTags(tags))
}

func (s *Session) recordBackendDestroyed(path string) {
	s.metrics.Count(
		"pg_proxy.sessions.connections.destroyed",
		1,
		newMetricTags(appendTags(s.obsVals.metricTags, "reason:client_disconnect", "path:"+path)),
	)
}

func isFatalPostgresError(err error) bool {
	return err != nil && strings.Contains(err.Error(), pgSeverityFatal)
}
