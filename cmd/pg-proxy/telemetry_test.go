// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
	collectorlog "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectormetric "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logpb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricpb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	pgproxy "github.com/DataDog/pg-proxy"
	"github.com/DataDog/pg-proxy/pkg/bindparams"
)

// Decode real OTLP requests so these tests cover the exporters, not just adapters.
type otlpReceiver struct {
	mu      sync.Mutex
	traces  []*tracepb.ResourceSpans
	metrics []*metricpb.ResourceMetrics
	logs    []*logpb.ResourceLogs
}

func newOTLPReceiver(t *testing.T) *otlpReceiver {
	t.Helper()
	receiver := &otlpReceiver{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		receiver.mu.Lock()
		defer receiver.mu.Unlock()
		switch r.URL.Path {
		case "/v1/traces":
			var request collectortrace.ExportTraceServiceRequest
			err = proto.Unmarshal(body, &request)
			receiver.traces = append(receiver.traces, request.ResourceSpans...)
		case "/v1/metrics":
			var request collectormetric.ExportMetricsServiceRequest
			err = proto.Unmarshal(body, &request)
			receiver.metrics = append(receiver.metrics, request.ResourceMetrics...)
		case "/v1/logs":
			var request collectorlog.ExportLogsServiceRequest
			err = proto.Unmarshal(body, &request)
			receiver.logs = append(receiver.logs, request.ResourceLogs...)
		default:
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
	}))
	t.Cleanup(server.Close)
	for _, signal := range []string{"", "TRACES_", "METRICS_", "LOGS_"} {
		for _, setting := range []string{"ENDPOINT", "HEADERS", "COMPRESSION", "PROTOCOL", "CERTIFICATE", "CLIENT_CERTIFICATE", "CLIENT_KEY"} {
			t.Setenv("OTEL_EXPORTER_OTLP_"+signal+setting, "")
		}
	}
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", server.URL)
	t.Setenv("OTEL_SERVICE_NAME", "proxy-test")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "deployment.environment.name=test")
	t.Setenv("OTEL_TRACES_SAMPLER", "always_on")
	// Force these tests to exercise flushing at shutdown, without timer races.
	t.Setenv("OTEL_BSP_SCHEDULE_DELAY", "600000")
	t.Setenv("OTEL_BLRP_SCHEDULE_DELAY", "600000")
	t.Setenv("OTEL_METRIC_EXPORT_INTERVAL", "600000")
	return receiver
}

// exportedSpans returns every span the receiver decoded, in export order.
func exportedSpans(receiver *otlpReceiver) []*tracepb.Span {
	receiver.mu.Lock()
	defer receiver.mu.Unlock()
	var spans []*tracepb.Span
	for _, resource := range receiver.traces {
		for _, scope := range resource.ScopeSpans {
			spans = append(spans, scope.Spans...)
		}
	}
	return spans
}

func stringAttribute(attrs []*commonpb.KeyValue, key string) string {
	for _, attr := range attrs {
		if attr.Key == key {
			return attr.Value.GetStringValue()
		}
	}
	return ""
}

func requireResource(t *testing.T, attrs []*commonpb.KeyValue) {
	t.Helper()
	require.Equal(t, "proxy-test", stringAttribute(attrs, "service.name"))
	require.Equal(t, "test", stringAttribute(attrs, "deployment.environment.name"))
}

func TestTelemetryOTLPExportAndShutdown(t *testing.T) {
	for _, signal := range []string{"none", "all", "traces", "metrics", "logs", "traces,metrics", "traces,logs", "metrics,logs"} {
		t.Run(signal, func(t *testing.T) {
			receiver := newOTLPReceiver(t)
			cfg := telemetryConfig{}
			traces, metrics, logs := signal == "all" || strings.Contains(signal, "traces"), signal == "all" || strings.Contains(signal, "metrics"), signal == "all" || strings.Contains(signal, "logs")
			if traces {
				cfg.Traces = traceOTel
			}
			if metrics {
				cfg.Metrics = metricOTel
			}
			if logs {
				cfg.Logs = logsOTel
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			deps, shutdown, err := setupTelemetry(ctx, cfg)
			require.NoError(t, err)
			t.Cleanup(func() { _ = shutdown() })
			root, propagated := deps.Tracer.StartRoot("query", "select 1")
			require.False(t, propagated)
			root.StartChild("execute").Finish(nil)
			root.Finish(nil)
			if metrics {
				require.NoError(t, deps.Metrics.Count("proxy.test.queries", 3, []string{"database:orders"}, 1))
			}
			slog.New(deps.LogHandler).WithGroup("proxy").With("database", "orders").InfoContext(ctx, "ready")
			// SIGTERM cancels the process context. All three providers must still flush.
			cancel()
			require.NoError(t, shutdown())
			receiver.mu.Lock()
			defer receiver.mu.Unlock()
			require.Equal(t, traces, len(receiver.traces) > 0)
			require.Equal(t, metrics, len(receiver.metrics) > 0)
			require.Equal(t, logs, len(receiver.logs) > 0)
			if traces {
				requireResource(t, receiver.traces[0].Resource.Attributes)
				require.Len(t, receiver.traces[0].ScopeSpans[0].Spans, 2)
			}
			if metrics {
				requireResource(t, receiver.metrics[0].Resource.Attributes)
				metric := receiver.metrics[0].ScopeMetrics[0].Metrics[0]
				require.Equal(t, "proxy.test.queries", metric.Name)
				require.Len(t, metric.GetSum().DataPoints, 1)
				point := metric.GetSum().DataPoints[0]
				require.Equal(t, int64(3), point.GetAsInt())
				require.Equal(t, "orders", stringAttribute(point.Attributes, "database"))
			}
			if logs {
				requireResource(t, receiver.logs[0].Resource.Attributes)
				require.Len(t, receiver.logs[0].ScopeLogs[0].LogRecords, 1)
				record := receiver.logs[0].ScopeLogs[0].LogRecords[0]
				require.Equal(t, "ready", record.Body.GetStringValue())
				require.Equal(t, logpb.SeverityNumber_SEVERITY_NUMBER_INFO, record.SeverityNumber)
				require.Equal(t, "orders", stringAttribute(record.Attributes, "proxy.database"))
			}
		})
	}
}

func TestTelemetryOTLPMetricTemporality(t *testing.T) {
	for _, tc := range []struct {
		preference string
		want       metricpb.AggregationTemporality
	}{
		{"", metricpb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE},
		{"delta", metricpb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA},
	} {
		t.Run(tc.preference, func(t *testing.T) {
			receiver := newOTLPReceiver(t)
			t.Setenv("OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE", tc.preference)
			deps, shutdown, err := setupTelemetry(t.Context(), telemetryConfig{Metrics: metricOTel})
			require.NoError(t, err)
			t.Cleanup(func() { _ = shutdown() })
			require.NoError(t, deps.Metrics.Count("pg_proxy.messages", 3, nil, 1))
			require.NoError(t, deps.Metrics.Incr("pg_proxy.messages", nil, 1))
			require.NoError(t, shutdown())
			receiver.mu.Lock()
			defer receiver.mu.Unlock()
			require.Len(t, receiver.metrics, 1)
			scope := receiver.metrics[0].ScopeMetrics[0]
			require.Equal(t, "pg-proxy", scope.Scope.Name)
			require.Len(t, scope.Metrics, 1)
			sum := scope.Metrics[0].GetSum()
			require.NotNil(t, sum)
			require.True(t, sum.IsMonotonic)
			require.Equal(t, tc.want, sum.AggregationTemporality)
			require.Len(t, sum.DataPoints, 1)
			require.Equal(t, int64(4), sum.DataPoints[0].GetAsInt())
			require.Positive(t, sum.DataPoints[0].StartTimeUnixNano)
			require.Greater(t, sum.DataPoints[0].TimeUnixNano, sum.DataPoints[0].StartTimeUnixNano)
			require.Empty(t, stringAttribute(sum.DataPoints[0].Attributes, "datadog.metric.as_type"))
		})
	}
}

func TestTelemetryOTLPBinaryParameters(t *testing.T) {
	for _, tc := range []struct {
		name, parameter, want string
	}{
		{"text", "café 世界 �", "café 世界 �"},
		{"binary int4", "\xff\xff\xff\xff", "\"\\xff\\xff\\xff\\xff\""},
		{"binary bytea", "\x00\xff\x80\x01", "\"\\x00\\xff\\x80\\x01\""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			receiver := newOTLPReceiver(t)
			deps, shutdown, err := setupTelemetry(t.Context(), telemetryConfig{Traces: traceOTel})
			require.NoError(t, err)
			t.Cleanup(func() { _ = shutdown() })
			unrelated, _ := deps.Tracer.StartRoot("unrelated query", "select 1")
			unrelated.Finish(nil)
			parameterized, _ := deps.Tracer.StartRoot("parameterized query", "select $1")
			parameterized.SetTag("db.query.parameter.0", tc.parameter)
			parameterized.Finish(nil)
			// Both spans share one export batch; invalid UTF-8 must not drop either.
			require.NoError(t, shutdown())
			spans := exportedSpans(receiver)
			require.Len(t, spans, 2)
			require.Equal(t, "unrelated query", spans[0].Name)
			require.Equal(t, "select 1", stringAttribute(spans[0].Attributes, "db.statement"))
			require.Equal(t, "parameterized query", spans[1].Name)
			require.Equal(t, tc.want, stringAttribute(spans[1].Attributes, "db.query.parameter.0"))
		})
	}
}

// The SDK keeps 128 attributes per span by default, fewer than a Bind with
// every position rendered needs.
func TestTelemetryOTLPKeepsEveryBindPosition(t *testing.T) {
	receiver := newOTLPReceiver(t)
	deps, shutdown, err := setupTelemetry(t.Context(), telemetryConfig{Traces: traceOTel})
	require.NoError(t, err)
	t.Cleanup(func() { _ = shutdown() })
	root, _ := deps.Tracer.StartRoot("parameterized query", "select $1")
	var capture bindparams.Capture
	capture.Record(&pgproto3.Bind{Parameters: make([][]byte, bindparams.MaxParams+10)}, nil, nil)
	capture.Render(root.SetTag)
	root.Finish(nil)
	require.NoError(t, shutdown())

	spans := exportedSpans(receiver)
	require.Len(t, spans, 1)
	keys := map[string]bool{}
	for _, attr := range spans[0].Attributes {
		keys[attr.Key] = true
	}
	last := strconv.Itoa(bindparams.MaxParams - 1)
	require.True(t, keys[bindparams.TruncatedTag])
	require.True(t, keys[bindparams.TagPrefix+last], "the last rendered position is kept")
	require.True(t, keys[bindparams.TypeTagPrefix+last])
	require.Zero(t, spans[0].DroppedAttributesCount)
}

func TestSpanLimitsHonorTheEnvironment(t *testing.T) {
	require.Equal(t, spanAttributeCountLimit, spanLimits().AttributeCountLimit)
	t.Run("generic limit", func(t *testing.T) {
		t.Setenv("OTEL_ATTRIBUTE_COUNT_LIMIT", "0")
		require.Equal(t, 0, spanLimits().AttributeCountLimit)
	})
	t.Run("span limit", func(t *testing.T) {
		t.Setenv("OTEL_ATTRIBUTE_COUNT_LIMIT", "0")
		t.Setenv("OTEL_SPAN_ATTRIBUTE_COUNT_LIMIT", "10")
		require.Equal(t, 10, spanLimits().AttributeCountLimit)
	})
}

func TestTelemetryOTLPSQLText(t *testing.T) {
	for _, tc := range []struct {
		name, sql, want string
	}{
		{"utf8", "SELECT 'café 世界 �'", "SELECT 'café 世界 �'"},
		{"latin1", "SELECT 'caf\xe9'", `"SELECT 'caf\xe9'"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			receiver := newOTLPReceiver(t)
			deps, shutdown, err := setupTelemetry(t.Context(), telemetryConfig{Traces: traceOTel})
			require.NoError(t, err)
			t.Cleanup(func() { _ = shutdown() })
			unrelated, _ := deps.Tracer.StartRoot("unrelated query", "SELECT 1")
			unrelated.Finish(nil)
			query, _ := deps.Tracer.StartRoot("query", tc.sql)
			query.Finish(nil)
			// Invalid SQL text must not prevent either span from being exported.
			require.NoError(t, shutdown())
			receiver.mu.Lock()
			defer receiver.mu.Unlock()
			require.Len(t, receiver.traces, 1)
			spans := receiver.traces[0].ScopeSpans[0].Spans
			require.Len(t, spans, 2)
			require.Equal(t, "SELECT 1", stringAttribute(spans[0].Attributes, "db.statement"))
			require.Equal(t, tc.want, stringAttribute(spans[1].Attributes, "db.statement"))
		})
	}
}

func TestTelemetryOTLPErrorText(t *testing.T) {
	for _, tc := range []struct{ name, message, want string }{
		{"utf8", "relation café 世界 does not exist", "relation café 世界 does not exist"},
		{"latin1", "relation caf\xe9 does not exist", `"relation caf\xe9 does not exist"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			receiver := newOTLPReceiver(t)
			deps, shutdown, err := setupTelemetry(t.Context(), telemetryConfig{Traces: traceOTel})
			require.NoError(t, err)
			t.Cleanup(func() { _ = shutdown() })
			healthy, _ := deps.Tracer.StartRoot("healthy query", "SELECT 1")
			healthy.Finish(nil)
			failed, _ := deps.Tracer.StartRoot("failed query", "SELECT 2")
			queryErr := errors.New(tc.message)
			failed.Finish(queryErr)
			require.Equal(t, tc.message, queryErr.Error(), "the original error must remain unchanged")
			require.NoError(t, shutdown())
			receiver.mu.Lock()
			defer receiver.mu.Unlock()
			// One bad error must not discard the unrelated span in the same batch.
			require.Len(t, receiver.traces, 1)
			spans := receiver.traces[0].ScopeSpans[0].Spans
			require.Len(t, spans, 2)
			require.Equal(t, "healthy query", spans[0].Name)
			require.Equal(t, "SELECT 1", stringAttribute(spans[0].Attributes, "db.statement"))
			require.Equal(t, tracepb.Status_STATUS_CODE_ERROR, spans[1].Status.Code)
			require.Equal(t, tc.want, spans[1].Status.Message)
			require.Len(t, spans[1].Events, 1)
			require.Equal(t, "exception", spans[1].Events[0].Name)
			require.Equal(t, tc.want, stringAttribute(spans[1].Events[0].Attributes, "exception.message"))
		})
	}
}

func TestTelemetryOTLPLogText(t *testing.T) {
	for _, tc := range []struct{ name, text, want string }{
		{"utf8", "SELECT 'café 世界'", "SELECT 'café 世界'"},
		{"latin1", "SELECT 'caf\xe9'", `"SELECT 'caf\xe9'"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			receiver := newOTLPReceiver(t)
			deps, shutdown, err := setupTelemetry(t.Context(), telemetryConfig{Traces: traceOTel, Logs: logsOTel})
			require.NoError(t, err)
			t.Cleanup(func() { _ = shutdown() })
			t.Cleanup(pgproxy.Bootstrap(deps))
			pgproxy.Logger().Info("healthy log")
			pgproxy.Logger().With("cached", tc.text).WithGroup("query").Warn(tc.text,
				"sql", tc.text, "error", errors.New(tc.text), slog.Group("statement", "name", tc.text))
			deps.Tracer.WarnNamedPreparedStatement("stmt", tc.text, "test")
			require.NoError(t, shutdown())
			receiver.mu.Lock()
			defer receiver.mu.Unlock()
			// Body, cached/grouped attributes, and formatted errors all share a batch
			// with the healthy log and the real named-statement warning.
			require.Len(t, receiver.logs, 1)
			records := receiver.logs[0].ScopeLogs[0].LogRecords
			require.Len(t, records, 3)
			require.Equal(t, "healthy log", records[0].Body.GetStringValue())
			require.Empty(t, records[0].Attributes)
			require.Equal(t, tc.want, records[1].Body.GetStringValue())
			require.Equal(t, logpb.SeverityNumber_SEVERITY_NUMBER_WARN, records[1].SeverityNumber)
			for _, key := range []string{"cached", "query.sql", "query.error", "query.statement.name"} {
				require.Equal(t, tc.want, stringAttribute(records[1].Attributes, key), key)
			}
			require.Equal(t, tc.want, stringAttribute(records[2].Attributes, "query"))
			require.Equal(t, "stmt", stringAttribute(records[2].Attributes, "name"))
			require.Equal(t, "test", stringAttribute(records[2].Attributes, "service"))
		})
	}
}

func TestTelemetrySelection(t *testing.T) {
	newOTLPReceiver(t)
	// Endpoint settings alone must not enable export, including invalid protocols.
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")
	deps, shutdown, err := setupTelemetry(t.Context(), telemetryConfig{})
	require.NoError(t, err)
	require.IsType(t, pgproxy.NoopTracer{}, deps.Tracer)
	require.Nil(t, deps.Metrics)
	require.NotNil(t, deps.LogHandler)
	require.NoError(t, shutdown())
	for _, cfg := range []telemetryConfig{{Traces: traceBackend(255)}, {Metrics: metricBackend(255)}, {Logs: logOutput(255)}} {
		_, _, err := setupTelemetry(t.Context(), cfg)
		require.Error(t, err)
	}
	for signal, cfg := range map[string]telemetryConfig{
		"TRACES": {Traces: traceOTel}, "METRICS": {Metrics: metricOTel}, "LOGS": {Logs: logsOTel},
	} {
		t.Run(signal, func(t *testing.T) {
			t.Setenv("OTEL_EXPORTER_OTLP_"+signal+"_PROTOCOL", "")
			_, _, err := setupTelemetry(t.Context(), cfg)
			require.ErrorContains(t, err, "http/protobuf")
			t.Setenv("OTEL_EXPORTER_OTLP_"+signal+"_PROTOCOL", "http/protobuf")
			_, shutdown, err := setupTelemetry(t.Context(), cfg)
			require.NoError(t, err)
			require.NoError(t, shutdown())
		})
	}
}

func TestTelemetryFailedStartup(t *testing.T) {
	newOTLPReceiver(t)
	for _, cfg := range []config{{}, {telemetry: telemetryConfig{Traces: traceOTel, Metrics: metricOTel, Logs: logsOTel}}} {
		require.Error(t, runWithTelemetry(t.Context(), cfg), "invalid proxy settings must still shut down the providers")
	}
}

func TestStartupFailureExportsErrorLog(t *testing.T) {
	receiver := newOTLPReceiver(t)
	cfg := backendConfig(t, "postgres://postgres:postgres@127.0.0.1:1/postgres?sslmode=disable&connect_timeout=1")
	cfg.telemetry.Logs = logsOTel
	err := runWithTelemetry(t.Context(), cfg)
	require.ErrorContains(t, err, "connect to backend")
	receiver.mu.Lock()
	defer receiver.mu.Unlock()
	require.Len(t, receiver.logs, 1, "startup failures must be logged before the exporter shuts down")
	requireResource(t, receiver.logs[0].Resource.Attributes)
	records := receiver.logs[0].ScopeLogs[0].LogRecords
	require.Len(t, records, 1)
	require.Equal(t, "pg-proxy stopped", records[0].Body.GetStringValue())
	require.Equal(t, logpb.SeverityNumber_SEVERITY_NUMBER_ERROR, records[0].SeverityNumber)
	require.Contains(t, stringAttribute(records[0].Attributes, "error"), "connect to backend")
	t.Logf("OTLP startup failure: service=%s severity=%s body=%q error includes connect to backend",
		stringAttribute(receiver.logs[0].Resource.Attributes, "service.name"), records[0].SeverityText, records[0].Body.GetStringValue())
}

func TestStartupReportsTelemetryOverride(t *testing.T) {
	for _, mode := range []string{"none", "otel"} {
		for _, level := range []string{"INFO", "ERROR"} {
			t.Run(mode+"/"+level, func(t *testing.T) {
				receiver := newOTLPReceiver(t)
				t.Setenv("OTEL_TRACES_EXPORTER", "")
				t.Setenv("OTEL_LOGS_EXPORTER", "")
				exporter, backend := "otlp", "disabled metrics"
				if mode == "otel" {
					exporter, backend = "none", "OTLP metrics"
				}
				t.Setenv("OTEL_METRICS_EXPORTER", exporter)
				data, err := json.Marshal(ossConfig())
				require.NoError(t, err)
				cfg, err := parseConfig([]string{"-config", writeConfig(t, "json", string(data)), "-telemetry", mode, "-log-level", level})
				require.NoError(t, err)
				// Warnings were resolved with the config, not from a later environment.
				t.Setenv("OTEL_METRICS_EXPORTER", "")
				// Stop before opening pools; startup diagnostics must still be flushed.
				cfg.databases = nil
				terminal, err := os.CreateTemp(t.TempDir(), "stderr")
				require.NoError(t, err)
				previousStderr := os.Stderr
				os.Stderr = terminal
				t.Cleanup(func() { os.Stderr = previousStderr; _ = terminal.Close() })
				require.ErrorContains(t, runWithTelemetry(t.Context(), cfg), "must not be empty")
				message := "ignoring OTEL_METRICS_EXPORTER: -telemetry " + mode + " selects " + backend
				wantWarnings := 0
				if level == "INFO" {
					wantWarnings = 1
				}
				contents, err := os.ReadFile(terminal.Name())
				require.NoError(t, err)
				require.Equal(t, wantWarnings, strings.Count(string(contents), message))
				receiver.mu.Lock()
				defer receiver.mu.Unlock()
				exportedWarnings := 0
				for _, resource := range receiver.logs {
					for _, scope := range resource.ScopeLogs {
						for _, record := range scope.LogRecords {
							if record.Body.GetStringValue() == message {
								exportedWarnings++
								require.Equal(t, logpb.SeverityNumber_SEVERITY_NUMBER_WARN, record.SeverityNumber)
							}
						}
					}
				}
				if mode == "none" {
					wantWarnings = 0
				}
				require.Equal(t, wantWarnings, exportedWarnings)
			})
		}
	}
}

func TestFanoutKeepsTerminalLogs(t *testing.T) {
	var terminal, exported bytes.Buffer
	logger := slog.New(fanoutLogHandler{
		level: slog.LevelDebug,
		handlers: []slog.Handler{
			slog.NewTextHandler(&terminal, &slog.HandlerOptions{Level: slog.LevelInfo}),
			slog.NewTextHandler(&exported, &slog.HandlerOptions{Level: slog.LevelDebug}),
		},
	})
	child := logger.With("service", "proxy").WithGroup("query")
	child.Debug("details", "id", 7)
	child.Info("ready", "id", 7)
	logger.Info("root")
	require.NotContains(t, terminal.String(), "details")
	require.Contains(t, exported.String(), "details")
	for _, output := range []string{terminal.String(), exported.String()} {
		require.Contains(t, output, "msg=ready service=proxy query.id=7")
		require.Contains(t, output, "msg=root\n")
	}
}

func TestTelemetryLogLevels(t *testing.T) {
	for name, output := range map[string]logOutput{"text": logsText, "json": logsJSON, "otlp": logsOTel} {
		for _, minimum := range []slog.Level{slog.LevelInfo, slog.LevelDebug, slog.LevelWarn, slog.LevelError} {
			t.Run(name+"/"+minimum.String(), func(t *testing.T) {
				var receiver *otlpReceiver
				if output == logsOTel {
					receiver = newOTLPReceiver(t)
				}
				terminal, err := os.CreateTemp(t.TempDir(), "stderr")
				require.NoError(t, err)
				previousStderr := os.Stderr
				os.Stderr = terminal
				t.Cleanup(func() { os.Stderr = previousStderr; _ = terminal.Close() })
				deps, shutdown, err := setupTelemetry(t.Context(), telemetryConfig{Logs: output, LogLevel: minimum})
				require.NoError(t, err)
				t.Cleanup(func() { _ = shutdown() })
				logger := slog.New(deps.LogHandler).With("component", "proxy").WithGroup("query")
				var want []string
				for _, level := range []slog.Level{slog.LevelDebug, slog.LevelInfo, slog.LevelWarn, slog.LevelError} {
					require.Equal(t, level >= minimum, logger.Enabled(t.Context(), level))
					message := "message-" + level.String()
					logger.Log(t.Context(), level, message, "id", 7)
					if level >= minimum {
						want = append(want, message)
					}
				}
				require.NoError(t, shutdown())
				contents, err := os.ReadFile(terminal.Name())
				require.NoError(t, err)
				require.Len(t, strings.Split(strings.TrimSpace(string(contents)), "\n"), len(want))
				for _, message := range want {
					require.Contains(t, string(contents), message)
				}
				if receiver != nil {
					receiver.mu.Lock()
					defer receiver.mu.Unlock()
					var got []string
					for _, resource := range receiver.logs {
						for _, scope := range resource.ScopeLogs {
							for _, record := range scope.LogRecords {
								got = append(got, record.Body.GetStringValue())
								require.Equal(t, "proxy", stringAttribute(record.Attributes, "component"))
							}
						}
					}
					require.Equal(t, want, got)
				}
			})
		}
	}
}

func TestShutdownOTelProvidersShareDeadlineAndCollectErrors(t *testing.T) {
	// Fake time makes the five-second bound independent of scheduler delays.
	synctest.Test(t, func(t *testing.T) {
		firstErr := errors.New("first exporter failed")
		lastErr := errors.New("last exporter failed")
		var contexts [3]context.Context
		start := time.Now()
		err := shutdownOTelProviders([]func(context.Context) error{
			func(ctx context.Context) error {
				contexts[0] = ctx
				return firstErr
			},
			func(ctx context.Context) error {
				contexts[1] = ctx
				<-ctx.Done()
				return ctx.Err()
			},
			func(ctx context.Context) error {
				contexts[2] = ctx
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(2 * time.Second):
					return lastErr
				}
			},
		})
		require.ErrorIs(t, err, firstErr)
		require.ErrorIs(t, err, lastErr)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Equal(t, 5*time.Second, time.Since(start))
		for _, ctx := range contexts {
			require.Same(t, contexts[0], ctx)
			deadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.Equal(t, start.Add(5*time.Second), deadline)
		}
	})
}

// Called by TestProxyAgainstPostgres to reuse its real PostgreSQL container.
func verifyProxyOTLP(t *testing.T, backendURL string) {
	for _, capture := range []bool{false, true} {
		t.Run(map[bool]string{false: "parameters disabled", true: "parameters enabled"}[capture], func(t *testing.T) {
			verifyProxyOTLPParameters(t, backendURL, capture)
		})
	}
}

func verifyProxyOTLPParameters(t *testing.T, backendURL string, capture bool) {
	receiver := newOTLPReceiver(t)
	ctx, stop := context.WithCancel(t.Context())
	defer stop()
	cfg := backendConfig(t, backendURL)
	cfg.telemetry = telemetryConfig{Traces: traceOTel, Metrics: metricOTel, Logs: logsOTel, CaptureBindParameters: capture}
	deps, shutdown, err := setupTelemetry(ctx, cfg.telemetry)
	require.NoError(t, err)
	t.Cleanup(func() { _ = shutdown() })
	restore := pgproxy.Bootstrap(deps)
	t.Cleanup(restore)
	proxy, err := newProxy(ctx, cfg)
	require.NoError(t, err)
	done := make(chan struct{})
	var serveErr error
	go func() {
		serveErr = serve(ctx, stop, proxy.server, proxy.sessions, proxy.inventory.Close, 5*time.Second)
		close(done)
	}()
	defer func() {
		stop()
		select {
		case <-done:
			require.NoError(t, serveErr)
		case <-time.After(10 * time.Second):
			t.Error("proxy did not stop")
		}
	}()
	client, err := pgx.Connect(ctx, "postgres://postgres@"+proxy.server.Addr().String()+"/local_db?sslmode=disable")
	require.NoError(t, err)
	defer client.Close(context.Background())
	const traceID = "0123456789abcdef0123456789abcdef"
	const parentID = "0123456789abcdef"
	const query = "/* report query */ select $1::text /*traceparent='00-" + traceID + "-" + parentID + "-01'*/"
	var value string
	require.NoError(t, client.QueryRow(ctx, query, pgx.QueryExecModeExec, "otel actual parameter").Scan(&value))
	require.Equal(t, "otel actual parameter", value)
	// Send binary Bind parameters and request binary results so the PostgreSQL
	// round trip checks byte-for-byte preservation with capture enabled or disabled.
	for _, tc := range []struct {
		sql   string
		value []byte
	}{
		{"select $1::int4", []byte{0xff, 0xff, 0xff, 0xff}}, // -1
		{"select $1::bytea", []byte{0x00, 0xff, 0x80, 0x01}},
	} {
		result := client.PgConn().ExecParams(ctx, tc.sql, [][]byte{tc.value}, nil, []int16{1}, []int16{1}).Read()
		require.NoError(t, result.Err)
		require.Equal(t, [][][]byte{{tc.value}}, result.Rows)
	}
	require.NoError(t, client.Close(ctx))
	// Stop and drain sessions before flushing telemetry.
	stop()
	select {
	case <-done:
		require.NoError(t, serveErr)
	case <-time.After(10 * time.Second):
		t.Fatal("proxy did not stop")
	}
	require.NoError(t, shutdown())
	receiver.mu.Lock()
	defer receiver.mu.Unlock()
	var statement, parameters, binaryInt, binaryBytes, queryMetric, listening bool
	for _, resource := range receiver.traces {
		requireResource(t, resource.Resource.Attributes)
		for _, scope := range resource.ScopeSpans {
			for _, span := range scope.Spans {
				// ExecParams declares no types and describes only the portal, so
				// the values' types are unknown and they render as hex.
				binaryInt = binaryInt || stringAttribute(span.Attributes, "db.query.parameter.0") == `\xffffffff`
				binaryBytes = binaryBytes || stringAttribute(span.Attributes, "db.query.parameter.0") == `\x00ff8001`
				if !capture {
					for _, attr := range span.Attributes {
						require.False(t, strings.HasPrefix(attr.Key, "db.query.parameter"), attr.Key)
						require.False(t, strings.HasPrefix(attr.Key, "pgproxy.bind."), attr.Key)
						require.NotContains(t, attr.Value.GetStringValue(), "otel actual parameter")
					}
				}
				if hex.EncodeToString(span.TraceId) != traceID {
					continue
				}
				if stringAttribute(span.Attributes, "db.statement") == query {
					statement = true
					require.Equal(t, parentID, hex.EncodeToString(span.ParentSpanId))
				}
				// The root span, query.TransactionOpName (shadowed here by the SQL constant).
				if span.Name == "Proxy forwarding " && stringAttribute(span.Attributes, "db.query.parameter.0") == "otel actual parameter" {
					parameters = true
					require.NotEmpty(t, stringAttribute(span.Attributes, "pgproxy.bind.type.0"))
				}
			}
		}
	}
	for _, resource := range receiver.metrics {
		requireResource(t, resource.Resource.Attributes)
		for _, scope := range resource.ScopeMetrics {
			for _, metric := range scope.Metrics {
				if metric.Name == "datadog.pg_proxy.messages.queries" {
					for _, point := range metric.GetSum().GetDataPoints() {
						queryMetric = queryMetric || point.GetAsInt() > 0
					}
				}
			}
		}
	}
	for _, resource := range receiver.logs {
		requireResource(t, resource.Resource.Attributes)
		for _, scope := range resource.ScopeLogs {
			for _, record := range scope.LogRecords {
				listening = listening || record.Body.GetStringValue() == "Listening"
			}
		}
	}
	require.True(t, statement, "query SQL and W3C parent must reach the OTLP receiver")
	require.Equal(t, capture, parameters, "bind value export must follow the parameter-capture setting")
	require.Equal(t, capture, binaryInt, "binary int4 parameters must be safely exported only when enabled")
	require.Equal(t, capture, binaryBytes, "binary bytea parameters must be safely exported only when enabled")
	require.True(t, queryMetric, "query counts must reach the OTLP receiver")
	require.True(t, listening, "proxy startup logs must reach the OTLP receiver")
	t.Logf("OTLP query: trace_id=%s parent_id=%s db.statement=%q parameter_capture=%t", traceID, parentID, query, parameters)
	t.Logf("OTLP shutdown: query_count_received=%t startup_log_received=%t", queryMetric, listening)
}
