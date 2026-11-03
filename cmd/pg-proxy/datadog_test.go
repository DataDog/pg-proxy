package main

import (
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"

	pgproxy "github.com/DataDog/pg-proxy"
)

type exportedSpan struct {
	Name     string            `msgpack:"name"`
	Service  string            `msgpack:"service"`
	TraceID  uint64            `msgpack:"trace_id"`
	SpanID   uint64            `msgpack:"span_id"`
	ParentID uint64            `msgpack:"parent_id"`
	Meta     map[string]string `msgpack:"meta"`
}

func TestDatadogPresetExportAndShutdown(t *testing.T) {
	var mu sync.Mutex
	var spans []exportedSpan
	agent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/v0.4/traces" {
			var traces [][]exportedSpan
			if err := msgpack.NewDecoder(r.Body).Decode(&traces); err != nil {
				t.Error(err)
				w.WriteHeader(400)
				return
			}
			mu.Lock()
			for _, trace := range traces {
				spans = append(spans, trace...)
			}
			mu.Unlock()
		}
		_, _ = io.WriteString(w, `{}`)
	}))
	defer agent.Close()
	udp, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	defer udp.Close()
	for key, value := range map[string]string{
		"DD_TRACE_AGENT_URL": agent.URL, "DD_TRACE_AGENT_PROTOCOL_VERSION": "0.4",
		"DD_DOGSTATSD_URL": "udp://" + udp.LocalAddr().String(),
		"DD_SERVICE":       "proxy-test", "DD_ENV": "test", "DD_VERSION": "1.2.3",
		"DD_TRACE_ENABLED": "true", "DD_TRACE_SAMPLE_RATE": "1",
		"DD_TRACE_STATS_COMPUTATION_ENABLED": "false",
		"DD_TRACE_STARTUP_LOGS":              "false", "DD_TRACE_DEBUG": "false",
		"DD_REMOTE_CONFIGURATION_ENABLED": "false", "DD_INSTRUMENTATION_TELEMETRY_ENABLED": "false",
		"DD_APPSEC_ENABLED": "false", "DD_RUNTIME_METRICS_ENABLED": "false",
		// DD_TRACE_ENABLED explicitly overrides the SDK's normalized OTel setting.
		"OTEL_TRACES_EXPORTER": " NONE ", "DD_TAGS": "",
	} {
		t.Setenv(key, value)
	}
	logs, err := os.CreateTemp(t.TempDir(), "proxy-logs-*.json")
	require.NoError(t, err)
	previousStderr := os.Stderr
	os.Stderr = logs
	t.Cleanup(func() { os.Stderr = previousStderr; _ = logs.Close() })
	contents, err := json.Marshal(ossConfig())
	require.NoError(t, err)
	cfg, err := parseConfig([]string{"-config", writeConfig(t, "json", string(contents)), "-telemetry", "datadog"})
	require.NoError(t, err)
	deps, shutdown, err := setupTelemetry(t.Context(), cfg.telemetry)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, shutdown()) })
	restore := pgproxy.Bootstrap(deps)
	defer restore()
	sql := "SELECT $1 /*traceparent='00-0000000000000000000000000000002a-000000000000000b-01'*/"
	root, propagated := pgproxy.Tracing().StartRoot("Proxy forwarding", sql)
	require.True(t, propagated)
	child := root.StartChild("Handling messages")
	child.SetTag("db.parameters", "42,alice")
	child.Finish(nil)
	root.Finish(nil)
	require.NoError(t, pgproxy.Metrics().Count("dd.orgstore.pg_proxy.query", 3, []string{"database:orders"}, 1))
	require.NoError(t, pgproxy.Metrics().Distribution("dd.orgstore.pg_proxy.size", 12, nil, 1))
	require.NoError(t, pgproxy.Metrics().Timing("dd.orgstore.pg_proxy.latency", 5*time.Millisecond, nil, 1))
	pgproxy.Logger().Warn("ready", "database", "orders")
	// Close must flush buffered traces and metrics, and be safe to call twice.
	require.NoError(t, shutdown())
	require.NoError(t, shutdown())
	mu.Lock()
	finishedSpans := append([]exportedSpan(nil), spans...)
	mu.Unlock()
	require.Len(t, finishedSpans, 2)
	var rootSpan, childSpan exportedSpan
	for _, span := range finishedSpans {
		require.Equal(t, "server", span.Meta["span.kind"])
		require.Equal(t, "proxy-test", span.Service)
		require.Equal(t, "test", span.Meta["env"])
		require.Equal(t, "1.2.3", span.Meta["version"])
		require.Equal(t, uint64(42), span.TraceID)
		if span.Name == "Proxy forwarding" {
			rootSpan = span
		} else {
			childSpan = span
		}
	}
	require.Equal(t, uint64(11), rootSpan.ParentID)
	require.Equal(t, rootSpan.SpanID, childSpan.ParentID)
	require.Equal(t, sql, rootSpan.Meta["db.statement"])
	require.Equal(t, "42,alice", childSpan.Meta["db.parameters"])
	require.NoError(t, udp.SetReadDeadline(time.Now().Add(3*time.Second)))
	var packets strings.Builder
	for !strings.Contains(packets.String(), "dd.orgstore.pg_proxy.query:3|c") ||
		!strings.Contains(packets.String(), "dd.orgstore.pg_proxy.size:12|d") ||
		!strings.Contains(packets.String(), "dd.orgstore.pg_proxy.latency:5.000000|ms") {
		buffer := make([]byte, 65535)
		n, _, err := udp.ReadFrom(buffer)
		require.NoError(t, err, packets.String())
		packets.Write(buffer[:n])
	}
	for _, tag := range []string{"database:orders", "service:proxy-test", "env:test", "version:1.2.3"} {
		require.Contains(t, packets.String(), tag)
	}
	var record map[string]any
	contents, err = os.ReadFile(logs.Name())
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(contents, &record))
	for key, value := range map[string]string{"message": "ready", "status": "WARN", "service": "proxy-test", "env": "test", "version": "1.2.3", "database": "orders"} {
		require.Equal(t, value, record[key])
	}
	t.Logf("Datadog trace: service=%s trace_id=%d parent_id=%d db.parameters=%q", rootSpan.Service, rootSpan.TraceID, rootSpan.ParentID, childSpan.Meta["db.parameters"])
	for _, packet := range strings.Split(packets.String(), "\n") {
		if strings.HasPrefix(packet, "dd.orgstore.pg_proxy.") {
			t.Logf("DogStatsD received: %s", packet)
		}
	}
	t.Logf("Datadog JSON log: %s", contents)
	t.Run("startup failure keeps JSON identity", func(t *testing.T) {
		require.NoError(t, logs.Truncate(0))
		_, err := logs.Seek(0, io.SeekStart)
		require.NoError(t, err)
		cfg := backendConfig(t, "postgres://postgres:postgres@127.0.0.1:1/postgres?sslmode=disable&connect_timeout=1")
		cfg.telemetry = telemetryConfig{Traces: traceDatadog, Metrics: metricDogStatsD, Logs: logsJSON}
		require.ErrorContains(t, runWithTelemetry(t.Context(), cfg), "connect to backend")
		contents, err := os.ReadFile(logs.Name())
		require.NoError(t, err)
		var record map[string]any
		require.NoError(t, json.Unmarshal(contents, &record))
		for key, value := range map[string]string{"message": "pg-proxy stopped", "status": "ERROR", "service": "proxy-test", "env": "test", "version": "1.2.3"} {
			require.Equal(t, value, record[key])
		}
		require.Contains(t, record["error"], "connect to backend")
		t.Logf("Datadog startup failure: %s", contents)
	})
}

func TestNativeSignalSelection(t *testing.T) {
	for name, cfg := range map[string]telemetryConfig{
		"logs only":    {Logs: logsJSON},
		"metrics only": {Metrics: metricDogStatsD},
	} {
		t.Run(name, func(t *testing.T) {
			// These settings would fail if startup accidentally enabled traces.
			t.Setenv("OTEL_TRACES_EXPORTER", "none")
			t.Setenv("DD_TRACE_ENABLED", "")
			udp, err := net.ListenPacket("udp", "127.0.0.1:0")
			require.NoError(t, err)
			defer udp.Close()
			if cfg.Metrics == metricDogStatsD {
				t.Setenv("DD_DOGSTATSD_URL", "udp://"+udp.LocalAddr().String())
			} else {
				// Log selection must not try to initialize a metrics client either.
				t.Setenv("DD_DOGSTATSD_URL", "unix:///missing/socket")
			}
			deps, shutdown, err := setupTelemetry(t.Context(), cfg)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, shutdown()) })
			require.IsType(t, pgproxy.NoopTracer{}, deps.Tracer)
			if cfg.Metrics == metricDogStatsD {
				require.NoError(t, deps.Metrics.Count("proxy.selection", 3, nil, 1))
				require.NoError(t, shutdown())
				require.NoError(t, udp.SetReadDeadline(time.Now().Add(3*time.Second)))
				buffer := make([]byte, 65535)
				n, _, err := udp.ReadFrom(buffer)
				require.NoError(t, err)
				require.Contains(t, string(buffer[:n]), "proxy.selection:3|c")
				require.IsType(t, &slog.TextHandler{}, deps.LogHandler)
			} else {
				require.IsType(t, pgproxy.NoopMetrics{}, deps.Metrics)
				require.IsType(t, &slog.JSONHandler{}, deps.LogHandler)
			}
		})
	}
}
