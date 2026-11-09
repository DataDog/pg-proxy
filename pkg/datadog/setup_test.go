// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package datadog_test

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/DataDog/dd-trace-go/v2/ddtrace/mocktracer"
	"github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"
	"github.com/stretchr/testify/require"

	pgproxy "github.com/DataDog/pg-proxy"
	proxydatadog "github.com/DataDog/pg-proxy/pkg/datadog"
)

func TestReuseExistingIntegrations(t *testing.T) {
	mt := mocktracer.Start()
	defer mt.Stop()
	var logs bytes.Buffer
	metrics := &ownedMetrics{}
	handler := slog.NewTextHandler(&logs, nil)
	want := pgproxy.Dependencies{Tracer: proxydatadog.Tracer{}, Metrics: metrics, LogHandler: handler}
	// These invalid settings must not be read when the caller owns the clients.
	t.Setenv("DD_DOGSTATSD_URL", "unix:///missing/socket")
	t.Setenv("OTEL_TRACES_EXPORTER", "otlp")
	deps, shutdown, err := proxydatadog.Start(proxydatadog.Options{Dependencies: want, LogLevel: slog.LevelDebug})
	require.NoError(t, err)
	require.Equal(t, want, deps)
	require.False(t, deps.LogHandler.Enabled(t.Context(), slog.LevelDebug), "supplied handlers retain their own level")
	require.NoError(t, shutdown())
	require.False(t, metrics.closed)
	root, _ := deps.Tracer.StartRoot("still running", "SELECT 1")
	root.Finish(nil)
	require.Len(t, mt.FinishedSpans(), 1, "shutdown must not stop the caller's tracer")
}

func TestNativeLogLevel(t *testing.T) {
	for _, minimum := range []slog.Level{slog.LevelInfo, slog.LevelDebug, slog.LevelWarn, slog.LevelError} {
		t.Run(minimum.String(), func(t *testing.T) {
			var output bytes.Buffer
			deps, shutdown, err := proxydatadog.Start(proxydatadog.Options{
				Dependencies: pgproxy.Dependencies{Tracer: pgproxy.NoopTracer{}, Metrics: pgproxy.NoopMetrics{}},
				LogWriter:    &output,
				LogLevel:     minimum,
			})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, shutdown()) })
			logger := slog.New(deps.LogHandler).With("component", "proxy").WithGroup("query")
			for _, level := range []slog.Level{slog.LevelDebug, slog.LevelInfo, slog.LevelWarn, slog.LevelError} {
				logger.Log(t.Context(), level, "message-"+level.String(), "id", 7)
				if level >= minimum {
					require.Contains(t, output.String(), `"message":"message-`+level.String()+`"`)
					require.Contains(t, output.String(), `"status":"`+level.String()+`"`)
				} else {
					require.NotContains(t, output.String(), "message-"+level.String())
				}
			}
			require.Contains(t, output.String(), `"component":"proxy","query":{"id":7}`)
		})
	}
}

type ownedMetrics struct {
	pgproxy.NoopMetrics
	closed bool
}

func (m *ownedMetrics) Close() error { m.closed = true; return nil }

func TestNativeSetupErrorsLeaveExistingTracerRunning(t *testing.T) {
	mt := mocktracer.Start()
	defer mt.Stop()
	t.Setenv("OTEL_TRACES_EXPORTER", "")
	t.Setenv("DD_DOGSTATSD_URL", "")
	t.Setenv("DD_AGENT_HOST", "127.0.0.1")
	t.Setenv("DD_DOGSTATSD_PORT", "invalid-private-value")
	_, shutdown, err := proxydatadog.Start(proxydatadog.Options{})
	require.ErrorContains(t, err, "metrics client")
	require.NotContains(t, err.Error(), "invalid-private-value")
	require.Nil(t, shutdown)
	tracer.StartSpan("still running").Finish()
	require.Len(t, mt.FinishedSpans(), 1)
	for _, exporter := range []string{"otlp", "OTLP", " otlp ", "none", "NONE", " none "} {
		t.Setenv("OTEL_TRACES_EXPORTER", exporter)
		t.Setenv("DD_TRACE_AGENT_PROTOCOL_VERSION", "")
		t.Setenv("DD_TRACE_ENABLED", "")
		_, _, err := proxydatadog.Start(proxydatadog.Options{})
		require.ErrorContains(t, err, "OTEL_TRACES_EXPORTER", exporter)
	}
}

func TestFailedTracerStartStopsOwnedTracer(t *testing.T) {
	agent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"endpoints":["/evp_proxy/v2"]}`)
	}))
	defer agent.Close()
	for key, value := range map[string]string{
		"DD_TRACE_AGENT_URL": agent.URL, "DD_TRACE_AGENT_PROTOCOL_VERSION": "0.4",
		"DD_TRACE_ENABLED": "true", "DD_TRACE_STARTUP_LOGS": "false", "DD_TRACE_DEBUG": "false",
		"DD_REMOTE_CONFIGURATION_ENABLED": "false", "DD_INSTRUMENTATION_TELEMETRY_ENABLED": "false",
		"DD_APPSEC_ENABLED": "false", "DD_RUNTIME_METRICS_ENABLED": "false", "DD_RUNTIME_METRICS_V2_ENABLED": "false",
		"DD_TRACE_STATS_COMPUTATION_ENABLED": "false", "OTEL_TRACES_EXPORTER": "",
	} {
		t.Setenv(key, value)
	}
	t.Cleanup(tracer.Stop)
	metrics := &ownedMetrics{}
	opts := proxydatadog.Options{
		Dependencies: pgproxy.Dependencies{Metrics: metrics, LogHandler: slog.NewTextHandler(io.Discard, nil)},
		// Missing ML app fails after the SDK installs the tracer and starts workers.
		TracerOptions: []tracer.StartOption{tracer.WithLLMObsEnabled(true), tracer.WithLLMObsMLApp(""), tracer.WithLLMObsAgentlessEnabled(false)},
	}
	_, shutdown, err := proxydatadog.Start(opts)
	require.ErrorContains(t, err, "could not initialize Datadog tracer")
	require.Nil(t, shutdown)
	span := tracer.StartSpan("after failed startup")
	defer span.Finish()
	require.Nil(t, span, "failed startup must restore the no-op tracer")
	require.False(t, metrics.closed, "caller-owned metrics must remain open")

	// A failed attempt must not prevent a later successful start and shutdown.
	opts.TracerOptions = []tracer.StartOption{tracer.WithLLMObsEnabled(false)}
	_, shutdown, err = proxydatadog.Start(opts)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, shutdown()) })
	span = tracer.StartSpan("after restart")
	require.NotNil(t, span)
	span.Finish()
	require.NoError(t, shutdown())
	require.False(t, metrics.closed)
}
