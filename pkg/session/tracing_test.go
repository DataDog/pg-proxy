// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package session

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/DataDog/dd-trace-go/v2/ddtrace/mocktracer"
	pgproxy "github.com/DataDog/pg-proxy"
	proxydatadog "github.com/DataDog/pg-proxy/pkg/datadog"
	proxyotel "github.com/DataDog/pg-proxy/pkg/otel"
	"github.com/DataDog/pg-proxy/pkg/query"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

const privateParameter = "actual private value"
const parameterQuery = "SELECT $1 /*traceparent='00-0000000000000000000000000000002a-000000000000000b-01'*/"
const unsampledParameterQuery = "SELECT $1 /*traceparent='00-0000000000000000000000000000002a-000000000000000b-00'*/"

var parameterCaptureCases = []struct {
	name, sql              string
	enable, wantParameters bool
}{
	{"default", parameterQuery, false, false},
	{"enabled", parameterQuery, true, true},
	{"unsampled", unsampledParameterQuery, true, false},
}

// recordOnly keeps unsampled spans visible to span processors without
// exporting them, so tests can inspect what an unsampled trace carried.
type recordOnly struct{}

func (recordOnly) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	return sdktrace.SamplingResult{Decision: sdktrace.RecordOnly, Tracestate: trace.SpanContextFromContext(p.ParentContext).TraceState()}
}
func (recordOnly) Description() string { return "RecordOnly" }

// Exercise real protocol forwarding, including preservation of the Bind payload.
func runParameterQuery(t *testing.T, sql string, enable bool) {
	t.Helper()
	_, client, postgres, _, closeSession := newRunningTestSessionWithBackend(t, func(cfg *Config) {
		cfg.EnableTraceParameters = enable
	})
	defer closeSession()
	client.Send(&pgproto3.Parse{Query: sql})
	client.Send(&pgproto3.Bind{Parameters: [][]byte{[]byte(privateParameter)}})
	client.Send(&pgproto3.Execute{})
	client.Send(&pgproto3.Sync{})
	require.NoError(t, client.Flush())
	for i := range 4 {
		msg, err := postgres.Receive()
		require.NoError(t, err)
		if i == 1 {
			bind, ok := msg.(*pgproto3.Bind)
			require.True(t, ok)
			require.Equal(t, [][]byte{[]byte(privateParameter)}, bind.Parameters)
		}
	}
	responses := []pgproto3.BackendMessage{&pgproto3.ParseComplete{}, &pgproto3.BindComplete{}, &pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")}, &pgproto3.ReadyForQuery{TxStatus: 'I'}}
	for _, response := range responses {
		postgres.Send(response)
	}
	require.NoError(t, postgres.Flush())
	for range responses {
		_, err := client.Receive()
		require.NoError(t, err)
	}
	closeSession() // Join the session before reading its finished spans.
}

func TestExtendedQueryOTelParameterCapture(t *testing.T) {
	for _, tc := range parameterCaptureCases {
		t.Run(tc.name, func(t *testing.T) {
			recorder := tracetest.NewSpanRecorder()
			sampler := sdktrace.ParentBased(sdktrace.AlwaysSample(),
				sdktrace.WithRemoteParentNotSampled(recordOnly{}), sdktrace.WithLocalParentNotSampled(recordOnly{}))
			provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder), sdktrace.WithSampler(sampler))
			t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
			t.Cleanup(pgproxy.Bootstrap(pgproxy.Dependencies{Tracer: proxyotel.NewTracer(provider.Tracer("test"))}))
			runParameterQuery(t, tc.sql, tc.enable)
			foundRoot, foundParameters := false, false
			for _, span := range recorder.Ended() {
				switch span.Name() {
				case query.TransactionOpName:
					foundRoot = true
					require.Equal(t, "0000000000000000000000000000002a", span.SpanContext().TraceID().String())
					require.Equal(t, "000000000000000b", span.Parent().SpanID().String())
				}
				for _, attr := range span.Attributes() {
					require.NotEqual(t, attribute.Key("db.parameters"), attr.Key, "the comma-joined tag was removed")
					if attr.Key == "db.parameters.0" {
						foundParameters = true
						require.Equal(t, query.TransactionOpName, span.Name(), "positions are tagged on the root span")
						require.Equal(t, privateParameter, attr.Value.AsString())
					}
					if attr.Key == "db.statement" {
						require.Equal(t, tc.sql, attr.Value.AsString())
					}
					if !tc.wantParameters {
						require.NotContains(t, attr.Value.AsString(), privateParameter)
					}
				}
			}
			require.True(t, foundRoot, "the span that carries the parameters must be recorded")
			require.Equal(t, tc.wantParameters, foundParameters)
		})
	}
}

func TestExtendedQueryDatadogParameterCapture(t *testing.T) {
	for _, tc := range parameterCaptureCases {
		t.Run(tc.name, func(t *testing.T) {
			tracer := mocktracer.Start()
			t.Cleanup(tracer.Stop)
			t.Cleanup(pgproxy.Bootstrap(pgproxy.Dependencies{Tracer: proxydatadog.Tracer{}}))
			runParameterQuery(t, tc.sql, tc.enable)
			foundRoot, foundParameters := false, false
			for _, span := range tracer.FinishedSpans() {
				if span.OperationName() == query.TransactionOpName {
					foundRoot = true
					require.Equal(t, uint64(42), span.TraceID())
					require.Equal(t, uint64(11), span.ParentID())
					require.Equal(t, tc.sql, span.Tag("db.statement"))
				}
				require.Nil(t, span.Tag("db.parameters"), "the comma-joined tag was removed")
				if value := span.Tag("db.parameters.0"); value != nil {
					foundParameters = true
					require.Equal(t, query.TransactionOpName, span.OperationName())
					require.Equal(t, privateParameter, value)
				}
			}
			require.True(t, foundRoot, "mocktracer keeps unsampled spans, so the gate is what drops the tags")
			require.Equal(t, tc.wantParameters, foundParameters)
		})
	}
}

func TestDefaultParametersDoNotReachDebugLogs(t *testing.T) {
	// Enabled capture without a root trace must not fall back to the debug log
	// that reports tags set outside a trace.
	for _, enable := range []bool{false, true} {
		t.Run(map[bool]string{false: "default", true: "enabled without trace"}[enable], func(t *testing.T) {
			var logs bytes.Buffer
			t.Cleanup(pgproxy.Bootstrap(pgproxy.Dependencies{
				LogHandler: slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}),
			}))
			s := New(Config{EnableTraceParameters: enable})
			bind := &pgproto3.Bind{Parameters: [][]byte{[]byte(privateParameter)}}
			action, err := s.DecideClientMessageAction(bind)
			require.NoError(t, err)
			require.Same(t, bind, action.ProxyMsgToPg)
			require.NotContains(t, logs.String(), privateParameter)
			require.NotContains(t, logs.String(), "db.parameters")
		})
	}
}
