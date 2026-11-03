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
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

const privateParameter = "actual private value"
const parameterQuery = "SELECT $1 /*traceparent='00-0000000000000000000000000000002a-000000000000000b-01'*/"

// Exercise real protocol forwarding, including preservation of the Bind payload.
func runParameterQuery(t *testing.T, enable bool) {
	t.Helper()
	_, client, postgres, _, closeSession := newRunningTestSessionWithBackend(t, func(cfg *Config) {
		cfg.EnableTraceParameters = enable
	})
	defer closeSession()
	client.Send(&pgproto3.Parse{Query: parameterQuery})
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
	for _, enable := range []bool{false, true} {
		t.Run(map[bool]string{false: "default", true: "enabled"}[enable], func(t *testing.T) {
			recorder := tracetest.NewSpanRecorder()
			provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
			t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
			t.Cleanup(pgproxy.Bootstrap(pgproxy.Dependencies{Tracer: proxyotel.NewTracer(provider.Tracer("test"))}))
			runParameterQuery(t, enable)
			foundRoot, foundParameters := false, false
			for _, span := range recorder.Ended() {
				if span.Name() == query.TransactionOpName {
					foundRoot = true
					require.Equal(t, "0000000000000000000000000000002a", span.SpanContext().TraceID().String())
					require.Equal(t, "000000000000000b", span.Parent().SpanID().String())
				}
				for _, attr := range span.Attributes() {
					if attr.Key == "db.parameters" {
						foundParameters = true
						require.Equal(t, privateParameter, attr.Value.AsString())
					}
					if attr.Key == "db.statement" {
						require.Equal(t, parameterQuery, attr.Value.AsString())
					}
					if !enable {
						require.NotContains(t, attr.Value.AsString(), privateParameter)
					}
				}
			}
			require.True(t, foundRoot)
			require.Equal(t, enable, foundParameters)
		})
	}
}

func TestExtendedQueryDatadogParameterCapture(t *testing.T) {
	for _, enable := range []bool{false, true} {
		t.Run(map[bool]string{false: "default", true: "enabled"}[enable], func(t *testing.T) {
			tracer := mocktracer.Start()
			t.Cleanup(tracer.Stop)
			t.Cleanup(pgproxy.Bootstrap(pgproxy.Dependencies{Tracer: proxydatadog.Tracer{}}))
			runParameterQuery(t, enable)
			foundRoot, foundParameters := false, false
			for _, span := range tracer.FinishedSpans() {
				if span.OperationName() == query.TransactionOpName {
					foundRoot = true
					require.Equal(t, uint64(42), span.TraceID())
					require.Equal(t, uint64(11), span.ParentID())
					require.Equal(t, parameterQuery, span.Tag("db.statement"))
				}
				if value := span.Tag("db.parameters"); value != nil {
					foundParameters = true
					require.Equal(t, privateParameter, value)
				}
			}
			require.True(t, foundRoot)
			require.Equal(t, enable, foundParameters)
		})
	}
}

func TestDefaultParametersDoNotReachDebugLogs(t *testing.T) {
	var logs bytes.Buffer
	t.Cleanup(pgproxy.Bootstrap(pgproxy.Dependencies{
		LogHandler: slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}),
	}))
	s := New(Config{})
	bind := &pgproto3.Bind{Parameters: [][]byte{[]byte(privateParameter)}}
	action, err := s.DecideClientMessageAction(bind)
	require.NoError(t, err)
	require.Same(t, bind, action.ProxyMsgToPg)
	require.NotContains(t, logs.String(), privateParameter)
	require.NotContains(t, logs.String(), "db.parameters")
}
