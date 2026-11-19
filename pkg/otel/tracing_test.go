// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package otel_test

import (
	"context"
	"errors"
	"io"
	"strconv"
	"strings"
	"testing"

	pgproxy "github.com/DataDog/pg-proxy"
	proxyotel "github.com/DataDog/pg-proxy/pkg/otel"
	"github.com/DataDog/pg-proxy/pkg/query"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

const parent = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"

func installTracer(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder), sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
	t.Cleanup(pgproxy.Bootstrap(pgproxy.Dependencies{Tracer: proxyotel.NewTracer(provider.Tracer("pg-proxy"))}))
	return recorder
}
func tags(span sdktrace.ReadOnlySpan) map[string]any {
	result := map[string]any{}
	for _, a := range span.Attributes() {
		result[string(a.Key)] = a.Value.AsInterface()
	}
	return result
}
func TestQueryTraceParentageAndCapture(t *testing.T) {
	for _, extended := range []bool{false, true} {
		for _, sql := range []string{"select $1", "/*traceparent='" + parent + "',tracestate='vendor%3Dvalue'*/ select $1", "select $1 /*traceparent='" + parent + "'*/"} {
			t.Run(sql+map[bool]string{true: " extended", false: " simple"}[extended], func(t *testing.T) {
				recorder := installTracer(t)
				state := &query.TraceState{}
				var msg pgproto3.FrontendMessage = &pgproto3.Query{String: sql}
				if extended {
					msg = &pgproto3.Parse{Query: sql}
				}
				state.StartRootTrace(msg)
				require.Equal(t, strings.Contains(sql, "traceparent"), state.UsingFullPropagation())
				state.SetTag("db.query.parameter.0", "[private actual value]")
				state.AddMessage(&pgproto3.Bind{})
				state.AddMessage(&pgproto3.DataRow{})
				state.StartTrace(query.QueryExecutionAndProxying)
				state.FinishRootTrace()
				spans := recorder.Ended()
				require.Len(t, spans, 3)
				root := spans[2]
				require.Equal(t, query.TransactionOpName, root.Name())
				require.Equal(t, trace.SpanKindServer, root.SpanKind())
				require.Equal(t, sql, tags(root)["db.statement"])
				require.Equal(t, "postgres", tags(root)["db.system"])
				if stateParent := root.Parent(); strings.Contains(sql, "traceparent") {
					require.Equal(t, "0123456789abcdef0123456789abcdef", root.SpanContext().TraceID().String())
					require.Equal(t, "0123456789abcdef", stateParent.SpanID().String())
					require.True(t, stateParent.IsRemote())
					if strings.Contains(sql, "tracestate") {
						require.Equal(t, "vendor=value", root.SpanContext().TraceState().String())
					}
				} else {
					require.False(t, root.Parent().IsValid())
				}
				for _, child := range spans[:2] {
					require.Equal(t, trace.SpanKindInternal, child.SpanKind())
					require.Equal(t, root.SpanContext().SpanID(), child.Parent().SpanID())
					require.Equal(t, root.SpanContext().TraceID(), child.SpanContext().TraceID())
					require.False(t, child.EndTime().Before(child.StartTime()))
				}
				require.Equal(t, "[private actual value]", tags(spans[0])["db.query.parameter.0"])
				require.Equal(t, "*pgproto3.Bind", tags(spans[0])["messages"])
				require.Nil(t, state.RootTrace())
				require.False(t, state.UsingFullPropagation())
			})
		}
	}
}

func TestInvalidCommentAndUnsampledParent(t *testing.T) {
	for _, sql := range []string{
		"", "/*", "*/", "/*/", "select 1 /*/",
		"select 1", "/*traceparent='broken'*/ select 1", "/*traceparent='" + parent + "',traceparent='" + parent + "'*/ select 1",
		"select '/*traceparent=bad*/'", "/*traceparent='" + parent + "'", "/*traceparent='" + parent + "',tracestate='%zz'*/ select 1",
		"/*traceparent='00-00000000000000000000000000000000-0123456789abcdef-01'*/ select 1",
		"/*traceparent='" + parent + "' /* nested */ */ select 1",
	} {
		t.Run(sql, func(t *testing.T) {
			recorder := installTracer(t)
			root, propagated := pgproxy.Tracing().StartRoot("query", sql)
			require.False(t, propagated)
			root.Finish(nil)
			require.Len(t, recorder.Ended(), 1)
			require.False(t, recorder.Ended()[0].Parent().IsValid())
		})
	}
	t.Run("unsampled", func(t *testing.T) {
		recorder := installTracer(t)
		root, propagated := pgproxy.Tracing().StartRoot("query", "select 1 /*traceparent='"+strings.TrimSuffix(parent, "01")+"00'*/")
		require.True(t, propagated)
		root.StartChild("child").Finish(nil)
		root.Finish(nil)
		require.Empty(t, recorder.Ended())
	})
}

func TestNonUTF8SQLPreservesQueryAndParent(t *testing.T) {
	const sql = "SELECT 'caf\xe9' /*traceparent='" + parent + "'*/"
	for name, msg := range map[string]pgproto3.FrontendMessage{
		"Query": &pgproto3.Query{String: sql},
		"Parse": &pgproto3.Parse{Query: sql},
	} {
		t.Run(name, func(t *testing.T) {
			recorder := installTracer(t)
			before, err := msg.Encode(nil)
			require.NoError(t, err)
			state := &query.TraceState{}
			state.StartRootTrace(msg)
			require.True(t, state.UsingFullPropagation())
			require.Equal(t, sql, state.RootQuery())
			state.FinishRootTrace()
			after, err := msg.Encode(nil)
			require.NoError(t, err)
			require.Equal(t, before, after, "only the telemetry attribute may be escaped")
			spans := recorder.Ended()
			require.Len(t, spans, 2)
			root := spans[1]
			require.Equal(t, strconv.Quote(sql), tags(root)["db.statement"])
			require.Equal(t, "0123456789abcdef0123456789abcdef", root.SpanContext().TraceID().String())
			require.Equal(t, "0123456789abcdef", root.Parent().SpanID().String())
		})
	}
}

func TestSQLCommentParentSelection(t *testing.T) {
	const trailing = " /*traceparent='" + parent + "',tracestate='vendor%3Dvalue'*/"
	const otherParent = "00-0000000000000000000000000000002a-000000000000000b-01"
	for _, tc := range []struct {
		name, sql, traceID, parentID, state string
	}{
		{"ordinary leading comment", "/* report query */ SELECT 1" + trailing, "0123456789abcdef0123456789abcdef", "0123456789abcdef", "vendor=value"},
		{"query hint", "/*+ SeqScan(orders) */ SELECT 1" + trailing, "0123456789abcdef0123456789abcdef", "0123456789abcdef", "vendor=value"},
		{"metadata only", "/*application='reports'*/ SELECT 1" + trailing, "0123456789abcdef0123456789abcdef", "0123456789abcdef", "vendor=value"},
		{"invalid leading context", "/*traceparent='broken'*/ SELECT 1" + trailing, "0123456789abcdef0123456789abcdef", "0123456789abcdef", "vendor=value"},
		{"duplicate leading fields", "/*traceparent='" + otherParent + "',traceparent='" + otherParent + "'*/ SELECT 1" + trailing, "0123456789abcdef0123456789abcdef", "0123456789abcdef", "vendor=value"},
		{"valid leading context takes precedence", "/*traceparent='" + otherParent + "'*/ SELECT 1" + trailing, "0000000000000000000000000000002a", "000000000000000b", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recorder := installTracer(t)
			root, propagated := pgproxy.Tracing().StartRoot("query", tc.sql)
			root.Finish(nil)
			require.True(t, propagated)
			require.True(t, pgproxy.Tracing().HasQueryContext(tc.sql))
			require.Len(t, recorder.Ended(), 1)
			span := recorder.Ended()[0]
			require.Equal(t, tc.traceID, span.SpanContext().TraceID().String())
			require.Equal(t, tc.parentID, span.Parent().SpanID().String())
			require.Equal(t, tc.state, span.SpanContext().TraceState().String())
		})
	}
}

func TestTraceErrorsBatchPingAndReset(t *testing.T) {
	recorder := installTracer(t)
	state := &query.TraceState{}
	state.StartRootTrace(&pgproto3.Query{String: "-- ping"})
	require.Nil(t, state.RootTrace())
	state.StartRootTrace(&pgproto3.Parse{Query: "select 1"})
	state.StartRootTrace(&pgproto3.Parse{Query: "select 2"})
	state.StartTrace(query.QueryExecutionAndProxying)
	state.FinishRootTrace(errors.New("backend failed"))
	spans := recorder.Ended()
	require.Len(t, spans, 3)
	require.Equal(t, query.BatchExecutionAndProxying, spans[1].Name())
	require.Equal(t, codes.Error, spans[1].Status().Code)
	require.Len(t, spans[1].Events(), 1)
	require.Equal(t, codes.Error, spans[2].Status().Code)
	state.StartRootTrace(&pgproto3.Query{String: "select 3"})
	state.StartTrace(query.QueryExecutionAndProxying)
	state.FinishRootTrace(io.EOF)
	spans = recorder.Ended()
	require.Len(t, spans, 6)
	require.Equal(t, query.QueryExecutionAndProxying, spans[4].Name())
	require.Equal(t, codes.Unset, spans[4].Status().Code)
	require.NotEqual(t, spans[2].SpanContext().TraceID(), spans[5].SpanContext().TraceID())
}

func TestNilProvidersAreNoop(t *testing.T) {
	tracer := proxyotel.NewTracer(nil)
	root, _ := tracer.StartRoot("query", "select 1")
	root.SetTag("value", attribute.StringValue("value"))
	root.StartChild("child").Finish(errors.New("error"))
	root.Finish(nil)
	require.NoError(t, proxyotel.NewMetrics(nil).Count("count", 1, nil, 1))
}

func TestApplicationAndProxyShareTrace(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
	appContext, app := provider.Tracer("application").Start(t.Context(), "application request")
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(appContext, carrier)
	proxy := proxyotel.NewTracer(provider.Tracer("pg-proxy"))
	root, propagated := proxy.StartRoot("proxy query", "select 1 /*traceparent='"+carrier.Get("traceparent")+"'*/")
	require.True(t, propagated)
	root.StartChild("execute").Finish(nil)
	root.Finish(nil)
	app.End()
	spans := recorder.Ended()
	require.Len(t, spans, 3)
	require.Equal(t, app.SpanContext().SpanID(), spans[1].Parent().SpanID())
	for _, span := range spans {
		require.Equal(t, app.SpanContext().TraceID(), span.SpanContext().TraceID())
	}
}

func TestSpanSampled(t *testing.T) {
	unsampled := strings.TrimSuffix(parent, "01") + "00"
	for _, tc := range []struct {
		name, sql string
		want      bool
	}{
		{"local root", "select 1", true},
		{"sampled parent", "select 1 /*traceparent='" + parent + "'*/", true},
		{"unsampled parent", "select 1 /*traceparent='" + unsampled + "'*/", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			installTracer(t)
			root, _ := pgproxy.Tracing().StartRoot("query", tc.sql)
			defer root.Finish(nil)
			require.Equal(t, tc.want, root.(pgproxy.SamplingReporter).Sampled())
		})
	}
	t.Run("never sample", func(t *testing.T) {
		provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.NeverSample()))
		t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
		root, _ := proxyotel.NewTracer(provider.Tracer("pg-proxy")).StartRoot("query", "select 1")
		defer root.Finish(nil)
		require.False(t, root.(pgproxy.SamplingReporter).Sampled())
	})
	t.Run("nil provider with sampled parent", func(t *testing.T) {
		root, _ := proxyotel.NewTracer(nil).StartRoot("query", "select 1 /*traceparent='"+parent+"'*/")
		defer root.Finish(nil)
		require.False(t, root.(pgproxy.SamplingReporter).Sampled(), "non-recording spans export nothing")
	})
}
