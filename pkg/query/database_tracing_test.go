// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package query_test

import (
	"context"
	"errors"
	"io"
	"testing"

	pgproxy "github.com/DataDog/pg-proxy"
	proxyotel "github.com/DataDog/pg-proxy/pkg/otel"
	"github.com/DataDog/pg-proxy/pkg/query"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func installDatabaseTracer(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
	t.Cleanup(pgproxy.Bootstrap(pgproxy.Dependencies{Tracer: proxyotel.NewTracer(provider.Tracer("test"))}))
	return recorder
}

func TestDatabaseExchangeTransportFailureFinishesAllSpans(t *testing.T) {
	recorder := installDatabaseTracer(t)
	state := &query.TraceState{}
	state.StartRootTrace(&pgproto3.Query{String: "SELECT 1"})
	state.StartDatabaseTrace(pgproxy.DatabaseInfo{})
	state.FinishRootTrace(io.EOF)
	var client, root sdktrace.ReadOnlySpan
	for _, span := range recorder.Ended() {
		if span.Name() == query.DatabaseRequestOpName {
			client = span
		}
		if span.Name() == query.TransactionOpName {
			root = span
		}
	}
	require.NotNil(t, client)
	require.NotNil(t, root)
	require.Equal(t, codes.Error, client.Status().Code)
	require.Equal(t, codes.Error, root.Status().Code)
	require.Equal(t, len(recorder.Started()), len(recorder.Ended()))
}

func TestClientFailureAfterBackendCompletionDoesNotFailDatabase(t *testing.T) {
	recorder := installDatabaseTracer(t)
	state := &query.TraceState{}
	state.StartRootTrace(&pgproto3.Query{String: "SELECT 1"})
	state.StartDatabaseTrace(pgproxy.DatabaseInfo{})
	state.FinishDatabaseTrace(nil)
	state.FinishRootTrace(errors.New("client write failed"))
	for _, span := range recorder.Ended() {
		if span.Name() == query.DatabaseRequestOpName {
			require.NotEqual(t, codes.Error, span.Status().Code)
		}
		if span.Name() == query.TransactionOpName {
			require.Equal(t, codes.Error, span.Status().Code)
		}
	}
}

func TestBatchDoesNotClaimOnlyFirstStatement(t *testing.T) {
	recorder := installDatabaseTracer(t)
	state := &query.TraceState{}
	state.StartRootTrace(&pgproto3.Parse{Query: "SELECT 1"})
	state.StartRootTrace(&pgproto3.Parse{Query: "SELECT 2"})
	state.StartDatabaseTrace(pgproxy.DatabaseInfo{})
	state.FinishRootTrace()
	for _, span := range recorder.Ended() {
		if span.Name() == query.DatabaseRequestOpName {
			for _, attr := range span.Attributes() {
				require.NotEqual(t, "db.statement", string(attr.Key))
			}
		}
	}
}

func TestRootErrorDoesNotLeakToNextRequest(t *testing.T) {
	recorder := installDatabaseTracer(t)
	state := &query.TraceState{}
	state.StartRootTrace(&pgproto3.Query{String: "SELECT 1/0"})
	state.StartDatabaseTrace(pgproxy.DatabaseInfo{})
	state.RecordDatabaseError(errors.New("division by zero"), "22012")
	state.FinishDatabaseTrace(nil)
	state.FinishRootTrace()
	state.StartRootTrace(&pgproto3.Query{String: "SELECT 1"})
	state.StartDatabaseTrace(pgproxy.DatabaseInfo{})
	state.FinishRootTrace()
	var clients []sdktrace.ReadOnlySpan
	for _, span := range recorder.Ended() {
		if span.Name() == query.DatabaseRequestOpName {
			clients = append(clients, span)
		}
	}
	require.Len(t, clients, 2)
	require.Equal(t, codes.Error, clients[0].Status().Code)
	require.NotEqual(t, codes.Error, clients[1].Status().Code)
}
