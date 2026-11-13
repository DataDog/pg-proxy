// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package session

import (
	"context"
	"testing"
	"time"

	"github.com/DataDog/dd-trace-go/v2/ddtrace/mocktracer"
	pgproxy "github.com/DataDog/pg-proxy"
	proxydatadog "github.com/DataDog/pg-proxy/pkg/datadog"
	"github.com/DataDog/pg-proxy/pkg/link"
	proxyotel "github.com/DataDog/pg-proxy/pkg/otel"
	"github.com/DataDog/pg-proxy/pkg/query"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

type databaseTestLink struct{ link.Link }

func (databaseTestLink) DatabaseInfo() pgproxy.DatabaseInfo {
	return pgproxy.DatabaseInfo{Name: "orders_actual", Host: "database.example.test", Port: 5433}
}

func runDatabaseExchange(t *testing.T, extended, fail bool, beforeReply func(), afterReply func(time.Time)) {
	t.Helper()
	_, client, postgres, _, closeSession := newRunningTestSessionWithBackend(t, func(cfg *Config) {
		backend := cfg.Backend.(*fixedBackend)
		backend.backendLink = databaseTestLink{backend.backendLink}
	})
	defer closeSession()
	sql := "SELECT 1 /*traceparent='00-0000000000000000000000000000002a-000000000000000b-01'*/"
	count := 1
	if extended {
		client.Send(&pgproto3.Parse{Query: sql})
		client.Send(&pgproto3.Bind{})
		client.Send(&pgproto3.Execute{})
		client.Send(&pgproto3.Sync{})
		count = 4
	} else {
		client.Send(&pgproto3.Query{String: sql})
	}
	require.NoError(t, client.Flush())
	for range count {
		_, err := postgres.Receive()
		require.NoError(t, err)
	}
	beforeReply() // The send has completed, but the database exchange must still be open.
	responseTime := time.Now()
	if fail {
		postgres.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "22012", Message: "division by zero"})
		require.NoError(t, postgres.Flush())
		beforeReply() // ErrorResponse does not complete the exchange; ReadyForQuery does.
		count = 2
	} else if extended {
		postgres.Send(&pgproto3.ParseComplete{})
		postgres.Send(&pgproto3.BindComplete{})
		postgres.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
		count = 4
	} else {
		postgres.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{Name: []byte("result")}}})
		postgres.Send(&pgproto3.DataRow{Values: [][]byte{[]byte("1")}})
		postgres.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
		count = 4
	}
	postgres.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	require.NoError(t, postgres.Flush())
	afterReply(responseTime) // The database span must finish even before the final client read.
	for range count {
		_, err := client.Receive()
		require.NoError(t, err)
	}
	closeSession()
}

func TestDatabaseExchangeDatadog(t *testing.T) {
	for _, extended := range []bool{false, true} {
		for _, fail := range []bool{false, true} {
			t.Run(map[bool]string{false: "simple", true: "extended"}[extended]+map[bool]string{false: "_success", true: "_error"}[fail], func(t *testing.T) {
				mt := mocktracer.Start()
				defer mt.Stop()
				t.Cleanup(pgproxy.Bootstrap(pgproxy.Dependencies{Tracer: proxydatadog.Tracer{}}))
				var database *mocktracer.Span
				runDatabaseExchange(t, extended, fail, func() {
					var open int
					for _, span := range mt.OpenSpans() {
						if span.OperationName() == query.DatabaseRequestOpName {
							open++
							database = span
						}
					}
					require.Equal(t, 1, open)
					for _, span := range mt.FinishedSpans() {
						require.NotEqual(t, query.DatabaseRequestOpName, span.OperationName())
					}
				}, func(responseTime time.Time) {
					require.Eventually(t, func() bool {
						for _, span := range mt.FinishedSpans() {
							if span.SpanID() == database.SpanID() {
								database = span
								return true
							}
						}
						return false
					}, time.Second, time.Millisecond)
					require.False(t, database.FinishTime().Before(responseTime))
				})
				require.Empty(t, mt.OpenSpans())
				require.Equal(t, "client", database.Tag("span.kind"))
				require.Equal(t, "sql", database.Tag("span.type"))
				require.Equal(t, "postgres", database.Tag("db.system"))
				require.Equal(t, "orders_actual", database.Tag("db.instance"))
				require.Equal(t, "database.example.test", database.Tag("out.host"))
				require.EqualValues(t, 5433, database.Tag("out.port"))
				var root *mocktracer.Span
				for _, span := range mt.FinishedSpans() {
					if span.OperationName() == query.TransactionOpName {
						root = span
					} else if span.SpanID() != database.SpanID() {
						require.Equal(t, "internal", span.Tag("span.kind"))
					}
				}
				require.NotNil(t, root)
				require.Equal(t, root.SpanID(), database.ParentID())
				require.Equal(t, uint64(42), database.TraceID())
				require.Equal(t, uint64(11), root.ParentID())
				if fail {
					require.Equal(t, "22012", database.Tag("db.response.status_code"))
					require.Contains(t, database.Tag("error.message"), "division by zero")
					require.Contains(t, root.Tag("error.message"), "division by zero")
				} else {
					require.Nil(t, database.Tag("error.message"))
					require.Nil(t, root.Tag("error.message"))
				}
			})
		}
	}
}

func TestDatabaseExchangeOTel(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "error"}[fail], func(t *testing.T) {
			recorder := tracetest.NewSpanRecorder()
			provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
			t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
			t.Cleanup(pgproxy.Bootstrap(pgproxy.Dependencies{Tracer: proxyotel.NewTracer(provider.Tracer("test"))}))
			runDatabaseExchange(t, false, fail, func() {
				var open int
				for _, span := range recorder.Started() {
					if span.Name() == query.DatabaseRequestOpName {
						open++
					}
				}
				require.Equal(t, 1, open)
				for _, span := range recorder.Ended() {
					require.NotEqual(t, query.DatabaseRequestOpName, span.Name())
				}
			}, func(responseTime time.Time) {
				require.Eventually(t, func() bool {
					for _, span := range recorder.Ended() {
						if span.Name() == query.DatabaseRequestOpName {
							return !span.EndTime().Before(responseTime)
						}
					}
					return false
				}, time.Second, time.Millisecond)
			})
			var database, root sdktrace.ReadOnlySpan
			for _, span := range recorder.Ended() {
				switch span.Name() {
				case query.DatabaseRequestOpName:
					database = span
				case query.TransactionOpName:
					root = span
				default:
					require.Equal(t, trace.SpanKindInternal, span.SpanKind())
				}
			}
			require.NotNil(t, database)
			require.NotNil(t, root)
			require.Equal(t, trace.SpanKindClient, database.SpanKind())
			require.Equal(t, root.SpanContext().SpanID(), database.Parent().SpanID())
			attrs := map[string]any{}
			for _, attr := range database.Attributes() {
				attrs[string(attr.Key)] = attr.Value.AsInterface()
			}
			require.Equal(t, "postgresql", attrs["db.system"])
			require.Equal(t, "orders_actual", attrs["db.namespace"])
			require.Equal(t, "database.example.test", attrs["server.address"])
			require.EqualValues(t, 5433, attrs["server.port"])
			if fail {
				require.Equal(t, codes.Error, database.Status().Code)
				require.Equal(t, codes.Error, root.Status().Code)
				require.Equal(t, "22012", attrs["db.response.status_code"])
			} else {
				require.NotEqual(t, codes.Error, database.Status().Code)
			}
		})
	}
}
