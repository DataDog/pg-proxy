// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package session

import (
	"encoding/binary"
	"strconv"
	"testing"

	"github.com/DataDog/dd-trace-go/v2/ddtrace/mocktracer"
	pgproxy "github.com/DataDog/pg-proxy"
	proxydatadog "github.com/DataDog/pg-proxy/pkg/datadog"
	"github.com/DataDog/pg-proxy/pkg/query"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// exchange sends one client cycle and plays a Postgres that answers each
// forwarded message until Sync, describing statements with describedOIDs. It
// reads what the proxy forwards rather than expecting the client's messages,
// since the proxy may add messages of its own.
func exchange(t *testing.T, client *pgproto3.Frontend, postgres *pgproto3.Backend, describedOIDs []uint32, sends ...pgproto3.FrontendMessage) {
	t.Helper()
	for _, msg := range sends {
		client.Send(msg)
	}
	require.NoError(t, client.Flush())
	for done := false; !done; {
		msg, err := postgres.Receive()
		require.NoError(t, err)
		switch msg.(type) {
		case *pgproto3.Parse:
			postgres.Send(&pgproto3.ParseComplete{})
		case *pgproto3.Describe:
			postgres.Send(&pgproto3.ParameterDescription{ParameterOIDs: describedOIDs})
			postgres.Send(&pgproto3.NoData{})
		case *pgproto3.Bind:
			postgres.Send(&pgproto3.BindComplete{})
		case *pgproto3.Execute:
			postgres.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
		case *pgproto3.Sync:
			// In a transaction, so the session keeps the one link the test
			// backend can hand out.
			postgres.Send(&pgproto3.ReadyForQuery{TxStatus: 'T'})
			done = true
		default:
			t.Fatalf("unexpected forwarded message %T", msg)
		}
	}
	require.NoError(t, postgres.Flush())
	for {
		msg, err := client.Receive()
		require.NoError(t, err)
		if _, ok := msg.(*pgproto3.ReadyForQuery); ok {
			return
		}
	}
}

func int8Param(v int64) []byte { return binary.BigEndian.AppendUint64(nil, uint64(v)) }

// rootTags returns the tags of the finished root spans, in order.
func rootTags(tracer mocktracer.Tracer) []map[string]any {
	var roots []map[string]any
	for _, span := range tracer.FinishedSpans() {
		if span.OperationName() == query.TransactionOpName {
			roots = append(roots, span.Tags())
		}
	}
	return roots
}

func startMockTracer(t *testing.T) mocktracer.Tracer {
	tracer := mocktracer.Start()
	t.Cleanup(tracer.Stop)
	t.Cleanup(pgproxy.Bootstrap(pgproxy.Dependencies{Tracer: proxydatadog.Tracer{}}))
	return tracer
}

func TestBindParametersUseParseTypes(t *testing.T) {
	tracer := startMockTracer(t)
	_, client, postgres, _, closeSession := newRunningTestSessionWithBackend(t, func(cfg *Config) {
		cfg.EnableTraceParameters = true
	})
	defer closeSession()
	exchange(t, client, postgres, nil,
		&pgproto3.Parse{Query: parameterQuery + " AND $2", ParameterOIDs: []uint32{pgtype.Int8OID, 0}},
		&pgproto3.Bind{ParameterFormatCodes: []int16{1, 0}, Parameters: [][]byte{int8Param(-42), nil}},
		&pgproto3.Execute{},
		&pgproto3.Sync{})
	closeSession()

	roots := rootTags(tracer)
	require.Len(t, roots, 1)
	assert.Equal(t, "-42", roots[0]["db.parameters.0"])
	assert.Equal(t, "null", roots[0]["db.parameters.1"])
	assert.NotContains(t, roots[0], "db.parameters.truncated")
}

// pgx's describe-exec mode parses a statement without types, learns them from
// the backend's ParameterDescription, and binds binary values in a later cycle.
// (Named statements take the same path, but need a pooled connection the test
// backend does not have.)
func TestBindParametersUseDescribedTypes(t *testing.T) {
	tracer := startMockTracer(t)
	_, client, postgres, _, closeSession := newRunningTestSessionWithBackend(t, func(cfg *Config) {
		cfg.EnableTraceParameters = true
	})
	defer closeSession()
	described := []uint32{pgtype.Int8OID, pgtype.BoolOID}
	exchange(t, client, postgres, described,
		&pgproto3.Parse{Query: parameterQuery + " AND $2"},
		&pgproto3.Describe{ObjectType: 'S'},
		&pgproto3.Sync{})
	exchange(t, client, postgres, described,
		&pgproto3.Bind{ParameterFormatCodes: []int16{1}, Parameters: [][]byte{int8Param(7), {1}}},
		&pgproto3.Execute{},
		&pgproto3.Sync{})
	closeSession()

	roots := rootTags(tracer)
	require.Len(t, roots, 2)
	assert.NotContains(t, roots[0], "db.parameters.0", "the describe cycle has no Bind")
	assert.Equal(t, "7", roots[1]["db.parameters.0"])
	assert.Equal(t, "true", roots[1]["db.parameters.1"])
}

func TestBindParametersKeepFirstBindOfRoot(t *testing.T) {
	tracer := startMockTracer(t)
	_, client, postgres, _, closeSession := newRunningTestSessionWithBackend(t, func(cfg *Config) {
		cfg.EnableTraceParameters = true
	})
	defer closeSession()
	exchange(t, client, postgres, nil,
		&pgproto3.Parse{Query: parameterQuery},
		&pgproto3.Bind{Parameters: [][]byte{[]byte("first")}},
		&pgproto3.Execute{},
		&pgproto3.Bind{Parameters: [][]byte{[]byte("second")}},
		&pgproto3.Execute{},
		&pgproto3.Sync{})
	closeSession()

	roots := rootTags(tracer)
	require.Len(t, roots, 1)
	assert.Equal(t, "first", roots[0]["db.parameters.0"])
}

func TestBindParametersOnlyOnSampledRoots(t *testing.T) {
	for _, tc := range parameterCaptureCases {
		t.Run(tc.name, func(t *testing.T) {
			tracer := startMockTracer(t)
			runParameterQuery(t, tc.sql, tc.enable)
			roots := rootTags(tracer)
			require.Len(t, roots, 1)
			if tc.wantParameters {
				assert.Equal(t, privateParameter, roots[0]["db.parameters.0"])
			} else {
				assert.NotContains(t, roots[0], "db.parameters.0")
			}
		})
	}
}

func TestDescribeTrackingIsBounded(t *testing.T) {
	s := New(Config{EnableTraceParameters: true})
	for range maxPendingDescribes + 10 {
		s.observeClientMessage(&pgproto3.Describe{ObjectType: 'S'}, false)
	}
	assert.Len(t, s.paramTypes.describing, maxPendingDescribes)
	s.observeBackendMessage(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	assert.Empty(t, s.paramTypes.describing, "unanswered describes are dropped at the end of the cycle")

	for i := range maxDescribedStmts + 1 {
		s.paramTypes.describing = append(s.paramTypes.describing, strconv.Itoa(i))
		s.observeBackendMessage(&pgproto3.ParameterDescription{ParameterOIDs: []uint32{pgtype.Int4OID}})
		s.observeBackendMessage(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	}
	assert.LessOrEqual(t, len(s.paramTypes.described), maxDescribedStmts)
	assert.Contains(t, s.paramTypes.described, strconv.Itoa(maxDescribedStmts), "the newest statement survives the reset")
}

func TestParameterDescriptionsPairWithDescribesInOrder(t *testing.T) {
	s := New(Config{EnableTraceParameters: true})
	s.paramTypes.describing = []string{"a", "b"}
	reused := []uint32{pgtype.Int4OID}
	s.observeBackendMessage(&pgproto3.ParameterDescription{ParameterOIDs: reused})
	reused[0] = pgtype.TextOID // pgproto3 reuses the slice for the next message
	s.observeBackendMessage(&pgproto3.ParameterDescription{ParameterOIDs: reused})
	s.observeBackendMessage(&pgproto3.ParameterDescription{ParameterOIDs: []uint32{pgtype.BoolOID}})
	assert.Equal(t, map[string][]uint32{"a": {pgtype.Int4OID}, "b": {pgtype.TextOID}}, s.paramTypes.described,
		"a reply with no Describe left is ignored")
}

func TestUnnamedStatementTypesAreForgottenWhenReplaced(t *testing.T) {
	s := New(Config{EnableTraceParameters: true})
	s.observeClientMessage(&pgproto3.Parse{ParameterOIDs: []uint32{pgtype.Int4OID}}, false)
	s.observeClientMessage(&pgproto3.Describe{ObjectType: 'S'}, false)
	s.observeBackendMessage(&pgproto3.ParameterDescription{ParameterOIDs: []uint32{pgtype.Int4OID}})
	require.Contains(t, s.paramTypes.described, "")

	s.observeClientMessage(&pgproto3.Parse{}, false)
	assert.NotContains(t, s.paramTypes.described, "")
	assert.Empty(t, s.paramTypes.unnamed)

	s.observeClientMessage(&pgproto3.Parse{ParameterOIDs: []uint32{pgtype.Int4OID}}, false)
	s.observeClientMessage(&pgproto3.Query{String: "SELECT 1"}, false)
	assert.Empty(t, s.paramTypes.unnamed, "a simple query destroys the unnamed statement")
}
