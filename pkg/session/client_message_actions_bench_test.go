// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package session

import (
	"encoding/binary"
	"testing"

	pgproxy "github.com/DataDog/pg-proxy"
	"github.com/DataDog/pg-proxy/pkg/query"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

// benchSpan is a span that does no work, so the benchmarks measure the proxy's
// cost rather than a tracer's.
type benchSpan struct{ sampled bool }

func (s benchSpan) StartChild(string) pgproxy.Span { return s }
func (benchSpan) SetTag(string, any)               {}
func (benchSpan) Finish(error)                     {}
func (s benchSpan) Sampled() bool                  { return s.sampled }

type benchTracer struct{ sampled bool }

func (t benchTracer) StartRoot(string, string) (pgproxy.Span, bool) {
	return benchSpan{t.sampled}, false
}
func (benchTracer) HasQueryContext(string) bool                       { return false }
func (benchTracer) WarnNamedPreparedStatement(string, string, string) {}

// BenchmarkDecideClientMessageActionBind measures one traced Bind cycle: the
// root trace starts, the Bind is decided, and the root finishes, which is where
// sampled values are rendered. Unsampled traces must cost the same as with
// capture disabled.
func BenchmarkDecideClientMessageActionBind(b *testing.B) {
	int8s := make([][]byte, 4)
	for i := range int8s {
		int8s[i] = binary.BigEndian.AppendUint64(nil, uint64(i))
	}
	array, err := pgtype.NewMap().Encode(pgtype.Int8ArrayOID, pgtype.BinaryFormatCode, make([]int64, 5000), nil)
	if err != nil {
		b.Fatal(err)
	}
	shapes := []struct {
		name string
		oids []uint32
		bind *pgproto3.Bind
	}{
		{"4_int8_binary", []uint32{pgtype.Int8OID, pgtype.Int8OID, pgtype.Int8OID, pgtype.Int8OID},
			&pgproto3.Bind{ParameterFormatCodes: []int16{1}, Parameters: int8s}},
		{"int8_array_5000_binary", []uint32{pgtype.Int8ArrayOID},
			&pgproto3.Bind{ParameterFormatCodes: []int16{1}, Parameters: [][]byte{array}}},
	}
	modes := []struct {
		name            string
		enable, sampled bool
	}{
		{"disabled", false, true},
		{"unsampled", true, false},
		{"sampled", true, true},
	}
	for _, shape := range shapes {
		for _, mode := range modes {
			b.Run(shape.name+"/"+mode.name, func(b *testing.B) {
				b.Cleanup(pgproxy.Bootstrap(pgproxy.Dependencies{Tracer: benchTracer{sampled: mode.sampled}}))
				traceState := &query.TraceState{}
				s := New(Config{EnableTraceParameters: mode.enable, TraceState: traceState})
				parse := &pgproto3.Parse{Query: "SELECT $1", ParameterOIDs: shape.oids}
				if _, err := s.DecideClientMessageAction(parse); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				for b.Loop() {
					traceState.StartRootTrace(parse)
					if _, err := s.DecideClientMessageAction(shape.bind); err != nil {
						b.Fatal(err)
					}
					traceState.FinishRootTrace()
				}
			})
		}
	}
}
