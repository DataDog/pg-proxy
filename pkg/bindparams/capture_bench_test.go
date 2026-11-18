// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package bindparams

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

type benchCase struct {
	name string
	bind *pgproto3.Bind
	oids []uint32
}

// benchCases are the Bind shapes that matter for cost: the common few scalars,
// pgx's binary formats, and the outliers (huge arrays, huge values, many
// positions) that the limits exist for.
func benchCases(b *testing.B) []benchCase {
	m := pgtype.NewMap()
	enc := func(oid uint32, v any) []byte {
		buf, err := m.Encode(oid, pgtype.BinaryFormatCode, v, nil)
		if err != nil {
			b.Fatal(err)
		}
		return buf
	}
	repeat := func(n int, oid uint32) []uint32 {
		oids := make([]uint32, n)
		for i := range oids {
			oids[i] = oid
		}
		return oids
	}
	words := make([]string, 5000)
	for i := range words {
		words[i] = fmt.Sprintf("item-%d", i)
	}
	var numeric pgtype.Numeric
	if err := numeric.Scan("12345.6789"); err != nil {
		b.Fatal(err)
	}
	many := make([][]byte, 1000)
	for i := range many {
		many[i] = enc(pgtype.Int4OID, int32(i))
	}
	binary := []int16{1}
	return []benchCase{
		{"4_int8_binary", &pgproto3.Bind{ParameterFormatCodes: binary, Parameters: [][]byte{
			enc(pgtype.Int8OID, int64(1)), enc(pgtype.Int8OID, int64(42)), enc(pgtype.Int8OID, int64(1<<40)), enc(pgtype.Int8OID, int64(-7)),
		}}, repeat(4, pgtype.Int8OID)},
		{"4_short_text", &pgproto3.Bind{Parameters: [][]byte{
			[]byte("42"), []byte("alice@example.com"), []byte("2026-11-18"), []byte("true"),
		}}, nil},
		{"uuid_timestamptz_numeric_binary", &pgproto3.Bind{ParameterFormatCodes: binary, Parameters: [][]byte{
			enc(pgtype.UUIDOID, [16]byte{1, 2, 3}), enc(pgtype.TimestamptzOID, time.Date(2026, 11, 18, 13, 0, 0, 0, time.UTC)), enc(pgtype.NumericOID, numeric),
		}}, []uint32{pgtype.UUIDOID, pgtype.TimestamptzOID, pgtype.NumericOID}},
		{"int8_array_5000_binary", &pgproto3.Bind{ParameterFormatCodes: binary, Parameters: [][]byte{
			enc(pgtype.Int8ArrayOID, make([]int64, 5000)),
		}}, []uint32{pgtype.Int8ArrayOID}},
		{"text_array_5000_binary", &pgproto3.Bind{ParameterFormatCodes: binary, Parameters: [][]byte{
			enc(pgtype.TextArrayOID, words),
		}}, []uint32{pgtype.TextArrayOID}},
		{"text_array_5000_text", &pgproto3.Bind{Parameters: [][]byte{
			[]byte("{" + strings.Join(words, ",") + "}"),
		}}, nil},
		{"text_1MiB", &pgproto3.Bind{Parameters: [][]byte{[]byte(strings.Repeat("x", 1<<20))}}, nil},
		{"1000_int4_binary", &pgproto3.Bind{ParameterFormatCodes: binary, Parameters: many}, repeat(1000, pgtype.Int4OID)},
	}
}

// BenchmarkRecord measures the copy on the Bind path of a sampled trace, with
// the buffers a session reuses (steady) and on a session's first capture (cold).
func BenchmarkRecord(b *testing.B) {
	for _, bc := range benchCases(b) {
		b.Run(bc.name+"/steady", func(b *testing.B) {
			b.ReportAllocs()
			var c Capture
			for b.Loop() {
				c.Record(bc.bind, bc.oids, nil)
			}
		})
		b.Run(bc.name+"/cold", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				var c Capture
				c.Record(bc.bind, bc.oids, nil)
			}
		})
	}
}
