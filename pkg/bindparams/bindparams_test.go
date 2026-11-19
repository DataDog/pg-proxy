// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package bindparams

import (
	"encoding/binary"
	"math"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func render(t *testing.T, b *pgproto3.Bind, parseOIDs, describedOIDs []uint32) map[string]any {
	t.Helper()
	var c Capture
	c.Record(b, parseOIDs, describedOIDs)
	tags := map[string]any{}
	c.Render(func(key string, value any) {
		_, dup := tags[key]
		require.False(t, dup, "tag %s set twice", key)
		if s, ok := value.(string); ok {
			require.True(t, utf8.ValidString(s), "tag %s is not valid UTF-8: %q", key, s)
		}
		tags[key] = value
	})
	require.False(t, c.Recorded(), "Render must reset the capture")
	// Every value has a type tag at the same index. Tests of values ignore them;
	// TestRenderTypeNames checks them.
	types := 0
	for key := range tags {
		if i, ok := strings.CutPrefix(key, TypeTagPrefix); ok {
			require.Contains(t, tags, TagPrefix+i)
			delete(tags, key)
			types++
		}
	}
	for key := range tags {
		if strings.HasPrefix(key, TagPrefix) {
			types--
		}
	}
	require.Zero(t, types, "one type tag per value tag")
	return tags
}

// encode encodes value in PostgreSQL's binary format with pgtype, the codec
// pgx clients use, so the decoders are checked against real client bytes.
func encode(t *testing.T, oid uint32, value any) []byte {
	t.Helper()
	buf, err := pgtype.NewMap().Encode(oid, pgtype.BinaryFormatCode, value, nil)
	require.NoError(t, err)
	return buf
}

func binaryBind(values ...[]byte) *pgproto3.Bind {
	return &pgproto3.Bind{ParameterFormatCodes: []int16{1}, Parameters: values}
}

func TestRenderBinaryTypes(t *testing.T) {
	ts := time.Date(2026, 11, 18, 13, 11, 3, 823_000_000, time.UTC)
	uuid := [16]byte{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00}
	cases := []struct {
		name  string
		oid   uint32
		value any
		want  string
	}{
		{"bool true", pgtype.BoolOID, true, "true"},
		{"bool false", pgtype.BoolOID, false, "false"},
		{"int2", pgtype.Int2OID, int16(-32768), "-32768"},
		{"int4", pgtype.Int4OID, int32(2147483647), "2147483647"},
		{"int8", pgtype.Int8OID, int64(-9223372036854775808), "-9223372036854775808"},
		{"oid", pgtype.OIDOID, uint32(4294967295), "4294967295"},
		{"float4", pgtype.Float4OID, float32(1.5), "1.5"},
		{"float8", pgtype.Float8OID, 0.1, "0.1"},
		{"float8 NaN", pgtype.Float8OID, math.NaN(), "NaN"},
		{"float8 -inf", pgtype.Float8OID, math.Inf(-1), "-Infinity"},
		{"uuid", pgtype.UUIDOID, uuid, "550e8400-e29b-41d4-a716-446655440000"},
		{"date", pgtype.DateOID, ts, "2026-11-18"},
		{"date before epoch", pgtype.DateOID, time.Date(1969, 7, 20, 0, 0, 0, 0, time.UTC), "1969-07-20"},
		{"date infinity", pgtype.DateOID, pgtype.Date{InfinityModifier: pgtype.Infinity, Valid: true}, "infinity"},
		{"timestamp", pgtype.TimestampOID, ts, "2026-11-18 13:11:03.823"},
		{"timestamptz", pgtype.TimestamptzOID, ts, "2026-11-18 13:11:03.823+00"},
		{"timestamp whole seconds", pgtype.TimestampOID, time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), "2026-01-02 03:04:05"},
		{"timestamp microseconds", pgtype.TimestampOID, time.Date(2026, 1, 2, 3, 4, 5, 1000, time.UTC), "2026-01-02 03:04:05.000001"},
		{"timestamp BC", pgtype.TimestampOID, time.Date(-43, 3, 15, 12, 0, 0, 0, time.UTC), "0044-03-15 12:00:00 BC"},
		{"date BC", pgtype.DateOID, time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), "0001-01-01 BC"},
		{"timestamptz -infinity", pgtype.TimestamptzOID, pgtype.Timestamptz{InfinityModifier: pgtype.NegativeInfinity, Valid: true}, "-infinity"},
		{"text", pgtype.TextOID, "a, b", "a, b"},
		{"varchar", pgtype.VarcharOID, "x", "x"},
		{"jsonb", pgtype.JSONBOID, map[string]any{"k": 1}, `{"k":1}`},
		{"bytea", pgtype.ByteaOID, []byte{0xde, 0xad}, `\xdead`},
		{"int4 array", pgtype.Int4ArrayOID, []int32{1, 2, 3}, "array[len=3]{1,2,3}"},
		{"empty array", pgtype.Int8ArrayOID, []int64{}, "array[len=0]{}"},
		{"array with null", pgtype.Int8ArrayOID, []*int64{nil, new(int64)}, "array[len=2]{null,0}"},
		{"text array quotes commas", pgtype.TextArrayOID, []string{"a,b", `q"\`}, `array[len=2]{"a,b","q\"\\"}`},
		{"uuid array", pgtype.UUIDArrayOID, [][16]byte{uuid}, "array[len=1]{550e8400-e29b-41d4-a716-446655440000}"},
		{"2-D array", pgtype.Int2ArrayOID, [][]int16{{1, 2}, {3, 4}}, "array[len=4]{1,2,3,4}"},
		{"long array", pgtype.Int8ArrayOID, make([]int64, 5000), "array[len=5000]{0,0,0,0,0,0,0,0,0,0,…}"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tags := render(t, binaryBind(encode(t, tc.oid, tc.value)), []uint32{tc.oid}, nil)
			assert.Equal(t, map[string]any{"db.query.parameter.0": tc.want}, tags)
		})
	}
}

func TestRenderNumericMatchesPgtype(t *testing.T) {
	m := pgtype.NewMap()
	for _, text := range []string{
		"0", "1", "-1", "12345678", "1.5", "-0.0001", "0.000000012", "123456789012345678901234567890.123456789",
		strings.Repeat("9", 100),
		"100000000000000000000", "10000", "99990000", "3.14159", "NaN", "Infinity", "-Infinity",
	} {
		t.Run(text, func(t *testing.T) {
			var n pgtype.Numeric
			require.NoError(t, n.Scan(text))
			want, err := m.Encode(pgtype.NumericOID, pgtype.TextFormatCode, n, nil)
			require.NoError(t, err)
			tags := render(t, binaryBind(encode(t, pgtype.NumericOID, n)), []uint32{pgtype.NumericOID}, nil)
			assert.Equal(t, string(want), tags["db.query.parameter.0"])
		})
	}
}

// numeric builds a binary numeric from its fields, to reach values pgx would
// not send.
func numeric(weight int16, sign, dscale uint16, digits ...uint16) []byte {
	v := binary.BigEndian.AppendUint16(nil, uint16(len(digits)))
	v = binary.BigEndian.AppendUint16(v, uint16(weight))
	v = binary.BigEndian.AppendUint16(v, sign)
	v = binary.BigEndian.AppendUint16(v, dscale)
	for _, d := range digits {
		v = binary.BigEndian.AppendUint16(v, d)
	}
	return v
}

func TestRenderNumericScale(t *testing.T) {
	// 1.50 keeps its display scale; 0.00 has no digits at all.
	tags := render(t, binaryBind(numeric(0, numericPos, 2, 1, 5000), numeric(0, numericPos, 2)),
		[]uint32{pgtype.NumericOID, pgtype.NumericOID}, nil)
	assert.Equal(t, "1.50", tags["db.query.parameter.0"])
	assert.Equal(t, "0.00", tags["db.query.parameter.1"])
}

func TestRenderNumericHugeExponentIsBounded(t *testing.T) {
	cases := map[string][]byte{
		"weight":     numeric(math.MaxInt16, numericPos, 0, 1),
		"dscale":     numeric(0, numericPos, 0x3FFF, 1),
		"neg weight": numeric(math.MinInt16, numericPos, 0x3FFF, 1),
	}
	for name, v := range cases {
		t.Run(name, func(t *testing.T) {
			start := time.Now()
			got := render(t, binaryBind(v), []uint32{pgtype.NumericOID}, nil)["db.query.parameter.0"].(string)
			assert.Less(t, time.Since(start), 50*time.Millisecond)
			assert.LessOrEqual(t, len(got), ValueLimit+8)
			assert.True(t, strings.HasSuffix(got, "…"), got)
		})
	}
}

func TestRenderNull(t *testing.T) {
	b := &pgproto3.Bind{Parameters: [][]byte{nil, {}, []byte("x")}}
	tags := render(t, b, nil, nil)
	assert.Equal(t, map[string]any{"db.query.parameter.0": "null", "db.query.parameter.1": "", "db.query.parameter.2": "x"}, tags)
}

func TestRenderTextTruncation(t *testing.T) {
	long := strings.Repeat("a", 1000)
	// é is two bytes; the cut must not split it.
	split := strings.Repeat("a", ValueLimit-1) + "é" + "tail"
	tags := render(t, &pgproto3.Bind{Parameters: [][]byte{[]byte(long), []byte(split)}}, nil, nil)
	assert.Equal(t, strings.Repeat("a", ValueLimit)+"(+744 bytes)", tags["db.query.parameter.0"])
	assert.Equal(t, strings.Repeat("a", ValueLimit-1)+"(+6 bytes)", tags["db.query.parameter.1"])
}

func TestRenderTypeResolution(t *testing.T) {
	four := encode(t, pgtype.Int4OID, int32(7))
	cases := []struct {
		name                     string
		parseOIDs, describedOIDs []uint32
		want                     string
	}{
		{"parse", []uint32{pgtype.Int4OID}, []uint32{pgtype.Float4OID}, "7"},
		{"described when parse is unspecified", []uint32{0}, []uint32{pgtype.Int4OID}, "7"},
		// PostgreSQL resolves a declared unknown, so the description wins.
		{"described over declared unknown", []uint32{pgtype.UnknownOID}, []uint32{pgtype.Int4OID}, "7"},
		{"parse when not described", []uint32{pgtype.Int4OID}, []uint32{0}, "7"},
		{"unknown", nil, nil, `\x00000007`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tags := render(t, binaryBind(four), tc.parseOIDs, tc.describedOIDs)
			assert.Equal(t, tc.want, tags["db.query.parameter.0"])
		})
	}
}

func TestRenderTypeNames(t *testing.T) {
	oids := []uint32{pgtype.Int8OID, pgtype.TimestamptzOID, pgtype.Int4ArrayOID, 0, 999_999}
	var c Capture
	c.Record(&pgproto3.Bind{Parameters: make([][]byte, len(oids))}, oids, nil)
	types := map[string]any{}
	c.Render(func(key string, value any) {
		if strings.HasPrefix(key, TypeTagPrefix) {
			types[key] = value
		}
	})
	assert.Equal(t, map[string]any{
		TypeTagPrefix + "0": "int8",
		TypeTagPrefix + "1": "timestamptz",
		TypeTagPrefix + "2": "int4[]",
		TypeTagPrefix + "3": "unknown",
		TypeTagPrefix + "4": "unknown",
	}, types)
}

func TestRenderFormatCodes(t *testing.T) {
	four := encode(t, pgtype.Int4OID, int32(7))
	oids := []uint32{pgtype.Int4OID, pgtype.Int4OID, pgtype.Int4OID}
	b := &pgproto3.Bind{ParameterFormatCodes: []int16{0, 1, 2}, Parameters: [][]byte{[]byte("7"), four, four}}
	assert.Equal(t, map[string]any{"db.query.parameter.0": "7", "db.query.parameter.1": "7", "db.query.parameter.2": `\x00000007`},
		render(t, b, oids, nil), "a format other than text or binary is shown as hex")
}

func TestRenderHexFallback(t *testing.T) {
	long := make([]byte, 300)
	tags := render(t, binaryBind([]byte{1, 2}, long), nil, nil)
	assert.Equal(t, `\x0102`, tags["db.query.parameter.0"])
	assert.Equal(t, `\x`+strings.Repeat("00", hexLimit)+"(+172 bytes)", tags["db.query.parameter.1"])
}

func TestRenderMalformedBinaryFallsBackToHex(t *testing.T) {
	arrayHeader := func(ndim, flags int32, elem uint32, dims ...int32) []byte {
		v := binary.BigEndian.AppendUint32(nil, uint32(ndim))
		v = binary.BigEndian.AppendUint32(v, uint32(flags))
		v = binary.BigEndian.AppendUint32(v, elem)
		for _, d := range dims {
			v = binary.BigEndian.AppendUint32(v, uint32(d))
			v = binary.BigEndian.AppendUint32(v, 1)
		}
		return v
	}
	cases := []struct {
		name string
		oid  uint32
		v    []byte
	}{
		{"short int4", pgtype.Int4OID, []byte{1, 2, 3}},
		{"long int8", pgtype.Int8OID, make([]byte, 9)},
		{"bool 2", pgtype.BoolOID, []byte{2}},
		{"short uuid", pgtype.UUIDOID, make([]byte, 15)},
		{"timestamp overflow", pgtype.TimestampOID, binary.BigEndian.AppendUint64(nil, math.MaxInt64-1)},
		{"numeric short", pgtype.NumericOID, []byte{0, 1}},
		{"numeric digit count", pgtype.NumericOID, numeric(0, numericPos, 0, 1)[:9]},
		{"numeric digit range", pgtype.NumericOID, numeric(0, numericPos, 0, 10000)},
		{"numeric sign", pgtype.NumericOID, numeric(0, 0x1234, 0, 1)},
		{"jsonb version", pgtype.JSONBOID, []byte{2, '{', '}'}},
		{"array short header", pgtype.Int4ArrayOID, []byte{0, 0, 0, 1}},
		{"array 7 dims", pgtype.Int4ArrayOID, arrayHeader(7, 0, pgtype.Int4OID, 1, 1, 1, 1, 1, 1, 1)},
		{"array negative dims", pgtype.Int4ArrayOID, arrayHeader(-1, 0, pgtype.Int4OID)},
		{"array flags", pgtype.Int4ArrayOID, arrayHeader(1, 2, pgtype.Int4OID, 0)},
		{"array element type", pgtype.Int4ArrayOID, arrayHeader(1, 0, pgtype.Int8OID, 0)},
		{"array negative length", pgtype.Int4ArrayOID, arrayHeader(1, 0, pgtype.Int4OID, -1)},
		{"array huge count", pgtype.Int4ArrayOID, arrayHeader(2, 0, pgtype.Int4OID, math.MaxInt32, 2)},
		{"array missing dims", pgtype.Int4ArrayOID, arrayHeader(2, 0, pgtype.Int4OID, 1)},
		{"array missing element", pgtype.Int4ArrayOID, arrayHeader(1, 0, pgtype.Int4OID, 1)},
		{"array element overrun", pgtype.Int4ArrayOID, binary.BigEndian.AppendUint32(arrayHeader(1, 0, pgtype.Int4OID, 1), 4)},
		{"array bad element", pgtype.Int4ArrayOID, append(binary.BigEndian.AppendUint32(arrayHeader(1, 0, pgtype.Int4OID, 1), 2), 0, 0)},
		{"array element length", pgtype.Int4ArrayOID, binary.BigEndian.AppendUint32(arrayHeader(1, 0, pgtype.Int4OID, 1), uint32(0xFFFFFFF0))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tags := render(t, binaryBind(tc.v), []uint32{tc.oid}, nil)
			got := tags["db.query.parameter.0"].(string)
			assert.True(t, strings.HasPrefix(got, `\x`), "want a hex fallback, got %q", got)
		})
	}
}

// FuzzRender checks that no input panics or yields invalid UTF-8, whatever the
// declared type.
func FuzzRender(f *testing.F) {
	for _, oid := range []uint32{pgtype.Int4ArrayOID, pgtype.NumericOID, pgtype.TextArrayOID, pgtype.TimestampOID} {
		f.Add(oid, []byte{0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 23, 0, 0, 0, 2, 0, 0, 0, 1}, true)
	}
	f.Add(uint32(pgtype.TextOID), []byte("\xff\xfe text"), false)
	f.Fuzz(func(t *testing.T, oid uint32, v []byte, bin bool) {
		format := int16(0)
		if bin {
			format = 1
		}
		var c Capture
		c.Record(&pgproto3.Bind{ParameterFormatCodes: []int16{format}, Parameters: [][]byte{v}}, []uint32{oid}, nil)
		c.Render(func(key string, value any) {
			if s, ok := value.(string); ok && !utf8.ValidString(s) {
				t.Fatalf("%s is not valid UTF-8: %q", key, s)
			}
		})
	})
}

func TestRenderInvalidUTF8(t *testing.T) {
	b := &pgproto3.Bind{Parameters: [][]byte{[]byte("ok\xffend")}}
	assert.Equal(t, `ok\xffend`, render(t, b, nil, nil)["db.query.parameter.0"])

	quoted := encode(t, pgtype.TextArrayOID, []string{"a\xffb"})
	assert.Equal(t, `array[len=1]{"a\xffb"}`, render(t, binaryBind(quoted), []uint32{pgtype.TextArrayOID}, nil)["db.query.parameter.0"])
}

func TestRenderTotalCap(t *testing.T) {
	values := make([][]byte, 40)
	for i := range values {
		values[i] = []byte(strings.Repeat("x", 200))
	}
	tags := render(t, &pgproto3.Bind{Parameters: values}, nil, nil)
	assert.Equal(t, true, tags[TruncatedTag])
	total := 0
	positions := 0
	for key, value := range tags {
		if strings.HasPrefix(key, TagPrefix) {
			total += len(value.(string))
			positions++
		}
	}
	assert.LessOrEqual(t, total, TotalLimit)
	assert.Equal(t, TotalLimit/200, positions, "positions are kept in order until the cap")
	assert.Equal(t, positions, tags[RenderedTag])
	assert.Contains(t, tags, "db.query.parameter.19")
	assert.NotContains(t, tags, "db.query.parameter.20")
}

func TestRenderPositionCap(t *testing.T) {
	values := make([][]byte, MaxParams+10)
	tags := render(t, &pgproto3.Bind{Parameters: values}, nil, nil)
	assert.Len(t, tags, MaxParams+2)
	assert.Equal(t, true, tags[TruncatedTag])
	assert.Equal(t, MaxParams, tags[RenderedTag])
}

func TestRecordBoundsTheCopy(t *testing.T) {
	big := make([]byte, 1<<20)
	values := [][]byte{big, big, big, big, big, big, big, big, big, big}
	var c Capture
	c.Record(&pgproto3.Bind{Parameters: values}, nil, nil)
	assert.LessOrEqual(t, len(c.data), copyTotalLimit)
	tags := map[string]any{}
	c.Render(func(key string, value any) { tags[key] = value })
	assert.Equal(t, strings.Repeat("\x00", ValueLimit)+"(+1048320 bytes)", tags["db.query.parameter.0"])
	assert.LessOrEqual(t, cap(c.data), retainLimit, "large buffers are not kept")
}

func TestRecordCopiesValues(t *testing.T) {
	// pgproto3 reuses its read buffer, so Record must not keep the slices.
	buf := []byte("before")
	var c Capture
	c.Record(&pgproto3.Bind{Parameters: [][]byte{buf}}, nil, nil)
	copy(buf, "after!")
	tags := map[string]any{}
	c.Render(func(key string, value any) { tags[key] = value })
	assert.Equal(t, "before", tags[TagPrefix+"0"])
}

func TestRenderWithoutRecordDoesNothing(t *testing.T) {
	var c Capture
	c.Render(func(string, any) { t.Fatal("no tags expected") })
}

func TestRenderTruncatedArrayCopy(t *testing.T) {
	// An array longer than the copy keeps its count and shows the copied elements.
	words := make([]string, 50)
	for i := range words {
		words[i] = strings.Repeat("w", 40)
	}
	got := render(t, binaryBind(encode(t, pgtype.TextArrayOID, words)), []uint32{pgtype.TextArrayOID}, nil)["db.query.parameter.0"].(string)
	assert.True(t, strings.HasPrefix(got, `array[len=50]{"wwww`), got)
	assert.True(t, strings.HasSuffix(got, "…}"), got)
}

// arrayOf builds a one-dimensional binary array of elem from raw element values.
func arrayOf(elem uint32, values ...[]byte) []byte {
	v := binary.BigEndian.AppendUint32(nil, 1)
	v = binary.BigEndian.AppendUint32(v, 0)
	v = binary.BigEndian.AppendUint32(v, elem)
	v = binary.BigEndian.AppendUint32(v, uint32(len(values)))
	v = binary.BigEndian.AppendUint32(v, 1)
	for _, value := range values {
		v = binary.BigEndian.AppendUint32(v, uint32(len(value)))
		v = append(v, value...)
	}
	return v
}

func TestRenderPartlyCopiedTextElementIsMarked(t *testing.T) {
	// The copy ends partway into the second element, fewer bytes in than
	// elementLimit, so only the cut mark tells it apart from a complete value.
	v := arrayOf(pgtype.TextOID, []byte(strings.Repeat("a", 430)), []byte(strings.Repeat("b", 2000)))
	copied := copyLimit - 20 - (4 + 430) - 4 // header, first element, second length
	require.Less(t, copied, elementLimit)
	got := render(t, binaryBind(v), []uint32{pgtype.TextArrayOID}, nil)["db.query.parameter.0"]
	assert.Equal(t, `array[len=2]{"`+strings.Repeat("a", elementLimit)+`"…,"`+strings.Repeat("b", copied)+`"…}`, got)
}

func TestRenderPartlyCopiedFirstElementKeepsBrace(t *testing.T) {
	v := arrayOf(pgtype.NumericOID, make([]byte, 600))
	got := render(t, binaryBind(v), []uint32{pgtype.NumericArrayOID}, nil)["db.query.parameter.0"]
	assert.Equal(t, "array[len=1]{…}", got)
}

func TestRecordDropsPositionsPastTheCopyBudget(t *testing.T) {
	values := make([][]byte, 40)
	for i := range values {
		values[i] = make([]byte, 500)
	}
	var c Capture
	c.Record(&pgproto3.Bind{Parameters: values}, nil, nil)
	require.Len(t, c.params, 33, "32 whole values, then one cut short")
	assert.Equal(t, uint32(copyTotalLimit-32*500), c.params[32].n)
	assert.True(t, c.truncated)

	c.Record(&pgproto3.Bind{Parameters: values[:32]}, nil, nil)
	assert.False(t, c.truncated, "shortening values alone drops no position")
}
