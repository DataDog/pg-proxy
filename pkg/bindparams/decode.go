// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package bindparams

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"math"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgtype"
)

// The decoders below follow PostgreSQL's binary send formats, as pgtype's codecs
// do, but append straight into the output buffer: pgtype's DecodeValue boxes
// each value in an interface, and its numeric scan does big.Int work bounded
// only by the untrusted dscale and weight fields.

const (
	numericPos    = 0x0000
	numericNeg    = 0x4000
	numericNaN    = 0xC000
	numericPosInf = 0xD000
	numericNegInf = 0xF000
)

// pgEpochMicros is 2000-01-01 00:00:00 UTC, the PostgreSQL epoch, in Unix microseconds.
const pgEpochMicros = 946_684_800_000_000

// appendBinary appends the decoded binary value v of a type, where v is a
// prefix of the size bytes sent. It reports false, leaving the caller to fall
// back to hex, when the type is unknown or v is malformed.
func appendBinary(dst []byte, oid uint32, v []byte, size int) ([]byte, bool) {
	// A value of unknown type, sent by clients that declare no types and
	// describe nothing, can only be shown as hex.
	if oid == 0 {
		return dst, false
	}
	// Fixed-width scalars, the common case, skip the array type lookup.
	if fixedWidth(oid) == 0 {
		if elem, ok := arrayElementOID(oid); ok {
			return appendArray(dst, elem, v, size)
		}
	}
	if oid == pgtype.JSONBOID {
		text, ok := jsonbText(v)
		if !ok {
			return dst, false
		}
		v, size, oid = text, size-1, pgtype.TextOID
	}
	// Text and arrays have meaningful prefixes; other scalars are shown whole
	// or not at all.
	if isTextType(oid) {
		return appendText(dst, v, size, ValueLimit), true
	}
	if len(v) != size {
		return dst, false
	}
	return appendScalar(dst, oid, v, ValueLimit)
}

// jsonbText returns the JSON text of a binary jsonb value: a version byte, then
// the text.
func jsonbText(v []byte) ([]byte, bool) {
	if len(v) == 0 || v[0] != 1 {
		return nil, false
	}
	return v[1:], true
}

// appendScalar appends a complete binary array element. Text elements are
// quoted, so that commas inside them cannot be read as separators.
func appendScalar(dst []byte, oid uint32, v []byte, limit int) ([]byte, bool) {
	if w := fixedWidth(oid); w > 0 && len(v) != w {
		return dst, false
	}
	switch oid {
	case pgtype.BoolOID:
		if v[0] > 1 {
			return dst, false
		}
		// PostgreSQL writes booleans as t and f.
		if v[0] == 1 {
			return append(dst, 't'), true
		}
		return append(dst, 'f'), true
	case pgtype.Int2OID:
		return strconv.AppendInt(dst, int64(int16(binary.BigEndian.Uint16(v))), 10), true
	case pgtype.Int4OID:
		return strconv.AppendInt(dst, int64(int32(binary.BigEndian.Uint32(v))), 10), true
	case pgtype.Int8OID:
		return strconv.AppendInt(dst, int64(binary.BigEndian.Uint64(v)), 10), true
	case pgtype.OIDOID:
		return strconv.AppendUint(dst, uint64(binary.BigEndian.Uint32(v)), 10), true
	case pgtype.Float4OID:
		return appendFloat(dst, float64(math.Float32frombits(binary.BigEndian.Uint32(v))), 32), true
	case pgtype.Float8OID:
		return appendFloat(dst, math.Float64frombits(binary.BigEndian.Uint64(v)), 64), true
	case pgtype.UUIDOID:
		return appendUUID(dst, v), true
	case pgtype.DateOID:
		return appendDate(dst, int32(binary.BigEndian.Uint32(v))), true
	case pgtype.TimestampOID, pgtype.TimestamptzOID:
		return appendTimestamp(dst, int64(binary.BigEndian.Uint64(v)), oid == pgtype.TimestamptzOID)
	case pgtype.NumericOID:
		return appendNumeric(dst, v, limit)
	case pgtype.JSONBOID:
		var ok bool
		if v, ok = jsonbText(v); !ok {
			return dst, false
		}
	default:
		if !isTextType(oid) {
			return dst, false
		}
	}
	return appendQuoted(dst, v, limit, false), true
}

func isTextType(oid uint32) bool {
	switch oid {
	case pgtype.TextOID, pgtype.VarcharOID, pgtype.BPCharOID, pgtype.NameOID,
		pgtype.JSONOID, pgtype.XMLOID, pgtype.UnknownOID:
		return true
	}
	return false
}

// fixedWidth returns the size of a fixed-width type's binary value, or 0.
func fixedWidth(oid uint32) int {
	switch oid {
	case pgtype.BoolOID:
		return 1
	case pgtype.Int2OID:
		return 2
	case pgtype.Int4OID, pgtype.OIDOID, pgtype.Float4OID, pgtype.DateOID:
		return 4
	case pgtype.Int8OID, pgtype.Float8OID, pgtype.TimestampOID, pgtype.TimestamptzOID:
		return 8
	case pgtype.UUIDOID:
		return 16
	}
	return 0
}

// pgTypes maps array types to their element types. Lookups only read it, so
// every session shares it.
var pgTypes = pgtype.NewMap()

func arrayElementOID(oid uint32) (uint32, bool) {
	if t, ok := pgTypes.TypeForOID(oid); ok {
		if array, ok := t.Codec.(*pgtype.ArrayCodec); ok {
			return array.ElementType.OID, true
		}
	}
	return 0, false
}

// appendArray appends array[len=N]{…} for a binary array whose prefix v was
// copied. The header is trusted only if it is consistent: flags 0 or 1, the
// expected element type, and dimension lengths that are present, non-negative
// and multiply to an array PostgreSQL could store. The dimension count is not
// capped at PostgreSQL's MAXDIM, so a future larger limit still decodes.
func appendArray(dst []byte, elem uint32, v []byte, size int) ([]byte, bool) {
	if len(v) < 12 {
		return dst, false
	}
	ndim := int32(binary.BigEndian.Uint32(v))
	flags := int32(binary.BigEndian.Uint32(v[4:]))
	if ndim < 0 || flags&^1 != 0 || binary.BigEndian.Uint32(v[8:]) != elem {
		return dst, false
	}
	rp := 12
	if len(v) < rp+8*int(ndim) {
		return dst, false
	}
	count := int64(0)
	if ndim > 0 {
		count = 1
	}
	for range ndim {
		n := int32(binary.BigEndian.Uint32(v[rp:]))
		if n < 0 {
			return dst, false
		}
		// PostgreSQL caps arrays at MaxAllocSize; anything larger is not a real array.
		if count *= int64(n); count > math.MaxInt32 {
			return dst, false
		}
		rp += 8
	}
	complete := len(v) == size

	dst = append(dst, "array[len="...)
	dst = strconv.AppendInt(dst, count, 10)
	dst = append(dst, "]{"...)
	start := len(dst)
	shown := int64(0)
	for ; shown < count && shown < arrayElementLimit && len(dst)-start < ValueLimit; shown++ {
		if len(v)-rp < 4 {
			if complete {
				return dst, false
			}
			break // the rest was not copied
		}
		n := int32(binary.BigEndian.Uint32(v[rp:]))
		rp += 4
		if shown > 0 {
			dst = append(dst, ',')
		}
		if n == -1 {
			dst = append(dst, "null"...)
			continue
		}
		if n < 0 {
			return dst, false
		}
		if len(v)-rp < int(n) {
			if complete {
				return dst, false
			}
			// A partly copied element: show text, marked as cut; drop
			// anything else, and the comma before it.
			if isTextType(elem) {
				dst = appendQuoted(dst, v[rp:], elementLimit, true)
				shown++
			} else if shown > 0 {
				dst = dst[:len(dst)-1]
			}
			break
		}
		var ok bool
		if dst, ok = appendScalar(dst, elem, v[rp:rp+int(n)], elementLimit); !ok {
			return dst, false
		}
		rp += int(n)
	}
	if shown < count {
		if shown > 0 {
			dst = append(dst, ',')
		}
		dst = append(dst, "…"...)
	}
	return append(dst, '}'), true
}

// appendNumeric appends the text form of a binary numeric, as PostgreSQL's
// get_str_from_var writes it. The digit loops stop at limit bytes of output, so
// the untrusted weight and dscale cannot make it do unbounded work.
func appendNumeric(dst []byte, v []byte, limit int) ([]byte, bool) {
	if len(v) < 8 {
		return dst, false
	}
	ndigits := int(binary.BigEndian.Uint16(v))
	weight := int(int16(binary.BigEndian.Uint16(v[2:])))
	sign := binary.BigEndian.Uint16(v[4:])
	dscale := int(binary.BigEndian.Uint16(v[6:]))
	if len(v) != 8+2*ndigits || dscale&^0x3FFF != 0 {
		return dst, false
	}
	switch sign {
	case numericNaN:
		return append(dst, "NaN"...), true
	case numericPosInf:
		return append(dst, "Infinity"...), true
	case numericNegInf:
		return append(dst, "-Infinity"...), true
	case numericNeg:
		dst = append(dst, '-')
	case numericPos:
	default:
		return dst, false
	}
	digits := v[8:]
	digit := func(i int) (int, bool) {
		if i < 0 || i >= ndigits {
			return 0, true
		}
		d := int(binary.BigEndian.Uint16(digits[2*i:]))
		return d, d <= 9999
	}

	start := len(dst)
	if weight < 0 {
		dst = append(dst, '0')
	}
	for i := 0; i <= weight; i++ {
		d, ok := digit(i)
		if !ok {
			return dst, false
		}
		if i == 0 {
			dst = strconv.AppendInt(dst, int64(d), 10)
		} else {
			dst = appendDigits4(dst, d)
		}
		if len(dst)-start > limit {
			return append(dst, "…"...), true
		}
	}
	if dscale > 0 {
		dst = append(dst, '.')
		for i, written := weight+1, 0; written < dscale; i++ {
			d, ok := digit(i)
			if !ok {
				return dst, false
			}
			var buf [4]byte
			group := appendDigits4(buf[:0], d)
			n := min(4, dscale-written)
			dst = append(dst, group[:n]...)
			written += n
			if len(dst)-start > limit {
				return append(dst, "…"...), true
			}
		}
	}
	return dst, true
}

func appendDigits4(dst []byte, d int) []byte {
	return append(dst, byte('0'+d/1000), byte('0'+d/100%10), byte('0'+d/10%10), byte('0'+d%10))
}

func appendFloat(dst []byte, f float64, bits int) []byte {
	switch {
	case math.IsInf(f, 1):
		return append(dst, "Infinity"...)
	case math.IsInf(f, -1):
		return append(dst, "-Infinity"...)
	case math.IsNaN(f):
		return append(dst, "NaN"...)
	}
	// PostgreSQL writes the shortest digits in fixed notation while the decimal
	// exponent is in [-4, 15) for float8, or [-4, 6) for float4, and in e
	// notation otherwise. strconv's 'g' switches at 6 for both.
	maxFixed := 15
	if bits == 32 {
		maxFixed = 6
	}
	var buf [32]byte
	e := strconv.AppendFloat(buf[:0], f, 'e', -1, bits)
	if exp := floatExponent(e); exp < -4 || exp >= maxFixed {
		return append(dst, e...)
	}
	return strconv.AppendFloat(dst, f, 'f', -1, bits)
}

// floatExponent returns the exponent of a number strconv wrote in 'e' format,
// such as -5 for "1.5e-05".
func floatExponent(e []byte) int {
	i := bytes.LastIndexByte(e, 'e')
	exp := 0
	for _, c := range e[i+2:] {
		exp = exp*10 + int(c-'0')
	}
	if e[i+1] == '-' {
		return -exp
	}
	return exp
}

func appendUUID(dst []byte, v []byte) []byte {
	dst = hex.AppendEncode(dst, v[0:4])
	dst = append(dst, '-')
	dst = hex.AppendEncode(dst, v[4:6])
	dst = append(dst, '-')
	dst = hex.AppendEncode(dst, v[6:8])
	dst = append(dst, '-')
	dst = hex.AppendEncode(dst, v[8:10])
	dst = append(dst, '-')
	return hex.AppendEncode(dst, v[10:16])
}

func appendDate(dst []byte, days int32) []byte {
	switch days {
	case math.MaxInt32:
		return append(dst, "infinity"...)
	case math.MinInt32:
		return append(dst, "-infinity"...)
	}
	t := time.Unix((int64(days)*86400)+pgEpochMicros/1_000_000, 0).UTC()
	dst, bc := appendYMD(dst, t)
	if bc {
		dst = append(dst, " BC"...)
	}
	return dst
}

func appendTimestamp(dst []byte, micros int64, tz bool) ([]byte, bool) {
	switch micros {
	case math.MaxInt64:
		return append(dst, "infinity"...), true
	case math.MinInt64:
		return append(dst, "-infinity"...), true
	}
	// Shifting to the Unix epoch must not overflow; PostgreSQL's own range is
	// far inside this.
	if micros > math.MaxInt64-pgEpochMicros {
		return dst, false
	}
	t := time.UnixMicro(micros + pgEpochMicros).UTC()
	dst, bc := appendYMD(dst, t)
	hour, minute, sec := t.Clock()
	dst = append(dst, ' ')
	dst = appendPadded(dst, hour, 2)
	dst = append(dst, ':')
	dst = appendPadded(dst, minute, 2)
	dst = append(dst, ':')
	dst = appendPadded(dst, sec, 2)
	if us := t.Nanosecond() / 1000; us > 0 {
		dst = append(dst, '.')
		width := 6
		for us%10 == 0 {
			us /= 10
			width--
		}
		dst = appendPadded(dst, us, width)
	}
	if tz {
		dst = append(dst, "+00"...)
	}
	if bc {
		dst = append(dst, " BC"...)
	}
	return dst, true
}

// appendYMD appends t's date as PostgreSQL writes it, reporting whether the
// year needs a BC suffix. Go's year 0 is 1 BC. Formatting by hand avoids
// time.Time.AppendFormat's layout parsing, which dominated the cost.
func appendYMD(dst []byte, t time.Time) ([]byte, bool) {
	year, month, day := t.Date()
	bc := year <= 0
	if bc {
		year = 1 - year
	}
	dst = appendPadded(dst, year, 4)
	dst = append(dst, '-')
	dst = appendPadded(dst, int(month), 2)
	dst = append(dst, '-')
	return appendPadded(dst, day, 2), bc
}

// appendPadded appends a non-negative v with leading zeros to width digits.
func appendPadded(dst []byte, v, width int) []byte {
	var buf [20]byte
	digits := strconv.AppendInt(buf[:0], int64(v), 10)
	for i := len(digits); i < width; i++ {
		dst = append(dst, '0')
	}
	return append(dst, digits...)
}

// appendText appends at most limit bytes of v, cut on a character boundary and
// with invalid UTF-8 escaped, then "(+N bytes)" for the size bytes not shown.
func appendText(dst []byte, v []byte, size, limit int) []byte {
	shown := cutText(v, limit)
	dst = appendEscaped(dst, shown)
	return appendRemainder(dst, size-len(shown))
}

// appendQuoted appends an array element as a double-quoted string, escaping
// quotes and backslashes as PostgreSQL array literals do. A "…" follows when
// the element is longer than limit or cut reports that v is only its prefix.
func appendQuoted(dst []byte, v []byte, limit int, cut bool) []byte {
	shown := cutText(v, limit)
	dst = append(dst, '"')
	for len(shown) > 0 {
		i := bytes.IndexAny(shown, `"\`)
		if i < 0 {
			dst = appendEscaped(dst, shown)
			break
		}
		dst = appendEscaped(dst, shown[:i])
		dst = append(dst, '\\', shown[i])
		shown = shown[i+1:]
	}
	dst = append(dst, '"')
	if cut || len(v) > limit {
		dst = append(dst, "…"...)
	}
	return dst
}

func cutText(v []byte, limit int) []byte {
	if len(v) <= limit {
		return v
	}
	// Back off to the start of a character so a valid multi-byte character is
	// not split into bytes that would then be escaped.
	cut := limit
	for i := 0; i < utf8.UTFMax-1 && cut > 0 && !utf8.RuneStart(v[cut]); i++ {
		cut--
	}
	if !utf8.RuneStart(v[cut]) {
		cut = limit // not valid UTF-8 here anyway
	}
	return v[:cut]
}

// appendHex appends \x and the hex of at most hexLimit bytes of v, then
// "(+N bytes)" for the size bytes not shown.
func appendHex(dst []byte, v []byte, size int) []byte {
	shown := v[:min(len(v), hexLimit)]
	dst = append(dst, `\x`...)
	dst = hex.AppendEncode(dst, shown)
	return appendRemainder(dst, size-len(shown))
}

func appendRemainder(dst []byte, rest int) []byte {
	if rest <= 0 {
		return dst
	}
	dst = append(dst, "(+"...)
	dst = strconv.AppendInt(dst, int64(rest), 10)
	return append(dst, " bytes)"...)
}

// appendEscaped appends v with each byte of invalid UTF-8 written as \xNN, so
// that tag values are valid UTF-8 for every tracer's encoder.
func appendEscaped(dst []byte, v []byte) []byte {
	if utf8.Valid(v) {
		return append(dst, v...)
	}
	for len(v) > 0 {
		r, n := utf8.DecodeRune(v)
		if r == utf8.RuneError && n == 1 {
			dst = append(dst, `\x`...)
			dst = hex.AppendEncode(dst, v[:1])
		} else {
			dst = append(dst, v[:n]...)
		}
		v = v[n:]
	}
	return dst
}
