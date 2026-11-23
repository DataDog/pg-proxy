// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package bindparams

import (
	"slices"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

const (
	// MaxParams is the number of positions rendered; later positions are dropped
	// and pgproxy.bind.truncated is set. Tracers store and encode tags one by
	// one, so each position adds cost; a full Bind adds at most 34 attributes,
	// within the OpenTelemetry SDK's default of 128 per span.
	MaxParams = 32
	// copyLimit bounds the bytes copied per binary value. It covers everything
	// Render can show: ValueLimit bytes of text, hexLimit bytes of hex, or an
	// array header and its first arrayElementLimit small elements. With
	// MaxParams positions, a Bind copies at most 16 KiB.
	copyLimit = 512
	// textCopyLimit bounds the bytes copied per text-format value: Render shows
	// ValueLimit bytes and reads one more to see where a character starts.
	textCopyLimit = ValueLimit + 1
	// retainLimit is the largest buffer a Capture keeps between traces, so idle
	// sessions do not each hold the largest Bind they once saw.
	retainLimit = 2 << 10
)

// param describes one captured Bind position. Its value is the next n bytes of
// Capture.data, a prefix of the size bytes the client sent.
type param struct {
	oid    uint32
	size   int32 // -1 for NULL
	n      uint32
	format int16
}

// Capture holds a copy of one Bind's parameters until the trace is rendered.
// The zero value is ready to use. A Capture is not safe for concurrent use.
type Capture struct {
	params   []param
	data     []byte
	out      []byte
	recorded bool
	sent     int // positions the client sent; Render reports any it does not emit
}

// Recorded reports whether Record has run since the last Render. A nil Capture
// has recorded nothing.
func (c *Capture) Recorded() bool {
	return c != nil && c.recorded
}

// Record copies b's parameters, replacing any earlier capture. A binary value's
// type is its OID in parseOIDs, else its OID in describedOIDs (the backend's
// ParameterDescription, which resolves what Parse left unspecified), else
// unknown. PostgreSQL treats a declared 0 or unknown as unspecified. Text
// values need no type.
func (c *Capture) Record(b *pgproto3.Bind, parseOIDs, describedOIDs []uint32) {
	c.reset()
	c.recorded = true
	values := b.Parameters
	c.sent = len(values)
	values = values[:min(len(values), MaxParams)]

	// Size the buffers once, so a capture costs at most one allocation each and
	// none once a session's buffers have grown.
	formats := b.ParameterFormatCodes
	need := 0
	for i, v := range values {
		need += min(len(v), takeLimit(format(i, formats)))
	}
	c.data = slices.Grow(c.data, need)
	if cap(c.params) < len(values) {
		c.params = make([]param, len(values))
	}
	c.params = c.params[:len(values)]

	for i, v := range values {
		p := &c.params[i]
		*p = param{oid: typeOID(i, parseOIDs, describedOIDs), size: -1, format: format(i, formats)}
		// pgproto3 leaves NULL values nil; an empty value is a non-nil empty slice.
		if v == nil {
			continue
		}
		take := min(len(v), takeLimit(p.format))
		p.size = int32(len(v))
		p.n = uint32(take)
		c.data = append(c.data, v[:take]...)
	}
}

// reset discards the capture, keeping small buffers for reuse. params never
// grows past MaxParams entries, so it is always kept.
func (c *Capture) reset() {
	if cap(c.data) > retainLimit {
		c.data = nil
	}
	if cap(c.out) > retainLimit {
		c.out = nil
	}
	c.params = c.params[:0]
	c.data = c.data[:0]
	c.recorded = false
	c.sent = 0
}

// NeedsTypes reports whether Record would use types for b: only binary values
// are decoded by type, so a Bind of text values needs none.
func NeedsTypes(b *pgproto3.Bind) bool {
	for i := range min(len(b.Parameters), MaxParams) {
		if format(i, b.ParameterFormatCodes) == 1 {
			return true
		}
	}
	return false
}

// format returns position i's format code: one code applies to every position,
// and no codes mean text.
func format(i int, formats []int16) int16 {
	switch {
	case len(formats) == 1:
		return formats[0]
	case i < len(formats):
		return formats[i]
	}
	return 0
}

func takeLimit(code int16) int {
	if code == 0 {
		return textCopyLimit
	}
	return copyLimit
}

func typeOID(i int, parseOIDs, describedOIDs []uint32) uint32 {
	if i < len(parseOIDs) && parseOIDs[i] != 0 && parseOIDs[i] != pgtype.UnknownOID {
		return parseOIDs[i]
	}
	if i < len(describedOIDs) {
		return describedOIDs[i]
	}
	return 0
}
