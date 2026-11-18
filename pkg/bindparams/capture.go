// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package bindparams

import "github.com/jackc/pgx/v5/pgproto3"

const (
	// MaxParams is the number of positions rendered; later positions are dropped
	// and db.parameters.truncated is set.
	MaxParams = 256
	// copyLimit bounds the bytes copied per value. It covers everything Render
	// can show: ValueLimit bytes of text, hexLimit bytes of hex, or an array
	// header and its first ArrayElementLimit small elements.
	copyLimit = 512
	// copyTotalLimit bounds the bytes copied per Bind, so a Bind with many large
	// values costs no more to copy than TotalLimit can show. A value it cuts
	// short renders its prefix and "(+N bytes)"; positions after it are dropped.
	copyTotalLimit = 4 * TotalLimit
	// retainLimit is the largest buffer a Capture keeps between traces, so idle
	// sessions do not each hold the largest Bind they once saw.
	retainLimit = 2 << 10
)

// param describes one captured Bind position. Its value is data[off : off+n],
// a prefix of the size bytes the client sent; end is where its rendering ends
// in Capture.out.
type param struct {
	oid         uint32
	size        int32 // -1 for NULL
	off, n, end uint32
	binary      bool
}

// Capture holds a copy of one Bind's parameters until the trace is rendered.
// The zero value is ready to use. A Capture is not safe for concurrent use.
type Capture struct {
	params    []param
	data      []byte
	out       []byte
	recorded  bool
	truncated bool // positions were dropped while copying
}

// Recorded reports whether Record has run since the last Reset.
func (c *Capture) Recorded() bool {
	return c.recorded
}

// Record copies b's parameters, replacing any earlier capture. A value's type is
// its non-zero OID in parseOIDs, else its OID in describedOIDs (the backend's
// ParameterDescription), else unknown.
func (c *Capture) Record(b *pgproto3.Bind, parseOIDs, describedOIDs []uint32) {
	c.Reset()
	c.recorded = true
	values := b.Parameters
	if len(values) > MaxParams {
		values = values[:MaxParams]
		c.truncated = true
	}

	// Size the buffers once and fill them by index, so a capture costs at most
	// one allocation each and none once a session's buffers have grown.
	need := 0
	for _, v := range values {
		need += min(len(v), copyLimit)
	}
	need = min(need, copyTotalLimit)
	if cap(c.data) < need {
		c.data = make([]byte, need)
	}
	data := c.data[:need]
	if cap(c.params) < len(values) {
		c.params = make([]param, len(values))
	}
	c.params = c.params[:len(values)]

	formats := b.ParameterFormatCodes
	allBinary := len(formats) == 1 && formats[0] == 1
	off := 0
	for i, v := range values {
		p := &c.params[i]
		*p = param{oid: typeOID(i, parseOIDs, describedOIDs), size: -1, off: uint32(off)}
		p.binary = allBinary || len(formats) > 1 && i < len(formats) && formats[i] == 1
		// pgproto3 leaves NULL values nil; an empty value is a non-nil empty slice.
		if v == nil {
			continue
		}
		rest := need - off
		if rest == 0 && len(v) > 0 {
			// The copy budget is spent: drop this and later positions, as the
			// render cap would, rather than tag them with no value.
			c.params = c.params[:i]
			c.truncated = true
			break
		}
		take := min(len(v), copyLimit, rest)
		p.size = int32(len(v))
		p.n = uint32(copy(data[off:], v[:take]))
		off += take
	}
	c.data = data[:off]
}

// Reset discards the capture, keeping small buffers for reuse.
func (c *Capture) Reset() {
	if cap(c.data) > retainLimit {
		c.data = nil
	}
	if cap(c.out) > TotalLimit+retainLimit {
		c.out = nil
	}
	if cap(c.params) > MaxParams/4 {
		c.params = nil
	}
	c.params = c.params[:0]
	c.data = c.data[:0]
	c.recorded = false
	c.truncated = false
}

func typeOID(i int, parseOIDs, describedOIDs []uint32) uint32 {
	if i < len(parseOIDs) && parseOIDs[i] != 0 {
		return parseOIDs[i]
	}
	if i < len(describedOIDs) {
		return describedOIDs[i]
	}
	return 0
}
