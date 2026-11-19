// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package bindparams

import (
	"slices"
	"strconv"
)

const (
	// TagPrefix starts each position's value tag: db.parameters.0, …
	// Only numbered keys live under it, so redaction can drop the subtree.
	// It is not OTel's db.query.parameter: Datadog's intake moves db.query.*
	// under db.statement, which replaces the SQL text.
	TagPrefix = "db.parameters."
	// TruncatedTag is set to true when a limit dropped positions.
	TruncatedTag = "pgproxy.bind.truncated"
	// RenderedTag is set with TruncatedTag to the number of positions emitted.
	RenderedTag = "pgproxy.bind.rendered"

	// ValueLimit is the number of bytes of a value shown before "(+N bytes)".
	ValueLimit = 256
	// arrayElementLimit is the number of array elements shown.
	arrayElementLimit = 10
	// TotalLimit bounds the rendered bytes across all positions of a Bind.
	TotalLimit = 4 << 10

	// hexLimit is the number of raw bytes shown in hex (two characters each).
	hexLimit = ValueLimit / 2
	// elementLimit is the number of bytes of a text array element shown.
	elementLimit = 64
)

var valueKeys = func() (keys [MaxParams]string) {
	for i := range keys {
		keys[i] = TagPrefix + strconv.Itoa(i)
	}
	return keys
}()

// Render passes a value tag per captured position to setTag, then resets the
// capture. It does nothing if nothing was recorded. The truncation tags come
// first: tracers that cap a span's attributes, such as the OpenTelemetry SDK,
// keep the earliest ones and drop the rest.
func (c *Capture) Render(setTag func(key string, value any)) {
	if !c.recorded {
		return
	}
	// Render every value into one buffer and convert it to a string once; each
	// tag value is a substring of it, so the values cost one allocation, plus
	// one per tag to pass it as an interface. Hex doubles the copied bytes, and
	// each position may add a "(+N bytes)" suffix.
	out := slices.Grow(c.out[:0], min(2*len(c.data)+16*len(c.params), TotalLimit))
	data := c.data
	var ends [MaxParams]int
	rendered := 0
	for i := range c.params {
		p := &c.params[i]
		start := len(out)
		out = appendParam(out, p, data[:p.n])
		data = data[p.n:]
		if len(out) > TotalLimit {
			out = out[:start]
			break
		}
		ends[i] = len(out)
		rendered++
	}
	c.out = out
	// Positions past MaxParams or TotalLimit were not emitted.
	if rendered < c.sent {
		setTag(TruncatedTag, true)
		setTag(RenderedTag, rendered)
	}
	values := string(out)
	start := 0
	for i := range rendered {
		setTag(valueKeys[i], values[start:ends[i]])
		start = ends[i]
	}
	c.reset()
}

func appendParam(dst []byte, p *param, v []byte) []byte {
	if p.size < 0 {
		return append(dst, "null"...)
	}
	size := int(p.size)
	switch p.format {
	case 0:
		return appendText(dst, v, size)
	case 1:
		if out, ok := appendBinary(dst, p.oid, v, size); ok {
			return out
		}
	}
	// Malformed or unknown binary values, and any future format. dst still
	// ends where it did, so this overwrites what a failed decode appended.
	return appendHex(dst, v, size)
}
