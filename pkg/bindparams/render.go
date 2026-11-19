// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package bindparams

import (
	"strconv"
	"strings"
)

const (
	// TagPrefix starts each position's value tag: db.query.parameter.0, …
	// Only numbered keys live under it, so redaction can drop the subtree.
	TagPrefix = "db.query.parameter."
	// TypeTagPrefix starts each rendered position's type tag:
	// pgproxy.bind.type.0, … It uses the same index as TagPrefix.
	TypeTagPrefix = "pgproxy.bind.type."
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

var valueKeys, typeKeys = func() (values, types [MaxParams]string) {
	for i := range values {
		values[i] = TagPrefix + strconv.Itoa(i)
		types[i] = TypeTagPrefix + strconv.Itoa(i)
	}
	return values, types
}()

// typeNames maps the OIDs pgtype knows to PostgreSQL's names for them, with
// arrays written as int4[] rather than _int4. Built-in types all have OIDs
// below FirstNormalObjectId (16384). Names are stored as interfaces so passing
// one to setTag does not allocate.
var typeNames = func() map[uint32]any {
	names := make(map[uint32]any)
	for oid := range uint32(16384) {
		if t, ok := pgTypes.TypeForOID(oid); ok {
			name := t.Name
			if elem, ok := strings.CutPrefix(name, "_"); ok {
				if _, isArray := arrayElementOID(oid); isArray {
					name = elem + "[]"
				}
			}
			names[oid] = name
		}
	}
	return names
}()

var unknownType any = "unknown"

func typeName(oid uint32) any {
	if name, ok := typeNames[oid]; ok {
		return name
	}
	return unknownType
}

// Render passes a value tag and a type tag per captured position to setTag,
// then resets the capture. It does nothing if nothing was recorded. The
// truncation tags come first: tracers that cap a span's attributes, such as the
// OpenTelemetry SDK, keep the earliest ones and drop the rest.
func (c *Capture) Render(setTag func(key string, value any)) {
	if !c.recorded {
		return
	}
	truncated := c.truncated
	// Render every value into one buffer and convert it to a string once; each
	// tag value is a substring of it, so the values cost one allocation, plus
	// one per tag to pass it as an interface.
	out := c.out[:0]
	rendered := 0
	for i := range c.params {
		p := &c.params[i]
		start := len(out)
		out = appendParam(out, p, c.data[p.off:p.off+p.n])
		if len(out) > TotalLimit {
			out = out[:start]
			truncated = true
			break
		}
		p.end = uint32(len(out))
		rendered++
	}
	c.out = out
	if truncated {
		setTag(TruncatedTag, true)
		setTag(RenderedTag, rendered)
	}
	values := string(out)
	start := 0
	for i := range rendered {
		end := int(c.params[i].end)
		setTag(valueKeys[i], values[start:end])
		setTag(typeKeys[i], typeName(c.params[i].oid))
		start = end
	}
	c.Reset()
}

func appendParam(dst []byte, p *param, v []byte) []byte {
	if p.size < 0 {
		return append(dst, "null"...)
	}
	size := int(p.size)
	switch p.format {
	case 0:
		return appendText(dst, v, size, ValueLimit)
	case 1:
		if out, ok := appendBinary(dst, p.oid, v, size); ok {
			return out
		}
	}
	// Malformed or unknown binary values, and any future format. dst still
	// ends where it did, so this overwrites what a failed decode appended.
	return appendHex(dst, v, size)
}
