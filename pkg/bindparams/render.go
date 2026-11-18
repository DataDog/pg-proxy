// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package bindparams

import "strconv"

const (
	// TagPrefix starts each position's tag key: db.parameters.0, db.parameters.1, …
	TagPrefix = "db.parameters."
	// TruncatedTag is set to true when a limit dropped positions.
	TruncatedTag = "db.parameters.truncated"

	// ValueLimit is the number of bytes of a value shown before "(+N bytes)".
	ValueLimit = 256
	// ArrayElementLimit is the number of array elements shown.
	ArrayElementLimit = 10
	// TotalLimit bounds the rendered bytes across all positions of a Bind.
	TotalLimit = 4 << 10

	// hexLimit is the number of raw bytes shown in hex (two characters each).
	hexLimit = ValueLimit / 2
	// elementLimit is the number of bytes of a text array element shown.
	elementLimit = 64
)

var tagKeys = func() (keys [MaxParams]string) {
	for i := range keys {
		keys[i] = TagPrefix + strconv.Itoa(i)
	}
	return keys
}()

// Render passes one tag per captured position to setTag, then resets the
// capture. It does nothing if nothing was recorded.
func (c *Capture) Render(setTag func(key string, value any)) {
	if !c.recorded {
		return
	}
	c.render(setTag)
	c.Reset()
}

func (c *Capture) render(setTag func(key string, value any)) {
	truncated := c.truncated
	// Render every value into one buffer and convert it to a string once; each
	// tag value is a substring of it, so the tags cost one allocation in all.
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
	values := string(out)
	start := 0
	for i := range rendered {
		end := int(c.params[i].end)
		setTag(tagKeys[i], values[start:end])
		start = end
	}
	if truncated {
		setTag(TruncatedTag, true)
	}
}

func appendParam(dst []byte, p *param, v []byte) []byte {
	if p.size < 0 {
		return append(dst, "null"...)
	}
	size := int(p.size)
	if !p.binary {
		return appendText(dst, v, size, ValueLimit)
	}
	mark := len(dst)
	if out, ok := appendBinary(dst, p.oid, v, size); ok {
		return out
	}
	return appendHex(dst[:mark], v, size)
}
