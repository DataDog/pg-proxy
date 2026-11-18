// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package bindparams

import "testing"

func discardTag(string, any) {}

// BenchmarkRender measures decoding and formatting when the root trace
// finishes, excluding the tracer's own SetTag cost.
func BenchmarkRender(b *testing.B) {
	for _, bc := range benchCases(b) {
		b.Run(bc.name, func(b *testing.B) {
			b.ReportAllocs()
			var c Capture
			c.Record(bc.bind, bc.oids, nil)
			for b.Loop() {
				c.render(discardTag)
			}
		})
	}
}

// BenchmarkRecordAndRender is the whole added cost of one sampled trace.
func BenchmarkRecordAndRender(b *testing.B) {
	for _, bc := range benchCases(b) {
		b.Run(bc.name, func(b *testing.B) {
			b.ReportAllocs()
			var c Capture
			for b.Loop() {
				c.Record(bc.bind, bc.oids, nil)
				c.Render(discardTag)
			}
		})
	}
}
