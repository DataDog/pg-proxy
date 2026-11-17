// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package setdetect

// Verdict is a detector's answer for one Query or Parse message.
type Verdict uint8

const (
	// Clean means nothing in the SQL can have changed session state the
	// reset restores, so the reset may be skipped.
	Clean Verdict = iota
	// Dirty means the SQL may have changed session state. Detectors also
	// answer Dirty when they cannot tell (fail closed).
	Dirty
)

func (v Verdict) String() string {
	if v == Dirty {
		return "dirty"
	}
	return "clean"
}

// Detector classifies the full text of one Query or Parse message, which may
// hold several statements. scsOn is the backend's reported
// standard_conforming_strings: when it is off, backslashes escape quotes in
// plain '...' literals.
type Detector interface {
	Name() string
	Classify(sql string, scsOn bool) Verdict
}

// Info describes a detector for the report.
type Info struct {
	Detector Detector
	// Family groups variants of one approach in the report.
	Family string
	// Cgo is true when Classify calls into C (pg_query).
	Cgo bool
	// Files lists the source files that implement it, for line counts.
	Files []string
	// Note is a one-line description for the report.
	Note string
}

// funcDetector adapts a function to Detector.
type funcDetector struct {
	name string
	fn   func(sql string, scsOn bool) Verdict
}

func (d funcDetector) Name() string                            { return d.name }
func (d funcDetector) Classify(sql string, scsOn bool) Verdict { return d.fn(sql, scsOn) }

// NewFunc returns a Detector named name that calls fn.
func NewFunc(name string, fn func(sql string, scsOn bool) Verdict) Detector {
	return funcDetector{name: name, fn: fn}
}

func verdict(dirty bool) Verdict {
	if dirty {
		return Dirty
	}
	return Clean
}
