// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package setdetect

import "math/bits"

// Substring is the proposal's current rule: "set" anywhere, ASCII case
// insensitive, comments and literals included.
type Substring struct{}

// Name implements Detector.
func (Substring) Name() string { return "substring" }

// Classify implements Detector. It finds the same matches as
// MayChangeSession, eight bytes at a time.
func (Substring) Classify(sql string, _ bool) Verdict {
	return verdict(findSet(sql, func(string, int) bool { return true }))
}

// findSet calls accept with the index of each "set" (any case) in sql, in
// order, until accept returns true. It finds candidates as an 'e' followed
// by a 't' with a SWAR test, then checks the 's' before.
func findSet(sql string, accept func(sql string, i int) bool) bool {
	n := len(sql)
	check := func(j int) bool {
		// j is the index of the 'e'.
		return j > 0 && j+1 < n && sql[j-1]|0x20 == 's' && sql[j]|0x20 == 'e' && sql[j+1]|0x20 == 't' && accept(sql, j-1)
	}
	i := 0
	for ; i+9 <= n; i += 8 {
		f := load64(sql, i) | 0x2020202020202020
		fn := load64(sql, i+1) | 0x2020202020202020
		cand := zeroBytes(f^(0x65*lo8)) & zeroBytes(fn^(0x74*lo8))
		for cand != 0 {
			if check(i + bits.TrailingZeros64(cand)/8) {
				return true
			}
			cand &= cand - 1
		}
	}
	for ; i < n; i++ {
		if check(i) {
			return true
		}
	}
	return false
}

// MayChangeSession is the proposal's matcher, verbatim.
func MayChangeSession(sql string) bool {
	for i := 0; i+2 < len(sql); i++ {
		if sql[i]|0x20 == 's' && sql[i+1]|0x20 == 'e' && sql[i+2]|0x20 == 't' {
			return true
		}
	}
	return false
}

// WordBoundary matches "set" as a whole word, where word characters are
// those of Postgres identifiers (letters, digits, _, $ and non-ASCII bytes).
// It therefore misses set_config, which is one identifier.
type WordBoundary struct{}

// Name implements Detector.
func (WordBoundary) Name() string { return "word-boundary" }

// Classify implements Detector.
func (WordBoundary) Classify(sql string, _ bool) Verdict {
	return verdict(findSet(sql, func(sql string, i int) bool {
		return (i == 0 || !identCont[sql[i-1]]) && (i+3 >= len(sql) || !identCont[sql[i+3]])
	}))
}
