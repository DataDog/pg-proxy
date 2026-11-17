// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package setdetect

import (
	"math/bits"
	"unsafe"
)

// TwoStageSWAR finds the byte pairs "et" and "ex" (any case) eight bytes at
// a time, and ';' and '&' with the same word trick, before falling back to
// exact checks around each candidate.
type TwoStageSWAR struct{}

// Name implements Detector.
func (TwoStageSWAR) Name() string { return "two-stage-swar" }

// Classify implements Detector.
func (TwoStageSWAR) Classify(sql string, scsOn bool) Verdict {
	if !stage1SWAR(sql) {
		return verdict(leadingDoCall(sql))
	}
	return verdict(lexDirty(sql, scsOn))
}

const (
	lo8 = 0x0101010101010101
	hi8 = 0x8080808080808080
)

// zeroBytes has the high bit set in each byte of x that is zero, and
// possibly in a byte above a zero byte (which only adds candidates).
func zeroBytes(x uint64) uint64 { return (x - lo8) &^ x & hi8 }

func stage1SWAR(sql string) bool {
	n := len(sql)
	i := 0
	for ; i+9 <= n; i += 8 {
		w := load64(sql, i)
		nx := load64(sql, i+1) // byte j+1 under byte j
		// 'e' then 't' or 'x': folding case (|0x20) and the two bits where
		// 't' and 'x' differ (|0x0c) maps both to 0x7c. 'p' and '|' map
		// there too and only add candidates.
		f, fn := w|0x2020202020202020, nx|0x2c2c2c2c2c2c2c2c
		cand := zeroBytes(f^(0x65*lo8)) & zeroBytes(fn^(0x7c*lo8))
		cand |= zeroBytes(w ^ (';' * lo8))
		cand |= zeroBytes(nx ^ ('&' * lo8))
		for cand != 0 {
			j := i + bits.TrailingZeros64(cand)/8
			if candidateAt(sql, j) {
				return true
			}
			cand &= cand - 1
		}
	}
	for ; i < n; i++ {
		if candidateAt(sql, i) {
			return true
		}
	}
	return false
}

// candidateAt checks the patterns exactly at i: "set" with e at i, "exec"
// starting at i, ";" at i, or "u&" starting at i.
func candidateAt(sql string, i int) bool {
	n := len(sql)
	switch sql[i] | 0x20 {
	case 'e':
		if i > 0 && sql[i-1]|0x20 == 's' && i+1 < n && sql[i+1]|0x20 == 't' {
			return true
		}
		return i+3 < n && sql[i+1]|0x20 == 'x' && sql[i+2]|0x20 == 'e' && sql[i+3]|0x20 == 'c'
	case 'u':
		return i+1 < n && sql[i+1] == '&'
	}
	return sql[i] == ';'
}

// load64 reads 8 bytes of s at i, little endian. The caller bounds-checks.
func load64(s string, i int) uint64 {
	return *(*uint64)(unsafe.Add(unsafe.Pointer(unsafe.StringData(s)), i))
}
