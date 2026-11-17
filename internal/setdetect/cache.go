// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package setdetect

import (
	"hash/maphash"
	"sync/atomic"
)

// Cached puts a fixed-size verdict cache, keyed by a hash of the text, in
// front of another detector. Applications send the same few hundred texts
// over and over, so the expensive detector runs once per distinct text.
//
// The table is direct-mapped (slot = hash % size, evicted on collision) and
// lock-free: each slot is one atomic word holding the top 63 bits of the
// hash and the verdict. A hash collision would return another text's
// verdict; at 2^-63 per pair that is below any other risk here, but a
// production version could also compare the length or the text.
type Cached struct {
	Inner Detector
	seed  maphash.Seed
	slots []atomic.Uint64
}

// cacheSlot is the slot layout of the first, mutex-guarded version, kept for
// reference_test.go.
type cacheSlot struct {
	key     uint64
	verdict Verdict
	full    bool
}

// NewCached returns a cache of size slots in front of inner.
func NewCached(inner Detector, size int) *Cached {
	return &Cached{Inner: inner, seed: maphash.MakeSeed(), slots: make([]atomic.Uint64, size)}
}

// Name implements Detector.
func (c *Cached) Name() string { return c.Inner.Name() + "+cache" }

// Classify implements Detector. scsOn is part of the key.
func (c *Cached) Classify(sql string, scsOn bool) Verdict {
	h := maphash.String(c.seed, sql)
	if !scsOn {
		h = ^h
	}
	s := &c.slots[h%uint64(len(c.slots))]
	tag := h | 1 // never 0, so 0 means empty; bit 0 is replaced by the verdict
	if w := s.Load(); w != 0 && w|1 == tag {
		return Verdict(w & 1)
	}
	v := c.Inner.Classify(sql, scsOn)
	s.Store(tag&^1 | uint64(v))
	return v
}
