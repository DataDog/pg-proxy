// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package mirror

import (
	"sync/atomic"

	pgproxy "github.com/DataDog/pg-proxy"
	"github.com/DataDog/pg-proxy/pkg/limiter"
)

// Process-wide byte budget shared by every per-session mirror queue. Each session's
// queue is individually capped by config.Mirroring.QueueSize, but many concurrently
// mirrored sessions could still sum to an OOM; this bounds the aggregate.
// budget = max(floor, cgroupMemoryHardLimit × fraction), falling back to the floor
// when the cgroup is unavailable. Derived once at startup (mirrors SessionLimiter).
const (
	// mirrorBudgetMemFraction is the share of the container's hard memory limit that
	// all mirror queues may collectively hold. Small on purpose: mirroring is a
	// best-effort shadow feature and must never crowd out the primary path.
	mirrorBudgetMemFraction = 0.05

	// mirrorBudgetFloorBytes is the budget when the cgroup limit is unavailable, and
	// the minimum regardless of pod size. Holds a handful of max-size (10 MB) queues.
	mirrorBudgetFloorBytes int64 = 64 * 1024 * 1024 // 64 MiB
)

// mirrorBudget is a shared, process-wide cap on the total bytes held across all
// per-session mirror queues. Reserve/release are atomic and lock-free.
type Budget struct {
	used atomic.Int64
	cap  int64
}

func NewBudget(capBytes int64) *Budget {
	return &Budget{cap: capBytes}
}

func newMirrorBudget(capBytes int64) *Budget { return NewBudget(capBytes) }

// tryReserve accounts n bytes against the budget, returning false (and reserving
// nothing) if that would exceed the cap. Strict: under contention it may reject at
// exactly the cap, which is fine for a best-effort feature.
func (b *Budget) tryReserve(n int) bool {
	if b.used.Add(int64(n)) > b.cap {
		b.used.Add(-int64(n))
		return false
	}
	return true
}

// forceReserve accounts n bytes unconditionally, even past the cap. Reserved for
// tiny control sentinels (flush/release) whose guaranteed admission keeps a batch
// boundary from being dropped; the small, bounded overshoot is returned as the
// worker drains, exactly like a tryReserve'd reservation.
func (b *Budget) forceReserve(n int) {
	if n > 0 {
		b.used.Add(int64(n))
	}
}

// release returns n previously reserved bytes to the budget.
func (b *Budget) release(n int) {
	if n > 0 {
		b.used.Add(-int64(n))
	}
}

// UsedBytes reports the bytes currently reserved across all queues.
func (b *Budget) UsedBytes() int64 { return b.used.Load() }

// computeMirrorBudget reads the cgroup memory hard limit and applies the fraction,
// falling back to the floor when the cgroup limit is unavailable.
func ComputeBudget() int64 {
	hard := limiter.ReadHardLimit()
	if !hard.Available || hard.Bytes <= 0 {
		pgproxy.Logger().Warn("mirror budget memory hard limit unavailable; using fallback", "fallback_bytes", mirrorBudgetFloorBytes)
		return mirrorBudgetFloorBytes
	}
	b := mirrorBudgetForMemory(hard.Bytes)
	pgproxy.Logger().Info("computed mirror budget", "cgroup_hard_limit_bytes", hard.Bytes, "mirror_global_budget_bytes", b)
	return b
}

// mirrorBudgetForMemory applies the formula to a given memory budget. Split out so
// it can be unit-tested without depending on cgroup state.
func mirrorBudgetForMemory(memBytes int64) int64 {
	if memBytes <= 0 {
		return mirrorBudgetFloorBytes
	}
	b := int64(float64(memBytes) * mirrorBudgetMemFraction)
	if b < mirrorBudgetFloorBytes {
		return mirrorBudgetFloorBytes
	}
	return b
}
