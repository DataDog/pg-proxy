// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package session

import (
	"cmp"
	"math"
	"runtime"
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/require"
)

// protocolCycleAllocBudgets are the heap allocations per protocol cycle, with
// tracing off and no-op metrics, measured on the protocolCycles harness with
// Go 1.26 and 1.27, which agree. Each budget is a ratchet set to the measured
// count: TestProtocolCycleAllocationBudgets fails when a change allocates more,
// and when it removes a whole allocation, until the budget is lowered in the
// same change, so the gain is kept. Raise a budget only with the reason in the
// commit message. To print the current counts, run:
//
//	go test -run TestProtocolCycleAllocationBudgets -v ./pkg/session
//
// BenchmarkProtocolCycle runs the same cycles for benchstat comparisons.
var protocolCycleAllocBudgets = map[string]float64{
	"one_shot_extended":     51,
	"simple_query":          27,
	"named_statement_reuse": 34,
	"pipeline_30":           493,
}

func TestProtocolCycleAllocationBudgets(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector changes allocation counts")
	}
	for _, pc := range protocolCycles {
		t.Run(pc.name, func(t *testing.T) {
			budget, ok := protocolCycleAllocBudgets[pc.name]
			require.True(t, ok, "add an allocation budget for this cycle")
			client, script := pc.prepare(t, newCycleHarness(t, false))
			var err error
			allocs := cycleAllocsPerRun(func() { err = cmp.Or(err, client.run(script)) })
			require.NoError(t, err)
			t.Logf("%s allocates %v times per cycle; its budget is %v", pc.name, allocs, budget)
			require.LessOrEqualf(t, allocs, budget,
				"a %s cycle allocates more than its budget; find the new allocation with BenchmarkProtocolCycle and -memprofile", pc.name)
			require.Greaterf(t, allocs, budget-1,
				"a whole allocation is gone from the %s cycle: lower its budget to %v", pc.name, math.Ceil(allocs))
		})
	}
}

// allocWindow is the number of cycles in one measurement. pgxpool allocates its
// Conn wrappers this many at a time (connResource.getConn makes a []Conn of 128
// when it runs out), so each window holds exactly one of those allocations.
const allocWindow = 128

// cycleAllocsPerRun returns the heap allocations per cycle of run, without
// pgxpool's Conn wrapper batch. After one warm-up window, so that slices and
// maps reach their steady size, it measures 20 windows and keeps the one with
// the fewest allocations, since other goroutines now and then allocate during
// a window, more often on a busy machine. Unlike testing.AllocsPerRun, it does
// not round the mean down, so an allocation made on only some cycles still
// exceeds a budget. Garbage collection is off within a window: a collection
// empties sync.Pools, and refilling them would add allocations that depend on
// when it runs.
func cycleAllocsPerRun(run func()) float64 {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))
	defer debug.SetGCPercent(debug.SetGCPercent(-1))
	for range allocWindow {
		run()
	}
	fewest := uint64(math.MaxUint64)
	for range 20 {
		runtime.GC() // bounds the heap
		run()        // re-creates the sync.Pool caches that the collection dropped
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		for range allocWindow {
			run()
		}
		runtime.ReadMemStats(&after)
		fewest = min(fewest, after.Mallocs-before.Mallocs)
	}
	return float64(fewest-1) / allocWindow // less connResource.getConn's one allocation
}

func BenchmarkProtocolCycle(b *testing.B) {
	for _, pc := range protocolCycles {
		b.Run(pc.name, func(b *testing.B) {
			client, script := pc.prepare(b, newCycleHarness(b, false))
			b.ReportAllocs()
			for b.Loop() {
				if err := client.run(script); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
