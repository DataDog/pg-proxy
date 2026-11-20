// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package session

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// protocolCycleAllocBudgets are the heap allocations per protocol cycle, with
// tracing off and no-op metrics, measured on the protocolCycles harness with
// Go 1.26 and 1.27, which agree. Each budget is a ratchet set to the measured
// count: a change that allocates more fails TestProtocolCycleAllocationBudgets.
// A change that allocates less should lower the budget in the same change, so
// the gain is kept; the test logs the new count. Raise a budget only with the
// reason in the commit message. To print the current counts, run:
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
			allocs := testing.AllocsPerRun(100, func() {
				if runErr := client.run(script); runErr != nil && err == nil {
					err = runErr
				}
			})
			require.NoError(t, err)
			require.LessOrEqualf(t, allocs, budget,
				"a %s cycle allocates more than its budget; find the new allocation with BenchmarkProtocolCycle and -memprofile", pc.name)
			if allocs < budget {
				t.Logf("%s allocates %v times per cycle: lower its budget from %v", pc.name, allocs, budget)
			}
		})
	}
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
