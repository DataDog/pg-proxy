// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package pool

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
)

// These scenarios cover discovery and cleanup with their inventory and
// health expectations. Discover supplies the health worker's retained set,
// so the pool must not apply a second DNS-miss threshold.
func TestSetupPool_PGK(t *testing.T) {
	const repeatedHealthChecks = 3

	tests := []struct {
		name               string
		initialInstances   []string
		resolvedIps        []string
		healthyPgInstances []string
		// on the ith iteration forward, connection pgInstances will no longer be healthy
		notHealthyAfterIterationNum  int
		expectedHealthyPgInstanceIps []string
		expectedPgInstanceHealths    map[string]bool
		numIterations                int
	}{
		// Replica pool tests
		{
			name:             "replica pool - all healthy pools",
			initialInstances: []string{},
			resolvedIps: []string{
				"10.0.0.76",
				"10.0.0.77",
			},
			healthyPgInstances: []string{
				"10.0.0.76",
				"10.0.0.77",
			},
			expectedPgInstanceHealths: map[string]bool{
				"10.0.0.76": true,
				"10.0.0.77": true,
			},
			expectedHealthyPgInstanceIps: []string{
				"10.0.0.76",
				"10.0.0.77",
			},
			numIterations: repeatedHealthChecks,
		},
		{
			name: "replica pool - no resolved ips, but still healthy",
			initialInstances: []string{
				"10.0.0.76",
				"10.0.0.77",
			},
			resolvedIps: []string{},
			healthyPgInstances: []string{
				"10.0.0.76",
				"10.0.0.77",
			},
			expectedPgInstanceHealths: map[string]bool{
				"10.0.0.76": true,
				"10.0.0.77": true,
			},
			expectedHealthyPgInstanceIps: []string{
				"10.0.0.76",
				"10.0.0.77",
			},
			numIterations: repeatedHealthChecks,
		},
		{
			name: "replica pool - could not health check or resolve 10.0.0.77, pg instance removed from map",
			initialInstances: []string{
				"10.0.0.76",
				"10.0.0.77",
			},
			resolvedIps: []string{},
			healthyPgInstances: []string{
				"10.0.0.76",
			},
			expectedPgInstanceHealths: map[string]bool{
				"10.0.0.76": true,
				// 10.0.0.77 not resolved and not last known healthy, so it is removed
			},
			expectedHealthyPgInstanceIps: []string{
				"10.0.0.76",
			},
			numIterations: 1,
		},
		{
			name: "replica pool - retains healthy instance through repeated discovery misses",
			initialInstances: []string{
				"10.0.0.76",
				"10.0.0.77",
			},
			resolvedIps: []string{
				"10.0.0.76",
			},
			healthyPgInstances: []string{
				"10.0.0.76",
				"10.0.0.77",
			},
			expectedPgInstanceHealths: map[string]bool{
				"10.0.0.76": true,
				"10.0.0.77": true,
			},
			expectedHealthyPgInstanceIps: []string{
				"10.0.0.76",
				"10.0.0.77",
			},
			numIterations: repeatedHealthChecks,
		},
		{
			name: "replica pool - removes unhealthy instance absent from discovery",
			initialInstances: []string{
				"10.0.0.76",
				"10.0.0.77",
			},
			resolvedIps: []string{
				"10.0.0.76",
			},
			healthyPgInstances: []string{
				"10.0.0.76",
				// 10.0.0.77 is no longer healthy
			},
			expectedPgInstanceHealths: map[string]bool{
				"10.0.0.76": true,
				// 10.0.0.77 should be removed from pool when absent from discovery and not healthy
			},
			expectedHealthyPgInstanceIps: []string{
				"10.0.0.76",
				// 10.0.0.77 should be removed from pool
			},
			numIterations: repeatedHealthChecks,
		},
		// Leader pool tests
		{
			name:             "leader pool - 1 healthy leader, 3 resolved ips",
			initialInstances: []string{},
			resolvedIps: []string{
				"10.0.0.76",
				"10.0.0.77",
				"10.0.0.78",
			},
			healthyPgInstances: []string{
				"10.0.0.76",
			},
			expectedPgInstanceHealths: map[string]bool{
				"10.0.0.76": true,
				"10.0.0.77": false,
				"10.0.0.78": false,
			},
			expectedHealthyPgInstanceIps: []string{
				"10.0.0.76",
			},
			numIterations: repeatedHealthChecks,
		},
		{
			name: "leader pool - retains healthy leader through repeated discovery misses",
			initialInstances: []string{
				"10.0.0.76",
				"10.0.0.77",
				"10.0.0.78",
			},
			resolvedIps: []string{
				// "10.0.0.76", do not resolve, but should remain
				"10.0.0.77",
				"10.0.0.78",
			},
			healthyPgInstances: []string{
				"10.0.0.76",
			},
			expectedPgInstanceHealths: map[string]bool{
				"10.0.0.76": true, // Should remain healthy and in pool for leader
				"10.0.0.77": false,
				"10.0.0.78": false,
			},
			expectedHealthyPgInstanceIps: []string{
				"10.0.0.76",
			},
			numIterations: repeatedHealthChecks,
		},
		{
			name: "leader pool - retains last healthy leader through discovery and health failures",
			initialInstances: []string{
				"10.0.0.76",
				"10.0.0.77",
				"10.0.0.78",
			},
			resolvedIps: []string{
				// "10.0.0.76", do not resolve, but should remain
				"10.0.0.77",
				"10.0.0.78",
			},
			healthyPgInstances: []string{
				"10.0.0.76",
			},
			notHealthyAfterIterationNum: 1,
			expectedPgInstanceHealths: map[string]bool{
				"10.0.0.76": false, // Becomes unhealthy, but not removed because was last known healthy instance
				"10.0.0.77": false,
				"10.0.0.78": false,
			},
			expectedHealthyPgInstanceIps: []string{},
			numIterations:                repeatedHealthChecks,
		},
		{
			name: "leader pool - no leader, 4 initial instances, 0 resolved IPs",
			initialInstances: []string{
				"10.0.0.76",
				"10.0.0.77",
				"10.0.0.78",
				"10.0.0.79",
			},
			resolvedIps: []string{
				// No IPs resolved
			},
			healthyPgInstances: []string{
				// None connectable, so no leader
			},
			expectedPgInstanceHealths: map[string]bool{
				// none resolved and none last known healthy, so all are removed
			},
			expectedHealthyPgInstanceIps: []string{
				// No healthy instances
			},
			numIterations: 1,
		},
		{
			name: "leader pool - has leader, 4 initial instances, 3 resolved IPs (including leader)",
			initialInstances: []string{
				"10.0.0.76",
				"10.0.0.77",
				"10.0.0.78",
				"10.0.0.79", // This one won't be in resolved IPs and should be cleaned up
			},
			resolvedIps: []string{
				"10.0.0.76", // Leader IP
				"10.0.0.77",
				"10.0.0.78",
			},
			healthyPgInstances: []string{
				"10.0.0.76", // Only this one is connectable (becomes the leader)
			},
			expectedPgInstanceHealths: map[string]bool{
				"10.0.0.76": true,  // Leader, healthy
				"10.0.0.77": false, // In resolution but not connectable
				"10.0.0.78": false, // In resolution but not connectable
				// 10.0.0.79 should be removed (not in current resolution, not the leader)
			},
			expectedHealthyPgInstanceIps: []string{
				"10.0.0.76", // Only the leader
			},
			numIterations: repeatedHealthChecks,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			currentTestIteration := 0
			newInstance := func(ip string) PostgresInstance {
				return NewMockInstance(ip, nil, nil, func(ip string) bool {
					stillConnectable := test.notHealthyAfterIterationNum < 1 || currentTestIteration < test.notHealthyAfterIterationNum
					return stillConnectable && slices.Contains(test.healthyPgInstances, ip)
				})
			}
			direct := NewDirectRuntime(DirectRuntimeConfig{})
			for _, ip := range test.initialInstances {
				direct.State().Store(ip, newInstance(ip))
			}
			runtime := NewPGKPool(PGKPoolConfig{
				Direct:      direct,
				Discover:    func() []string { return test.resolvedIps },
				NewInstance: newInstance,
			})
			t.Cleanup(runtime.ClosePools)

			for currentTestIteration < test.numIterations {
				_ = runtime.Setup()
				currentTestIteration++
			}

			actualHealth := make(map[string]bool)
			runtime.State().Range(func(ip string, instance PostgresInstance) bool {
				actualHealth[ip] = instance.IsHealthy()
				return true
			})
			assert.Equal(t, test.expectedPgInstanceHealths, actualHealth)
			assert.ElementsMatch(t, test.expectedHealthyPgInstanceIps, runtime.State().HealthyAddresses())
		})
	}
}
