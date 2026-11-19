// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package pool

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHybridRoutingDowntimeUsesSelectedSource(t *testing.T) {
	for _, patroniFirst := range []bool{false, true} {
		name := "SQL selects replacement first"
		if patroniFirst {
			name = "Patroni restores route before SQL completes"
		}
		t.Run(name, func(t *testing.T) {
			p := newHybridHealthPolicy(true)
			at := time.Now()
			ips := []string{"A", "B"}
			p.observePatroni(map[string]bool{"A": true}, at)
			p.observeSQL(ips, SQLHealthCheckOutcome{PrimaryIPs: []string{"A"}}, at)
			p.observeSQL(ips, SQLHealthCheckOutcome{}, at.Add(time.Second))
			var comparison *LeaderRoutingComparison
			if patroniFirst {
				decision := p.observePatroni(map[string]bool{"B": true}, at.Add(2*time.Second))
				require.True(t, decision.publish)
				comparison = decision.routingComparison
				// SQL-only detection is slower, but does not extend the routing outage.
				lateSQL := p.observeSQL(ips, SQLHealthCheckOutcome{PrimaryIPs: []string{"B"}}, at.Add(4*time.Second))
				require.NotNil(t, lateSQL.leaderDetection)
				assert.Equal(t, float64(-200), lateSQL.leaderDetection.DowntimeReductionPercent)
				assert.Nil(t, lateSQL.routingComparison)
			} else {
				p.observeSQL(ips, SQLHealthCheckOutcome{PrimaryIPs: []string{"B"}}, at.Add(2*time.Second))
				comparison = p.observePatroni(map[string]bool{"B": true}, at.Add(5*time.Second)).routingComparison
			}
			require.NotNil(t, comparison)
			require.True(t, comparison.HasDowntimeComparison)
			assert.Equal(t, time.Second, comparison.HybridDowntime)
			if patroniFirst {
				assert.Equal(t, time.Second, comparison.PatroniDowntime)
				assert.Zero(t, comparison.Improvement)
				assert.Zero(t, comparison.DowntimeReductionPercent)
				assert.Equal(t, SQLRoutingResultPatroniRecovery, comparison.HybridSource)
			} else {
				assert.Equal(t, 4*time.Second, comparison.PatroniDowntime)
				assert.Equal(t, 3*time.Second, comparison.Improvement)
				assert.Equal(t, float64(75), comparison.DowntimeReductionPercent)
			}
		})
	}
}

func TestHybridRoutingDowntimeWindowBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name           string
		intermediate   map[string]bool
		reset          bool
		patroniAt      time.Duration
		wantComparison bool
		wantReduction  float64
	}{
		{name: "repeated empty publication keeps first start", intermediate: map[string]bool{}, patroniAt: 5 * time.Second, wantComparison: true, wantReduction: 50},
		{name: "cached fallback published after Patroni is slower", intermediate: map[string]bool{}, patroniAt: 2 * time.Second, wantComparison: true, wantReduction: -100},
		{name: "Patroni before outage cannot define percentage", intermediate: map[string]bool{}, patroniAt: time.Second},
		{name: "same leader recovery clears old outage", intermediate: map[string]bool{"A": true}, patroniAt: 5 * time.Second},
		{name: "ambiguous publication clears old outage", intermediate: map[string]bool{"A": true, "B": true}, patroniAt: 5 * time.Second},
		{name: "mode change clears old outage", intermediate: map[string]bool{"A": true}, reset: true, patroniAt: 5 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newHybridHealthPolicy(true)
			at := time.Now()
			p.recordRoutingLeaderLocked("hybrid", map[string]bool{"A": true}, at, SQLRoutingResultConclusive)
			p.recordRoutingLeaderLocked("patroni", map[string]bool{"A": true}, at, "")
			p.recordRoutingLeaderLocked("hybrid", nil, at.Add(time.Second), SQLRoutingResultConclusive)
			if tc.reset {
				p.setRoutingEnabled(false)
				p.setRoutingEnabled(true)
				p.recordRoutingLeaderLocked("hybrid", map[string]bool{"A": true}, at.Add(1500*time.Millisecond), SQLRoutingResultConclusive)
				p.recordRoutingLeaderLocked("patroni", map[string]bool{"A": true}, at.Add(1500*time.Millisecond), "")
			}
			p.recordRoutingLeaderLocked("hybrid", tc.intermediate, at.Add(1800*time.Millisecond), SQLRoutingResultConclusive)
			var comparison *LeaderRoutingComparison
			if tc.patroniAt < 3*time.Second {
				p.recordRoutingLeaderLocked("patroni", map[string]bool{"C": true}, at.Add(tc.patroniAt), "")
				comparison = p.recordRoutingLeaderLocked("hybrid", map[string]bool{"C": true}, at.Add(3*time.Second), SQLRoutingResultCachedFallback)
			} else {
				p.recordRoutingLeaderLocked("hybrid", map[string]bool{"C": true}, at.Add(3*time.Second), SQLRoutingResultConclusive)
				comparison = p.recordRoutingLeaderLocked("patroni", map[string]bool{"C": true}, at.Add(tc.patroniAt), "")
			}
			require.NotNil(t, comparison)
			assert.Equal(t, tc.wantComparison, comparison.HasDowntimeComparison)
			assert.Equal(t, tc.wantReduction, comparison.DowntimeReductionPercent)
			assert.Equal(t, tc.patroniAt-3*time.Second, comparison.Improvement)
		})
	}
}
