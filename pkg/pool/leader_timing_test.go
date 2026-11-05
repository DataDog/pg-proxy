package pool

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func baselineLeaderTiming(t *testing.T) *leaderTiming[struct{}] {
	t.Helper()
	p := &leaderTiming[struct{}]{}
	require.Empty(t, p.observe(true, leaderTimingIPs("A"), leaderTimingAt(0), struct{}{}, everyNewLeader))
	require.Empty(t, p.observe(false, leaderTimingIPs("A"), leaderTimingAt(0), struct{}{}, everyNewLeader))
	return p
}

func TestLeaderTimingSignedTiming(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		sqlSecond, patroniSecond int
	}{
		{"SQL first", 2, 5},
		{"Patroni first", 4, 2},
		{"same time", 3, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := baselineLeaderTiming(t)
			require.Empty(t, p.observe(true, nil, leaderTimingAt(1), struct{}{}, everyNewLeader))
			require.Empty(t, p.observe(true, nil, leaderTimingAt(1), struct{}{}, everyNewLeader))
			var got []leaderTimingComparison[struct{}]
			if tc.sqlSecond <= tc.patroniSecond {
				require.Empty(t, p.observe(true, leaderTimingIPs("B"), leaderTimingAt(tc.sqlSecond), struct{}{}, everyNewLeader))
				got = p.observe(false, leaderTimingIPs("B"), leaderTimingAt(tc.patroniSecond), struct{}{}, everyNewLeader)
			} else {
				require.Empty(t, p.observe(false, leaderTimingIPs("B"), leaderTimingAt(tc.patroniSecond), struct{}{}, everyNewLeader))
				got = p.observe(true, leaderTimingIPs("B"), leaderTimingAt(tc.sqlSecond), struct{}{}, everyNewLeader)
			}
			require.Len(t, got, 1)
			c := got[0]
			assert.Equal(t, "B", c.ip)
			assert.Equal(t, leaderTimingAt(tc.sqlSecond), c.candidate.at)
			assert.Equal(t, leaderTimingAt(tc.patroniSecond), c.reference.at)
			assert.Equal(t, time.Duration(tc.patroniSecond-tc.sqlSecond)*time.Second, c.speedup())
			require.Empty(t, p.observe(true, leaderTimingIPs("B"), leaderTimingAt(10), struct{}{}, everyNewLeader))
			require.Empty(t, p.observe(false, leaderTimingIPs("B"), leaderTimingAt(11), struct{}{}, everyNewLeader))
		})
	}
}

func TestLeaderTimingPreservesFirstSnapshotTimestamp(t *testing.T) {
	p := baselineLeaderTiming(t)
	// Repeated completed snapshots do not move the first timestamp for B.
	require.Empty(t, p.observe(true, leaderTimingIPs("A", "B"), leaderTimingAt(1), struct{}{}, everyNewLeader))
	require.Empty(t, p.observe(true, leaderTimingIPs("A", "B"), leaderTimingAt(2), struct{}{}, everyNewLeader))
	require.Empty(t, p.observe(false, leaderTimingIPs("A"), leaderTimingAt(3), struct{}{}, everyNewLeader))
	require.Empty(t, p.observe(true, leaderTimingIPs("B"), leaderTimingAt(4), struct{}{}, everyNewLeader))
	got := p.observe(false, leaderTimingIPs("B"), leaderTimingAt(5), struct{}{}, everyNewLeader)
	require.Len(t, got, 1)
	assert.Equal(t, 4*time.Second, got[0].speedup())
}

func TestLeaderTimingSupersededAndInterruptedObservations(t *testing.T) {
	for _, sqlFirst := range []bool{false, true} {
		for _, next := range []string{"", "C"} {
			t.Run(leaderTimingCase(sqlFirst, next), func(t *testing.T) {
				p := baselineLeaderTiming(t)
				require.Empty(t, p.observe(leaderTimingSource(sqlFirst), leaderTimingIPs("B"), leaderTimingAt(1), struct{}{}, everyNewLeader))
				var following map[string]bool
				if next != "" {
					following = leaderTimingIPs(next)
				}
				require.Empty(t, p.observe(leaderTimingSource(sqlFirst), following, leaderTimingAt(2), struct{}{}, everyNewLeader))
				require.Empty(t, p.observe(leaderTimingSource(!sqlFirst), leaderTimingIPs("B"), leaderTimingAt(60), struct{}{}, everyNewLeader))
				got := p.observe(leaderTimingSource(sqlFirst), leaderTimingIPs("B"), leaderTimingAt(61), struct{}{}, everyNewLeader)
				require.Len(t, got, 1)
				expected := time.Second
				if sqlFirst {
					expected = -time.Second
				}
				assert.Equal(t, expected, got[0].speedup(), "must not reuse the observation from 1s")
			})
		}
	}
}
func TestLeaderTimingStartupAndSameLeaderRecovery(t *testing.T) {
	p := &leaderTiming[struct{}]{}
	require.Empty(t, p.observe(true, nil, leaderTimingAt(0), struct{}{}, everyNewLeader))
	require.Empty(t, p.observe(true, leaderTimingIPs("A"), leaderTimingAt(1), struct{}{}, everyNewLeader))
	require.Empty(t, p.observe(false, leaderTimingIPs("A"), leaderTimingAt(2), struct{}{}, everyNewLeader))
	require.Empty(t, p.observe(true, nil, leaderTimingAt(3), struct{}{}, everyNewLeader))
	require.Empty(t, p.observe(true, leaderTimingIPs("A"), leaderTimingAt(4), struct{}{}, everyNewLeader))
	require.Empty(t, p.observe(false, leaderTimingIPs("A"), leaderTimingAt(5), struct{}{}, everyNewLeader))
}

func TestLeaderTimingAgreementDiscardsUnmatchedPromotions(t *testing.T) {
	for _, sqlFirst := range []bool{false, true} {
		t.Run(leaderTimingCase(sqlFirst, "A"), func(t *testing.T) {
			p := baselineLeaderTiming(t)
			require.Empty(t, p.observe(leaderTimingSource(sqlFirst), leaderTimingIPs("B"), leaderTimingAt(1), struct{}{}, everyNewLeader))
			require.Empty(t, p.observe(leaderTimingSource(sqlFirst), leaderTimingIPs("A"), leaderTimingAt(2), struct{}{}, everyNewLeader))
			require.Empty(t, p.pending, "returning to the other source's baseline is agreement, not a pending promotion")
			require.Empty(t, p.observe(leaderTimingSource(sqlFirst), leaderTimingIPs("A"), leaderTimingAt(3), struct{}{}, everyNewLeader))
			require.Empty(t, p.observe(leaderTimingSource(!sqlFirst), leaderTimingIPs("A"), leaderTimingAt(4), struct{}{}, everyNewLeader))
			require.Empty(t, p.observe(leaderTimingSource(!sqlFirst), leaderTimingIPs("B"), leaderTimingAt(3600), struct{}{}, everyNewLeader))
			require.Empty(t, p.observe(leaderTimingSource(!sqlFirst), leaderTimingIPs("A"), leaderTimingAt(3601), struct{}{}, everyNewLeader), "must not emit a one-hour comparison")
			require.Empty(t, p.pending)
			// A later real matched promotion must still produce a fresh comparison.
			require.Empty(t, p.observe(leaderTimingSource(sqlFirst), leaderTimingIPs("B"), leaderTimingAt(3602), struct{}{}, everyNewLeader))
			got := p.observe(leaderTimingSource(!sqlFirst), leaderTimingIPs("B"), leaderTimingAt(3603), struct{}{}, everyNewLeader)
			require.Len(t, got, 1)
			expected := time.Second
			if !sqlFirst {
				expected = -time.Second
			}
			assert.Equal(t, expected, got[0].speedup())
		})
	}
}

func TestLeaderTimingAgreementOnInitialBaselineDiscardsPending(t *testing.T) {
	p := &leaderTiming[struct{}]{}
	require.Empty(t, p.observe(true, leaderTimingIPs("A"), leaderTimingAt(0), struct{}{}, everyNewLeader))
	require.Empty(t, p.observe(true, leaderTimingIPs("B"), leaderTimingAt(1), struct{}{}, everyNewLeader))
	require.Empty(t, p.observe(false, leaderTimingIPs("B"), leaderTimingAt(2), struct{}{}, everyNewLeader))
	assert.Empty(t, p.pending, "the second source's first baseline is not a comparable promotion")
}

func TestLeaderTimingAgreementUsesCurrentSnapshot(t *testing.T) {
	for _, sqlFirst := range []bool{false, true} {
		t.Run(leaderTimingCase(sqlFirst, "A"), func(t *testing.T) {
			p := baselineLeaderTiming(t)
			require.Empty(t, p.observe(leaderTimingSource(!sqlFirst), nil, leaderTimingAt(1), struct{}{}, everyNewLeader))
			require.Empty(t, p.observe(leaderTimingSource(sqlFirst), leaderTimingIPs("B"), leaderTimingAt(2), struct{}{}, everyNewLeader))
			require.Empty(t, p.observe(leaderTimingSource(sqlFirst), leaderTimingIPs("A"), leaderTimingAt(3), struct{}{}, everyNewLeader))
			require.Contains(t, p.pending, "A", "the other source's remembered A baseline is not current agreement")
			require.Empty(t, p.observe(leaderTimingSource(!sqlFirst), leaderTimingIPs("A"), leaderTimingAt(4), struct{}{}, everyNewLeader))
			assert.Empty(t, p.pending, "actual agreement closes the unmatched excursion")
		})
	}
}

func leaderTimingAt(seconds int) time.Time { return time.Unix(100+int64(seconds), 0) }
func leaderTimingIPs(ips ...string) map[string]bool {
	m := make(map[string]bool, len(ips))
	for _, ip := range ips {
		m[ip] = true
	}
	return m
}

func leaderTimingCase(sqlFirst bool, next string) string {
	prefix := "Patroni"
	if sqlFirst {
		prefix = "SQL"
	}
	if next == "" {
		next = "none"
	}
	return prefix + " leaves B for " + next
}

func TestLeaderTimingEligibilityAndMetadata(t *testing.T) {
	for _, mode := range []leaderTimingMode{everyNewLeader, oneNewLeader, oneHealthyLeader} {
		p := &leaderTiming[string]{}
		require.Empty(t, p.observe(candidateLeaderSource, leaderTimingIPs("A"), leaderTimingAt(0), "baseline", mode))
		require.Empty(t, p.observe(referenceLeaderSource, leaderTimingIPs("A"), leaderTimingAt(0), "baseline", mode))
		require.Empty(t, p.observe(candidateLeaderSource, leaderTimingIPs("B", "C"), leaderTimingAt(1), "ambiguous", mode))
		got := p.observe(referenceLeaderSource, leaderTimingIPs("B", "C"), leaderTimingAt(2), "ambiguous", mode)
		if mode == everyNewLeader {
			require.Len(t, got, 2)
		} else {
			require.Empty(t, got)
			require.Empty(t, p.pending)
		}
		// Establish A again, then verify a unique transition carries the first observation's data.
		p.observe(candidateLeaderSource, leaderTimingIPs("A"), leaderTimingAt(3), "baseline", mode)
		p.observe(referenceLeaderSource, leaderTimingIPs("A"), leaderTimingAt(3), "baseline", mode)
		require.Empty(t, p.observe(candidateLeaderSource, map[string]bool{"A": false, "B": true}, leaderTimingAt(4), "conclusive_sql", mode))
		require.Empty(t, p.observe(candidateLeaderSource, leaderTimingIPs("B"), leaderTimingAt(5), "later", mode))
		got = p.observe(referenceLeaderSource, leaderTimingIPs("B"), leaderTimingAt(6), "patroni", mode)
		require.Len(t, got, 1)
		assert.Equal(t, 2*time.Second, got[0].speedup())
		assert.Equal(t, "conclusive_sql", got[0].candidate.data)
		assert.Equal(t, "patroni", got[0].reference.data)
	}
}
