package pool

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func potentialAt(seconds int) time.Time { return time.Unix(100+int64(seconds), 0) }
func potentialIPs(ips ...string) map[string]bool {
	m := make(map[string]bool, len(ips))
	for _, ip := range ips {
		m[ip] = true
	}
	return m
}
func baselinePotential(t *testing.T) *potentialTiming {
	t.Helper()
	p := &potentialTiming{}
	require.Empty(t, p.observe(true, potentialIPs("A"), potentialAt(0)))
	require.Empty(t, p.observe(false, potentialIPs("A"), potentialAt(0)))
	return p
}

func TestSQLPotentialSignedTimingAndDowntime(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		sqlSecond, patroniSecond int
		reduction                float64
	}{
		{"SQL first", 2, 5, 75},
		{"Patroni first", 4, 2, -200},
		{"same time", 3, 3, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := baselinePotential(t)
			require.Empty(t, p.observe(true, nil, potentialAt(1)))
			require.Empty(t, p.observe(true, nil, potentialAt(1)))
			var got []SQLPotentialComparison
			if tc.sqlSecond <= tc.patroniSecond {
				require.Empty(t, p.observe(true, potentialIPs("B"), potentialAt(tc.sqlSecond)))
				got = p.observe(false, potentialIPs("B"), potentialAt(tc.patroniSecond))
			} else {
				require.Empty(t, p.observe(false, potentialIPs("B"), potentialAt(tc.patroniSecond)))
				got = p.observe(true, potentialIPs("B"), potentialAt(tc.sqlSecond))
			}
			require.Len(t, got, 1)
			c := got[0]
			assert.Equal(t, "B", c.NewLeaderIP)
			assert.Equal(t, potentialAt(tc.sqlSecond), c.SQLObservedAt)
			assert.Equal(t, potentialAt(tc.patroniSecond), c.PatroniObservedAt)
			assert.Equal(t, time.Duration(tc.patroniSecond-tc.sqlSecond)*time.Second, c.Speedup)
			require.True(t, c.HasDowntimeComparison)
			assert.Equal(t, potentialAt(1), c.DowntimeStartedAt)
			assert.Equal(t, time.Duration(tc.sqlSecond-1)*time.Second, c.SQLDowntime)
			assert.Equal(t, time.Duration(tc.patroniSecond-1)*time.Second, c.PatroniDowntime)
			assert.Equal(t, tc.reduction, c.DowntimeReductionPercent)
			require.Empty(t, p.observe(true, potentialIPs("B"), potentialAt(10)))
			require.Empty(t, p.observe(false, potentialIPs("B"), potentialAt(11)))
		})
	}
}

func TestSQLPotentialPreservesEarliestSuccessWithoutWaitingForOtherIPs(t *testing.T) {
	p := baselinePotential(t)
	// A's previous success has not yet been replaced. This is deliberately optimistic.
	require.Empty(t, p.observe(true, potentialIPs("A", "B"), potentialAt(1)))
	require.Empty(t, p.observe(true, potentialIPs("A", "B"), potentialAt(2)))
	require.Empty(t, p.observe(false, potentialIPs("A"), potentialAt(3)))
	require.Empty(t, p.observe(true, potentialIPs("B"), potentialAt(4)))
	got := p.observe(false, potentialIPs("B"), potentialAt(5))
	require.Len(t, got, 1)
	assert.Equal(t, 4*time.Second, got[0].Speedup)
	assert.False(t, got[0].HasDowntimeComparison, "leader timing does not invent an availability gap")
}

func TestSQLPotentialSupersededAndInterruptedObservations(t *testing.T) {
	for _, sqlFirst := range []bool{false, true} {
		for _, next := range []string{"", "C"} {
			t.Run(fmtPotentialCase(sqlFirst, next), func(t *testing.T) {
				p := baselinePotential(t)
				require.Empty(t, p.observe(sqlFirst, potentialIPs("B"), potentialAt(1)))
				var following map[string]bool
				if next != "" {
					following = potentialIPs(next)
				}
				require.Empty(t, p.observe(sqlFirst, following, potentialAt(2)))
				require.Empty(t, p.observe(!sqlFirst, potentialIPs("B"), potentialAt(60)))
				got := p.observe(sqlFirst, potentialIPs("B"), potentialAt(61))
				require.Len(t, got, 1)
				expected := time.Second
				if sqlFirst {
					expected = -time.Second
				}
				assert.Equal(t, expected, got[0].Speedup, "must not reuse the observation from 1s")
			})
		}
	}
}
func fmtPotentialCase(sqlFirst bool, next string) string {
	prefix := "Patroni"
	if sqlFirst {
		prefix = "SQL"
	}
	if next == "" {
		next = "none"
	}
	return prefix + " leaves B for " + next
}

func TestSQLPotentialDowntimeEligibility(t *testing.T) {
	t.Run("startup", func(t *testing.T) {
		p := &potentialTiming{}
		require.Empty(t, p.observe(true, nil, potentialAt(0)))
		require.Empty(t, p.observe(true, potentialIPs("A"), potentialAt(1)))
		require.Empty(t, p.observe(false, potentialIPs("A"), potentialAt(2)))
		assert.True(t, p.downSince.IsZero())
	})
	t.Run("same leader recovers", func(t *testing.T) {
		p := baselinePotential(t)
		require.Empty(t, p.observe(true, nil, potentialAt(1)))
		require.Empty(t, p.observe(true, potentialIPs("A"), potentialAt(2)))
		assert.True(t, p.downSince.IsZero())
		require.Empty(t, p.observe(true, potentialIPs("B"), potentialAt(3)))
		got := p.observe(false, potentialIPs("B"), potentialAt(4))
		require.Len(t, got, 1)
		assert.False(t, got[0].HasDowntimeComparison)
	})
	t.Run("nonpositive Patroni denominator", func(t *testing.T) {
		p := baselinePotential(t)
		require.Empty(t, p.observe(false, potentialIPs("B"), potentialAt(1)))
		require.Empty(t, p.observe(true, nil, potentialAt(2)))
		got := p.observe(true, potentialIPs("B"), potentialAt(3))
		require.Len(t, got, 1)
		assert.Equal(t, -2*time.Second, got[0].Speedup)
		assert.False(t, got[0].HasDowntimeComparison)
	})
	t.Run("old promotion window", func(t *testing.T) {
		p := baselinePotential(t)
		require.Empty(t, p.observe(true, nil, potentialAt(1)))
		require.Empty(t, p.observe(true, potentialIPs("B"), potentialAt(2)))
		require.Empty(t, p.observe(true, potentialIPs("C"), potentialAt(3)))
		require.Len(t, p.observe(false, potentialIPs("C"), potentialAt(4)), 1)
		require.Empty(t, p.observe(false, potentialIPs("B"), potentialAt(10)))
		got := p.observe(true, potentialIPs("B"), potentialAt(11))
		require.Len(t, got, 1)
		assert.False(t, got[0].HasDowntimeComparison)
	})
}

func TestSQLPotentialCredentialEvidenceAndInvalidation(t *testing.T) {
	for _, invalidation := range []string{"disable", "retire", "evict"} {
		t.Run(invalidation, func(t *testing.T) {
			s := &PoolHealth{}
			a, b := &sqlHealthTarget{}, &sqlHealthTarget{}
			var got []SQLPotentialComparison
			w := &HealthWorker{hooks: HealthWorkerHooks{SQLPotentialCompared: func(c SQLPotentialComparison, _ []string) {
				// The public callback must run after releasing the comparison lock.
				s.sqlComparisonMu.Lock()
				s.sqlComparisonMu.Unlock()
				got = append(got, c)
			}}}
			w.observePatroniPotential(s, potentialIPs("A"))
			w.observeSQLPotential(s, a, 0, SQLHealthCheckResult{IP: "A"})
			w.observeSQLPotential(s, b, 0, SQLHealthCheckResult{IP: "A"})
			w.observeSQLPotential(s, b, 0, SQLHealthCheckResult{IP: "A", Err: context.DeadlineExceeded})
			assert.True(t, s.potentialTiming.downSince.IsZero(), "another credential still succeeds")
			w.observeSQLPotential(s, a, 0, SQLHealthCheckResult{IP: "A", Err: context.DeadlineExceeded})
			assert.False(t, s.potentialTiming.downSince.IsZero(), "only completed failures open the observed gap")
			w.observeSQLPotential(s, a, 0, SQLHealthCheckResult{IP: "B"})
			switch invalidation {
			case "disable":
				s.disableSQLComparisonTarget(a)
			case "retire":
				s.retireSQLComparisonTarget(a)
			case "evict":
				w.prepareSQLPotential(s, a, 0, []string{"C"})
			}
			if invalidation != "evict" {
				w.observeSQLPotential(s, a, 0, SQLHealthCheckResult{IP: "B"})
				assert.NotContains(t, s.potentialResults, a, "late result cannot restore retired/disabled evidence")
			}
			w.observePatroniPotential(s, potentialIPs("B"))
			assert.Empty(t, got, "population change resets pending comparisons")
		})
	}
}

func TestSQLPotentialOptInAndReplicaIsolation(t *testing.T) {
	for _, replica := range []bool{false, true} {
		s := &PoolHealth{isReadReplica: replica}
		w := &HealthWorker{}
		if replica {
			w.hooks.SQLPotentialCompared = func(SQLPotentialComparison, []string) { t.Fatal("replica emitted timing") }
		}
		w.observeSQLPotential(s, &sqlHealthTarget{}, 0, SQLHealthCheckResult{IP: "A"})
		w.observePatroniPotential(s, potentialIPs("A"))
		assert.Nil(t, s.potentialResults)
		assert.Nil(t, s.potentialTiming.patroni.comparable)
	}
}

func TestSQLPotentialWorkerReportsBeforeSlowProbeCompletes(t *testing.T) {
	const oldIP, newIP = "10.0.0.1", "10.0.0.2"
	patroni := newMockProber(map[string][]string{"primary.local": {oldIP, newIP}}, newIP)
	w := newTestWorker(patroni)
	prober := &partiallyBlockingSQLHealthProber{
		blockingIP: oldIP, blockingRecovery: true,
		blocked: make(chan struct{}), release: make(chan struct{}),
	}
	var once sync.Once
	release := func() { once.Do(func() { close(prober.release) }) }
	defer release()
	checker := newTestSQLHealthChecker(t, prober, 1, SQLHealthCheckHooks{})
	w.newSQLChecker = func(context.Context) *SQLHealthChecker { return checker }
	updates := w.Subscribe("primary.local", false, "db", nil, &SQLHealthConfig{ConnStringForIP: func(ip string) string { return ip }})
	s := receivePoolHealth(t, updates)
	t.Cleanup(func() { w.Unsubscribe(updates) })
	comparisons := make(chan SQLPotentialComparison, 1)
	completedSnapshots := make(chan struct{}, 1)
	w.hooks.SQLPatroniCompared = func(SQLPatroniHealthSnapshot, []string) { completedSnapshots <- struct{}{} }
	w.hooks.SQLPotentialCompared = func(c SQLPotentialComparison, _ []string) { comparisons <- c }
	// A completed old-leader baseline, followed by Patroni observing the replacement.
	s.potentialTiming.observe(true, potentialIPs(oldIP), time.Now().Add(-time.Second))
	s.potentialTiming.observe(false, potentialIPs(oldIP), time.Now().Add(-time.Second))
	w.Refresh()
	done := make(chan struct{})
	go func() { w.RefreshSQL(); close(done) }()
	select {
	case <-prober.blocked:
	case <-time.After(time.Second):
		t.Fatal("slow probe did not start")
	}
	select {
	case c := <-comparisons:
		assert.Equal(t, newIP, c.NewLeaderIP)
		assert.LessOrEqual(t, c.Speedup, time.Duration(0), "Patroni was first")
	case <-time.After(time.Second):
		t.Fatal("timing waited for slow IP")
	}
	select {
	case <-done:
		t.Fatal("test did not exercise an incomplete round")
	default:
	}
	select {
	case <-completedSnapshots:
		t.Fatal("completed comparison must still wait for the slow IP")
	default:
	}
	assert.True(t, s.IsHealthy(newIP), "Patroni routing remains authoritative")
	release()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("round did not complete")
	}
}

func TestSQLPotentialRejectsLateResultForEvictedIP(t *testing.T) {
	s := &PoolHealth{sqlHealthState: NewHealthState()}
	s.sqlHealthState.Track([]string{"C"})
	target := &sqlHealthTarget{}
	s.potentialResults = map[*sqlHealthTarget]map[string]bool{target: {"B": true}}
	w := &HealthWorker{hooks: HealthWorkerHooks{SQLPotentialCompared: func(SQLPotentialComparison, []string) { t.Fatal("evicted result emitted timing") }}}
	// An older plan's candidates cannot override current discovery.
	w.prepareSQLPotential(s, target, 0, []string{"B"})
	require.NotContains(t, s.potentialResults[target], "B")
	w.observeSQLPotential(s, target, 0, SQLHealthCheckResult{IP: "B"})
	require.NotContains(t, s.potentialResults[target], "B")
}
