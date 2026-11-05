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

func TestSQLPotentialSignedTiming(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		sqlSecond, patroniSecond int
	}{
		{"SQL first", 2, 5},
		{"Patroni first", 4, 2},
		{"same time", 3, 3},
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
			require.Empty(t, p.observe(true, potentialIPs("B"), potentialAt(10)))
			require.Empty(t, p.observe(false, potentialIPs("B"), potentialAt(11)))
		})
	}
}

func TestSQLPotentialPreservesFirstSnapshotTimestamp(t *testing.T) {
	p := baselinePotential(t)
	// Repeated completed snapshots do not move the first timestamp for B.
	require.Empty(t, p.observe(true, potentialIPs("A", "B"), potentialAt(1)))
	require.Empty(t, p.observe(true, potentialIPs("A", "B"), potentialAt(2)))
	require.Empty(t, p.observe(false, potentialIPs("A"), potentialAt(3)))
	require.Empty(t, p.observe(true, potentialIPs("B"), potentialAt(4)))
	got := p.observe(false, potentialIPs("B"), potentialAt(5))
	require.Len(t, got, 1)
	assert.Equal(t, 4*time.Second, got[0].Speedup)
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

func TestSQLPotentialStartupAndSameLeaderRecovery(t *testing.T) {
	p := &potentialTiming{}
	require.Empty(t, p.observe(true, nil, potentialAt(0)))
	require.Empty(t, p.observe(true, potentialIPs("A"), potentialAt(1)))
	require.Empty(t, p.observe(false, potentialIPs("A"), potentialAt(2)))
	require.Empty(t, p.observe(true, nil, potentialAt(3)))
	require.Empty(t, p.observe(true, potentialIPs("A"), potentialAt(4)))
	require.Empty(t, p.observe(false, potentialIPs("A"), potentialAt(5)))
}

func recordPotentialPoll(s *PoolHealth, target *sqlHealthTarget, generation uint64, at time.Time, results ...SQLHealthCheckResult) *sqlHealthComparisonSnapshot {
	return s.recordSQLComparisonPoll(true, target, generation, at, at, s.sqlHealthState.Resolved(), SQLHealthCheckOutcome{Results: results})
}

func TestSQLPotentialUsesCompletedComparisonSnapshot(t *testing.T) {
	s := &PoolHealth{sqlHealthState: NewHealthState()}
	s.sqlHealthState.Track([]string{"A", "B"})
	a, b := &sqlHealthTarget{}, &sqlHealthTarget{}
	w := &HealthWorker{hooks: HealthWorkerHooks{SQLPotentialCompared: func(SQLPotentialComparison, []string) {
		s.sqlComparisonMu.Lock()
		s.sqlComparisonMu.Unlock()
	}}}
	w.observePatroniPotential(s, potentialIPs("A"))
	recordPotentialPoll(s, a, 0, potentialAt(0), SQLHealthCheckResult{IP: "A"})
	recordPotentialPoll(s, b, 0, potentialAt(1), SQLHealthCheckResult{IP: "A", Err: context.DeadlineExceeded})
	assert.True(t, s.potentialTiming.sql.comparable["A"], "another credential still supplies a primary")
	// Conflicting successful roles are excluded using the exact mismatch-comparison aggregate.
	snapshot := recordPotentialPoll(s, b, 0, potentialAt(2), SQLHealthCheckResult{IP: "A", InRecovery: true})
	require.Contains(t, snapshot.conflictingIPs, "A")
	require.NotContains(t, snapshot.health, "A")
	snapshot = recordPotentialPoll(s, a, 0, potentialAt(3), SQLHealthCheckResult{IP: "B"})
	require.Empty(t, snapshot.potentialComparisons)
	w.observePatroniPotential(s, potentialIPs("B")) // Callback can acquire the snapshot lock.
	assert.Equal(t, potentialAt(3), s.sqlComparisonLatest[a].ObservedAt)
}

func TestSQLPotentialInvalidationAndOptIn(t *testing.T) {
	for _, invalidation := range []string{"disable", "retire", "evict"} {
		t.Run(invalidation, func(t *testing.T) {
			s := &PoolHealth{sqlHealthState: NewHealthState()}
			s.sqlHealthState.Track([]string{"A", "B"})
			target := &sqlHealthTarget{}
			recordPotentialPoll(s, target, 0, potentialAt(0), SQLHealthCheckResult{IP: "A"})
			s.potentialTiming.observe(false, potentialIPs("A"), potentialAt(0))
			recordPotentialPoll(s, target, 0, potentialAt(1), SQLHealthCheckResult{IP: "B"})
			require.Contains(t, s.potentialTiming.pending, "B")
			switch invalidation {
			case "disable":
				s.disableSQLComparisonTarget(target)
			case "retire":
				s.retireSQLComparisonTarget(target)
			case "evict":
				s.sqlHealthState = NewHealthState()
				s.sqlHealthState.Track([]string{"C"})
			}
			late := recordPotentialPoll(s, target, 0, potentialAt(2), SQLHealthCheckResult{IP: "B"})
			if invalidation == "evict" {
				require.NotNil(t, late)
				assert.Empty(t, late.health, "evicted result remains diagnostic only")
				assert.Empty(t, late.potentialComparisons)
			} else {
				assert.Nil(t, late)
			}
			assert.Empty(t, s.potentialTiming.pending)
			assert.Empty(t, s.potentialTiming.observe(false, potentialIPs("B"), potentialAt(3)))
		})
	}
	for _, replica := range []bool{false, true} {
		s := &PoolHealth{isReadReplica: replica, sqlHealthState: NewHealthState()}
		s.sqlHealthState.Track([]string{"A"})
		s.recordSQLComparisonPoll(replica, &sqlHealthTarget{}, 0, potentialAt(0), potentialAt(0), []string{"A"}, SQLHealthCheckOutcome{Results: []SQLHealthCheckResult{{IP: "A"}}})
		assert.Nil(t, s.potentialTiming.sql.comparable, "replicas and disabled hooks do not track timing")
	}
}

func TestSQLPotentialWorkerWaitsForCompleteSnapshot(t *testing.T) {
	for _, patroniFirst := range []bool{true, false} {
		name := "SQL first"
		if patroniFirst {
			name = "Patroni first"
		}
		t.Run(name, func(t *testing.T) {
			const oldIP, newIP = "10.0.0.1", "10.0.0.2"
			w := newTestWorker(newMockProber(map[string][]string{"primary.local": {oldIP, newIP}}, newIP))
			prober := &partiallyBlockingSQLHealthProber{blockingIP: oldIP, blockingRecovery: true, blocked: make(chan struct{}), release: make(chan struct{})}
			var once sync.Once
			release := func() { once.Do(func() { close(prober.release) }) }
			defer release()
			checker := newTestSQLHealthChecker(t, prober, 1, SQLHealthCheckHooks{})
			w.newSQLChecker = func(context.Context) *SQLHealthChecker { return checker }
			updates := w.Subscribe("primary.local", false, "db", nil, &SQLHealthConfig{ConnStringForIP: func(ip string) string { return ip }})
			s := receivePoolHealth(t, updates)
			t.Cleanup(func() { w.Unsubscribe(updates) })
			availability, err := s.SubscribeSQLHealth()
			require.NoError(t, err)
			t.Cleanup(availability.Close)
			comparisons := make(chan SQLPotentialComparison, 1)
			completed := make(chan SQLPatroniHealthSnapshot, 1)
			w.hooks.SQLPotentialCompared = func(c SQLPotentialComparison, _ []string) { comparisons <- c }
			w.hooks.SQLPatroniCompared = func(c SQLPatroniHealthSnapshot, _ []string) { completed <- c }
			s.potentialTiming.observe(true, potentialIPs(oldIP), time.Now().Add(-time.Second))
			s.potentialTiming.observe(false, potentialIPs(oldIP), time.Now().Add(-time.Second))
			if patroniFirst {
				w.Refresh()
			}
			done := make(chan struct{})
			go func() { w.RefreshSQL(); close(done) }()
			select {
			case <-prober.blocked:
			case <-time.After(time.Second):
				t.Fatal("slow probe did not start")
			}
			require.True(t, receiveSQLHealthObservation(t, availability).Writable, "early availability remains independent")
			select {
			case <-comparisons:
				t.Fatal("individual probe emitted timing before snapshot")
			default:
			}
			select {
			case <-completed:
				t.Fatal("snapshot completed before slow IP")
			default:
			}
			releasedAt := time.Now()
			release()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("round did not finish")
			}
			require.Len(t, completed, 1, "completed poll must publish its comparison snapshot")
			snapshot := <-completed
			require.Len(t, snapshot.SQLResults, 1)
			if !patroniFirst {
				select {
				case <-comparisons:
					t.Fatal("pair emitted before Patroni snapshot")
				default:
				}
				w.Refresh()
			}
			select {
			case c := <-comparisons:
				assert.Equal(t, newIP, c.NewLeaderIP)
				assert.False(t, c.SQLObservedAt.Before(releasedAt), "timestamp must include waiting for the slow IP")
				assert.Equal(t, snapshot.SQLResults[0].ObservedAt, c.SQLObservedAt)
				if patroniFirst {
					assert.Negative(t, c.Speedup)
				} else {
					assert.Positive(t, c.Speedup)
				}
			case <-time.After(time.Second):
				t.Fatal("completed snapshot did not emit timing")
			}
			assert.True(t, s.IsHealthy(newIP), "Patroni still owns routing")
		})
	}
}

func TestSQLPotentialAggregateTimestampIncludesNewerEvidence(t *testing.T) {
	s := &PoolHealth{sqlHealthState: NewHealthState()}
	s.sqlHealthState.Track([]string{"A", "B"})
	early, later := &sqlHealthTarget{}, &sqlHealthTarget{}
	recordPotentialPoll(s, early, 0, potentialAt(0), SQLHealthCheckResult{IP: "A"}, SQLHealthCheckResult{IP: "B", InRecovery: true})
	s.potentialTiming.observe(false, potentialIPs("A"), potentialAt(0))
	require.Empty(t, s.potentialTiming.observe(false, potentialIPs("B"), potentialAt(4)))
	// Accepted first at 5s, but B is still conflicted with the other credential's old snapshot.
	conflict := recordPotentialPoll(s, later, 0, potentialAt(5), SQLHealthCheckResult{IP: "A", InRecovery: true}, SQLHealthCheckResult{IP: "B"})
	require.Contains(t, conflict.conflictingIPs, "B")
	require.Empty(t, conflict.potentialComparisons)
	// This poll completed at 2s but was delayed before acquiring the snapshot lock. It resolves
	// the conflict, so the resulting aggregate must include the other credential's 5s timestamp.
	completed := recordPotentialPoll(s, early, 0, potentialAt(2), SQLHealthCheckResult{IP: "A", InRecovery: true}, SQLHealthCheckResult{IP: "B"})
	require.Len(t, completed.potentialComparisons, 1)
	comparison := completed.potentialComparisons[0]
	assert.Equal(t, potentialAt(5), comparison.SQLObservedAt)
	assert.Equal(t, -time.Second, comparison.Speedup, "cannot report +2s using evidence only available at 5s")
	assert.Equal(t, potentialAt(2), s.sqlComparisonLatest[early].ObservedAt, "preserve the raw credential timestamp")
}

func TestSQLPotentialAgreementDiscardsUnmatchedPromotions(t *testing.T) {
	for _, sqlFirst := range []bool{false, true} {
		t.Run(fmtPotentialCase(sqlFirst, "A"), func(t *testing.T) {
			p := baselinePotential(t)
			require.Empty(t, p.observe(sqlFirst, potentialIPs("B"), potentialAt(1)))
			require.Empty(t, p.observe(sqlFirst, potentialIPs("A"), potentialAt(2)))
			require.Empty(t, p.pending, "returning to the other source's baseline is agreement, not a pending promotion")
			require.Empty(t, p.observe(sqlFirst, potentialIPs("A"), potentialAt(3)))
			require.Empty(t, p.observe(!sqlFirst, potentialIPs("A"), potentialAt(4)))
			require.Empty(t, p.observe(!sqlFirst, potentialIPs("B"), potentialAt(3600)))
			require.Empty(t, p.observe(!sqlFirst, potentialIPs("A"), potentialAt(3601)), "must not emit a one-hour comparison")
			require.Empty(t, p.pending)
			// A later real matched promotion must still produce a fresh comparison.
			require.Empty(t, p.observe(sqlFirst, potentialIPs("B"), potentialAt(3602)))
			got := p.observe(!sqlFirst, potentialIPs("B"), potentialAt(3603))
			require.Len(t, got, 1)
			expected := time.Second
			if !sqlFirst {
				expected = -time.Second
			}
			assert.Equal(t, expected, got[0].Speedup)
		})
	}
}

func TestSQLPotentialAgreementOnInitialBaselineDiscardsPending(t *testing.T) {
	p := &potentialTiming{}
	require.Empty(t, p.observe(true, potentialIPs("A"), potentialAt(0)))
	require.Empty(t, p.observe(true, potentialIPs("B"), potentialAt(1)))
	require.Empty(t, p.observe(false, potentialIPs("B"), potentialAt(2)))
	assert.Empty(t, p.pending, "the second source's first baseline is not a comparable promotion")
}

func TestSQLPotentialDNSInvalidatesBeforeNextCompletion(t *testing.T) {
	w := newTestWorker(newMockProber(map[string][]string{"primary.local": {"C"}}, "C"))
	w.absenceThreshold = 1
	prober := &partiallyBlockingSQLHealthProber{blockingIP: "C", blocked: make(chan struct{}), release: make(chan struct{})}
	var once sync.Once
	release := func() { once.Do(func() { close(prober.release) }) }
	defer release()
	checker := newTestSQLHealthChecker(t, prober, 1, SQLHealthCheckHooks{})
	w.newSQLChecker = func(context.Context) *SQLHealthChecker { return checker }
	updates := w.Subscribe("primary.local", false, "db", nil, &SQLHealthConfig{ConnStringForIP: func(ip string) string { return ip }})
	s := receivePoolHealth(t, updates)
	t.Cleanup(func() { w.Unsubscribe(updates) })
	target := s.subscriptionTargets[updates]
	otherCredential := &sqlHealthTarget{}
	s.sqlHealthState.Track([]string{"A", "B", "C"})
	recordPotentialPoll(s, otherCredential, 0, potentialAt(0), SQLHealthCheckResult{IP: "A"})
	s.potentialTiming.observe(false, potentialIPs("A"), potentialAt(0))
	recordPotentialPoll(s, otherCredential, 0, potentialAt(1), SQLHealthCheckResult{IP: "B"})
	recordPotentialPoll(s, target, 0, potentialAt(2), SQLHealthCheckResult{IP: "C"})
	// Another credential's latest successful C poll makes B eligible for DNS eviction, while
	// the completed comparison cache still contains the independent credential's B success.
	s.sqlHealthState.RecordHealthAt(potentialAt(2), potentialIPs("C"))
	require.Contains(t, s.potentialTiming.pending, "B")
	comparisons := make(chan SQLPotentialComparison, 4)
	w.hooks.SQLPotentialCompared = func(c SQLPotentialComparison, _ []string) { comparisons <- c }
	done := make(chan struct{})
	go func() { w.RefreshSQL(); close(done) }()
	select {
	case <-prober.blocked:
	case <-time.After(time.Second):
		t.Fatal("next SQL poll did not start")
	}
	require.NotContains(t, s.sqlHealthState.Resolved(), "B", "DNS eviction must have occurred")
	w.observePatroniPotential(s, potentialIPs("B"))
	select {
	case c := <-comparisons:
		t.Errorf("Patroni matched pre-eviction evidence before the next SQL completion: %+v", c)
	default:
	}
	release()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("SQL poll did not complete")
	}
}

func TestSQLPotentialAgreementUsesCurrentSnapshot(t *testing.T) {
	for _, sqlFirst := range []bool{false, true} {
		t.Run(fmtPotentialCase(sqlFirst, "A"), func(t *testing.T) {
			p := baselinePotential(t)
			require.Empty(t, p.observe(!sqlFirst, nil, potentialAt(1)))
			require.Empty(t, p.observe(sqlFirst, potentialIPs("B"), potentialAt(2)))
			require.Empty(t, p.observe(sqlFirst, potentialIPs("A"), potentialAt(3)))
			require.Contains(t, p.pending, "A", "the other source's remembered A baseline is not current agreement")
			require.Empty(t, p.observe(!sqlFirst, potentialIPs("A"), potentialAt(4)))
			assert.Empty(t, p.pending, "actual agreement closes the unmatched excursion")
		})
	}
}
