package pool

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	w.observePatroniPotential(s, leaderTimingIPs("A"), time.Now())
	recordPotentialPoll(s, a, 0, leaderTimingAt(0), SQLHealthCheckResult{IP: "A"})
	recordPotentialPoll(s, b, 0, leaderTimingAt(1), SQLHealthCheckResult{IP: "A", Err: context.DeadlineExceeded})
	assert.True(t, s.potentialTiming.candidate.comparable["A"], "another credential still supplies a primary")
	// Conflicting successful roles are excluded using the exact mismatch-comparison aggregate.
	snapshot := recordPotentialPoll(s, b, 0, leaderTimingAt(2), SQLHealthCheckResult{IP: "A", InRecovery: true})
	require.Contains(t, snapshot.conflictingIPs, "A")
	require.NotContains(t, snapshot.health, "A")
	snapshot = recordPotentialPoll(s, a, 0, leaderTimingAt(3), SQLHealthCheckResult{IP: "B"})
	require.Empty(t, snapshot.potentialComparisons)
	w.observePatroniPotential(s, leaderTimingIPs("B"), time.Now()) // Callback can acquire the snapshot lock.
	assert.Equal(t, leaderTimingAt(3), s.sqlComparisonLatest[a].ObservedAt)
}

func TestSQLPotentialTargetInvalidationAndOptIn(t *testing.T) {
	for _, invalidation := range []string{"disable", "retire"} {
		t.Run(invalidation, func(t *testing.T) {
			s := &PoolHealth{sqlHealthState: NewHealthState()}
			s.sqlHealthState.Track([]string{"A", "B"})
			target := &sqlHealthTarget{}
			recordPotentialPoll(s, target, 0, leaderTimingAt(0), SQLHealthCheckResult{IP: "A"})
			s.potentialTiming.observe(false, leaderTimingIPs("A"), leaderTimingAt(0))
			recordPotentialPoll(s, target, 0, leaderTimingAt(1), SQLHealthCheckResult{IP: "B"})
			require.Contains(t, s.potentialTiming.pending, "B")
			switch invalidation {
			case "disable":
				s.disableSQLComparisonTarget(target)
			case "retire":
				s.retireSQLComparisonTarget(target)
			}
			late := recordPotentialPoll(s, target, 0, leaderTimingAt(2), SQLHealthCheckResult{IP: "B"})
			assert.Nil(t, late)
			assert.Empty(t, s.potentialTiming.pending)
			assert.Empty(t, s.potentialTiming.observe(false, leaderTimingIPs("B"), leaderTimingAt(3)))
		})
	}
	for _, replica := range []bool{false, true} {
		s := &PoolHealth{isReadReplica: replica, sqlHealthState: NewHealthState()}
		s.sqlHealthState.Track([]string{"A"})
		s.recordSQLComparisonPoll(replica, &sqlHealthTarget{}, 0, leaderTimingAt(0), leaderTimingAt(0), []string{"A"}, SQLHealthCheckOutcome{Results: []SQLHealthCheckResult{{IP: "A"}}})
		assert.Nil(t, s.potentialTiming.candidate.comparable, "replicas and disabled hooks do not track timing")
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
			s.potentialTiming.observe(true, leaderTimingIPs(oldIP), time.Now().Add(-time.Second))
			s.potentialTiming.observe(false, leaderTimingIPs(oldIP), time.Now().Add(-time.Second))
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
	recordPotentialPoll(s, early, 0, leaderTimingAt(0), SQLHealthCheckResult{IP: "A"}, SQLHealthCheckResult{IP: "B", InRecovery: true})
	s.potentialTiming.observe(false, leaderTimingIPs("A"), leaderTimingAt(0))
	require.Empty(t, s.potentialTiming.observe(false, leaderTimingIPs("B"), leaderTimingAt(4)))
	// Accepted first at 5s, but B is still conflicted with the other credential's old snapshot.
	conflict := recordPotentialPoll(s, later, 0, leaderTimingAt(5), SQLHealthCheckResult{IP: "A", InRecovery: true}, SQLHealthCheckResult{IP: "B"})
	require.Contains(t, conflict.conflictingIPs, "B")
	require.Empty(t, conflict.potentialComparisons)
	// This poll completed at 2s but was delayed before acquiring the snapshot lock. It resolves
	// the conflict, so the resulting aggregate must include the other credential's 5s timestamp.
	completed := recordPotentialPoll(s, early, 0, leaderTimingAt(2), SQLHealthCheckResult{IP: "A", InRecovery: true}, SQLHealthCheckResult{IP: "B"})
	require.Len(t, completed.potentialComparisons, 1)
	comparison := completed.potentialComparisons[0]
	assert.Equal(t, leaderTimingAt(5), comparison.SQLObservedAt)
	assert.Equal(t, -time.Second, comparison.Speedup, "cannot report +2s using evidence only available at 5s")
	assert.Equal(t, leaderTimingAt(2), s.sqlComparisonLatest[early].ObservedAt, "preserve the raw credential timestamp")
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
	recordPotentialPoll(s, otherCredential, 0, leaderTimingAt(0), SQLHealthCheckResult{IP: "A"})
	s.potentialTiming.observe(false, leaderTimingIPs("A"), leaderTimingAt(0))
	recordPotentialPoll(s, otherCredential, 0, leaderTimingAt(1), SQLHealthCheckResult{IP: "B"})
	recordPotentialPoll(s, target, 0, leaderTimingAt(2), SQLHealthCheckResult{IP: "C"})
	// Another credential's latest successful C poll makes B eligible for DNS eviction, while
	// the completed comparison cache still contains the independent credential's B success.
	s.sqlHealthState.RecordHealthAt(leaderTimingAt(2), leaderTimingIPs("C"))
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
	w.observePatroniPotential(s, leaderTimingIPs("B"), time.Now())
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

func TestSQLPotentialPatroniTimestampPrecedesCallbacks(t *testing.T) {
	const oldIP, newIP = "10.0.0.1", "10.0.0.2"
	w := newTestWorker(newMockProber(map[string][]string{"primary.local": {oldIP, newIP}}, newIP))
	s := newTestPoolHealth(oldIP, newIP)
	s.host = "primary.local"
	s.sqlHealthState = NewHealthState()
	s.sqlHealthState.Track([]string{oldIP, newIP})
	target := &sqlHealthTarget{}
	baseline := time.Now().Add(-time.Second)
	recordPotentialPoll(s, target, 0, baseline, SQLHealthCheckResult{IP: oldIP})
	s.potentialTiming.observe(false, leaderTimingIPs(oldIP), baseline)
	var got []SQLPotentialComparison
	var callbackAt, sqlAt time.Time
	w.hooks.SQLPotentialCompared = func(c SQLPotentialComparison, _ []string) { got = append(got, c) }
	w.hooks.CheckFinished = func(time.Duration, []string) {
		callbackAt = time.Now()
		// SQL completes while Patroni is still processing its completion callback.
		sqlAt = time.Now()
		snapshot := recordPotentialPoll(s, target, 0, sqlAt, SQLHealthCheckResult{IP: newIP})
		w.reportSQLPotential(s, snapshot.potentialComparisons)
	}
	w.checkPoolHealth(s)
	require.Len(t, got, 1)
	assert.False(t, got[0].PatroniObservedAt.After(callbackAt), "completion time must precede callbacks and cleanup")
	assert.Equal(t, sqlAt, got[0].SQLObservedAt)
	assert.Less(t, got[0].Speedup, time.Duration(0), "Patroni completed first, even though its callback returned later")
}

func TestSQLPotentialEvictOldLeaderPreservesReplacement(t *testing.T) {
	for _, sqlFirst := range []bool{true, false} {
		t.Run(leaderTimingCase(sqlFirst, "B"), func(t *testing.T) {
			s := &PoolHealth{sqlHealthState: NewHealthState()}
			s.sqlHealthState.Track([]string{"A", "B"})
			target := &sqlHealthTarget{}
			var got []SQLPotentialComparison
			w := &HealthWorker{hooks: HealthWorkerHooks{SQLPotentialCompared: func(c SQLPotentialComparison, _ []string) { got = append(got, c) }}}
			recordPotentialPoll(s, target, 0, leaderTimingAt(0), SQLHealthCheckResult{IP: "A"})
			w.observePatroniPotential(s, leaderTimingIPs("A"), leaderTimingAt(0))
			s.sqlHealthState.RecordHealthAt(leaderTimingAt(0), leaderTimingIPs("A"))
			require.ElementsMatch(t, []string{"A", "B"}, s.refreshSQLResolved([]string{"B"}, 1), "absent old primary stays protected")
			if sqlFirst {
				recordPotentialPoll(s, target, 0, leaderTimingAt(2), SQLHealthCheckResult{IP: "B"})
			} else {
				w.observePatroniPotential(s, leaderTimingIPs("B"), leaderTimingAt(2))
			}
			// A loses retention protection when SQL discovers B; evict A before the second observation.
			s.sqlHealthState.RecordHealthAt(leaderTimingAt(3), leaderTimingIPs("B"))
			require.Equal(t, []string{"B"}, s.refreshSQLResolved([]string{"B"}, 1))
			require.Contains(t, s.potentialTiming.pending, "B", "evict only A's timing evidence")
			if sqlFirst {
				w.observePatroniPotential(s, leaderTimingIPs("B"), leaderTimingAt(5))
			} else {
				got = recordPotentialPoll(s, target, 0, leaderTimingAt(5), SQLHealthCheckResult{IP: "B"}).potentialComparisons
			}
			require.Len(t, got, 1)
			want := 3 * time.Second
			if !sqlFirst {
				want = -want
			}
			assert.Equal(t, want, got[0].Speedup)
		})
	}
}

func TestSQLPotentialEvictionRejectsDelayedPatroniEvidence(t *testing.T) {
	for _, startup := range []bool{true, false} {
		phase := "established SQL baseline"
		if startup {
			phase = "before first SQL completion"
		}
		for _, delivery := range []string{"before rediscovery", "after rediscovery", "while absent", "gap callback after rediscovery", "after fresh Patroni"} {
			t.Run(phase+"/"+delivery, func(t *testing.T) {
				s := &PoolHealth{sqlHealthState: NewHealthState()}
				var got []SQLPotentialComparison
				w := &HealthWorker{hooks: HealthWorkerHooks{SQLPotentialCompared: func(c SQLPotentialComparison, _ []string) { got = append(got, c) }}}
				target := &sqlHealthTarget{}
				s.refreshSQLResolved([]string{"A", "B", "C"}, 1)
				baseline := time.Now().Add(-time.Minute)
				if !startup {
					recordPotentialPoll(s, target, 0, baseline, SQLHealthCheckResult{IP: "A"})
				}
				w.observePatroniPotential(s, leaderTimingIPs("A"), baseline)
				w.observePatroniPotential(s, leaderTimingIPs("B"), baseline.Add(time.Second))
				require.Contains(t, s.potentialTiming.pending, "B")

				// The poll completed before eviction, but its callback has not run yet.
				capturedAt := time.Now()
				s.refreshSQLResolved([]string{"C"}, 1)
				require.NotContains(t, s.potentialTiming.pending, "B")
				afterEviction := time.Now()
				recordPotentialPoll(s, target, 0, afterEviction, SQLHealthCheckResult{IP: "C"})

				if delivery == "while absent" || delivery == "gap callback after rediscovery" {
					capturedAt = afterEviction // New evidence is also ineligible while B is absent.
				}
				if delivery == "before rediscovery" || delivery == "while absent" {
					w.observePatroniPotential(s, leaderTimingIPs("B"), capturedAt)
				}
				s.refreshSQLResolved([]string{"B", "C"}, 1)
				var freshPatroniAt time.Time
				if delivery == "after fresh Patroni" {
					freshPatroniAt = time.Now()
					w.observePatroniPotential(s, leaderTimingIPs("B"), freshPatroniAt)
				}
				if delivery == "after rediscovery" || delivery == "gap callback after rediscovery" || delivery == "after fresh Patroni" {
					w.observePatroniPotential(s, leaderTimingIPs("B"), capturedAt)
				}
				if freshPatroniAt.IsZero() {
					assert.NotContains(t, s.potentialTiming.pending, "B", "late Patroni evidence must not recreate the evicted timestamp")
				}
				sqlAt := time.Now()
				snapshot := recordPotentialPoll(s, target, 0, sqlAt, SQLHealthCheckResult{IP: "B"})
				if !freshPatroniAt.IsZero() {
					require.Len(t, snapshot.potentialComparisons, 1, "the late callback must not discard fresh Patroni evidence")
					assert.Equal(t, freshPatroniAt, snapshot.potentialComparisons[0].PatroniObservedAt)
					assert.Equal(t, freshPatroniAt.Sub(sqlAt), snapshot.potentialComparisons[0].Speedup)
					return
				}
				require.Empty(t, snapshot.potentialComparisons, "do not pair post-rediscovery SQL with the old Patroni lifecycle")
				require.Empty(t, got)

				w.observePatroniPotential(s, leaderTimingIPs("B"), sqlAt.Add(time.Second))
				require.Len(t, got, 1, "fresh evidence after rediscovery must still compare")
				assert.Equal(t, sqlAt, got[0].SQLObservedAt)
				assert.Equal(t, time.Second, got[0].Speedup)
			})
		}
	}
}

func TestSQLPotentialDisableGapRequiresFreshPatroniEvidence(t *testing.T) {
	for _, previouslyCompleted := range []bool{false, true} {
		name := "before first SQL completion"
		if previouslyCompleted {
			name = "after SQL baseline"
		}
		t.Run(name, func(t *testing.T) {
			s := &PoolHealth{sqlHealthState: NewHealthState()}
			s.sqlHealthState.Track([]string{"A", "B", "C"})
			target := &sqlHealthTarget{}
			var got []SQLPotentialComparison
			w := &HealthWorker{hooks: HealthWorkerHooks{SQLPotentialCompared: func(c SQLPotentialComparison, _ []string) { got = append(got, c) }}}
			if previouslyCompleted {
				recordPotentialPoll(s, target, 0, leaderTimingAt(0), SQLHealthCheckResult{IP: "A"})
			}
			w.observePatroniPotential(s, leaderTimingIPs("A"), leaderTimingAt(0))
			s.disableSQLComparisonTarget(target)
			w.observePatroniPotential(s, leaderTimingIPs("A"), leaderTimingAt(2))
			s.disableSQLComparisonTarget(target) // Repeated disabled tick with no cached SQL snapshot.
			w.observePatroniPotential(s, leaderTimingIPs("B"), leaderTimingAt(3))
			generation := s.currentSQLComparisonGeneration(target)
			recordPotentialPoll(s, target, generation, leaderTimingAt(100), SQLHealthCheckResult{IP: "A"})
			// A callback captured during the gap may only acquire the observation lock after resume.
			w.observePatroniPotential(s, leaderTimingIPs("A"), leaderTimingAt(2))
			w.observePatroniPotential(s, leaderTimingIPs("B"), leaderTimingAt(3))
			snapshot := recordPotentialPoll(s, target, generation, leaderTimingAt(101), SQLHealthCheckResult{IP: "B"})
			require.Empty(t, snapshot.potentialComparisons, "must not report a -98s sample spanning disabled checks")
			require.Empty(t, got)
			// Establish a fresh reference baseline, then verify a later comparable promotion.
			w.observePatroniPotential(s, leaderTimingIPs("B"), leaderTimingAt(102))
			recordPotentialPoll(s, target, generation, leaderTimingAt(103), SQLHealthCheckResult{IP: "C"})
			w.observePatroniPotential(s, leaderTimingIPs("C"), leaderTimingAt(104))
			require.Len(t, got, 1)
			assert.Equal(t, time.Second, got[0].Speedup)
		})
	}
}

func TestSQLPotentialDisabledSiblingPreservesActiveTiming(t *testing.T) {
	s := &PoolHealth{sqlHealthState: NewHealthState()}
	s.sqlHealthState.Track([]string{"A", "B"})
	active, disabled := &sqlHealthTarget{}, &sqlHealthTarget{}
	recordPotentialPoll(s, active, 0, leaderTimingAt(0), SQLHealthCheckResult{IP: "A"})
	var got []SQLPotentialComparison
	w := &HealthWorker{hooks: HealthWorkerHooks{SQLPotentialCompared: func(c SQLPotentialComparison, _ []string) { got = append(got, c) }}}
	w.observePatroniPotential(s, leaderTimingIPs("A"), leaderTimingAt(0))
	recordPotentialPoll(s, active, 0, leaderTimingAt(1), SQLHealthCheckResult{IP: "B"})
	s.disableSQLComparisonTarget(disabled)
	s.disableSQLComparisonTarget(disabled)
	w.observePatroniPotential(s, leaderTimingIPs("B"), leaderTimingAt(2))
	require.Len(t, got, 1)
	assert.Equal(t, time.Second, got[0].Speedup)
}
