package pool

import (
	"time"
)

// SQLPotentialComparison compares completed SQL and Patroni snapshots for a replacement primary.
// SQL uses the same aggregate as SQLPatroniCompared: the completing credential's full multi-IP
// poll plus the other credentials' latest completed polls. No individual probe advances timing.
// This remains a comparison signal, not proof that routing there is safe or that clients recovered.
//
// Speedup is PatroniObservedAt - SQLObservedAt; negative values expose SQL regressions.
// Keep the signed distribution and sample count as well as maxima. This callback intentionally
// contains no downtime or percentage fields; it does not define a separate outage interval.
type SQLPotentialComparison struct {
	NewLeaderIP       string
	SQLObservedAt     time.Time
	PatroniObservedAt time.Time
	Speedup           time.Duration
}

// The snapshot adapter shares leader matching with hybrid detection and routing. Its caller
// holds sqlComparisonMu; the generic tracker itself owns neither synchronization nor policy.
type potentialTiming struct {
	leaderTiming[struct{}]
}

func (p *potentialTiming) observe(sql bool, healthy map[string]bool, at time.Time) []SQLPotentialComparison {
	source := referenceLeaderSource
	if sql {
		source = candidateLeaderSource
	}
	pairs := p.leaderTiming.observe(source, healthy, at, struct{}{}, everyNewLeader)
	var comparisons []SQLPotentialComparison
	for _, pair := range pairs {
		comparisons = append(comparisons, SQLPotentialComparison{
			NewLeaderIP:       pair.ip,
			SQLObservedAt:     pair.candidate.at,
			PatroniObservedAt: pair.reference.at,
			Speedup:           pair.speedup(),
		})
	}
	return comparisons
}

func (s *PoolHealth) resetSQLPotentialTargetLocked(target *sqlHealthTarget) {
	_, observed := s.sqlComparisonLatest[target]
	if observed {
		// Removing completed evidence changes the aggregate and breaks timing continuity.
		s.potentialTiming = potentialTiming{}
	}
	if len(s.sqlComparisonLatest) == 0 || observed && len(s.sqlComparisonLatest) == 1 {
		// This runs before deletion. With no completed sibling evidence left, suspend
		// Patroni tracking until a valid SQL completion establishes a fresh baseline.
		s.potentialTiming = potentialTiming{}
		s.potentialPaused = true
	}
}

// Called while accepting a completed poll under sqlComparisonMu, so timing and comparison
// snapshots share lifecycle validation, aggregation, and ordering. Callbacks run after unlock.
func (s *PoolHealth) observeSQLPotentialSnapshotLocked(snapshot sqlHealthComparisonSnapshot, observedAt time.Time) []SQLPotentialComparison {
	// A delayed completion can clear a conflict using another credential's newer snapshot.
	// Timestamp the aggregate no earlier than any completed evidence it includes, while leaving
	// the individual credential timestamps intact for diagnostics.
	for _, completed := range snapshot.results {
		if completed.ObservedAt.After(observedAt) {
			observedAt = completed.ObservedAt
		}
	}
	if s.potentialPaused {
		s.potentialTiming = potentialTiming{}
		s.potentialPaused = false
		s.potentialResumedAt = observedAt
	}
	s.updateSQLPotentialRetainedLocked(snapshot.retainedIPs)
	healthy := make(map[string]bool)
	for ip, primary := range snapshot.health {
		if primary {
			healthy[ip] = true
		}
	}
	return s.potentialTiming.observe(true, healthy, observedAt)
}

// Discovery calls this while holding the same lock as timing observations, before exposing the
// changed retained set to them. Snapshot acceptance also calls it to keep its population current.
func (s *PoolHealth) updateSQLPotentialRetainedLocked(ips []string) {
	retained := make(map[string]bool, len(ips))
	for _, ip := range ips {
		retained[ip] = true
	}
	for ip := range s.potentialRetainedIPs {
		if !retained[ip] {
			// Eviction invalidates only this address. A retained replacement can still
			// pair with its pending observation, even if the old leader disappears.
			delete(s.potentialTiming.pending, ip)
			delete(s.potentialTiming.candidate.current, ip)
			delete(s.potentialTiming.candidate.lastNonempty, ip)
			delete(s.potentialTiming.candidate.comparable, ip)
			delete(s.potentialTiming.reference.current, ip)
			delete(s.potentialTiming.reference.lastNonempty, ip)
			delete(s.potentialTiming.reference.comparable, ip)
		}
	}
	s.potentialRetainedIPs = retained
}

func (w *HealthWorker) observePatroniPotential(s *PoolHealth, health map[string]bool, observedAt time.Time) {
	if w.hooks.SQLPotentialCompared == nil || s.isReadReplica {
		return
	}
	healthy := make(map[string]bool)
	for ip, primary := range health {
		if primary {
			healthy[ip] = true
		}
	}
	s.sqlComparisonMu.Lock()
	if s.potentialPaused || observedAt.Before(s.potentialResumedAt) {
		// A Patroni poll completed during the gap can be delayed by callbacks or this lock.
		s.sqlComparisonMu.Unlock()
		return
	}
	comparisons := s.potentialTiming.observe(false, healthy, observedAt)
	s.sqlComparisonMu.Unlock()
	w.reportSQLPotential(s, comparisons)
}

func (w *HealthWorker) reportSQLPotential(s *PoolHealth, comparisons []SQLPotentialComparison) {
	for _, comparison := range comparisons {
		w.hooks.SQLPotentialCompared(comparison, s.patroniTags)
	}
}
