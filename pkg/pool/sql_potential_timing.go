package pool

import (
	"maps"
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

type potentialObservation struct {
	// comparable can be rearmed independently when an unmatched observation is interrupted.
	comparable map[string]bool
}

type potentialPair struct {
	sqlAt, patroniAt time.Time
}

// All methods run under PoolHealth.sqlComparisonMu. State is separate from routing and from the
// completed snapshots used by SQLPatroniCompared, so telemetry cannot change either consumer.
type potentialTiming struct {
	sql, patroni potentialObservation
	pending      map[string]potentialPair
}

func (p *potentialTiming) observe(sql bool, healthy map[string]bool, now time.Time) []SQLPotentialComparison {
	o := &p.patroni
	if sql {
		o = &p.sql
	}
	// An address that this source stopped reporting cannot pair with its later promotion.
	// Do not discard a newer observation belonging only to the other source.
	for ip, pair := range p.pending {
		owned := sql && !pair.sqlAt.IsZero() || !sql && !pair.patroniAt.IsZero()
		if owned && !healthy[ip] {
			delete(p.pending, ip)
			delete(o.comparable, ip)
		}
	}
	if len(healthy) == 0 {
		return nil
	}
	if o.comparable == nil {
		o.comparable = maps.Clone(healthy)
		return nil // First nonempty observation is a baseline, not a failover.
	}
	var comparisons []SQLPotentialComparison
	for ip := range healthy {
		if o.comparable[ip] {
			continue
		}
		if p.pending == nil {
			p.pending = make(map[string]potentialPair)
		}
		pair := p.pending[ip]
		if sql {
			pair.sqlAt = now
		} else {
			pair.patroniAt = now
		}
		if pair.sqlAt.IsZero() || pair.patroniAt.IsZero() {
			p.pending[ip] = pair
			continue
		}
		delete(p.pending, ip)
		c := SQLPotentialComparison{
			NewLeaderIP: ip, SQLObservedAt: pair.sqlAt, PatroniObservedAt: pair.patroniAt,
			Speedup: pair.patroniAt.Sub(pair.sqlAt),
		}
		comparisons = append(comparisons, c)
	}
	o.comparable = maps.Clone(healthy)
	return comparisons
}

func (s *PoolHealth) resetSQLPotentialTargetLocked(target *sqlHealthTarget) {
	if _, observed := s.sqlComparisonLatest[target]; observed {
		// Reset before removing the completed snapshot. A change in the observation population
		// breaks continuity; do not compare across disable or retirement gaps.
		s.potentialTiming = potentialTiming{}
	}
}

// Called while accepting a completed poll under sqlComparisonMu, so timing and comparison
// snapshots share lifecycle validation, aggregation, and ordering. Callbacks run after unlock.
func (s *PoolHealth) observeSQLPotentialSnapshotLocked(snapshot sqlHealthComparisonSnapshot, observedAt time.Time) []SQLPotentialComparison {
	retained := make(map[string]bool, len(snapshot.retainedIPs))
	for _, ip := range snapshot.retainedIPs {
		retained[ip] = true
	}
	for ip := range s.potentialRetainedIPs {
		if !retained[ip] {
			// DNS eviction is not a SQL failure. Rebaseline rather than compare across a changed address set.
			s.potentialTiming = potentialTiming{}
			break
		}
	}
	s.potentialRetainedIPs = retained
	healthy := make(map[string]bool)
	for ip, primary := range snapshot.health {
		if primary {
			healthy[ip] = true
		}
	}
	return s.potentialTiming.observe(true, healthy, observedAt)
}

func (w *HealthWorker) observePatroniPotential(s *PoolHealth, health map[string]bool) {
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
	comparisons := s.potentialTiming.observe(false, healthy, time.Now())
	s.sqlComparisonMu.Unlock()
	w.reportSQLPotential(s, comparisons)
}

func (w *HealthWorker) reportSQLPotential(s *PoolHealth, comparisons []SQLPotentialComparison) {
	for _, comparison := range comparisons {
		w.hooks.SQLPotentialCompared(comparison, s.patroniTags)
	}
}
