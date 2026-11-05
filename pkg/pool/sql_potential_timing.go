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

// sqlPotentialTimingState owns snapshot timing and its lifecycle guards.
// All access is protected by the owning PoolHealth's sqlComparisonMu.
type sqlPotentialTimingState struct {
	// Track previous primary sets and pair SQL/Patroni timestamps by replacement IP.
	leaderComparisons potentialTiming
	// Remember retained IPs to detect removals and reappearances on the next update.
	previousRetainedIPs map[string]bool
	// Per-IP eviction/rediscovery cutoffs used to reject stale Patroni evidence.
	dnsLifecycleCutoffByIP map[string]time.Time
	// Newest accepted Patroni timestamp; reject observations delivered out of order.
	lastAcceptedPatroniObservedAt time.Time
	// Ignore Patroni timing while invalidation leaves no completed SQL evidence.
	waitingForSQLSnapshot bool
	// First SQL snapshot time after a pause; reject Patroni observations captured earlier.
	sqlTimingResumedAt time.Time
}

// Snapshot timing tracks every newly reported primary independently. It shares
// only timestamp pairing with hybrid timing; its caller holds sqlComparisonMu.
type potentialTiming struct {
	sqlLeaders     map[string]bool
	patroniLeaders map[string]bool
	pending        map[string]leaderTimingPair
}

func (p *potentialTiming) observe(sql bool, healthy map[string]bool, at time.Time) []SQLPotentialComparison {
	current := make(map[string]bool)
	for ip, primary := range healthy {
		if primary {
			current[ip] = true
		}
	}
	if len(current) == 0 {
		return nil
	}
	previous := p.patroniLeaders
	if sql {
		previous = p.sqlLeaders
		p.sqlLeaders = current
	} else {
		p.patroniLeaders = current
	}
	if previous == nil {
		return nil // The first nonempty snapshot establishes this source's baseline.
	}
	var comparisons []SQLPotentialComparison
	for ip := range current {
		if previous[ip] {
			continue
		}
		if p.pending == nil {
			p.pending = make(map[string]leaderTimingPair)
		}
		pair := p.pending[ip]
		if !pair.observe(sql, at) {
			p.pending[ip] = pair
			continue
		}
		delete(p.pending, ip)
		comparisons = append(comparisons, SQLPotentialComparison{
			NewLeaderIP:       ip,
			SQLObservedAt:     pair.candidateAt,
			PatroniObservedAt: pair.patroniAt,
			Speedup:           pair.speedup(),
		})
	}
	return comparisons
}

func (s *PoolHealth) resetSQLPotentialTargetLocked(target *sqlHealthTarget) {
	_, observed := s.sqlComparisonLatest[target]
	s.potentialTiming.resetTarget(observed, len(s.sqlComparisonLatest))
}

// completedTargets is the count before the target is removed.
func (p *sqlPotentialTimingState) resetTarget(observed bool, completedTargets int) {
	if observed {
		// Removing completed evidence changes the aggregate and breaks timing continuity.
		p.leaderComparisons = potentialTiming{}
	}
	if completedTargets == 0 || observed && completedTargets == 1 {
		// With no completed sibling evidence left, suspend Patroni tracking until
		// a valid SQL completion establishes a fresh baseline.
		p.leaderComparisons = potentialTiming{}
		p.waitingForSQLSnapshot = true
	}
}

// Called while accepting a completed poll under sqlComparisonMu, so timing and comparison
// snapshots share lifecycle validation, aggregation, and ordering. Callbacks run after unlock.
func (p *sqlPotentialTimingState) observeSQLSnapshot(snapshot sqlHealthComparisonSnapshot, observedAt time.Time) []SQLPotentialComparison {
	// A delayed completion can clear a conflict using another credential's newer snapshot.
	// Timestamp the aggregate no earlier than any completed evidence it includes, while leaving
	// the individual credential timestamps intact for diagnostics.
	for _, completed := range snapshot.results {
		if completed.ObservedAt.After(observedAt) {
			observedAt = completed.ObservedAt
		}
	}
	if p.waitingForSQLSnapshot {
		p.leaderComparisons = potentialTiming{}
		p.waitingForSQLSnapshot = false
		p.sqlTimingResumedAt = observedAt
	}
	p.updateRetained(snapshot.retainedIPs, observedAt)
	healthy := make(map[string]bool)
	for ip, primary := range snapshot.health {
		if primary {
			healthy[ip] = true
		}
	}
	return p.leaderComparisons.observe(true, healthy, observedAt)
}

// Discovery calls this while holding the same lock as timing observations, before exposing the
// changed retained set to them. Snapshot acceptance also calls it to keep its population current.
func (p *sqlPotentialTimingState) updateRetained(ips []string, at time.Time) {
	retained := make(map[string]bool, len(ips))
	for _, ip := range ips {
		retained[ip] = true
		if cutoff, invalidated := p.dnsLifecycleCutoffByIP[ip]; invalidated && !p.previousRetainedIPs[ip] && at.After(cutoff) {
			// Rediscovery starts another lifecycle. Observations captured while
			// absent must not become eligible merely because their callback is late.
			p.dnsLifecycleCutoffByIP[ip] = at
		}
	}
	for ip := range p.previousRetainedIPs {
		if !retained[ip] {
			if p.dnsLifecycleCutoffByIP == nil {
				p.dnsLifecycleCutoffByIP = make(map[string]time.Time)
			}
			if at.After(p.dnsLifecycleCutoffByIP[ip]) {
				p.dnsLifecycleCutoffByIP[ip] = at
			}
			// Eviction invalidates only this address. A retained replacement can still
			// pair with its pending observation, even if the old leader disappears.
			delete(p.leaderComparisons.pending, ip)
			delete(p.leaderComparisons.sqlLeaders, ip)
			delete(p.leaderComparisons.patroniLeaders, ip)
		}
	}
	p.previousRetainedIPs = retained
}

func (w *HealthWorker) observePatroniPotential(s *PoolHealth, health map[string]bool, observedAt time.Time) {
	if w.hooks.SQLPotentialCompared == nil || s.isReadReplica {
		return
	}
	s.sqlComparisonMu.Lock()
	comparisons := s.potentialTiming.observePatroni(health, observedAt)
	s.sqlComparisonMu.Unlock()
	w.reportSQLPotential(s, comparisons)
}

func (p *sqlPotentialTimingState) observePatroni(health map[string]bool, observedAt time.Time) []SQLPotentialComparison {
	if p.waitingForSQLSnapshot || observedAt.Before(p.sqlTimingResumedAt) || observedAt.Before(p.lastAcceptedPatroniObservedAt) {
		// Callbacks can arrive after a lifecycle gap or after a newer Patroni poll.
		// Reject the old snapshot rather than letting it interrupt current evidence.
		return nil
	}
	p.lastAcceptedPatroniObservedAt = observedAt
	healthy := make(map[string]bool)
	for ip, primary := range health {
		if cutoff, invalidated := p.dnsLifecycleCutoffByIP[ip]; invalidated &&
			(!p.previousRetainedIPs[ip] || !observedAt.After(cutoff)) {
			// A callback from before eviction or during absence cannot recreate a
			// cleared timestamp after rediscovery. While absent, no evidence for
			// this IP can participate. Other retained IPs remain comparable.
			continue
		}
		if primary {
			healthy[ip] = true
		}
	}
	return p.leaderComparisons.observe(false, healthy, observedAt)
}

func (w *HealthWorker) reportSQLPotential(s *PoolHealth, comparisons []SQLPotentialComparison) {
	for _, comparison := range comparisons {
		w.hooks.SQLPotentialCompared(comparison, s.patroniTags)
	}
}
