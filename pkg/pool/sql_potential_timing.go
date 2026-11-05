package pool

import (
	"maps"
	"slices"
	"time"
)

// SQLPotentialComparison compares the earliest successful SQL probe of a replacement primary
// with the completed Patroni snapshot that reports that address healthy. Unlike a hybrid decision,
// SQL need not wait for other IPs or credentials, or rule out conflicting primaries. This is an
// optimistic observation bound, not proof that routing there is safe or that clients recovered.
//
// Speedup is PatroniObservedAt - SQLObservedAt; negative values expose SQL regressions. A paired
// downtime comparison uses the same SQL-observed availability gap start for both durations. Errors
// (including timeouts) remove that credential/IP's successful evidence; missed polls do not. This
// differs from PR9's conclusive no-leader window and must not be combined with it as one series.
// Consumers may aggregate maxima, but should also retain the signed distribution and sample count.
type SQLPotentialComparison struct {
	NewLeaderIP              string
	SQLObservedAt            time.Time
	PatroniObservedAt        time.Time
	Speedup                  time.Duration
	DowntimeStartedAt        time.Time
	SQLDowntime              time.Duration
	PatroniDowntime          time.Duration
	DowntimeReductionPercent float64
	HasDowntimeComparison    bool
}

type potentialObservation struct {
	// lastNonempty excludes startup and same-leader recovery from downtime comparisons.
	lastNonempty map[string]bool
	// comparable can be rearmed independently when an unmatched observation is interrupted.
	comparable map[string]bool
}

type potentialPair struct {
	sqlAt, patroniAt time.Time
	downtimeStart    time.Time
}

// All methods run under PoolHealth.sqlComparisonMu. State is separate from routing and from the
// completed snapshots used by SQLPatroniCompared, so telemetry cannot change either consumer.
type potentialTiming struct {
	sql, patroni potentialObservation
	pending      map[string]potentialPair
	downSince    time.Time
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
		if sql && len(o.lastNonempty) > 0 && p.downSince.IsZero() {
			p.downSince = now
		}
		return nil
	}

	start := p.downSince
	if sql {
		p.downSince = time.Time{}
	}
	previous := o.lastNonempty
	o.lastNonempty = maps.Clone(healthy)
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
			if !previous[ip] {
				pair.downtimeStart = start
			}
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
		if !pair.downtimeStart.IsZero() {
			sqlDown, patroniDown := pair.sqlAt.Sub(pair.downtimeStart), pair.patroniAt.Sub(pair.downtimeStart)
			if sqlDown >= 0 && patroniDown > 0 {
				c.DowntimeStartedAt = pair.downtimeStart
				c.SQLDowntime, c.PatroniDowntime = sqlDown, patroniDown
				c.DowntimeReductionPercent = 100 * float64(patroniDown-sqlDown) / float64(patroniDown)
				c.HasDowntimeComparison = true
			}
		}
		comparisons = append(comparisons, c)
	}
	o.comparable = maps.Clone(healthy)
	return comparisons
}

func (s *PoolHealth) resetSQLPotentialTargetLocked(target *sqlHealthTarget) {
	if _, observed := s.potentialResults[target]; observed {
		delete(s.potentialResults, target)
		// Changing the observation population breaks continuity. Rebaseline instead of comparing
		// across feature disable, credential retirement, or re-enable gaps.
		s.potentialTiming = potentialTiming{}
	}
}

func (w *HealthWorker) prepareSQLPotential(s *PoolHealth, target *sqlHealthTarget, generation uint64, ips []string) {
	if w.hooks.SQLPotentialCompared == nil || s.isReadReplica {
		return
	}
	s.sqlComparisonMu.Lock()
	defer s.sqlComparisonMu.Unlock()
	if target.sqlComparisonRetired || generation != target.sqlComparisonGeneration {
		return
	}
	if s.sqlHealthState != nil {
		ips = s.sqlHealthState.Resolved()
	}
	retained := make(map[string]bool, len(ips))
	for _, ip := range ips {
		retained[ip] = true
	}
	// A DNS eviction is not a SQL failure. Drop obsolete evidence and reset timing continuity.
	for _, results := range s.potentialResults {
		for ip := range results {
			if !retained[ip] {
				delete(results, ip)
				s.potentialTiming = potentialTiming{}
			}
		}
	}
}

func (w *HealthWorker) observeSQLPotential(s *PoolHealth, target *sqlHealthTarget, generation uint64, result SQLHealthCheckResult) {
	if w.hooks.SQLPotentialCompared == nil || s.isReadReplica {
		return
	}
	s.sqlComparisonMu.Lock()
	if target.sqlComparisonRetired || generation != target.sqlComparisonGeneration {
		s.sqlComparisonMu.Unlock()
		return
	}
	if s.sqlHealthState != nil && !slices.Contains(s.sqlHealthState.Resolved(), result.IP) {
		s.sqlComparisonMu.Unlock()
		return // Discovery has evicted this address while its probe was running.
	}
	if s.potentialResults == nil {
		s.potentialResults = make(map[*sqlHealthTarget]map[string]bool)
	}
	if s.potentialResults[target] == nil {
		s.potentialResults[target] = make(map[string]bool)
	}
	s.potentialResults[target][result.IP] = result.HealthyPrimary()
	healthy := make(map[string]bool)
	for _, results := range s.potentialResults {
		for ip, primary := range results {
			if primary {
				healthy[ip] = true
			}
		}
	}
	comparisons := s.potentialTiming.observe(true, healthy, time.Now())
	s.sqlComparisonMu.Unlock()
	w.reportSQLPotential(s, comparisons)
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
