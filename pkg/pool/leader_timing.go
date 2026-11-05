package pool

import "time"

// leaderTiming pairs independently sampled leader observations. It owns no routing policy,
// clock, or lock: callers select eligible observations and serialize access with their state.
// Metadata follows the originating observation (e.g. a downtime window or hybrid source).
type leaderTiming[T any] struct {
	candidate, reference leaderTimingObservation
	pending              map[string]leaderTimingPair[T]
}

type leaderTimingObservation struct {
	lastNonempty map[string]bool
}

type timedLeader[T any] struct {
	at   time.Time
	data T
}

type leaderTimingPair[T any] struct {
	candidate, reference timedLeader[T]
}

type leaderTimingComparison[T any] struct {
	ip                   string
	candidate, reference timedLeader[T]
}

func (c leaderTimingComparison[T]) speedup() time.Duration { return c.reference.at.Sub(c.candidate.at) }

type leaderTimingSource bool

const (
	candidateLeaderSource leaderTimingSource = true
	referenceLeaderSource leaderTimingSource = false
)

type leaderTimingMode uint8

const (
	// Compare each newly observed primary in the completed comparison snapshot.
	everyNewLeader leaderTimingMode = iota
	// Detection compares only one newly added primary, while retaining the full observed set.
	oneNewLeader
	// Routing requires exactly one healthy primary before recording a selection.
	oneHealthyLeader
)

func (p *leaderTiming[T]) observe(source leaderTimingSource, healthy map[string]bool, at time.Time, data T, mode leaderTimingMode) []leaderTimingComparison[T] {
	// Normalize role maps so standby entries are not mistaken for healthy membership.
	current := make(map[string]bool)
	for ip, primary := range healthy {
		if primary {
			current[ip] = true
		}
	}
	observation := &p.reference
	if source == candidateLeaderSource {
		observation = &p.candidate
	}
	// Preserve the original hybrid matching policy: empty/ineligible observations
	// do not advance the baseline, and unmatched timestamps survive leader changes.
	if len(current) == 0 || mode == oneHealthyLeader && len(current) != 1 {
		return nil
	}
	previous := observation.lastNonempty
	observation.lastNonempty = current
	if previous == nil {
		return nil
	}
	var added []string
	for ip := range current {
		if !previous[ip] {
			added = append(added, ip)
		}
	}
	if mode == oneNewLeader && len(added) != 1 {
		return nil
	}
	var comparisons []leaderTimingComparison[T]
	for _, ip := range added {
		if p.pending == nil {
			p.pending = make(map[string]leaderTimingPair[T])
		}
		pair := p.pending[ip]
		sample := timedLeader[T]{at: at, data: data}
		if source == candidateLeaderSource {
			pair.candidate = sample
		} else {
			pair.reference = sample
		}
		if pair.candidate.at.IsZero() || pair.reference.at.IsZero() {
			p.pending[ip] = pair
			continue
		}
		delete(p.pending, ip)
		comparisons = append(comparisons, leaderTimingComparison[T]{ip: ip, candidate: pair.candidate, reference: pair.reference})
	}
	return comparisons
}
