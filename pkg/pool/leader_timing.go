package pool

import (
	"maps"
	"time"
)

// leaderTiming pairs independently sampled leader observations. It owns no routing policy,
// clock, or lock: callers select eligible observations and serialize access with their state.
// Metadata follows the originating observation (e.g. a downtime window or hybrid source).
type leaderTiming[T any] struct {
	candidate, reference leaderTimingObservation
	pending              map[string]leaderTimingPair[T]
}

type leaderTimingObservation struct {
	// current includes empty observations and is used only to detect agreement.
	current map[string]bool
	// lastNonempty supports policy-owned same-leader recovery accounting.
	lastNonempty map[string]bool
	// comparable can be rearmed independently when an unmatched observation is interrupted.
	comparable map[string]bool
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
	observation.current = current
	if mode == oneHealthyLeader && len(current) != 1 {
		// Ambiguous routing evidence cannot establish agreement on a selected leader.
		observation.current = nil
	}
	defer p.discardAgreedPending()
	for ip, pair := range p.pending {
		owned := source == candidateLeaderSource && !pair.candidate.at.IsZero() || source == referenceLeaderSource && !pair.reference.at.IsZero()
		if owned && !current[ip] {
			delete(p.pending, ip)
			delete(observation.comparable, ip)
		}
	}
	if len(current) == 0 || mode == oneHealthyLeader && len(current) != 1 {
		return nil
	}
	observation.lastNonempty = maps.Clone(current)
	if observation.comparable == nil {
		observation.comparable = maps.Clone(current)
		return nil
	}
	var added []string
	for ip := range current {
		if !observation.comparable[ip] {
			added = append(added, ip)
		}
	}
	observation.comparable = maps.Clone(current)
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

func (p *leaderTiming[T]) discardAgreedPending() {
	for ip := range p.pending {
		if p.candidate.current[ip] && p.reference.current[ip] {
			delete(p.pending, ip)
		}
	}
}
