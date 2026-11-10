// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

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
	pending              map[string]leaderTimingRecord[T]
}

type leaderTimingObservation struct {
	// current includes empty observations and is used only to detect agreement.
	current map[string]bool
	// lastNonempty supports policy-owned same-leader recovery accounting.
	lastNonempty map[string]bool
	// comparable can be rearmed independently when an unmatched observation is interrupted.
	comparable map[string]bool
}

// leaderTimingRecord adds observation metadata to the shared timestamp pair.
type leaderTimingRecord[T any] struct {
	leaderTimingPair
	candidateData, referenceData T
}

type leaderTimingComparison[T any] struct {
	leaderTimingRecord[T]
	ip string
}

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
		owned := source == candidateLeaderSource && !pair.candidateAt.IsZero() || source == referenceLeaderSource && !pair.patroniAt.IsZero()
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
			p.pending = make(map[string]leaderTimingRecord[T])
		}
		pair := p.pending[ip]
		if source == candidateLeaderSource {
			pair.candidateData = data
		} else {
			pair.referenceData = data
		}
		if !pair.observe(source == candidateLeaderSource, at) {
			p.pending[ip] = pair
			continue
		}
		delete(p.pending, ip)
		comparisons = append(comparisons, leaderTimingComparison[T]{ip: ip, leaderTimingRecord: pair})
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

// leaderTimingPair records timestamps for an IP already selected by the caller.
// Callers own leader eligibility, baselines, pending maps, and lifecycle cleanup.
// The candidate is SQL for detection/snapshot timing, or hybrid for routing timing.
type leaderTimingPair struct {
	candidateAt time.Time
	patroniAt   time.Time
}

// observe returns true once both sources have supplied a timestamp.
func (p *leaderTimingPair) observe(candidate bool, at time.Time) bool {
	if candidate {
		p.candidateAt = at
	} else {
		p.patroniAt = at
	}
	return !p.candidateAt.IsZero() && !p.patroniAt.IsZero()
}

func (p leaderTimingPair) speedup() time.Duration { return p.patroniAt.Sub(p.candidateAt) }
