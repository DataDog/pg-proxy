package pool

import "time"

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
