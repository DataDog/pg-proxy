// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package pool

import (
	"maps"
	"slices"
	"time"
)

type sqlPollMode uint8

const (
	sqlCompletePoll sqlPollMode = iota
	sqlRoutingPoll
	sqlRecoveryPoll
)

// routingBanSnapshot is used only to partition work; completion revalidates eligibility.
func (p *hybridHealthPolicy) routingBanSnapshot() map[string]time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.routingEnabled {
		return nil
	}
	return maps.Clone(p.routingBannedIPs)
}

// observeSQLRecovery changes per-IP eligibility without selecting a replacement route.
// The next routing poll must cover every IP that has become eligible again.
func (p *hybridHealthPolicy) observeSQLRecovery(ips []string, outcome SQLHealthCheckOutcome, fresh SQLHealthResultsSnapshot) hybridRoutingDecision {
	p.mu.Lock()
	defer p.mu.Unlock()
	decision := hybridRoutingDecision{routingEnabled: p.routingEnabled, routingResult: p.publishedRoutingResult}
	decision.routingBanChanges = p.routingBannedIPs.update(ips, outcome, fresh)
	decision.routingBannedIPs = maps.Clone(p.routingBannedIPs)
	return decision
}

// sqlBanEvidence keeps one credential's latest observation of one IP. It is
// used only for ban eligibility, never for choosing a routing role.
type sqlBanEvidence struct {
	routingGeneration uint64
	result            SQLHealthCheckResult
	scheduledAt       time.Time
	observedAt        time.Time
}

// recordSQLPollEvidenceLocked records ban evidence from either kind of probe,
// but replaces routing snapshots only with complete eligible-IP polls.
// sqlComparisonMu is held by the caller.
func (s *PoolHealth) recordSQLPollEvidenceLocked(target *sqlHealthTarget, scheduledAt, observedAt time.Time, outcome SQLHealthCheckOutcome, mode sqlPollMode, bannedIPs []string) *sqlHealthComparisonSnapshot {
	retained := make(map[string]struct{})
	for _, ip := range s.sqlHealthState.Snapshot().Resolved {
		retained[ip] = struct{}{}
	}
	results := slices.DeleteFunc(slices.Clone(outcome.Results), func(result SQLHealthCheckResult) bool {
		_, ok := retained[result.IP]
		return !ok
	})
	if s.sqlBanLatest == nil {
		s.sqlBanLatest = make(map[*sqlHealthTarget]map[string]sqlBanEvidence)
	}
	if s.sqlBanLatest[target] == nil {
		s.sqlBanLatest[target] = make(map[string]sqlBanEvidence)
	}
	for _, result := range results {
		previous, exists := s.sqlBanLatest[target][result.IP]
		if exists && previous.routingGeneration == outcome.routingGeneration && !isPostgresShutdownError(result.Err) {
			// An older recovery completion cannot replace a newer poll or
			// erase shutdown evidence observed while that recovery was running.
			if scheduledAt.Before(previous.scheduledAt) ||
				isPostgresShutdownError(previous.result.Err) && !scheduledAt.After(previous.observedAt) {
				continue
			}
		}
		s.sqlBanLatest[target][result.IP] = sqlBanEvidence{routingGeneration: outcome.routingGeneration, result: result, scheduledAt: scheduledAt, observedAt: observedAt}
	}

	if mode != sqlRecoveryPoll {
		if s.sqlRoutingLatest == nil {
			s.sqlRoutingLatest = make(map[*sqlHealthTarget]SQLHealthResultsSnapshot)
		}
		if previous := s.sqlRoutingLatest[target]; previous.routingGeneration != outcome.routingGeneration || !scheduledAt.Before(previous.ScheduledAt) {
			s.sqlRoutingLatest[target] = SQLHealthResultsSnapshot{
				routingGeneration: outcome.routingGeneration,
				StartedAt:         outcome.StartedAt, ScheduledAt: scheduledAt, ObservedAt: observedAt,
				CandidateIPs: slices.Collect(maps.Keys(retained)),
				BannedIPs:    slices.Clone(bannedIPs), Tags: slices.Clone(target.tags),
				Results: results,
			}
		}
	}
	snapshot := buildSQLHealthComparisonSnapshot(retained, s.sqlRoutingLatest)
	return &snapshot
}

// sqlBanOutcomeLocked includes cached shutdowns from every credential, including
// IPs omitted by routing polls. Recovery replaces only its own ban evidence.
// The caller holds sqlComparisonMu and routingMu; older routing periods are diagnostic only.
func (s *PoolHealth) sqlBanOutcomeLocked(retained []string) SQLHealthCheckOutcome {
	var results []SQLHealthCheckResult
	for _, byIP := range s.sqlBanLatest {
		for ip, evidence := range byIP {
			if evidence.routingGeneration == s.routingGeneration && slices.Contains(retained, ip) {
				results = append(results, evidence.result)
			}
		}
	}
	return aggregateSQLHealthCheckResults(results).Outcome
}

// routingOutcome includes only this credential's normal routing poll. IPs it
// skipped while banned remain excluded even if another credential unbans them.
func (s SQLHealthResultsSnapshot) routingOutcome(retained []string) SQLHealthCheckOutcome {
	outcome := SQLHealthCheckOutcome{StartedAt: s.StartedAt}
	for _, result := range s.Results {
		if !slices.Contains(retained, result.IP) || slices.Contains(s.BannedIPs, result.IP) {
			continue
		}
		if result.StartedAt.IsZero() {
			result.StartedAt = s.StartedAt
		}
		outcome.Results = append(outcome.Results, result)
	}
	return outcome
}

// recordSQLCompletePoll reports raw evidence only. Routing has already consumed the eligible
// results and recovery transitions separately; replaying this whole poll could roll it back.
func (w *HealthWorker) recordSQLCompletePoll(target *sqlHealthTarget, generation uint64, scheduledAt, observedAt time.Time, ips []string, outcome SQLHealthCheckOutcome) bool {
	s := target.poolHealth
	s.sqlComparisonMu.Lock()
	if !target.acceptsSQLRoundLocked(generation) || s.sqlComparisonLatest[target].ScheduledAt.After(scheduledAt) {
		s.sqlComparisonMu.Unlock()
		return false
	}
	snapshot := s.recordSQLComparisonPollLocked(w.hooks.SQLPotentialCompared != nil, target, generation, scheduledAt, observedAt, ips, outcome)
	s.sqlComparisonMu.Unlock()
	w.reportSQLPatroniComparison(s, snapshot)
	if snapshot != nil {
		w.reportSQLPotential(s, snapshot.potentialComparisons)
	}
	return snapshot != nil
}
