// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package pool

import (
	"context"
	"maps"
	"slices"
	"sync/atomic"
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

// recordSQLRoutingPollLocked keeps per-IP routing evidence independently of raw complete polls.
// A recovery completion replaces only its own IPs, preserving newer routing-poll results.
// sqlComparisonMu is held by the caller.
func (s *PoolHealth) recordSQLRoutingPollLocked(target *sqlHealthTarget, scheduledAt, observedAt time.Time, outcome SQLHealthCheckOutcome, mode sqlPollMode) *sqlHealthComparisonSnapshot {
	if s.sqlRoutingLatest == nil {
		s.sqlRoutingLatest = make(map[*sqlHealthTarget]SQLHealthResultsSnapshot)
		for target, snapshot := range s.sqlComparisonLatest {
			s.sqlRoutingLatest[target] = cloneSQLHealthResultsSnapshot(snapshot)
		}
	}
	retained := make(map[string]struct{})
	for _, ip := range s.sqlHealthState.Snapshot().Resolved {
		retained[ip] = struct{}{}
	}
	previous := s.sqlRoutingLatest[target]
	byIP := make(map[string][]SQLHealthCheckResult)
	for _, r := range previous.Results {
		if _, ok := retained[r.IP]; ok {
			byIP[r.IP] = append(byIP[r.IP], r)
		}
	}
	incoming := make(map[string][]SQLHealthCheckResult)
	for _, r := range outcome.Results {
		if _, ok := retained[r.IP]; ok {
			incoming[r.IP] = append(incoming[r.IP], r)
		}
	}
	for ip, results := range incoming {
		var oldAt, newAt time.Time
		for _, r := range byIP[ip] {
			if r.CompletedAt.After(oldAt) {
				oldAt = r.CompletedAt
			}
		}
		for _, r := range results {
			if r.CompletedAt.After(newAt) {
				newAt = r.CompletedAt
			}
		}
		if oldAt.After(newAt) || oldAt.IsZero() && newAt.IsZero() && previous.ScheduledAt.After(scheduledAt) {
			continue
		}
		byIP[ip] = slices.Clone(results)
	}
	previous.Results = nil
	for _, results := range byIP {
		previous.Results = append(previous.Results, results...)
	}
	previous.CandidateIPs = slices.Collect(maps.Keys(retained))
	previous.Tags = slices.Clone(target.tags)
	if mode != sqlRecoveryPoll && !scheduledAt.Before(previous.ScheduledAt) {
		previous.StartedAt = outcome.StartedAt
		previous.ScheduledAt = scheduledAt
		previous.ObservedAt = observedAt
	}
	s.sqlRoutingLatest[target] = previous
	snapshot := buildSQLHealthComparisonSnapshot(retained, s.sqlRoutingLatest)
	return &snapshot
}

// checkSQLHealthWithRecovery lets routing wait only for eligible candidates. Each credential
// has at most one independent recovery poll. Raw comparisons still wait for both halves of
// their original poll, and a late raw completion never publishes an older routing decision.
func (w *HealthWorker) checkSQLHealthWithRecovery(target *sqlHealthTarget, generation uint64, scheduledAt time.Time, ips []string, resolvedSet map[string]struct{}, waitForRecovery bool) bool {
	policy, _ := target.poolHealth.routingPolicy()
	if policy == nil {
		return false
	}
	bans := policy.routingBanSnapshot()
	var routingIPs, recoveryIPs []string
	for _, ip := range ips {
		if _, banned := bans[ip]; banned {
			recoveryIPs = append(recoveryIPs, ip)
		} else {
			routingIPs = append(routingIPs, ip)
		}
	}
	if len(recoveryIPs) == 0 {
		return false
	}
	target.recoveryMu.Lock()
	startRecovery := !target.recoveryRunning
	if startRecovery {
		target.recoveryRunning = true
	}
	target.recoveryMu.Unlock()

	var writable atomic.Bool
	onResult := func(result SQLHealthCheckResult) {
		if !result.HealthyPrimary() || !writable.CompareAndSwap(false, true) {
			return
		}
		target.poolHealth.sqlComparisonMu.Lock()
		valid := target.acceptsSQLRoundLocked(generation)
		target.poolHealth.sqlComparisonMu.Unlock()
		if valid {
			target.markSQLRoundWritable(scheduledAt)
			target.publishSQLHealth(SQLHealthObservation{ScheduledAt: scheduledAt, ObservedAt: time.Now(), Writable: true})
		}
	}
	check := func(candidates []string) (SQLHealthCheckOutcome, time.Time, bool) {
		target.sqlCheckMu.RLock()
		defer target.sqlCheckMu.RUnlock()
		if target.sqlClosed || target.checker == nil || target.connStringForIP == nil {
			return SQLHealthCheckOutcome{}, time.Time{}, false
		}
		target.checker.RetainIPs(ips)
		outcome := target.checker.Check(context.Background(), candidates, target.connStringForIP, target.poolHealth.patroniHealthState.Snapshot().Health, target.tags, onResult)
		return outcome, time.Now(), true
	}
	type completedPoll struct {
		outcome    SQLHealthCheckOutcome
		observedAt time.Time
	}
	recoveryDone := make(chan completedPoll, 1)
	if startRecovery {
		go func() {
			outcome, observedAt, ok := check(recoveryIPs)
			if ok {
				w.completeSQLHealthCheckMode(target, generation, scheduledAt, ips, resolvedSet, outcome, sqlRecoveryPoll)
			}
			recoveryDone <- completedPoll{outcome: outcome, observedAt: observedAt}
		}()
	}
	routingOutcome, observedAt, ok := check(routingIPs)
	if ok {
		w.completeSQLHealthCheckMode(target, generation, scheduledAt, ips, resolvedSet, routingOutcome, sqlRoutingPoll)
	}
	// Recovery must not keep the credential reserved and make its next routing tick miss.
	target.finishSQLRound(scheduledAt)
	if startRecovery {
		finish := func() {
			defer func() { target.recoveryMu.Lock(); target.recoveryRunning = false; target.recoveryMu.Unlock() }()
			recovered := <-recoveryDone
			combined := aggregateSQLHealthCheckOutcomes([]SQLHealthCheckOutcome{routingOutcome, recovered.outcome}).Outcome
			if recovered.observedAt.After(observedAt) {
				observedAt = recovered.observedAt
			}
			if w.recordSQLCompletePoll(target, generation, scheduledAt, observedAt, ips, combined) && !writable.Load() {
				target.publishSQLHealth(newSQLHealthObservation(scheduledAt, time.Now(), combined))
			}
		}
		if waitForRecovery {
			finish()
		} else {
			go finish()
		}
	}
	return true
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
	var detection *LeaderDetectionComparison
	var routingEnabled bool
	if snapshot != nil {
		s.routingMu.Lock()
		if policy := s.hybridPolicy; policy != nil && !scheduledAt.Before(s.routingChangedAt) {
			outcomes := make([]SQLHealthCheckOutcome, 0, len(snapshot.results))
			for _, completed := range snapshot.results {
				outcomes = append(outcomes, SQLHealthCheckOutcome{StartedAt: completed.StartedAt, Results: completed.Results})
			}
			detection = policy.observeSQLDetection(aggregateSQLHealthCheckOutcomes(outcomes).Outcome, observedAt)
			routingEnabled = policy.isRoutingEnabled()
		}
		s.routingMu.Unlock()
	}
	s.sqlComparisonMu.Unlock()
	if detection != nil && w.sqlHooks.LeaderDetectionCompared != nil {
		w.sqlHooks.LeaderDetectionCompared(*detection, w.sqlRoutingTags(s.patroniTags, routingEnabled))
	}
	w.reportSQLPatroniComparison(s, snapshot)
	if snapshot != nil {
		w.reportSQLPotential(s, snapshot.potentialComparisons)
	}
	return snapshot != nil
}
