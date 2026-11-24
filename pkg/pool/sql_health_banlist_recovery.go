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

// sqlBanEvidence keeps one credential's latest observation of one IP. It is
// used only for ban eligibility, never for choosing a routing role.
type sqlBanEvidence struct {
	result      SQLHealthCheckResult
	scheduledAt time.Time
	observedAt  time.Time
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
		if exists && !isPostgresShutdownError(result.Err) {
			// An older recovery completion cannot replace a newer poll or
			// erase shutdown evidence observed while that recovery was running.
			if scheduledAt.Before(previous.scheduledAt) ||
				isPostgresShutdownError(previous.result.Err) && !scheduledAt.After(previous.observedAt) {
				continue
			}
		}
		s.sqlBanLatest[target][result.IP] = sqlBanEvidence{result: result, scheduledAt: scheduledAt, observedAt: observedAt}
	}

	if mode != sqlRecoveryPoll {
		if s.sqlRoutingLatest == nil {
			s.sqlRoutingLatest = make(map[*sqlHealthTarget]SQLHealthResultsSnapshot)
		}
		if previous := s.sqlRoutingLatest[target]; !scheduledAt.Before(previous.ScheduledAt) {
			s.sqlRoutingLatest[target] = SQLHealthResultsSnapshot{
				StartedAt: outcome.StartedAt, ScheduledAt: scheduledAt, ObservedAt: observedAt,
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
func (s *PoolHealth) sqlBanOutcomeLocked(retained []string) SQLHealthCheckOutcome {
	var results []SQLHealthCheckResult
	for _, byIP := range s.sqlBanLatest {
		for ip, evidence := range byIP {
			if slices.Contains(retained, ip) {
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

	// Keep the callback without accessing checker lifetime after the probes finish.
	var multiplePrimaries func([]string, []string)
	if startRecovery {
		target.sqlCheckMu.RLock()
		if !target.sqlClosed && target.checker != nil {
			multiplePrimaries = target.checker.hooks.MultiplePrimaries
		}
		target.sqlCheckMu.RUnlock()
	}

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
				w.completeSQLHealthCheckMode(target, generation, scheduledAt, ips, resolvedSet, outcome, sqlRecoveryPoll, recoveryIPs)
			}
			recoveryDone <- completedPoll{outcome: outcome, observedAt: observedAt}
		}()
	}
	routingOutcome, observedAt, ok := check(routingIPs)
	if ok {
		w.completeSQLHealthCheckMode(target, generation, scheduledAt, ips, resolvedSet, routingOutcome, sqlRoutingPoll, recoveryIPs)
	}
	// This tick has no full-poll completion when an older recovery is still busy.
	// Report its incomplete availability rather than retaining an earlier writable result.
	if !startRecovery && ok && !writable.Load() {
		target.poolHealth.sqlComparisonMu.Lock()
		valid := target.acceptsSQLRoundLocked(generation)
		target.poolHealth.sqlComparisonMu.Unlock()
		if valid {
			publishMissedSQLAvailability(target, scheduledAt)
		}
	}
	// Recovery must not keep the credential reserved and make its next routing tick miss.
	target.finishSQLRound(scheduledAt)
	if startRecovery {
		finish := func() {
			defer func() { target.recoveryMu.Lock(); target.recoveryRunning = false; target.recoveryMu.Unlock() }()
			recovered := <-recoveryDone
			combined := aggregateSQLHealthCheckOutcomes([]SQLHealthCheckOutcome{routingOutcome, recovered.outcome}).Outcome
			// Each half already reports its own multiple primaries. Report the
			// combined anomaly only when it was visible in neither half alone.
			if multiplePrimaries != nil && len(combined.PrimaryIPs) > 1 &&
				len(routingOutcome.PrimaryIPs) < 2 && len(recovered.outcome.PrimaryIPs) < 2 {
				tags := append(slices.Clone(target.tags), "pool_role:primary")
				multiplePrimaries(combined.PrimaryIPs, tags)
			}
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
	s.sqlComparisonMu.Unlock()
	w.reportSQLPatroniComparison(s, snapshot)
	if snapshot != nil {
		w.reportSQLPotential(s, snapshot.potentialComparisons)
	}
	return snapshot != nil
}
