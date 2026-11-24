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
func (s *PoolHealth) recordSQLRoutingPollLocked(target *sqlHealthTarget, scheduledAt, observedAt time.Time, outcome SQLHealthCheckOutcome, mode sqlPollMode, bannedIPs []string) *sqlHealthComparisonSnapshot {
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
	excluded := make(map[string]struct{})
	for _, ip := range previous.BannedIPs {
		if _, ok := retained[ip]; ok {
			excluded[ip] = struct{}{}
		}
	}
	byIP := make(map[string][]SQLHealthCheckResult)
	for _, r := range previous.Results {
		if _, ok := retained[r.IP]; ok {
			if r.StartedAt.IsZero() {
				r.StartedAt = previous.StartedAt
			}
			byIP[r.IP] = append(byIP[r.IP], r)
		}
	}
	incoming := make(map[string][]SQLHealthCheckResult)
	for _, r := range outcome.Results {
		if _, ok := retained[r.IP]; ok {
			if r.StartedAt.IsZero() {
				r.StartedAt = outcome.StartedAt
			}
			incoming[r.IP] = append(incoming[r.IP], r)
		}
	}
	// The list describes IPs this poll skipped, not results it actually collected.
	for _, ip := range bannedIPs {
		if _, ok := retained[ip]; ok && len(incoming[ip]) == 0 {
			excluded[ip] = struct{}{}
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
		replace := !(oldAt.After(newAt) || oldAt.IsZero() && newAt.IsZero() && previous.ScheduledAt.After(scheduledAt))
		old := byIP[ip]
		if slices.Contains(previous.BannedIPs, ip) {
			if !replace {
				continue
			}
			old = nil // Invalidated cached roles cannot conflict with their replacement.
		}
		byIP[ip] = mergeSQLRoutingResults(old, results, replace)
		if replace {
			// Only this credential replacing the cached result clears its exclusion.
			delete(excluded, ip)
		}
	}
	previous.Results = nil
	for _, results := range byIP {
		previous.Results = append(previous.Results, results...)
	}
	previous.CandidateIPs = slices.Collect(maps.Keys(retained))
	previous.BannedIPs = slices.Collect(maps.Keys(excluded))
	slices.Sort(previous.BannedIPs)
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

// mergeSQLRoutingResults is called for one IP. Keep unresolved successful roles
// for the existing conflict/freshness checks, but keep errors only from the
// selected poll so a cached shutdown cannot block recovery forever.
func mergeSQLRoutingResults(old, incoming []SQLHealthCheckResult, replace bool) []SQLHealthCheckResult {
	selected := old
	if replace {
		selected = incoming
	}
	combined := filterSupersededSQLRoles(append(slices.Clone(old), incoming...))
	_, conflicts := aggregateSQLComparisonRoles(combined)
	keepRoles := len(conflicts) != 0
	var merged []SQLHealthCheckResult
	for _, result := range selected {
		if result.Err != nil {
			merged = append(merged, result)
		} else {
			keepRoles = true
		}
	}
	if !keepRoles {
		return merged
	}
	// For each role, the latest start can supersede an older opposite role;
	// the latest finish prevents an overlapping opposite role from being lost.
	// One unknown interval per role preserves uncertainty without keeping history.
	type roleEvidence struct {
		latestStartIndex  int
		latestFinishIndex int
		unknownTimeIndex  int
	}
	primary := roleEvidence{
		latestStartIndex:  -1,
		latestFinishIndex: -1,
		unknownTimeIndex:  -1,
	}
	standby := roleEvidence{
		latestStartIndex:  -1,
		latestFinishIndex: -1,
		unknownTimeIndex:  -1,
	}
	for i, result := range combined {
		if result.Err != nil {
			continue
		}
		evidence := &primary
		if result.InRecovery {
			evidence = &standby
		}
		if !result.hasProbeInterval() {
			evidence.unknownTimeIndex = i
			continue
		}
		if evidence.latestStartIndex == -1 || result.StartedAt.After(combined[evidence.latestStartIndex].StartedAt) {
			evidence.latestStartIndex = i
		}
		if evidence.latestFinishIndex == -1 || result.CompletedAt.After(combined[evidence.latestFinishIndex].CompletedAt) {
			evidence.latestFinishIndex = i
		}
	}
	for i, result := range combined {
		if result.Err == nil {
			evidence := &primary
			if result.InRecovery {
				evidence = &standby
			}
			if i == evidence.latestStartIndex || i == evidence.latestFinishIndex || i == evidence.unknownTimeIndex {
				merged = append(merged, result)
			}
		}
	}
	return merged
}

// routingOutcome omits roles skipped while banned, even if another credential
// subsequently unbans the IP. Preserve the start of each included probe rather
// than giving old results the timestamp of a partial routing refresh.
func (s SQLHealthResultsSnapshot) routingOutcome(retained []string) SQLHealthCheckOutcome {
	var outcome SQLHealthCheckOutcome
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

func sqlRoutingEvidenceStartedAt(results []SQLHealthCheckResult) time.Time {
	var startedAt time.Time
	for i, result := range results {
		if i == 0 || result.StartedAt.Before(startedAt) {
			startedAt = result.StartedAt
		}
	}
	return startedAt
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
