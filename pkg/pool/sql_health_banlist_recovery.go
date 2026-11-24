// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package pool

import (
	"context"
	"slices"
	"sync/atomic"
	"time"
)

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
