// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package pool

import (
	"context"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestRecoveryUnbanRequiresNewCompleteEligibleCoverage(t *testing.T) {
	const a, b = "10.0.0.1", "10.0.0.2"
	health := newTestPoolHealth(a, b)
	health.sqlHealthState = NewHealthState()
	health.sqlHealthState.Track([]string{a, b})
	health.hybridPolicy = newHybridHealthPolicy(true)
	health.routingEnabledFn = func(context.Context) SQLHealthFeatureFlagEvaluation { return validSQLHealthFeatureFlag(true) }
	w := newTestWorker(nil)
	now := time.Now()
	target := &sqlHealthTarget{poolHealth: health}
	health.hybridPolicy.routingBannedIPs[a] = now.Add(-time.Second)
	w.applyPatroniHealth(health, map[string]bool{a: false, b: true}, nil, []string{a, b}, now)
	retained := map[string]struct{}{a: {}, b: {}}
	outcome := SQLHealthCheckOutcome{StartedAt: now, PrimaryIPs: []string{a}, Results: []SQLHealthCheckResult{{IP: a, StartedAt: now, CompletedAt: now}}}
	w.completeSQLHealthCheckMode(target, 0, now, []string{a, b}, retained, outcome, sqlRecoveryPoll, nil)
	require.NotContains(t, health.hybridPolicy.routingBanSnapshot(), a)
	require.False(t, health.IsHealthy(a), "recovery alone must not publish a newly unbanned primary")
	require.Empty(t, health.sqlRoutingLatest, "unbanning does not supply routing roles")
	// This routing poll excluded A before it was unbanned. Cached recovery evidence cannot fill it in.
	w.completeSQLHealthCheckMode(target, 0, now, []string{a, b}, retained, SQLHealthCheckOutcome{StartedAt: now, PrimaryIPs: []string{b}, Results: []SQLHealthCheckResult{{IP: b}}}, sqlRoutingPoll, []string{a})
	require.False(t, health.IsHealthy(a))
	require.False(t, health.sqlRoutingActive, "an unchecked newly eligible IP prevents SQL publication")
	require.Equal(t, []string{a}, health.sqlRoutingLatest[target].BannedIPs)
	// A later poll checks both and sees two primaries: it must use Patroni fallback.
	w.completeSQLHealthCheck(target, 0, now.Add(time.Second), []string{a, b}, retained, SQLHealthCheckOutcome{StartedAt: now.Add(time.Second), PrimaryIPs: []string{a, b}, Results: []SQLHealthCheckResult{{IP: a}, {IP: b}}})
	require.False(t, health.sqlRoutingActive)
	require.False(t, health.IsHealthy(a))
	require.True(t, health.IsHealthy(b))
}

func TestRecoveryKeepsOtherCredentialsShutdownEvidence(t *testing.T) {
	const a, b = "10.0.0.1", "10.0.0.2"
	ips := []string{a, b}
	retained := map[string]struct{}{a: {}, b: {}}
	health := newTestPoolHealth(a, b)
	health.sqlHealthState = NewHealthState()
	health.sqlHealthState.Track(ips)
	health.hybridPolicy = newHybridHealthPolicy(true)
	health.routingEnabledFn = func(context.Context) SQLHealthFeatureFlagEvaluation { return validSQLHealthFeatureFlag(true) }
	w := newTestWorker(nil)
	now := time.Now()
	w.applyPatroniHealth(health, map[string]bool{a: false, b: true}, nil, ips, now)
	first, second := &sqlHealthTarget{poolHealth: health}, &sqlHealthTarget{poolHealth: health}
	shutdown := SQLHealthCheckOutcome{StartedAt: now, PrimaryIPs: []string{b}, Results: []SQLHealthCheckResult{{IP: a, Err: &pgconn.PgError{Code: "57P01"}}, {IP: b}}}
	w.completeSQLHealthCheck(first, 0, now, ips, retained, shutdown)
	w.completeSQLHealthCheck(second, 0, now, ips, retained, shutdown)
	recovery := SQLHealthCheckOutcome{StartedAt: now.Add(time.Second), Results: []SQLHealthCheckResult{{IP: a, InRecovery: true}}}
	w.completeSQLHealthCheckMode(first, 0, now.Add(time.Second), ips, retained, recovery, sqlRecoveryPoll, nil)
	require.Contains(t, health.hybridPolicy.routingBanSnapshot(), a, "the second credential still has shutdown evidence")
	w.completeSQLHealthCheckMode(second, 0, now.Add(2*time.Second), ips, retained, SQLHealthCheckOutcome{StartedAt: now.Add(2 * time.Second), Results: []SQLHealthCheckResult{{IP: b}}}, sqlRoutingPoll, nil)
	w.completeSQLHealthCheckMode(first, 0, now.Add(3*time.Second), ips, retained, recovery, sqlRecoveryPoll, nil)
	require.Contains(t, health.hybridPolicy.routingBanSnapshot(), a, "routing polls must preserve cached shutdown evidence on excluded IPs")
	w.completeSQLHealthCheckMode(second, 0, now.Add(4*time.Second), ips, retained, recovery, sqlRecoveryPoll, nil)
	require.NotContains(t, health.hybridPolicy.routingBanSnapshot(), a)
	require.True(t, health.IsHealthy(b))
}

func TestRecoveryDoesNotEvaluateTheRoutingFlag(t *testing.T) {
	const ip = "10.0.0.1"
	health := newTestPoolHealth(ip)
	health.sqlHealthState = NewHealthState()
	health.sqlHealthState.Track([]string{ip})
	health.hybridPolicy = newHybridHealthPolicy(true)
	health.hybridPolicy.routingBannedIPs[ip] = time.Now().Add(-time.Second)
	health.routingEnabledFn = func(context.Context) SQLHealthFeatureFlagEvaluation {
		t.Error("recovery must not block routing through the flag evaluator")
		return validSQLHealthFeatureFlag(true)
	}
	target := &sqlHealthTarget{poolHealth: health}
	now := time.Now()
	w := newTestWorker(nil)
	w.completeSQLHealthCheckMode(target, 0, now, []string{ip}, map[string]struct{}{ip: {}}, SQLHealthCheckOutcome{StartedAt: now, Results: []SQLHealthCheckResult{{IP: ip, InRecovery: true}}}, sqlRecoveryPoll, nil)
	require.Empty(t, health.hybridPolicy.routingBanSnapshot())
}

func TestCapturedBansDoNotReviveAnotherCredentialsCachedPrimary(t *testing.T) {
	const a, b = "10.0.0.1", "10.0.0.2"
	ips := []string{a, b}
	base := time.Now().Add(-time.Minute)
	at := func(n int) time.Time { return base.Add(time.Duration(n) * time.Second) }
	health := newTestPoolHealth(a, b)
	health.sqlHealthState = NewHealthState()
	health.sqlHealthState.Track(ips)
	first, second := &sqlHealthTarget{poolHealth: health}, &sqlHealthTarget{poolHealth: health}
	result := func(ip string, start int, standby bool, err error) SQLHealthCheckResult {
		return SQLHealthCheckResult{IP: ip, StartedAt: at(start), CompletedAt: at(start + 1), InRecovery: standby, Err: err}
	}
	health.recordSQLPollEvidenceLocked(first, at(0), at(1), SQLHealthCheckOutcome{
		StartedAt: at(0), Results: []SQLHealthCheckResult{result(a, 0, false, nil)},
	}, sqlCompletePoll, nil)
	// This credential skips banned A; the old role remains only in ban evidence.
	health.recordSQLPollEvidenceLocked(first, at(20), at(21), SQLHealthCheckOutcome{
		StartedAt: at(20), Results: []SQLHealthCheckResult{result(b, 20, true, nil)},
	}, sqlRoutingPoll, []string{a})
	stored := health.sqlRoutingLatest[first]
	require.Equal(t, []string{a}, stored.BannedIPs)
	require.Len(t, stored.Results, 1)
	// Another credential supplies fresh standby evidence, allowing A's global ban to clear.
	health.recordSQLPollEvidenceLocked(second, at(21), at(22), SQLHealthCheckOutcome{
		StartedAt: at(21), Results: []SQLHealthCheckResult{result(a, 21, true, nil), result(b, 21, true, nil)},
	}, sqlCompletePoll, nil)
	// Its next timeout replaces that standby. The first credential's old primary stays excluded.
	health.recordSQLPollEvidenceLocked(second, at(23), at(24), SQLHealthCheckOutcome{
		StartedAt: at(23), Results: []SQLHealthCheckResult{result(a, 23, false, context.DeadlineExceeded), result(b, 23, true, nil)},
	}, sqlCompletePoll, nil)
	var eligible []SQLHealthCheckOutcome
	for _, snapshot := range health.sqlRoutingLatest {
		eligible = append(eligible, snapshot.routingOutcome(ips))
	}
	raw := health.sqlBanOutcomeLocked(ips)
	routing := aggregateSQLHealthCheckOutcomes(eligible).Outcome
	require.Contains(t, raw.PrimaryIPs, a, "ban evidence must remain separate from routing")
	require.NotContains(t, routing.PrimaryIPs, a)
	policy := newHybridHealthPolicy(true)
	policy.patroniHealth = map[string]bool{a: false, b: true}
	policy.patroniRecoveryAt = at(10)
	policy.patroniLeaderChangedAt = at(10)
	decision := policy.observeSQLRoundWithRoutingEvidence(ips, raw, &routing, SQLHealthResultsSnapshot{}, at(30))
	require.False(t, decision.health[a])
	require.True(t, decision.health[b], "the timeout must use Patroni rather than revive cached A")
	require.Equal(t, SQLRoutingResultCachedFallback, decision.routingResult)
	// This credential's own next probe replaces the cached role, even when it times out.
	health.recordSQLPollEvidenceLocked(first, at(31), at(32), SQLHealthCheckOutcome{
		StartedAt: at(31), Results: []SQLHealthCheckResult{result(a, 31, false, context.DeadlineExceeded), result(b, 31, true, nil)},
	}, sqlCompletePoll, nil)
	require.Empty(t, health.sqlRoutingLatest[first].BannedIPs)
	require.Len(t, health.sqlRoutingLatest[first].routingOutcome(ips).Results, 2)
	// Captured exclusions must be owned copies.
	copy := cloneSQLHealthResultsSnapshot(stored)
	copy.BannedIPs[0] = b
	require.Equal(t, []string{a}, stored.BannedIPs)
}

func TestRecoveryCannotChangeRoutingEvidence(t *testing.T) {
	for _, recoveryArrivesLast := range []bool{false, true} {
		name := "recovery_arrives_first"
		if recoveryArrivesLast {
			name = "recovery_arrives_last"
		}
		t.Run(name, func(t *testing.T) {
			const a, b = "10.0.0.1", "10.0.0.2"
			ips := []string{a, b}
			retained := map[string]struct{}{a: {}, b: {}}
			base := time.Now().Add(-time.Minute)
			at := func(n int) time.Time { return base.Add(time.Duration(n) * time.Second) }
			health := newTestPoolHealth(a, b)
			health.sqlHealthState = NewHealthState()
			health.sqlHealthState.Track(ips)
			health.hybridPolicy = newHybridHealthPolicy(true)
			health.hybridPolicy.patroniHealth = map[string]bool{a: false, b: true}
			target := &sqlHealthTarget{poolHealth: health}
			w := newTestWorker(nil)
			recovery := SQLHealthCheckOutcome{StartedAt: at(1), Results: []SQLHealthCheckResult{
				{IP: a, StartedAt: at(1), CompletedAt: at(5)},
			}}
			routing := SQLHealthCheckOutcome{StartedAt: at(3), Results: []SQLHealthCheckResult{
				{IP: a, StartedAt: at(3), CompletedAt: at(4), InRecovery: true},
				{IP: b, StartedAt: at(3), CompletedAt: at(4)},
			}}
			completeRecovery := func() {
				w.completeSQLHealthCheckMode(target, 0, at(1), ips, retained, recovery, sqlRecoveryPoll, []string{a})
			}
			if !recoveryArrivesLast {
				completeRecovery()
				require.Empty(t, health.sqlRoutingLatest, "recovery must not create a routing snapshot")
			}
			w.completeSQLHealthCheckMode(target, 0, at(3), ips, retained, routing, sqlRoutingPoll, nil)
			if recoveryArrivesLast {
				completeRecovery()
			}
			stored := health.sqlRoutingLatest[target]
			require.Equal(t, routing.Results, stored.Results, "recovery must not add or replace routing roles")
			require.Equal(t, at(3), stored.StartedAt)
			require.True(t, health.IsHealthy(b))
			require.False(t, health.IsHealthy(a))

			// The delayed raw comparison still sees the original recovery role,
			// without changing routing or its evidence.
			var raw SQLPatroniHealthSnapshot
			w.hooks.SQLPatroniCompared = func(snapshot SQLPatroniHealthSnapshot, _ []string) { raw = snapshot }
			originalPoll := SQLHealthCheckOutcome{StartedAt: at(1), Results: []SQLHealthCheckResult{
				recovery.Results[0], {IP: b, StartedAt: at(1), CompletedAt: at(2)},
			}}
			require.True(t, w.recordSQLCompletePoll(target, 0, at(1), at(5), ips, originalPoll))
			require.True(t, raw.SQLHealth[a])
			require.True(t, raw.SQLHealth[b])
			require.Equal(t, stored, health.sqlRoutingLatest[target])
			require.True(t, health.IsHealthy(b))

			// A later normal timeout replaces the old standby answer; the
			// recovery primary cannot reappear or create a synthetic conflict.
			var iterations []SQLIteration
			w.sqlHooks.Iteration = func(i SQLIteration, _ []string) { iterations = append(iterations, i) }
			w.completeSQLHealthCheck(target, 0, at(6), ips, retained, SQLHealthCheckOutcome{StartedAt: at(6), Results: []SQLHealthCheckResult{
				{IP: a, StartedAt: at(6), CompletedAt: at(7), Err: context.DeadlineExceeded},
				{IP: b, StartedAt: at(6), CompletedAt: at(7)},
			}})
			require.Len(t, iterations, 1)
			require.Equal(t, SQLHealthFallbackTimeout, iterations[0].FallbackReason)
			require.Equal(t, SQLRoutingResultCachedFallback, iterations[0].RoutingResult)
			require.True(t, health.IsHealthy(b))
			require.False(t, health.IsHealthy(a))
		})
	}
}

func TestRecoveryDoesNotRefreshRoutingTimestamp(t *testing.T) {
	const a, b = "10.0.0.1", "10.0.0.2"
	ips := []string{a, b}
	base := time.Now().Add(-time.Minute)
	at := func(n int) time.Time { return base.Add(time.Duration(n) * time.Second) }
	health := newTestPoolHealth(a, b)
	health.sqlHealthState = NewHealthState()
	health.sqlHealthState.Track(ips)
	target := &sqlHealthTarget{poolHealth: health}
	health.recordSQLPollEvidenceLocked(target, at(5), at(6), SQLHealthCheckOutcome{
		StartedAt: at(5), Results: []SQLHealthCheckResult{{IP: b, StartedAt: at(5), CompletedAt: at(6)}},
	}, sqlRoutingPoll, []string{a})
	// Fresh recovery of A cannot make B's older routing observation fresh.
	health.recordSQLPollEvidenceLocked(target, at(20), at(25), SQLHealthCheckOutcome{
		StartedAt: at(20), Results: []SQLHealthCheckResult{{IP: a, StartedAt: at(20), CompletedAt: at(25), InRecovery: true}},
	}, sqlRecoveryPoll, []string{a})
	stored := health.sqlRoutingLatest[target]
	require.Equal(t, at(5), stored.StartedAt)
	require.Len(t, stored.Results, 1)
	routing := aggregateSQLHealthCheckOutcomes([]SQLHealthCheckOutcome{stored.routingOutcome(ips)}).Outcome
	policy := newHybridHealthPolicy(true)
	policy.patroniRecoveryAt = at(10)
	decision := policy.observeSQLRoundWithRoutingEvidence(ips, routing, &routing, SQLHealthResultsSnapshot{}, at(30))
	require.Equal(t, SQLRoutingResultPatroniRecovery, decision.routingResult)
	require.False(t, decision.publish)
}

func TestRoutingFreshnessIgnoresOnlyEmptyCredentialOutcomes(t *testing.T) {
	const a, b, c = "10.0.0.1", "10.0.0.2", "10.0.0.3"
	for _, keepTimeout := range []bool{false, true} {
		name := "empty_after_eviction"
		if keepTimeout {
			name = "nonempty_timeout_still_checks_freshness"
		}
		t.Run(name, func(t *testing.T) {
			base := time.Now().Add(-time.Minute)
			at := func(n int) time.Time { return base.Add(time.Duration(n) * time.Second) }
			health := newTestPoolHealth(a, b, c)
			health.sqlHealthState = NewHealthState()
			health.sqlHealthState.Track([]string{a, b, c})
			health.hybridPolicy = newHybridHealthPolicy(true)
			old, fresh := &sqlHealthTarget{poolHealth: health}, &sqlHealthTarget{poolHealth: health}
			results := []SQLHealthCheckResult{{IP: a, StartedAt: at(1), CompletedAt: at(2)}}
			banned := []string{b, c}
			if keepTimeout {
				results = append(results, SQLHealthCheckResult{IP: b, Err: context.DeadlineExceeded})
				banned = []string{c}
			}
			health.recordSQLPollEvidenceLocked(old, at(1), at(2), SQLHealthCheckOutcome{StartedAt: at(1), Results: results}, sqlRoutingPoll, banned)

			// Last-known-healthy protection survives five DNS misses. Only after
			// another IP is confirmed healthy can discovery evict the old leader.
			health.sqlHealthState.RecordHealthAt(at(1), map[string]bool{a: true})
			for range 5 {
				require.Contains(t, health.refreshSQLResolved(false, []string{b, c}, 5), a)
			}
			health.sqlHealthState.RecordHealthAt(at(2), map[string]bool{b: true})
			require.NotContains(t, health.refreshSQLResolved(false, []string{b, c}, 5), a)
			if !keepTimeout {
				require.Empty(t, health.sqlRoutingLatest[old].Results)
			}
			require.Equal(t, at(1), health.sqlRoutingLatest[old].StartedAt)

			w := newTestWorker(nil)
			w.applyPatroniHealth(health, map[string]bool{b: true, c: false}, nil, []string{b, c}, at(3))
			health.hybridPolicy.patroniRecoveryAt = at(3)
			w.completeSQLHealthCheck(fresh, 0, at(4), []string{b, c}, map[string]struct{}{b: {}, c: {}}, SQLHealthCheckOutcome{
				StartedAt: at(4), Results: []SQLHealthCheckResult{
					{IP: b, InRecovery: true, StartedAt: at(4), CompletedAt: at(5)},
					{IP: c, StartedAt: at(4), CompletedAt: at(5)},
				},
			})
			require.Equal(t, !keepTimeout, health.sqlRoutingActive)
			require.Equal(t, !keepTimeout, health.IsHealthy(c))
			require.Equal(t, keepTimeout, health.IsHealthy(b))
		})
	}
}

func TestOldRecoveryCannotEraseShutdownEvidence(t *testing.T) {
	const ip = "10.0.0.1"
	for _, shutdownScheduled := range []int{1, 3} {
		name := "probe_started_before_shutdown"
		if shutdownScheduled == 3 {
			name = "older_poll"
		}
		t.Run(name, func(t *testing.T) {
			base := time.Now().Add(-time.Minute)
			at := func(n int) time.Time { return base.Add(time.Duration(n) * time.Second) }
			health := newTestPoolHealth(ip)
			health.sqlHealthState = NewHealthState()
			health.sqlHealthState.Track([]string{ip})
			target := &sqlHealthTarget{poolHealth: health}
			policy := newHybridHealthPolicy(true)
			policy.routingBannedIPs[ip] = at(5)
			shutdown := SQLHealthCheckOutcome{Results: []SQLHealthCheckResult{{IP: ip, Err: &pgconn.PgError{Code: "57P01"}}}}
			health.recordSQLPollEvidenceLocked(target, at(shutdownScheduled), at(5), shutdown, sqlRecoveryPoll, nil)
			// Recovery was scheduled at 2, before shutdown was observed at 5.
			success := SQLHealthCheckOutcome{Results: []SQLHealthCheckResult{{IP: ip, InRecovery: true}}}
			health.recordSQLPollEvidenceLocked(target, at(2), at(6), success, sqlRecoveryPoll, nil)
			fresh := SQLHealthResultsSnapshot{ScheduledAt: at(2), ObservedAt: at(6), Results: success.Results}
			decision := policy.observeSQLRecovery([]string{ip}, health.sqlBanOutcomeLocked([]string{ip}), fresh)
			require.Contains(t, decision.routingBannedIPs, ip)
			require.True(t, isPostgresShutdownError(health.sqlBanLatest[target][ip].result.Err))

			// A new recovery scheduled after shutdown can replace the evidence
			// and clear the ban without supplying a routing role.
			health.recordSQLPollEvidenceLocked(target, at(7), at(8), success, sqlRecoveryPoll, nil)
			fresh.ScheduledAt, fresh.ObservedAt = at(7), at(8)
			decision = policy.observeSQLRecovery([]string{ip}, health.sqlBanOutcomeLocked([]string{ip}), fresh)
			require.NotContains(t, decision.routingBannedIPs, ip)
			require.Empty(t, health.sqlRoutingLatest)
		})
	}
}
