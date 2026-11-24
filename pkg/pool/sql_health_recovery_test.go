// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package pool

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

func TestBannedRecoveryDoesNotBlockRoutingOrItsNextPoll(t *testing.T) {
	const a, b, c = "10.0.0.1", "10.0.0.2", "10.0.0.3"
	ips := []string{a, b, c}
	dns := newMockProber(map[string][]string{"db.example.com": ips}, a)
	w := newTestWorker(dns)
	probe := &credentialSQLHealthProber{responses: map[string]fakeSQLHealthProbeResponse{a: {}, b: {inRecovery: true}, c: {inRecovery: true}}}
	w.newSQLChecker = func(context.Context) *SQLHealthChecker {
		return newTestSQLHealthChecker(t, probe, 1, SQLHealthCheckHooks{})
	}
	updates := w.Subscribe("db.example.com", false, "db", nil, &SQLHealthConfig{
		ConnStringForIP: func(ip string) string { return ip },
		RoutingEnabled:  func(context.Context) SQLHealthFeatureFlagEvaluation { return validSQLHealthFeatureFlag(true) },
	})
	health := receivePoolHealth(t, updates)
	w.RefreshSQL()
	probe.mu.Lock()
	probe.responses[a] = fakeSQLHealthProbeResponse{err: &pgconn.PgError{Code: "57P01"}}
	probe.mu.Unlock()
	w.RefreshSQL()
	require.Contains(t, health.hybridPolicy.routingBanSnapshot(), a)
	require.Empty(t, healthyIPs(health.Snapshot().Health))

	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(func() { unblock(); w.Unsubscribe(updates) })
	probe.mu.Lock()
	probe.responses[a] = fakeSQLHealthProbeResponse{err: context.DeadlineExceeded}
	probe.responses[b] = fakeSQLHealthProbeResponse{}
	probe.started = map[string]chan struct{}{a: started}
	probe.release = map[string]chan struct{}{a: release}
	probe.startOnce = map[string]*sync.Once{a: {}}
	before := probe.calls[a]
	probe.mu.Unlock()
	var raw atomic.Int64
	w.hooks.SQLPatroniCompared = func(SQLPatroniHealthSnapshot, []string) { raw.Add(1) }
	done := make(chan struct{})
	go func() { defer close(done); w.RefreshSQL() }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("recovery did not start")
	}
	require.Eventually(t, func() bool { return health.IsHealthy(b) }, time.Second, time.Millisecond, "B must route before banned A finishes")
	require.Zero(t, raw.Load(), "raw comparisons still wait for the complete poll")
	health.hybridPolicy.mu.Lock()
	rawLeaders := healthyIPs(health.hybridPolicy.detectionTiming.candidate.current)
	health.hybridPolicy.mu.Unlock()
	require.Empty(t, rawLeaders, "early routing must not advance legacy raw SQL detection")

	probe.mu.Lock()
	probe.responses[b] = fakeSQLHealthProbeResponse{inRecovery: true}
	probe.responses[c] = fakeSQLHealthProbeResponse{}
	probe.mu.Unlock()
	w.runSQLHealthGroup(w.sqlHealthGroups()[0], time.Now(), false)
	require.Eventually(t, func() bool { return health.IsHealthy(c) && !health.IsHealthy(b) }, time.Second, time.Millisecond, "the next routing poll must also run while A is blocked")
	require.Equal(t, before+1, probe.callCount(a), "one recovery poll per credential must prevent duplicate blocked probes")
	require.Zero(t, raw.Load())
	unblock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("complete raw poll did not finish")
	}
	require.EqualValues(t, 1, raw.Load())
	require.True(t, health.IsHealthy(c), "the older raw poll must not restore B")
	require.Contains(t, health.hybridPolicy.routingBanSnapshot(), a)
}

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
	w.completeSQLHealthCheckMode(target, 0, now, []string{a, b}, retained, outcome, sqlRecoveryPoll)
	require.NotContains(t, health.hybridPolicy.routingBanSnapshot(), a)
	require.False(t, health.IsHealthy(a), "recovery alone must not publish a newly unbanned primary")
	// This routing poll excluded A before it was unbanned. Cached recovery evidence cannot fill it in.
	w.completeSQLHealthCheckMode(target, 0, now, []string{a, b}, retained, SQLHealthCheckOutcome{StartedAt: now, PrimaryIPs: []string{b}, Results: []SQLHealthCheckResult{{IP: b}}}, sqlRoutingPoll)
	require.False(t, health.IsHealthy(a))
	require.False(t, health.sqlRoutingActive, "an unchecked newly eligible IP prevents SQL publication")
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
	w.completeSQLHealthCheckMode(first, 0, now.Add(time.Second), ips, retained, recovery, sqlRecoveryPoll)
	require.Contains(t, health.hybridPolicy.routingBanSnapshot(), a, "the second credential still has shutdown evidence")
	w.completeSQLHealthCheckMode(second, 0, now.Add(2*time.Second), ips, retained, SQLHealthCheckOutcome{StartedAt: now.Add(2 * time.Second), Results: []SQLHealthCheckResult{{IP: b}}}, sqlRoutingPoll)
	w.completeSQLHealthCheckMode(first, 0, now.Add(3*time.Second), ips, retained, recovery, sqlRecoveryPoll)
	require.Contains(t, health.hybridPolicy.routingBanSnapshot(), a, "routing polls must preserve cached shutdown evidence on excluded IPs")
	w.completeSQLHealthCheckMode(second, 0, now.Add(4*time.Second), ips, retained, recovery, sqlRecoveryPoll)
	require.NotContains(t, health.hybridPolicy.routingBanSnapshot(), a)
	require.True(t, health.IsHealthy(b))
}

func TestLateRecoveryCannotChangeDisabledTarget(t *testing.T) {
	const ip = "10.0.0.1"
	health := newTestPoolHealth(ip)
	health.sqlHealthState = NewHealthState()
	health.sqlHealthState.Track([]string{ip})
	health.hybridPolicy = newHybridHealthPolicy(true)
	target := &sqlHealthTarget{poolHealth: health}
	w := newTestWorker(nil)
	now := time.Now()
	health.disableSQLComparisonTarget(target)
	outcome := SQLHealthCheckOutcome{StartedAt: now, Results: []SQLHealthCheckResult{{IP: ip, Err: &pgconn.PgError{Code: "57P01"}}}}
	w.completeSQLHealthCheckMode(target, 0, now, []string{ip}, map[string]struct{}{ip: {}}, outcome, sqlRecoveryPoll)
	require.Empty(t, health.hybridPolicy.routingBanSnapshot())
	require.False(t, w.recordSQLCompletePoll(target, 0, now, now, []string{ip}, outcome))
	require.Empty(t, health.sqlComparisonLatest)
	require.Empty(t, health.sqlRoutingLatest)
}

func TestRetentionKeepsPoolsUsedByRecovery(t *testing.T) {
	const ip = "10.0.0.1"
	pools := newTestSQLHealthCheckPools(t)
	_, err := pools.GetOrAdd(ip, testSQLHealthCheckConnString)
	require.NoError(t, err)
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	probe := &credentialSQLHealthProber{
		responses: map[string]fakeSQLHealthProbeResponse{ip: {inRecovery: true}},
		started:   map[string]chan struct{}{ip: started}, release: map[string]chan struct{}{ip: release}, startOnce: map[string]*sync.Once{ip: {}},
	}
	checker := NewSQLHealthChecker(context.Background(), SQLHealthCheckPoolsConfig{}, SQLHealthCheckerConfig{Pools: pools, Prober: probe})
	done := make(chan struct{})
	go func() {
		defer close(done)
		checker.Check(context.Background(), []string{ip}, func(string) string { return ip }, nil, nil, nil)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("recovery probe did not start")
	}
	checker.RetainIPs(nil)
	require.Equal(t, 1, pools.Len(), "DNS eviction must not close a pool while recovery is using it")
	unblock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("recovery probe did not finish")
	}
	checker.RetainIPs(nil)
	require.Zero(t, pools.Len(), "the next retention pass must close the departed IP's idle pool")
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
	w.completeSQLHealthCheckMode(target, 0, now, []string{ip}, map[string]struct{}{ip: {}}, SQLHealthCheckOutcome{StartedAt: now, Results: []SQLHealthCheckResult{{IP: ip, InRecovery: true}}}, sqlRecoveryPoll)
	require.Empty(t, health.hybridPolicy.routingBanSnapshot())
}
