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

// Shared fixture for real worker scheduling and availability tests.
func newSQLRecoveryWorker(t *testing.T, ips ...string) (*HealthWorker, *PoolHealth, *credentialSQLHealthProber, HealthUpdates) {
	t.Helper()
	w := newTestWorker(newMockProber(map[string][]string{"db.example.com": ips}, ips[0]))
	probe := &credentialSQLHealthProber{responses: make(map[string]fakeSQLHealthProbeResponse)}
	for i, ip := range ips {
		probe.responses[ip] = fakeSQLHealthProbeResponse{inRecovery: i != 0}
	}
	w.newSQLChecker = func(context.Context) *SQLHealthChecker {
		return newTestSQLHealthChecker(t, probe, 1, SQLHealthCheckHooks{})
	}
	updates := w.Subscribe("db.example.com", false, "db", nil, &SQLHealthConfig{
		ConnStringForIP: func(ip string) string { return ip },
		RoutingEnabled:  func(context.Context) SQLHealthFeatureFlagEvaluation { return validSQLHealthFeatureFlag(true) },
	})
	t.Cleanup(func() { w.Unsubscribe(updates) })
	health := receivePoolHealth(t, updates)
	w.RefreshSQL()
	return w, health, probe, updates
}

func TestSplitSQLPollReportsMultiplePrimariesWithoutDuplicates(t *testing.T) {
	const a, b, c, d = "10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4"
	for _, tt := range []struct {
		name      string
		banned    []string
		primaries []string
		reports   [][]string
	}{
		{"one primary in each half", []string{a}, []string{a, b}, [][]string{{a, b}}},
		{"routing already reported", []string{a}, []string{a, b, c}, [][]string{{b, c}}},
		{"recovery already reported", []string{a, b}, []string{a, b, c}, [][]string{{a, b}}},
		{"both halves already reported", []string{a, b}, []string{a, b, c, d}, [][]string{{a, b}, {c, d}}},
		{"only one primary", []string{a}, []string{b}, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			w, health, probe, _ := newSQLRecoveryWorker(t, a, b, c, d)
			probe.mu.Lock()
			for _, ip := range tt.banned {
				probe.responses[ip] = fakeSQLHealthProbeResponse{err: &pgconn.PgError{Code: "57P01"}}
			}
			probe.mu.Unlock()
			w.RefreshSQL()
			for _, ip := range tt.banned {
				require.Contains(t, health.hybridPolicy.routingBanSnapshot(), ip)
			}

			var mu sync.Mutex
			var reports [][]string
			var reportTags [][]string
			target := health.sqlTargetValues()[0]
			target.checker.hooks.MultiplePrimaries = func(ips, tags []string) {
				mu.Lock()
				defer mu.Unlock()
				reports = append(reports, append([]string(nil), ips...))
				reportTags = append(reportTags, append([]string(nil), tags...))
			}
			probe.mu.Lock()
			for _, ip := range []string{a, b, c, d} {
				probe.responses[ip] = fakeSQLHealthProbeResponse{inRecovery: true}
			}
			for _, ip := range tt.primaries {
				probe.responses[ip] = fakeSQLHealthProbeResponse{}
			}
			probe.mu.Unlock()
			w.RefreshSQL()

			mu.Lock()
			defer mu.Unlock()
			require.ElementsMatch(t, tt.reports, reports)
			for _, tags := range reportTags {
				require.ElementsMatch(t, append(append([]string(nil), target.tags...), "pool_role:primary"), tags)
			}
		})
	}
}

func TestBannedRecoveryDoesNotBlockRoutingOrAvailability(t *testing.T) {
	const a, b, c = "10.0.0.1", "10.0.0.2", "10.0.0.3"
	w, health, probe, updates := newSQLRecoveryWorker(t, a, b, c)
	probe.mu.Lock()
	probe.responses[a] = fakeSQLHealthProbeResponse{err: &pgconn.PgError{Code: "57P01"}}
	probe.mu.Unlock()
	w.RefreshSQL()
	require.Contains(t, health.hybridPolicy.routingBanSnapshot(), a)
	subscriber, err := health.SubscribeSQLHealth(updates)
	require.NoError(t, err)
	t.Cleanup(subscriber.Close)
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(func() {
		unblock()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("recovery did not finish after release")
		}
	})
	probe.mu.Lock()
	probe.responses[a] = fakeSQLHealthProbeResponse{err: context.DeadlineExceeded}
	probe.responses[b] = fakeSQLHealthProbeResponse{}
	probe.started = map[string]chan struct{}{a: started}
	probe.release = map[string]chan struct{}{a: release}
	probe.startOnce = map[string]*sync.Once{a: {}}
	before := probe.calls[a]
	probe.mu.Unlock()
	var raw, missed atomic.Int64
	w.hooks.SQLPatroniCompared = func(SQLPatroniHealthSnapshot, []string) { raw.Add(1) }
	w.hooks.SQLCheckMissed = func([]string) { missed.Add(1) }
	go func() { defer close(done); w.RefreshSQL() }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("recovery did not start")
	}
	baseline := receiveSQLHealthObservation(t, subscriber)
	require.True(t, baseline.Writable)
	target := health.sqlTargetValues()[0]
	require.Eventually(t, func() bool { return health.IsHealthy(b) && !target.sqlRoundRunning() }, time.Second, time.Millisecond)
	require.Zero(t, raw.Load(), "raw comparisons wait for the original complete poll")

	// Registering another subscriber must not queue a writer behind recovery
	// and prevent the following routing tick from taking its read lock.
	subscribed := make(chan error, 1)
	go func() {
		another, err := health.SubscribeSQLHealth(updates)
		if err == nil {
			another.Close()
		}
		subscribed <- err
	}()
	select {
	case err := <-subscribed:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("subscriber waited for banned-IP recovery")
	}

	// A second routing tick must run while the original recovery is blocked.
	probe.mu.Lock()
	probe.responses[b] = fakeSQLHealthProbeResponse{inRecovery: true}
	probe.responses[c] = fakeSQLHealthProbeResponse{}
	probe.mu.Unlock()
	w.runSQLHealthGroup(w.sqlHealthGroups()[0], time.Now(), false)
	require.Eventually(t, func() bool { return health.IsHealthy(c) && !health.IsHealthy(b) && !target.sqlRoundRunning() }, time.Second, time.Millisecond)
	require.True(t, receiveSQLHealthObservation(t, subscriber).Writable)

	// Losing the eligible primary must replace the earlier writable observation.
	probe.mu.Lock()
	probe.responses[c] = fakeSQLHealthProbeResponse{inRecovery: true}
	probe.mu.Unlock()
	w.RefreshSQL()
	unavailable := receiveSQLHealthObservation(t, subscriber)
	require.False(t, unavailable.Writable)
	require.ErrorIs(t, unavailable.Err, ErrSQLHealthCheckMissed)
	require.True(t, unavailable.ScheduledAt.After(baseline.ScheduledAt))
	require.Equal(t, unavailable.ScheduledAt, unavailable.ObservedAt)
	require.Zero(t, raw.Load())
	require.Zero(t, missed.Load(), "eligible probes ran; the scheduler did not miss this tick")

	probe.mu.Lock()
	probe.responses[c] = fakeSQLHealthProbeResponse{}
	probe.mu.Unlock()
	w.RefreshSQL()
	healthy := receiveSQLHealthObservation(t, subscriber)
	require.True(t, healthy.Writable)
	require.True(t, healthy.ScheduledAt.After(unavailable.ScheduledAt))
	select {
	case extra := <-subscriber.Updates():
		t.Fatalf("unexpected observation after writable result: %+v", extra)
	default:
	}
	require.Equal(t, before+1, probe.callCount(a), "do not start duplicate recovery probes")

	// Shutdown still needs exclusive access and must wait for the active probe.
	closed := make(chan struct{})
	go func() {
		target.closeSQL()
		close(closed)
	}()
	require.Eventually(t, func() bool {
		if target.sqlCheckMu.TryRLock() {
			target.sqlCheckMu.RUnlock()
			return false
		}
		return true
	}, time.Second, time.Millisecond, "shutdown did not queue its exclusive lock")
	select {
	case <-closed:
		t.Fatal("checker closed while recovery was still probing")
	default:
	}
	unblock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("old recovery poll did not finish")
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("checker did not close after recovery finished")
	}
	select {
	case <-subscriber.Done():
	default:
		t.Fatal("shutdown left its subscriber open")
	}
	_, err = health.SubscribeSQLHealth(updates)
	require.ErrorIs(t, err, ErrSQLHealthUpdatesUnsupported)
	require.EqualValues(t, 1, raw.Load())
	require.True(t, health.IsHealthy(c), "the older raw completion must not restore B")
	require.Contains(t, health.hybridPolicy.routingBanSnapshot(), a)
	select {
	case extra := <-subscriber.Updates():
		t.Fatalf("old completion overwrote current availability: %+v", extra)
	default:
	}
}

func TestBusyRecoveryDoesNotPublishInvalidatedAvailability(t *testing.T) {
	for _, retired := range []bool{false, true} {
		name := "disabled"
		if retired {
			name = "retired"
		}
		t.Run(name, func(t *testing.T) {
			const a, b = "10.0.0.1", "10.0.0.2"
			w, health, probe, updates := newSQLRecoveryWorker(t, a, b)
			subscriber, err := health.SubscribeSQLHealth(updates)
			require.NoError(t, err)
			t.Cleanup(subscriber.Close)
			target := health.sqlTargetValues()[0]
			health.hybridPolicy.routingBannedIPs[a] = time.Now()
			target.recoveryRunning = true // Model an already active recovery from a prior tick.
			started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			t.Cleanup(func() {
				unblock()
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Error("invalidated poll did not finish after release")
				}
			})
			probe.mu.Lock()
			probe.started = map[string]chan struct{}{b: started}
			probe.release = map[string]chan struct{}{b: release}
			probe.startOnce = map[string]*sync.Once{b: {}}
			probe.mu.Unlock()
			go func() { defer close(done); w.RefreshSQL() }()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("eligible probe did not start")
			}
			if retired {
				health.retireSQLComparisonTarget(target)
			} else {
				health.disableSQLComparisonTarget(target)
			}
			unblock()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("invalidated poll did not finish")
			}
			select {
			case extra := <-subscriber.Updates():
				t.Fatalf("invalidated poll published availability: %+v", extra)
			default:
			}
		})
	}
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
	w.completeSQLHealthCheckMode(target, 0, now, []string{ip}, map[string]struct{}{ip: {}}, outcome, sqlRecoveryPoll, nil)
	require.Empty(t, health.hybridPolicy.routingBanSnapshot())
	require.False(t, w.recordSQLCompletePoll(target, 0, now, now, []string{ip}, outcome))
	require.Empty(t, health.sqlComparisonLatest)
	require.Empty(t, health.sqlRoutingLatest)
	require.Empty(t, health.sqlBanLatest)
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
