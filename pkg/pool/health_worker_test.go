// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package pool

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/puddle/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const maxFailedHostLookups = 5

func validSQLHealthFeatureFlag(enabled bool) SQLHealthFeatureFlagEvaluation {
	return SQLHealthFeatureFlagEvaluation{Enabled: enabled, Valid: true}
}

type mockProber struct {
	mu sync.Mutex

	ipsByHost      map[string][]string
	healthyIPs     map[string]bool
	probeErrors    map[string]error
	lookupCalls    int
	probeCalls     int
	recordLookup   func(host string, tags []string)
	recordCall     func(ip, host, dbName string, isReadReplica bool, tags []string)
	mockHealthPing func(ip string) (bool, error)
}

type blockingLookupProber struct {
	lookupStarted chan struct{}
	releaseLookup chan struct{}
	startedOnce   sync.Once
}

func (p *blockingLookupProber) Lookup(string, []string) []string {
	p.startedOnce.Do(func() { close(p.lookupStarted) })
	<-p.releaseLookup
	return []string{"10.0.0.1"}
}

func (*blockingLookupProber) Probe(string, string, string, bool, []string) (bool, string, error) {
	return true, "", nil
}

type poolCreatingSQLHealthProber struct {
	mu    sync.Mutex
	pools *SQLHealthCheckPools
	calls int
}

func (p *poolCreatingSQLHealthProber) Probe(_ context.Context, ip, connString string, _ []string) (bool, error) {
	p.mu.Lock()
	p.calls++
	p.mu.Unlock()
	_, err := p.pools.GetOrAdd(ip, connString)
	return false, err
}

func (p *poolCreatingSQLHealthProber) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

type blockingPoolCreatingSQLHealthProber struct {
	pools     *SQLHealthCheckPools
	started   chan struct{}
	release   chan struct{}
	once      sync.Once
	standbyIP string
}

func (p *blockingPoolCreatingSQLHealthProber) Probe(_ context.Context, ip, connString string, _ []string) (bool, error) {
	if _, err := p.pools.GetOrAdd(ip, connString); err != nil {
		return false, err
	}
	p.once.Do(func() { close(p.started) })
	<-p.release
	return ip == p.standbyIP, nil
}

type blockingRetrySQLHealthProber struct {
	mu           sync.Mutex
	calls        int
	retryStarted chan struct{}
	releaseRetry chan struct{}
	startedOnce  sync.Once
}

func (p *blockingRetrySQLHealthProber) Probe(context.Context, string, string, []string) (bool, error) {
	p.mu.Lock()
	p.calls++
	call := p.calls
	p.mu.Unlock()
	if call == 1 {
		return false, errors.New("temporary")
	}
	p.startedOnce.Do(func() { close(p.retryStarted) })
	<-p.releaseRetry
	return false, nil
}

func (p *blockingRetrySQLHealthProber) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

type partiallyBlockingSQLHealthProber struct {
	blockingIP       string
	blockingRecovery bool
	blocked          chan struct{}
	release          chan struct{}
	blockedOnce      sync.Once
}

func (p *partiallyBlockingSQLHealthProber) Probe(_ context.Context, ip, _ string, _ []string) (bool, error) {
	if ip != p.blockingIP {
		return false, nil
	}
	p.blockedOnce.Do(func() { close(p.blocked) })
	<-p.release
	return p.blockingRecovery, nil
}

type credentialBlockingSQLHealthProber struct {
	mu             sync.Mutex
	slowConnection string
	slowStarted    chan struct{}
	releaseSlow    chan struct{}
	startOnce      sync.Once
	calls          map[string]int
}

func (p *credentialBlockingSQLHealthProber) Probe(
	_ context.Context,
	_ string,
	connString string,
	_ []string,
) (bool, error) {
	p.mu.Lock()
	p.calls[connString]++
	p.mu.Unlock()
	if connString == p.slowConnection {
		p.startOnce.Do(func() { close(p.slowStarted) })
		<-p.releaseSlow
	}
	return false, nil
}

func (p *credentialBlockingSQLHealthProber) callCount(connString string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls[connString]
}

func (p *mockProber) Lookup(host string, tags []string) []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lookupCalls++
	if p.recordLookup != nil {
		p.recordLookup(host, tags)
	}
	ips, ok := p.ipsByHost[host]
	if !ok {
		return nil
	}
	out := make([]string, len(ips))
	copy(out, ips)
	return out
}

func (p *mockProber) Probe(ip, host, dbName string, isReadReplica bool, tags []string) (bool, string, error) {
	p.mu.Lock()
	p.probeCalls++
	if p.recordCall != nil {
		p.recordCall(ip, host, dbName, isReadReplica, tags)
	}
	hook := p.mockHealthPing
	p.mu.Unlock()

	if hook != nil {
		healthy, err := hook(ip)
		return healthy, "", err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.healthyIPs[ip], "", p.probeErrors[ip]
}

func newTestWorker(p *mockProber) *HealthWorker {
	w := NewHealthWorker(HealthWorkerConfig{Interval: time.Second, AbsenceThreshold: maxFailedHostLookups, Prober: p})
	return w
}

func newMockProber(ipsByHost map[string][]string, healthyIPs ...string) *mockProber {
	hset := make(map[string]bool, len(healthyIPs))
	for _, ip := range healthyIPs {
		hset[ip] = true
	}
	return &mockProber{ipsByHost: ipsByHost, healthyIPs: hset, probeErrors: map[string]error{}}
}

func receivePoolHealth(t *testing.T, updates HealthUpdates) *PoolHealth {
	t.Helper()
	select {
	case poolHealth, ok := <-updates:
		assert.True(t, ok, "health update channel closed unexpectedly")
		return poolHealth
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for health update")
		return nil
	}
}

func TestSubscribe_RegisterNewPoolNewHost(t *testing.T) {
	p := newMockProber(
		map[string][]string{"primary.local": {"10.0.0.1", "10.0.0.2", "10.0.0.3"}},
		"10.0.0.1", "10.0.0.3", // 0.2 is unhealthy
	)
	w := newTestWorker(p)

	poolHealth := receivePoolHealth(t, w.Subscribe("primary.local", false, "mydb", []string{"tag:value"}, nil))

	assert.ElementsMatch(t, []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}, poolHealth.Snapshot().Resolved)
	assert.True(t, poolHealth.IsHealthy("10.0.0.1"))
	assert.False(t, poolHealth.IsHealthy("10.0.0.2"))
	assert.True(t, poolHealth.IsHealthy("10.0.0.3"))
	assert.False(t, poolHealth.IsHealthy("10.0.0.99"), "unknown IPs report false")
}

func TestSubscribe_RegisterNewPoolReusesPoolHealth(t *testing.T) {
	p := newMockProber(map[string][]string{"primary.local": {"10.0.0.1"}}, "10.0.0.1")
	w := newTestWorker(p)

	u1 := w.Subscribe("primary.local", false, "mydb", nil, nil)
	s1 := receivePoolHealth(t, u1)
	u2 := w.Subscribe("primary.local", false, "mydb", nil, nil)
	s2 := receivePoolHealth(t, u2)
	assert.Same(t, s1, s2, "same key returns same poolHealth")

	s1.mu.RLock()
	assert.Equal(t, 2, s1.refcount, "refcount incremented")
	s1.mu.RUnlock()
}

func TestSubscribe_BroadcastsUpdatesToEverySubscriber(t *testing.T) {
	p := newMockProber(map[string][]string{"primary.local": {"10.0.0.1"}}, "10.0.0.1")
	w := newTestWorker(p)

	u1 := w.Subscribe("primary.local", false, "mydb", nil, nil)
	u2 := w.Subscribe("primary.local", false, "mydb", nil, nil)
	poolHealth := receivePoolHealth(t, u1)
	assert.Same(t, poolHealth, receivePoolHealth(t, u2))

	p.mu.Lock()
	p.ipsByHost["primary.local"] = []string{"10.0.0.1", "10.0.0.2"}
	p.healthyIPs["10.0.0.2"] = true
	p.mu.Unlock()
	w.Refresh()

	assert.Same(t, poolHealth, receivePoolHealth(t, u1))
	assert.Same(t, poolHealth, receivePoolHealth(t, u2))
	assert.ElementsMatch(t, []string{"10.0.0.1", "10.0.0.2"}, poolHealth.Snapshot().Resolved)
}

func TestSubscribe_DifferentHosts_SeparatePoolHealths(t *testing.T) {
	p := newMockProber(
		map[string][]string{
			"primary-a.local": {"10.0.0.1"},
			"primary-b.local": {"10.0.0.2"},
		},
		"10.0.0.1", "10.0.0.2",
	)
	w := newTestWorker(p)

	a := receivePoolHealth(t, w.Subscribe("primary-a.local", false, "db_a", nil, nil))
	b := receivePoolHealth(t, w.Subscribe("primary-b.local", false, "db_b", nil, nil))

	assert.ElementsMatch(t, []string{"10.0.0.1"}, a.Snapshot().Resolved)
	assert.ElementsMatch(t, []string{"10.0.0.2"}, b.Snapshot().Resolved)
}

func TestSubscribe_SQLConnectionStringsShareHostStateWhenSQLDisabled(t *testing.T) {
	p := newMockProber(map[string][]string{"primary.local": {"10.0.0.1"}}, "10.0.0.1")
	w := newTestWorker(p)

	firstUpdates := w.Subscribe("primary.local", false, "first", nil, &SQLHealthConfig{
		ConnStringForIP: func(string) string { return "user=first password=secret dbname=first" },
	})
	first := receivePoolHealth(t, firstUpdates)
	secondUpdates := w.Subscribe("primary.local", false, "second", nil, &SQLHealthConfig{
		ConnStringForIP: func(string) string { return "user=second password=secret dbname=second" },
	})
	second := receivePoolHealth(t, secondUpdates)

	assert.Same(t, first, second, "disabled SQL checking must preserve host-level Patroni sharing")
	assert.Equal(t, 1, p.probeCalls, "only the first host subscriber should probe inline")
	assert.Len(t, w.hosts, 1)
	w.Unsubscribe(firstUpdates)
	w.Unsubscribe(secondUpdates)
}

func TestSubscribe_SQLConnectionStringsSharePatroniStateWhenSQLCheckingEnabled(t *testing.T) {
	const (
		firstConnString  = "user=first password=secret dbname=first"
		secondConnString = "user=second password=secret dbname=second"
	)
	p := newMockProber(map[string][]string{"primary.local": {"10.0.0.1"}}, "10.0.0.1")
	w := newTestWorker(p)
	sqlProber := &fakeSQLHealthProber{
		responses: map[string][]fakeSQLHealthProbeResponse{
			firstConnString:  {{}},
			secondConnString: {{inRecovery: true}},
		},
		calls: make(map[string]int),
	}
	w.newSQLChecker = func(ctx context.Context) *SQLHealthChecker {
		return NewSQLHealthChecker(ctx, SQLHealthCheckPoolsConfig{}, SQLHealthCheckerConfig{
			Prober: sqlProber,
		})
	}

	firstUpdates := w.Subscribe("primary.local", false, "first", []string{
		"database:first",
		"database_host:first.proxy",
	}, &SQLHealthConfig{
		ConnStringForIP: func(string) string { return firstConnString },
	})
	first := receivePoolHealth(t, firstUpdates)
	secondUpdates := w.Subscribe("primary.local", false, "second", []string{
		"database:second",
		"database_host:second.proxy",
	}, &SQLHealthConfig{
		ConnStringForIP: func(string) string { return secondConnString },
	})
	second := receivePoolHealth(t, secondUpdates)

	assert.Same(t, first, second, "SQL connection strings must share the host-level Patroni snapshot")
	assert.Equal(t, 1, p.probeCalls, "only the first host subscriber should probe inline")
	assert.Len(t, w.hosts, 1)
	assert.Len(t, first.sqlTargetValues(), 2, "SQL connection strings retain independent targets")

	p.mu.Lock()
	p.lookupCalls = 0
	p.probeCalls = 0
	sharedTags := []string{"postgres_host:primary.local", "pool_role:primary"}
	p.recordLookup = func(host string, tags []string) {
		assert.Equal(t, "primary.local", host)
		assert.Equal(t, sharedTags, tags)
	}
	p.recordCall = func(ip, host, dbName string, isReadReplica bool, tags []string) {
		assert.Equal(t, "10.0.0.1", ip)
		assert.Equal(t, "primary.local", host)
		assert.Equal(t, "first", dbName, "the deterministic representative remains log context only")
		assert.False(t, isReadReplica)
		assert.Equal(t, sharedTags, tags)
	}
	p.mu.Unlock()
	w.hooks.CheckStarted = func(tags []string) {
		assert.Equal(t, sharedTags, tags)
	}
	w.hooks.CheckFinished = func(_ time.Duration, tags []string) {
		assert.Equal(t, sharedTags, tags)
	}
	w.Refresh()
	p.mu.Lock()
	assert.Equal(t, 1, p.lookupCalls, "periodic DNS lookups must be shared by host")
	assert.Equal(t, 1, p.probeCalls, "periodic Patroni checks must be shared by host")
	p.mu.Unlock()
	assert.Same(t, first, receivePoolHealth(t, firstUpdates))
	assert.Same(t, second, receivePoolHealth(t, secondUpdates))

	p.mu.Lock()
	p.lookupCalls = 0
	p.mu.Unlock()
	var comparisonsMu sync.Mutex
	var comparisons []SQLPatroniHealthSnapshot
	w.hooks.SQLPatroniCompared = func(comparison SQLPatroniHealthSnapshot, tags []string) {
		comparisonsMu.Lock()
		defer comparisonsMu.Unlock()
		assert.Equal(t, sharedTags, tags)
		comparisons = append(comparisons, comparison)
	}
	w.RefreshSQL()
	p.mu.Lock()
	assert.Equal(t, 1, p.lookupCalls, "SQL DNS lookup must be shared across credential targets")
	p.mu.Unlock()
	assert.Equal(t, 1, sqlProber.callCount(firstConnString))
	assert.Equal(t, 1, sqlProber.callCount(secondConnString))
	require.Len(t, comparisons, 2, "each credential completion must produce a comparison")
	assert.ElementsMatch(t, []int{1, 2}, []int{len(comparisons[0].SQLResults), len(comparisons[1].SQLResults)})
	assert.Equal(t, []string{"10.0.0.1"}, comparisons[1].SQLRetainedIPs)

	w.Unsubscribe(firstUpdates)
	w.Unsubscribe(secondUpdates)
}

func TestSQLRoundRestartsFinishedCredentialWhileAnotherCredentialIsStillRunning(t *testing.T) {
	const (
		ip             = "10.0.0.1"
		fastConnection = "user=fast"
		slowConnection = "user=slow"
	)
	patroniProber := newMockProber(map[string][]string{"primary.local": {ip}}, ip)
	w := newTestWorker(patroniProber)
	sqlProber := &credentialBlockingSQLHealthProber{
		slowConnection: slowConnection,
		slowStarted:    make(chan struct{}),
		releaseSlow:    make(chan struct{}),
		calls:          make(map[string]int),
	}
	w.newSQLChecker = func(ctx context.Context) *SQLHealthChecker {
		return NewSQLHealthChecker(ctx, SQLHealthCheckPoolsConfig{}, SQLHealthCheckerConfig{
			Prober: sqlProber,
		})
	}

	fastUpdates := w.Subscribe("primary.local", false, "fast", nil, &SQLHealthConfig{
		ConnStringForIP: func(string) string { return fastConnection },
	})
	health := receivePoolHealth(t, fastUpdates)
	slowUpdates := w.Subscribe("primary.local", false, "slow", []string{"database:slow"}, &SQLHealthConfig{
		ConnStringForIP: func(string) string { return slowConnection },
	})
	require.Same(t, health, receivePoolHealth(t, slowUpdates))
	fastSQLUpdates, err := health.SubscribeSQLHealth(fastUpdates)
	require.NoError(t, err)
	var releaseOnce sync.Once
	releaseSlow := func() { releaseOnce.Do(func() { close(sqlProber.releaseSlow) }) }
	t.Cleanup(func() {
		releaseSlow()
		fastSQLUpdates.Close()
		w.Unsubscribe(fastUpdates)
		w.Unsubscribe(slowUpdates)
	})
	missed := make(chan []string, 2)
	w.hooks.SQLCheckMissed = func(tags []string) {
		missed <- append([]string(nil), tags...)
	}

	var iterations atomic.Int32
	w.sqlHooks.Iteration = func(SQLIteration, []string) { iterations.Add(1) }
	refreshDone := make(chan struct{})
	go func() {
		w.RefreshSQL()
		close(refreshDone)
	}()
	select {
	case <-sqlProber.slowStarted:
	case <-time.After(time.Second):
		t.Fatal("slow credential did not start")
	}
	fastObservation := receiveSQLHealthObservation(t, fastSQLUpdates)
	require.True(t, fastObservation.Writable)
	require.Equal(t, 1, sqlProber.callCount(fastConnection))
	require.Equal(t, 1, sqlProber.callCount(slowConnection))
	var fastTarget *sqlHealthTarget
	for _, target := range health.sqlTargetValues() {
		if target.database == "fast" {
			fastTarget = target
			break
		}
	}
	require.NotNil(t, fastTarget)
	require.Eventually(t, func() bool { return !fastTarget.sqlRoundRunning() }, time.Second, time.Millisecond,
		"the early writable observation may arrive just before the fast target finishes its full poll")

	assert.EqualValues(t, 1, iterations.Load(), "fast credential evaluates without waiting for slow credential")

	w.checkSQLHealthGroup(sqlHealthGroup{
		poolHealth: health,
		targets:    health.sqlTargetValues(),
	}, time.Now())
	select {
	case tags := <-missed:
		assert.Contains(t, tags, "database:slow")
	case <-time.After(time.Second):
		t.Fatal("slow credential did not report its missed interval")
	}
	select {
	case tags := <-missed:
		t.Fatalf("fast credential's writable result was overwritten by missed tags %v", tags)
	default:
	}
	assert.Equal(t, 2, sqlProber.callCount(fastConnection),
		"the fast credential starts the new tick independently")
	assert.Equal(t, 1, sqlProber.callCount(slowConnection))
	assert.EqualValues(t, 2, iterations.Load())

	releaseSlow()
	select {
	case <-refreshDone:
	case <-time.After(time.Second):
		t.Fatal("host-wide SQL round did not finish")
	}
	for _, target := range health.sqlTargetValues() {
		assert.False(t, target.sqlRoundRunning())
	}

	assert.EqualValues(t, 3, iterations.Load())
	w.RefreshSQL()
	assert.EqualValues(t, 5, iterations.Load())
	assert.Equal(t, 3, sqlProber.callCount(fastConnection))
	assert.Equal(t, 2, sqlProber.callCount(slowConnection))
}

func TestUnsubscribe_RefcountAboveZero_KeepsPoolHealth(t *testing.T) {
	p := newMockProber(map[string][]string{"primary.local": {"10.0.0.1"}}, "10.0.0.1")
	w := newTestWorker(p)

	u1 := w.Subscribe("primary.local", false, "mydb", nil, nil)
	s := receivePoolHealth(t, u1)
	u2 := w.Subscribe("primary.local", false, "mydb", nil, nil)
	_ = receivePoolHealth(t, u2)

	w.Unsubscribe(u1)

	w.mu.Lock()
	_, present := w.hosts[s.host]
	w.mu.Unlock()
	assert.True(t, present, "poolHealth still in map while refcount > 0")

	s.mu.RLock()
	assert.Equal(t, 1, s.refcount)
	s.mu.RUnlock()
}

func TestUnsubscribe_RefcountZero_RemovesPoolHealth(t *testing.T) {
	p := newMockProber(map[string][]string{"primary.local": {"10.0.0.1"}}, "10.0.0.1")
	w := newTestWorker(p)

	updates := w.Subscribe("primary.local", false, "mydb", nil, nil)
	s := receivePoolHealth(t, updates)
	w.Unsubscribe(updates)

	w.mu.Lock()
	_, present := w.hosts[s.host]
	w.mu.Unlock()
	assert.False(t, present, "poolHealth removed from map at refcount 0")
	_, ok := <-updates
	assert.False(t, ok, "subscriber channel should close on unsubscribe")
}

func TestCentralizedSQLHealthCheckPublishesWithoutChangingPatroniRouting(t *testing.T) {
	p := newMockProber(map[string][]string{"primary.local": {"10.0.0.1"}}, "10.0.0.1")
	w := newTestWorker(p)
	sqlProber := &fakeSQLHealthProber{
		responses: map[string][]fakeSQLHealthProbeResponse{
			"10.0.0.1": {
				{inRecovery: false},
				{inRecovery: true},
			},
		},
		calls: make(map[string]int),
	}
	checker := newTestSQLHealthChecker(t, sqlProber, 1, SQLHealthCheckHooks{})
	w.newSQLChecker = func(context.Context) *SQLHealthChecker {
		return checker
	}
	updates := w.Subscribe("primary.local", false, "mydb", nil, &SQLHealthConfig{
		ConnStringForIP: func(ip string) string { return ip },
	})
	health := receivePoolHealth(t, updates)
	p.mu.Lock()
	lookupCallsAfterSubscribe := p.lookupCalls
	p.mu.Unlock()
	sqlUpdates, err := health.SubscribeSQLHealth()
	require.NoError(t, err)
	t.Cleanup(sqlUpdates.Close)

	firstStartedAt := time.Now()
	w.RefreshSQL()
	firstFinishedAt := time.Now()
	first := receiveSQLHealthObservation(t, sqlUpdates)
	assert.False(t, first.ObservedAt.Before(firstStartedAt))
	assert.False(t, first.ObservedAt.After(firstFinishedAt))
	assert.True(t, first.Writable)
	assert.NoError(t, first.Err)

	secondStartedAt := time.Now()
	w.RefreshSQL()
	secondFinishedAt := time.Now()
	second := receiveSQLHealthObservation(t, sqlUpdates)
	assert.False(t, second.ObservedAt.Before(secondStartedAt))
	assert.False(t, second.ObservedAt.After(secondFinishedAt))
	assert.False(t, second.Writable)
	assert.NoError(t, second.Err)

	assert.Equal(t, 2, sqlProber.callCount("10.0.0.1"), "shadow SQL checks must continue running")
	assert.True(t, health.IsHealthy("10.0.0.1"), "SQL observations must not alter routing in this PR")
	p.mu.Lock()
	assert.Equal(t, lookupCallsAfterSubscribe+2, p.lookupCalls, "each SQL refresh must run its own discovery")
	p.mu.Unlock()

	w.Unsubscribe(updates)
	select {
	case <-sqlUpdates.Done():
	case <-time.After(time.Second):
		t.Fatal("SQL health subscription was not closed")
	}
	w.RefreshSQL()
	assert.Equal(t, 2, sqlProber.callCount("10.0.0.1"), "unsubscribed SQL target still ran")
}

func TestHybridPolicyPublishesAggregatedSQLRouting(t *testing.T) {
	const patroniPrimary, sqlPrimary = "10.0.0.1", "10.0.0.2"
	patroniProber := newMockProber(
		map[string][]string{"primary.local": {patroniPrimary, sqlPrimary}},
		patroniPrimary,
	)
	w := newTestWorker(patroniProber)
	sqlProber := &fakeSQLHealthProber{
		responses: map[string][]fakeSQLHealthProbeResponse{
			patroniPrimary: {{inRecovery: true}},
			sqlPrimary:     {{inRecovery: false}},
		},
		calls: make(map[string]int),
	}
	var comparisonMu sync.Mutex
	patroniComparisons := make(map[string]bool)
	w.newSQLChecker = func(context.Context) *SQLHealthChecker {
		return newTestSQLHealthChecker(t, sqlProber, 1, SQLHealthCheckHooks{
			ProbeFinished: func(result SQLHealthCheckResult, patroniHealthy bool, _ time.Duration, _ []string) {
				comparisonMu.Lock()
				defer comparisonMu.Unlock()
				patroniComparisons[result.IP] = patroniHealthy
			},
		})
	}
	var iterations []SQLIteration
	w.sqlHooks.Iteration = func(iteration SQLIteration, _ []string) {
		iterations = append(iterations, iteration)
	}

	updates := w.Subscribe("primary.local", false, "mydb", nil, &SQLHealthConfig{
		ConnStringForIP: func(ip string) string { return ip },
		RoutingEnabled:  func(context.Context) SQLHealthFeatureFlagEvaluation { return validSQLHealthFeatureFlag(true) },
	})
	health := receivePoolHealth(t, updates)
	t.Cleanup(func() { w.Unsubscribe(updates) })

	w.RefreshSQL()

	require.Len(t, iterations, 1)
	assert.Equal(t, SQLRoutingResultConclusive, iterations[0].RoutingResult)
	assert.False(t, health.IsHealthy(patroniPrimary), "SQL must replace the old Patroni primary")
	assert.True(t, health.IsHealthy(sqlPrimary), "a conclusive aggregate must publish the SQL leader")

	// After SQL takes over routing, per-probe comparisons must still use Patroni's
	// independent results rather than reporting agreement with SQL's own route.
	w.RefreshSQL()
	assert.Equal(t, map[string]bool{patroniPrimary: true, sqlPrimary: false}, patroniComparisons)
}

func TestHybridEvaluationUsesCompleteCredentialSnapshots(t *testing.T) {
	const primary, standby = "10.0.0.1", "10.0.0.2"
	for _, tc := range []struct {
		name             string
		slowErr, fastErr error
		fallback         bool
	}{
		{name: "successful snapshots"},
		{name: "same IP timeout resolved", slowErr: context.DeadlineExceeded},
		{name: "different IP unresolved", slowErr: context.DeadlineExceeded, fastErr: context.DeadlineExceeded, fallback: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := NewHealthState()
			state.Track([]string{primary, standby})
			health := &PoolHealth{
				state:              NewHealthState(),
				patroniHealthState: NewHealthState(),
				sqlHealthState:     state,
				hybridPolicy:       newHybridHealthPolicy(true),
				routingEnabledFn:   func(context.Context) SQLHealthFeatureFlagEvaluation { return validSQLHealthFeatureFlag(true) },
			}
			fast := &sqlHealthTarget{poolHealth: health}
			slow := &sqlHealthTarget{poolHealth: health}
			now := time.Now()
			health.recordSQLComparisonPoll(false, slow, 0, now.Add(-time.Second), now.Add(-time.Millisecond), []string{primary, standby}, SQLHealthCheckOutcome{
				Results: []SQLHealthCheckResult{{IP: primary}, {IP: standby, InRecovery: true, Err: tc.slowErr}},
			})
			require.True(t, slow.tryStartSQLRound(now))
			var iterations []SQLIteration
			w := &HealthWorker{sqlHooks: SQLHealthWorkerHooks{
				Iteration: func(iteration SQLIteration, _ []string) { iterations = append(iterations, iteration) },
			}}
			w.completeSQLHealthCheck(fast, 0, now, []string{primary, standby}, map[string]struct{}{primary: {}, standby: {}}, SQLHealthCheckOutcome{
				Results: []SQLHealthCheckResult{{IP: primary}, {IP: standby, InRecovery: true, Err: tc.fastErr}},
			})
			require.Len(t, iterations, 1)
			assert.True(t, slow.sqlRoundRunning(), "evaluation must not wait for another credential's next poll")
			require.Len(t, health.sqlComparisonLatest, 2, "reuse the existing complete credential snapshots")
			if !tc.fallback {
				assert.Equal(t, SQLRoutingResultConclusive, iterations[0].RoutingResult)
				assert.True(t, health.IsHealthy(primary), "publish the aggregated route while the other credential remains busy")
				assert.False(t, health.IsHealthy(standby))
				assert.True(t, health.sqlRoutingActive)
			} else {
				assert.Equal(t, SQLRoutingResultCachedFallback, iterations[0].RoutingResult)
				assert.Equal(t, SQLHealthFallbackTimeout, iterations[0].FallbackReason)
			}
		})
	}
}

func TestHybridEvaluationLateTimeoutDoesNotOverwriteOtherCredentials(t *testing.T) {
	const ip = "10.0.0.1"
	state := NewHealthState()
	state.Track([]string{ip})
	health := &PoolHealth{
		state:              NewHealthState(),
		patroniHealthState: NewHealthState(),
		sqlHealthState:     state,
		hybridPolicy:       newHybridHealthPolicy(true),
		routingEnabledFn:   func(context.Context) SQLHealthFeatureFlagEvaluation { return validSQLHealthFeatureFlag(true) },
	}
	a, b, c := &sqlHealthTarget{poolHealth: health}, &sqlHealthTarget{poolHealth: health}, &sqlHealthTarget{poolHealth: health}
	var iterations []SQLIteration
	var conclusiveErrors []error
	w := &HealthWorker{sqlHooks: SQLHealthWorkerHooks{
		Iteration:       func(iteration SQLIteration, _ []string) { iterations = append(iterations, iteration) },
		ConclusiveError: func(result SQLHealthCheckResult, _ []string) { conclusiveErrors = append(conclusiveErrors, result.Err) },
	}}
	t0 := time.Now()
	complete := func(target *sqlHealthTarget, scheduledAt time.Time, err error) {
		w.completeSQLHealthCheck(target, 0, scheduledAt, []string{ip}, map[string]struct{}{ip: {}}, SQLHealthCheckOutcome{
			Results: []SQLHealthCheckResult{{IP: ip, Err: err}},
		})
	}
	// A and B finish twice while C's first round remains in flight.
	require.True(t, c.tryStartSQLRound(t0))
	complete(a, t0, nil)
	complete(b, t0, nil)
	complete(a, t0.Add(time.Second), nil)
	complete(b, t0.Add(time.Second), nil)
	complete(c, t0, context.DeadlineExceeded)
	c.finishSQLRound(t0)
	require.Len(t, iterations, 5)
	for _, iteration := range iterations {
		assert.Equal(t, SQLRoutingResultConclusive, iteration.RoutingResult)
	}
	assert.ErrorIs(t, health.sqlComparisonLatest[c].Results[0].Err, context.DeadlineExceeded)
	assert.NoError(t, health.sqlComparisonLatest[a].Results[0].Err)
	assert.NoError(t, health.sqlComparisonLatest[b].Results[0].Err)
	assert.Empty(t, conclusiveErrors, "a timeout resolved by another credential is still an indeterminate error")

	// Once all latest credential results are unknown, the raw comparison becomes unknown
	// while the hybrid policy retains its last SQL leader.
	complete(a, t0.Add(2*time.Second), context.DeadlineExceeded)
	assert.Equal(t, SQLRoutingResultConclusive, iterations[len(iterations)-1].RoutingResult)
	complete(b, t0.Add(2*time.Second), context.DeadlineExceeded)
	assert.Equal(t, SQLRoutingResultRetainedSQL, iterations[len(iterations)-1].RoutingResult)
	assert.Equal(t, SQLHealthFallbackTimeout, iterations[len(iterations)-1].FallbackReason)
	snapshot := buildSQLHealthComparisonSnapshot(map[string]struct{}{ip: {}}, health.sqlComparisonLatest)
	assert.Empty(t, snapshot.health, "comparison must derive roles from the latest credential results")

	// Determinate startup evidence resolves the same-IP timeouts and is reported once.
	startup := &pgconn.PgError{Code: "57P03", Message: "the database system is starting up"}
	complete(c, t0.Add(3*time.Second), startup)
	assert.Equal(t, SQLRoutingResultConclusive, iterations[len(iterations)-1].RoutingResult)
	require.Len(t, conclusiveErrors, 1)
	assert.Same(t, startup, conclusiveErrors[0])
	complete(a, t0.Add(3*time.Second), context.DeadlineExceeded)
	assert.Equal(t, SQLRoutingResultConclusive, iterations[len(iterations)-1].RoutingResult)
	assert.Len(t, conclusiveErrors, 1, "cached errors are evidence, not new probe events")
}

func TestHybridEvaluationWaitsForNewlyDiscoveredIP(t *testing.T) {
	const oldIP, newIP = "10.0.0.1", "10.0.0.2"
	state := NewHealthState()
	state.Track([]string{oldIP, newIP})
	health := &PoolHealth{
		state:              NewHealthState(),
		patroniHealthState: NewHealthState(),
		sqlHealthState:     state,
		hybridPolicy:       newHybridHealthPolicy(true),
		routingEnabledFn:   func(context.Context) SQLHealthFeatureFlagEvaluation { return validSQLHealthFeatureFlag(true) },
	}
	target := &sqlHealthTarget{poolHealth: health}
	var iterations int
	w := &HealthWorker{sqlHooks: SQLHealthWorkerHooks{Iteration: func(SQLIteration, []string) { iterations++ }}}
	now := time.Now()
	// Another credential discovered newIP while this old-IP-only round was running.
	w.completeSQLHealthCheck(target, 0, now, []string{oldIP}, map[string]struct{}{oldIP: {}}, SQLHealthCheckOutcome{
		Results: []SQLHealthCheckResult{{IP: oldIP}},
	})
	assert.Zero(t, iterations, "a completed old IP set must not stand in for a complete current IP set")
	w.completeSQLHealthCheck(target, 0, now.Add(time.Second), []string{oldIP, newIP}, map[string]struct{}{oldIP: {}, newIP: {}}, SQLHealthCheckOutcome{
		Results: []SQLHealthCheckResult{{IP: oldIP}, {IP: newIP, InRecovery: true}},
	})
	assert.Equal(t, 1, iterations)
}

func TestHybridEvaluationRejectsInvalidatedCredentialPoll(t *testing.T) {
	for _, invalidation := range []string{"disable", "unsubscribe"} {
		t.Run(invalidation, func(t *testing.T) {
			const ip, oldIP = "10.0.0.1", "10.0.0.2"
			w := newTestWorker(newMockProber(map[string][]string{"primary.local": {ip}}, ip))
			prober := &partiallyBlockingSQLHealthProber{
				blockingIP: ip, blocked: make(chan struct{}), release: make(chan struct{}),
			}
			w.newSQLChecker = func(context.Context) *SQLHealthChecker {
				return newTestSQLHealthChecker(t, prober, 1, SQLHealthCheckHooks{})
			}
			var enabled atomic.Bool
			enabled.Store(true)
			updates := w.Subscribe("primary.local", false, "mydb", nil, &SQLHealthConfig{
				ConnStringForIP: func(string) string { return "conn" },
				ChecksEnabled: func(context.Context) SQLHealthFeatureFlagEvaluation {
					return validSQLHealthFeatureFlag(enabled.Load())
				},
				RoutingEnabled: func(context.Context) SQLHealthFeatureFlagEvaluation { return validSQLHealthFeatureFlag(true) },
			})
			health := receivePoolHealth(t, updates)
			t.Cleanup(func() { w.Unsubscribe(updates) })
			policy, _ := health.routingPolicy()
			scheduledAt := time.Now()
			policy.observePatroni(map[string]bool{oldIP: true}, scheduledAt)
			policy.observeSQL([]string{oldIP}, SQLHealthCheckOutcome{PrimaryIPs: []string{oldIP}}, scheduledAt)
			policy.observePatroni(map[string]bool{ip: true}, scheduledAt.Add(time.Nanosecond))
			var iterations, comparisons int
			w.sqlHooks.Iteration = func(SQLIteration, []string) { iterations++ }
			w.hooks.SQLPatroniCompared = func(SQLPatroniHealthSnapshot, []string) { comparisons++ }
			group := w.sqlHealthGroups()[0]
			done := make(chan struct{})
			go func() { defer close(done); w.checkSQLHealthGroup(group, scheduledAt) }()
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(prober.release) }) }
			t.Cleanup(func() { release(); <-done })
			select {
			case <-prober.blocked:
			case <-time.After(5 * time.Second):
				t.Fatal("credential did not start")
			}
			if invalidation == "disable" {
				enabled.Store(false)
				w.checkSQLHealthGroup(group, scheduledAt.Add(time.Second))
				enabled.Store(true) // Re-enabling must not admit the pre-disable completion.
			} else {
				target := group.targets[0]
				unsubscribed := make(chan struct{})
				go func() { w.Unsubscribe(updates); close(unsubscribed) }()
				t.Cleanup(func() { release(); <-unsubscribed })
				require.Eventually(t, func() bool {
					health.sqlComparisonMu.Lock()
					defer health.sqlComparisonMu.Unlock()
					return target.sqlComparisonRetired
				}, 5*time.Second, time.Millisecond)
			}
			release()
			<-done
			assert.Zero(t, iterations)
			assert.Zero(t, comparisons)
			assert.Empty(t, health.sqlComparisonLatest)
			assert.Empty(t, health.sqlHealthState.lastKnownHealthy)
			assert.Empty(t, policy.routingTiming.pending, "an invalidated poll must not complete a routing comparison")
			if invalidation == "disable" {
				w.checkSQLHealthGroup(group, scheduledAt.Add(2*time.Second))
				assert.Equal(t, 1, iterations)
				assert.Equal(t, 1, comparisons)
			}
		})
	}
}

func TestPatroniRefreshAppliesRoutingDisableWhenSQLChecksAreSkipped(t *testing.T) {
	const oldPrimary, newPrimary = "10.0.0.1", "10.0.0.2"
	patroniProber := newMockProber(
		map[string][]string{"primary.local": {oldPrimary, newPrimary}},
		oldPrimary,
	)
	w := newTestWorker(patroniProber)
	w.newSQLChecker = func(context.Context) *SQLHealthChecker {
		return newTestSQLHealthChecker(t, &fakeSQLHealthProber{}, 1, SQLHealthCheckHooks{})
	}
	checksEnabled := false
	routingEnabled := true
	var routingComparisons []LeaderRoutingComparison
	w.sqlHooks.LeaderRoutingCompared = func(comparison LeaderRoutingComparison, _ []string) {
		routingComparisons = append(routingComparisons, comparison)
	}

	updates := w.Subscribe("primary.local", false, "mydb", nil, &SQLHealthConfig{
		ConnStringForIP: func(ip string) string { return ip },
		ChecksEnabled: func(context.Context) SQLHealthFeatureFlagEvaluation {
			return validSQLHealthFeatureFlag(checksEnabled)
		},
		RoutingEnabled: func(context.Context) SQLHealthFeatureFlagEvaluation {
			return validSQLHealthFeatureFlag(routingEnabled)
		},
	})
	poolHealth := receivePoolHealth(t, updates)
	t.Cleanup(func() { w.Unsubscribe(updates) })
	policy, _ := poolHealth.routingPolicy()
	require.NotNil(t, policy)

	started := time.Now()
	policy.observeSQL([]string{oldPrimary}, SQLHealthCheckOutcome{PrimaryIPs: []string{oldPrimary}}, started)
	policy.observeSQL([]string{newPrimary}, SQLHealthCheckOutcome{PrimaryIPs: []string{newPrimary}}, started.Add(time.Second))
	require.NotEmpty(t, policy.routingTiming.pending)

	// Even without another SQL round, the Patroni refresh must apply the disable before its
	// new-primary observation can complete the stale comparison.
	routingEnabled = false
	patroniProber.mu.Lock()
	patroniProber.healthyIPs[oldPrimary] = false
	patroniProber.healthyIPs[newPrimary] = true
	patroniProber.mu.Unlock()
	w.checkPoolHealth(poolHealth)

	assert.False(t, policy.isRoutingEnabled())
	assert.Empty(t, policy.routingTiming.pending)
	assert.Empty(t, routingComparisons)
}

func TestCentralizedSQLHealthCheckDiscoversNewIPBeforePatroniRefresh(t *testing.T) {
	const (
		patroniIP = "10.0.0.1"
		newSQLIP  = "10.0.0.2"
	)
	patroniProber := newMockProber(map[string][]string{"primary.local": {patroniIP}}, patroniIP)
	w := newTestWorker(patroniProber)
	sqlProber := &fakeSQLHealthProber{
		responses: map[string][]fakeSQLHealthProbeResponse{
			newSQLIP: {{inRecovery: false}},
		},
		calls: make(map[string]int),
	}
	w.newSQLChecker = func(context.Context) *SQLHealthChecker {
		return newTestSQLHealthChecker(t, sqlProber, 1, SQLHealthCheckHooks{})
	}
	health := receivePoolHealth(t, w.Subscribe("primary.local", false, "mydb", nil, &SQLHealthConfig{
		ConnStringForIP: func(ip string) string { return ip },
	}))
	sqlUpdates, err := health.SubscribeSQLHealth()
	require.NoError(t, err)
	t.Cleanup(sqlUpdates.Close)

	// DNS changes after the initial Patroni snapshot, but no Patroni refresh runs.
	patroniProber.mu.Lock()
	patroniProber.ipsByHost["primary.local"] = []string{newSQLIP}
	patroniProber.mu.Unlock()
	w.RefreshSQL()

	observation := receiveSQLHealthObservation(t, sqlUpdates)
	assert.True(t, observation.Writable)
	assert.NoError(t, observation.Err)
	assert.Equal(t, 1, sqlProber.callCount(newSQLIP))
	assert.Equal(t, []string{patroniIP}, health.Snapshot().Resolved, "SQL discovery must not mutate Patroni routing state")
}

func TestRefreshSQLPublishesWritableBeforeReportingMultiplePrimaries(t *testing.T) {
	const (
		writableIP = "10.0.0.1"
		blockingIP = "10.0.0.2"
	)
	patroniProber := newMockProber(
		map[string][]string{"primary.local": {writableIP, blockingIP}},
		writableIP,
		blockingIP,
	)
	w := newTestWorker(patroniProber)
	sqlProber := &partiallyBlockingSQLHealthProber{
		blockingIP:       blockingIP,
		blockingRecovery: false,
		blocked:          make(chan struct{}),
		release:          make(chan struct{}),
	}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(sqlProber.release) }) }
	t.Cleanup(release)
	multiplePrimaries := make(chan []string, 1)
	checker := newTestSQLHealthChecker(t, sqlProber, 1, SQLHealthCheckHooks{
		MultiplePrimaries: func(primaryIPs []string, _ []string) {
			multiplePrimaries <- primaryIPs
		},
	})
	w.newSQLChecker = func(context.Context) *SQLHealthChecker { return checker }
	healthUpdates := w.Subscribe("primary.local", false, "mydb", nil, &SQLHealthConfig{
		ConnStringForIP: func(ip string) string { return ip },
	})
	health := receivePoolHealth(t, healthUpdates)
	health.sqlMu.RLock()
	target := health.subscriptionTargets[healthUpdates]
	health.sqlMu.RUnlock()
	updates, err := health.SubscribeSQLHealth()
	require.NoError(t, err)
	t.Cleanup(updates.Close)
	iterations := make(chan SQLIteration, 1)
	w.sqlHooks.Iteration = func(iteration SQLIteration, _ []string) { iterations <- iteration }
	missedMetric := make(chan []string, 1)
	w.hooks.SQLCheckMissed = func(tags []string) {
		missedMetric <- append([]string(nil), tags...)
	}

	refreshDone := make(chan struct{})
	go func() {
		w.RefreshSQL()
		close(refreshDone)
	}()

	select {
	case <-sqlProber.blocked:
	case <-time.After(time.Second):
		t.Fatal("slow SQL probe did not start")
	}
	select {
	case observation := <-updates.Updates():
		assert.True(t, observation.Writable)
		assert.NoError(t, observation.Err)
	case <-time.After(time.Second):
		t.Fatal("writable SQL result was not published while the other instance was still blocked")
	}

	select {
	case <-iterations:
		t.Fatal("hybrid evaluation must wait for every IP in the credential round")
	default:
	}

	// The round already published availability, so a scheduler tick while its second probe is still
	// blocked is not a missed availability observation.
	w.checkSQLHealthGroup(sqlHealthGroup{poolHealth: health, targets: []*sqlHealthTarget{target}}, time.Now())
	select {
	case tags := <-missedMetric:
		t.Fatalf("early writable result unexpectedly emitted missed metric with tags %v", tags)
	default:
	}
	select {
	case observation := <-updates.Updates():
		t.Fatalf("early writable result was overwritten by missed observation: %+v", observation)
	default:
	}

	release()
	select {
	case primaryIPs := <-multiplePrimaries:
		assert.ElementsMatch(t, []string{writableIP, blockingIP}, primaryIPs)
	case <-time.After(time.Second):
		t.Fatal("completed SQL round did not report multiple primaries")
	}
	select {
	case <-refreshDone:
	case <-time.After(time.Second):
		t.Fatal("full SQL round did not finish after the slow probe was released")
	}
	require.Len(t, iterations, 1, "the complete multi-IP round must evaluate the hybrid snapshot")
}

func TestRefreshSQLTimestampsObservationAfterSuccessfulRetry(t *testing.T) {
	const ip = "10.0.0.1"
	patroniProber := newMockProber(map[string][]string{"primary.local": {ip}}, ip)
	w := newTestWorker(patroniProber)
	sqlProber := &blockingRetrySQLHealthProber{
		retryStarted: make(chan struct{}),
		releaseRetry: make(chan struct{}),
	}
	var releaseOnce sync.Once
	releaseRetry := func() { releaseOnce.Do(func() { close(sqlProber.releaseRetry) }) }
	t.Cleanup(releaseRetry)
	checker := newTestSQLHealthChecker(t, sqlProber, 2, SQLHealthCheckHooks{})
	w.newSQLChecker = func(context.Context) *SQLHealthChecker { return checker }
	health := receivePoolHealth(t, w.Subscribe("primary.local", false, "mydb", nil, &SQLHealthConfig{
		ConnStringForIP: func(string) string { return "conn" },
	}))
	updates, err := health.SubscribeSQLHealth()
	require.NoError(t, err)
	t.Cleanup(updates.Close)

	refreshStartedAt := time.Now()
	refreshDone := make(chan struct{})
	go func() {
		w.RefreshSQL()
		close(refreshDone)
	}()
	select {
	case <-sqlProber.retryStarted:
	case <-time.After(time.Second):
		t.Fatal("SQL health check did not start its retry")
	}

	retryReleasedAt := time.Now()
	releaseRetry()
	observation := receiveSQLHealthObservation(t, updates)
	select {
	case <-refreshDone:
	case <-time.After(time.Second):
		t.Fatal("SQL refresh did not finish after the retry was released")
	}
	assert.Equal(t, 2, sqlProber.callCount())
	assert.True(t, observation.Writable)
	assert.False(t, observation.ScheduledAt.Before(refreshStartedAt))
	assert.False(t, observation.ScheduledAt.After(retryReleasedAt))
	assert.False(t, observation.ObservedAt.Before(retryReleasedAt), "successful retry must not be backdated")
}

func TestScheduleSQLRefreshPublishesUnavailableObservationForMissedInterval(t *testing.T) {
	const ip = "10.0.0.1"
	patroniProber := newMockProber(map[string][]string{"primary.local": {ip}}, ip)
	w := newTestWorker(patroniProber)
	sqlProber := &blockingRetrySQLHealthProber{
		retryStarted: make(chan struct{}),
		releaseRetry: make(chan struct{}),
	}
	var releaseOnce sync.Once
	releaseRetry := func() { releaseOnce.Do(func() { close(sqlProber.releaseRetry) }) }
	t.Cleanup(releaseRetry)
	checker := newTestSQLHealthChecker(t, sqlProber, 2, SQLHealthCheckHooks{})
	w.newSQLChecker = func(context.Context) *SQLHealthChecker { return checker }
	missedMetric := make(chan []string, 1)
	w.hooks.SQLCheckMissed = func(tags []string) {
		missedMetric <- append([]string(nil), tags...)
	}
	healthUpdates := w.Subscribe("primary.local", false, "mydb", []string{"database:mydb"}, &SQLHealthConfig{
		ConnStringForIP: func(string) string { return "conn" },
	})
	health := receivePoolHealth(t, healthUpdates)
	t.Cleanup(func() { w.Unsubscribe(healthUpdates) })
	sqlUpdates, err := health.SubscribeSQLHealth()
	require.NoError(t, err)
	t.Cleanup(sqlUpdates.Close)

	comparisons := make(chan SQLPatroniHealthSnapshot, 2)
	w.hooks.SQLPatroniCompared = func(snapshot SQLPatroniHealthSnapshot, _ []string) {
		comparisons <- snapshot
	}
	target := health.sqlTargetValues()[0]
	previousAt := time.Now().Add(-time.Second)
	health.recordSQLComparisonPoll(false, target, 0, previousAt, previousAt, []string{ip},
		SQLHealthCheckOutcome{Results: []SQLHealthCheckResult{{IP: ip, InRecovery: true}}})

	firstScheduledAt := time.Now()
	w.scheduleSQLRefresh(firstScheduledAt)
	select {
	case <-sqlProber.retryStarted:
	case <-time.After(time.Second):
		t.Fatal("SQL health check did not start its retry")
	}

	missedAt := time.Now()
	w.scheduleSQLRefresh(missedAt)
	select {
	case tags := <-missedMetric:
		assert.Contains(t, tags, "database:mydb")
	case <-time.After(time.Second):
		t.Fatal("missed-observation metric hook was not called")
	}
	assert.Equal(t, 2, sqlProber.callCount(), "missed interval must not start an overlapping check")
	missed := receiveSQLHealthObservation(t, sqlUpdates)
	assert.Equal(t, missedAt, missed.ScheduledAt)
	assert.Equal(t, missedAt, missed.ObservedAt)
	assert.False(t, missed.Writable)
	assert.ErrorIs(t, missed.Err, ErrSQLHealthCheckMissed)

	health.sqlComparisonMu.Lock()
	assert.Equal(t, previousAt, health.sqlComparisonLatest[target].ScheduledAt,
		"a missed tick must preserve the current credential result")
	assert.Zero(t, target.sqlComparisonGeneration, "a missed tick must not invalidate the running poll")
	health.sqlComparisonMu.Unlock()
	select {
	case <-comparisons:
		t.Fatal("a missed tick must not publish a comparison")
	default:
	}

	retryReleasedAt := time.Now()
	releaseRetry()
	assert.Eventually(t, func() bool { return !target.sqlRoundRunning() }, time.Second, time.Millisecond)
	late := receiveSQLHealthObservation(t, sqlUpdates)
	assert.Equal(t, firstScheduledAt, late.ScheduledAt)
	assert.False(t, late.ObservedAt.Before(retryReleasedAt))
	assert.True(t, late.Writable)
	select {
	case comparison := <-comparisons:
		assert.Equal(t, map[string]bool{ip: true}, comparison.SQLHealth)
		require.Len(t, comparison.SQLResults, 1)
		assert.Equal(t, firstScheduledAt, comparison.SQLResults[0].ScheduledAt,
			"the running poll is accepted despite the newer missed interval")
	case <-time.After(time.Second):
		t.Fatal("completed poll did not publish its comparison")
	}

	nextScheduledAt := time.Now()
	w.scheduleSQLRefresh(nextScheduledAt)
	next := receiveSQLHealthObservation(t, sqlUpdates)
	assert.Equal(t, nextScheduledAt, next.ScheduledAt)
	assert.True(t, next.Writable)
}

func TestScheduleSQLRefreshReportsMissedBeforeBlockedLookupCompletes(t *testing.T) {
	const ip = "10.0.0.1"
	patroniProber := newMockProber(map[string][]string{"primary.local": {ip}}, ip)
	w := newTestWorker(patroniProber)
	sqlProber := &fakeSQLHealthProber{
		responses: map[string][]fakeSQLHealthProbeResponse{ip: {{}}},
		calls:     make(map[string]int),
	}
	w.newSQLChecker = func(ctx context.Context) *SQLHealthChecker {
		return NewSQLHealthChecker(ctx, SQLHealthCheckPoolsConfig{}, SQLHealthCheckerConfig{
			Prober: sqlProber,
		})
	}
	updates := w.Subscribe("primary.local", false, "mydb", []string{"database:mydb"}, &SQLHealthConfig{
		ConnStringForIP: func(string) string { return ip },
	})
	health := receivePoolHealth(t, updates)
	t.Cleanup(func() { w.Unsubscribe(updates) })

	lookupStarted := make(chan struct{})
	releaseLookup := make(chan struct{})
	var lookupOnce sync.Once
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseLookup) }) }
	t.Cleanup(release)
	patroniProber.mu.Lock()
	patroniProber.recordLookup = func(string, []string) {
		lookupOnce.Do(func() { close(lookupStarted) })
		<-releaseLookup
	}
	patroniProber.mu.Unlock()

	missed := make(chan []string, 1)
	w.hooks.SQLCheckMissed = func(tags []string) {
		missed <- append([]string(nil), tags...)
	}
	w.scheduleSQLRefresh(time.Now())
	select {
	case <-lookupStarted:
	case <-time.After(time.Second):
		t.Fatal("SQL lookup did not block")
	}

	// The next tick must observe the reserved target immediately instead of waiting for the blocked
	// lookup's sqlScheduleMu critical section.
	w.scheduleSQLRefresh(time.Now().Add(time.Second))
	select {
	case tags := <-missed:
		assert.Contains(t, tags, "database:mydb")
	case <-time.After(time.Second):
		t.Fatal("missed interval waited for the blocked SQL lookup")
	}

	release()
	require.Eventually(t, func() bool {
		for _, target := range health.sqlTargetValues() {
			if target.sqlRoundRunning() {
				return false
			}
		}
		return true
	}, time.Second, time.Millisecond)
}

func TestScheduleSQLRefreshPublishesDisabledObservationWhileRoundIsBusy(t *testing.T) {
	const ip = "10.0.0.1"
	patroniProber := newMockProber(map[string][]string{"primary.local": {ip}}, ip)
	w := newTestWorker(patroniProber)
	sqlProber := &blockingRetrySQLHealthProber{
		retryStarted: make(chan struct{}),
		releaseRetry: make(chan struct{}),
	}
	var releaseOnce sync.Once
	releaseRetry := func() { releaseOnce.Do(func() { close(sqlProber.releaseRetry) }) }
	t.Cleanup(releaseRetry)
	checker := newTestSQLHealthChecker(t, sqlProber, 2, SQLHealthCheckHooks{})
	w.newSQLChecker = func(context.Context) *SQLHealthChecker { return checker }

	var checksEnabled atomic.Bool
	checksEnabled.Store(true)
	healthUpdates := w.Subscribe("primary.local", false, "mydb", nil, &SQLHealthConfig{
		ConnStringForIP: func(string) string { return "conn" },
		ChecksEnabled: func(context.Context) SQLHealthFeatureFlagEvaluation {
			return validSQLHealthFeatureFlag(checksEnabled.Load())
		},
	})
	health := receivePoolHealth(t, healthUpdates)
	t.Cleanup(func() { w.Unsubscribe(healthUpdates) })
	sqlUpdates, err := health.SubscribeSQLHealth()
	require.NoError(t, err)
	t.Cleanup(sqlUpdates.Close)

	comparisons := make(chan SQLPatroniHealthSnapshot, 1)
	w.hooks.SQLPatroniCompared = func(snapshot SQLPatroniHealthSnapshot, _ []string) {
		comparisons <- snapshot
	}

	firstScheduledAt := time.Now()
	w.scheduleSQLRefresh(firstScheduledAt)
	select {
	case <-sqlProber.retryStarted:
	case <-time.After(time.Second):
		t.Fatal("SQL health check did not start its retry")
	}

	checksEnabled.Store(false)
	disabledAt := time.Now()
	w.scheduleSQLRefresh(disabledAt)
	disabled := receiveSQLHealthObservation(t, sqlUpdates)
	assert.Equal(t, disabledAt, disabled.ScheduledAt)
	assert.Equal(t, disabledAt, disabled.ObservedAt)
	assert.True(t, disabled.Disabled)
	assert.Equal(t, 2, sqlProber.callCount(), "disabled interval must not start another check")

	checksEnabled.Store(true)
	releaseRetry()
	late := receiveSQLHealthObservation(t, sqlUpdates)
	assert.Equal(t, firstScheduledAt, late.ScheduledAt)
	assert.True(t, late.Writable)
	target := health.sqlTargetValues()[0]
	require.Eventually(t, func() bool { return !target.sqlRoundRunning() }, time.Second, time.Millisecond)
	select {
	case <-comparisons:
		t.Fatal("pre-disable completion restored the comparison after re-enable")
	default:
	}
	w.RefreshSQL()
	select {
	case comparison := <-comparisons:
		assert.Equal(t, map[string]bool{ip: true}, comparison.SQLHealth)
		require.Len(t, comparison.SQLResults, 1)
		assert.True(t, comparison.SQLResults[0].ScheduledAt.After(disabledAt))
	case <-time.After(time.Second):
		t.Fatal("fresh poll after re-enable did not publish a comparison")
	}
}

func TestSQLHealthRoundRejectsStaleWritableResult(t *testing.T) {
	target := &sqlHealthTarget{}
	oldRound := time.Now()
	newRound := oldRound.Add(time.Second)

	require.True(t, target.tryStartSQLRound(oldRound))
	target.finishSQLRound(oldRound)
	require.True(t, target.tryStartSQLRound(newRound))

	assert.False(t, target.markSQLRoundWritable(oldRound))
	assert.False(t, target.activeSQLRoundPublishedWritable())
}

func TestSQLHealthPublicationReplacesStaleObservation(t *testing.T) {
	target := &sqlHealthTarget{sqlSubscribers: make(map[*sqlHealthUpdates]struct{})}
	updates := &sqlHealthUpdates{
		source:  target,
		updates: make(chan SQLHealthObservation, 1),
		done:    make(chan struct{}),
	}
	target.sqlSubscribers[updates] = struct{}{}
	t.Cleanup(updates.Close)

	staleRound := time.Now()
	latestRound := staleRound.Add(time.Second)
	stale := SQLHealthObservation{ScheduledAt: staleRound, ObservedAt: latestRound.Add(time.Second)}
	latest := SQLHealthObservation{ScheduledAt: latestRound, ObservedAt: latestRound, Writable: true}
	target.publishSQLHealth(stale)
	target.publishSQLHealth(latest)

	assert.Equal(t, latest, receiveSQLHealthObservation(t, updates))
}

func TestSQLHealthPublicationRetainsNewerBufferedObservation(t *testing.T) {
	target := &sqlHealthTarget{sqlSubscribers: make(map[*sqlHealthUpdates]struct{})}
	updates := &sqlHealthUpdates{
		source:  target,
		updates: make(chan SQLHealthObservation, 1),
		done:    make(chan struct{}),
	}
	target.sqlSubscribers[updates] = struct{}{}
	t.Cleanup(updates.Close)

	olderRound := time.Now()
	newerRound := olderRound.Add(time.Second)
	older := SQLHealthObservation{ScheduledAt: olderRound, ObservedAt: newerRound.Add(time.Second)}
	newer := SQLHealthObservation{ScheduledAt: newerRound, ObservedAt: newerRound, Writable: true}
	target.publishSQLHealth(newer)
	target.publishSQLHealth(older)

	assert.Equal(t, newer, receiveSQLHealthObservation(t, updates))
}

func TestRefreshSQLDoesNotBlockOnFullSubscription(t *testing.T) {
	p := newMockProber(map[string][]string{"primary.local": {"10.0.0.1"}}, "10.0.0.1")
	w := newTestWorker(p)
	sqlProber := &fakeSQLHealthProber{
		responses: map[string][]fakeSQLHealthProbeResponse{"10.0.0.1": {{}}},
		calls:     make(map[string]int),
	}
	checker := newTestSQLHealthChecker(t, sqlProber, 1, SQLHealthCheckHooks{})
	w.newSQLChecker = func(context.Context) *SQLHealthChecker { return checker }
	health := receivePoolHealth(t, w.Subscribe("primary.local", false, "mydb", nil, &SQLHealthConfig{
		ConnStringForIP: func(ip string) string { return ip },
	}))
	updates, err := health.SubscribeSQLHealth()
	require.NoError(t, err)
	t.Cleanup(updates.Close)

	w.RefreshSQL()
	latestStartedAt := time.Now()
	refreshDone := make(chan struct{})
	go func() {
		w.RefreshSQL()
		close(refreshDone)
	}()

	select {
	case <-refreshDone:
	case <-time.After(time.Second):
		t.Fatal("SQL refresh blocked on a full subscriber")
	}
	assert.False(t, receiveSQLHealthObservation(t, updates).ObservedAt.Before(latestStartedAt))
}

func TestRefreshSQL_ReplacesPoolWhenConnectionStringChanges(t *testing.T) {
	const ip = "127.0.0.1"
	firstConnString := "host=127.0.0.1 port=1 user=probeuser password=first dbname=probedb"
	secondConnString := "host=127.0.0.1 port=1 user=probeuser password=second dbname=probedb"
	connString := firstConnString

	patroniProber := newMockProber(map[string][]string{"primary.local": {ip}}, ip)
	w := newTestWorker(patroniProber)
	var pools *SQLHealthCheckPools
	w.newSQLChecker = func(ctx context.Context) *SQLHealthChecker {
		pools = NewSQLHealthCheckPools(ctx, SQLHealthCheckPoolsConfig{})
		return NewSQLHealthChecker(ctx, SQLHealthCheckPoolsConfig{}, SQLHealthCheckerConfig{
			Pools:  pools,
			Prober: &poolCreatingSQLHealthProber{pools: pools},
		})
	}

	updates := w.Subscribe("primary.local", false, "probedb", nil, &SQLHealthConfig{
		ConnStringForIP: func(string) string { return connString },
	})
	receivePoolHealth(t, updates)
	t.Cleanup(func() { w.Unsubscribe(updates) })

	w.RefreshSQL()
	pools.mu.Lock()
	firstPool := pools.pools[ip].pool
	pools.mu.Unlock()
	require.NotNil(t, firstPool)

	connString = secondConnString
	w.RefreshSQL()
	pools.mu.Lock()
	secondPool := pools.pools[ip].pool
	installedConnString := pools.pools[ip].connString
	pools.mu.Unlock()

	require.NotNil(t, secondPool)
	assert.NotSame(t, firstPool, secondPool)
	assert.Equal(t, secondConnString, installedConnString)
	assert.Equal(t, 1, pools.Len(), "credential rotation must replace, not accumulate, the pool for an IP")
	_, err := firstPool.Acquire(context.Background())
	require.ErrorIs(t, err, puddle.ErrClosedPool, "the pool using stale credentials must be closed")
}

func TestRefreshSQLWaitsForInitialPatroniCheck(t *testing.T) {
	patroniProber := &blockingLookupProber{
		lookupStarted: make(chan struct{}),
		releaseLookup: make(chan struct{}),
	}
	var releaseOnce sync.Once
	releaseLookup := func() { releaseOnce.Do(func() { close(patroniProber.releaseLookup) }) }
	t.Cleanup(releaseLookup)
	w := NewHealthWorker(HealthWorkerConfig{
		Interval:         time.Second,
		AbsenceThreshold: maxFailedHostLookups,
		Prober:           patroniProber,
	})
	sqlProber := &fakeSQLHealthProber{
		responses: map[string][]fakeSQLHealthProbeResponse{"10.0.0.1": {{}}},
		calls:     make(map[string]int),
	}
	checker := newTestSQLHealthChecker(t, sqlProber, 1, SQLHealthCheckHooks{})
	w.newSQLChecker = func(context.Context) *SQLHealthChecker { return checker }

	updatesReady := make(chan HealthUpdates, 1)
	go func() {
		updatesReady <- w.Subscribe("primary.local", false, "mydb", nil, &SQLHealthConfig{
			ConnStringForIP: func(ip string) string { return ip },
		})
	}()

	select {
	case <-patroniProber.lookupStarted:
	case <-time.After(time.Second):
		t.Fatal("initial Patroni lookup did not start")
	}

	w.RefreshSQL()
	assert.Zero(t, sqlProber.callCount("10.0.0.1"), "SQL check ran before initial Patroni check completed")

	releaseLookup()
	var updates HealthUpdates
	select {
	case updates = <-updatesReady:
	case <-time.After(time.Second):
		t.Fatal("subscription did not finish after Patroni lookup completed")
	}
	_ = receivePoolHealth(t, updates)
	t.Cleanup(func() { w.Unsubscribe(updates) })

	w.RefreshSQL()
	assert.Equal(t, 1, sqlProber.callCount("10.0.0.1"), "SQL check did not run after initial Patroni check completed")
}

func TestRefreshSQLFeatureFlagReleasesAndRecreatesPools(t *testing.T) {
	const ip = "127.0.0.1"
	enabled := true
	patroniProber := newMockProber(map[string][]string{"primary.local": {ip}}, ip)
	w := newTestWorker(patroniProber)
	pools := NewSQLHealthCheckPools(context.Background(), SQLHealthCheckPoolsConfig{})
	sqlProber := &poolCreatingSQLHealthProber{pools: pools}
	checker := NewSQLHealthChecker(context.Background(), SQLHealthCheckPoolsConfig{}, SQLHealthCheckerConfig{
		Pools:  pools,
		Prober: sqlProber,
	})
	t.Cleanup(checker.Close)
	w.newSQLChecker = func(context.Context) *SQLHealthChecker { return checker }

	updates := w.Subscribe("primary.local", false, "probedb", nil, &SQLHealthConfig{
		ConnStringForIP: func(string) string { return testSQLHealthCheckConnString },
		ChecksEnabled:   func(context.Context) SQLHealthFeatureFlagEvaluation { return validSQLHealthFeatureFlag(enabled) },
	})
	receivePoolHealth(t, updates)
	t.Cleanup(func() { w.Unsubscribe(updates) })

	w.RefreshSQL()
	require.Equal(t, 1, pools.Len())
	assert.Equal(t, 1, sqlProber.callCount())
	pools.mu.Lock()
	firstPool := pools.pools[ip].pool
	pools.mu.Unlock()
	require.NotNil(t, firstPool)

	enabled = false
	w.RefreshSQL()
	assert.Zero(t, pools.Len(), "disabling SQL checks must release existing probe pools")
	assert.Equal(t, 1, sqlProber.callCount(), "disabled SQL checks must not reach GetOrAdd")
	_, err := firstPool.Acquire(context.Background())
	require.ErrorIs(t, err, puddle.ErrClosedPool)

	enabled = true
	w.RefreshSQL()
	require.Equal(t, 1, pools.Len())
	assert.Equal(t, 2, sqlProber.callCount())
	pools.mu.Lock()
	secondPool := pools.pools[ip].pool
	pools.mu.Unlock()
	assert.NotSame(t, firstPool, secondPool, "re-enabling must create a fresh probe pool")
}

func TestNewSQLHealthObservation(t *testing.T) {
	scheduledAt := time.Now()
	observedAt := scheduledAt.Add(250 * time.Millisecond)
	probeErr := errors.New("probe failed")
	tests := map[string]struct {
		outcome  SQLHealthCheckOutcome
		writable bool
		wantErr  error
	}{
		"writable result": {
			outcome: SQLHealthCheckOutcome{
				Results:    []SQLHealthCheckResult{{IP: "10.0.0.1"}},
				PrimaryIPs: []string{"10.0.0.1"},
			},
			writable: true,
		},
		"writable result preserves another error": {
			outcome: SQLHealthCheckOutcome{
				Results:    []SQLHealthCheckResult{{IP: "10.0.0.1"}, {IP: "10.0.0.2", Err: probeErr}},
				PrimaryIPs: []string{"10.0.0.1"},
			},
			writable: true,
			wantErr:  probeErr,
		},
		"multiple primaries remain writable": {
			outcome: SQLHealthCheckOutcome{
				Results:    []SQLHealthCheckResult{{IP: "10.0.0.1"}, {IP: "10.0.0.2"}},
				PrimaryIPs: []string{"10.0.0.1", "10.0.0.2"},
			},
			writable: true,
		},
		"read-only result": {
			outcome: SQLHealthCheckOutcome{Results: []SQLHealthCheckResult{{IP: "10.0.0.1", InRecovery: true}}},
		},
		"probe error": {
			outcome: SQLHealthCheckOutcome{Results: []SQLHealthCheckResult{{IP: "10.0.0.1", Err: probeErr}}},
			wantErr: probeErr,
		},
		"no results": {wantErr: errNoSQLHealthCheckResults},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			observation := newSQLHealthObservation(scheduledAt, observedAt, test.outcome)
			assert.Equal(t, scheduledAt, observation.ScheduledAt)
			assert.Equal(t, observedAt, observation.ObservedAt)
			assert.Equal(t, test.writable, observation.Writable)
			if test.wantErr == nil {
				assert.NoError(t, observation.Err)
			} else {
				assert.ErrorIs(t, observation.Err, test.wantErr)
			}
		})
	}
}

func TestSubscribeSQLHealthConcurrentClose(t *testing.T) {
	for range 100 {
		checker := NewSQLHealthChecker(context.Background(), SQLHealthCheckPoolsConfig{}, SQLHealthCheckerConfig{
			Prober: &fakeSQLHealthProber{},
		})
		health := &PoolHealth{
			subscriptionTargets: make(map[HealthUpdates]*sqlHealthTarget),
		}
		healthTarget := &sqlHealthTarget{checker: checker, sqlSubscribers: make(map[*sqlHealthUpdates]struct{})}
		healthSubscription := HealthUpdates(make(chan *PoolHealth))
		health.subscriptionTargets[healthSubscription] = healthTarget
		start := make(chan struct{})
		var updates SQLHealthUpdates
		var err error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			updates, err = health.SubscribeSQLHealth(healthSubscription)
		}()
		go func() {
			defer wg.Done()
			<-start
			health.closeSQL()
		}()
		close(start)
		wg.Wait()

		if err != nil {
			require.ErrorIs(t, err, ErrSQLHealthUpdatesUnsupported)
			continue
		}
		select {
		case <-updates.Done():
		default:
			t.Fatal("subscription that raced with close remained open")
		}
	}
}

func receiveSQLHealthObservation(t *testing.T, updates SQLHealthUpdates) SQLHealthObservation {
	t.Helper()
	select {
	case observation := <-updates.Updates():
		return observation
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for SQL health observation")
		return SQLHealthObservation{}
	}
}

func TestTickOne_EmptyDNS_LeavesResolvedEmpty(t *testing.T) {
	p := newMockProber(nil) // every host resolves to nothing
	w := newTestWorker(p)

	poolHealth := receivePoolHealth(t, w.Subscribe("nowhere.local", false, "mydb", nil, nil))

	assert.Empty(t, poolHealth.Snapshot().Resolved)
	assert.False(t, poolHealth.IsHealthy("anything"))
}

// TestTickOne_AllUnhealthy verifies that when every probe returns false, the
// snapshot still has the IPs in Resolved but IsHealthy returns false for
// each of them.
// In this case, it is up to the pgInstance logic to handle pool selection (like during leader failover)
func TestTickOne_AllUnhealthy(t *testing.T) {
	p := newMockProber(map[string][]string{"primary.local": {"10.0.0.1", "10.0.0.2"}})
	// no healthy IPs in the prober
	w := newTestWorker(p)

	poolHealth := receivePoolHealth(t, w.Subscribe("primary.local", false, "mydb", nil, nil))

	assert.ElementsMatch(t, []string{"10.0.0.1", "10.0.0.2"}, poolHealth.Snapshot().Resolved)
	assert.False(t, poolHealth.IsHealthy("10.0.0.1"))
	assert.False(t, poolHealth.IsHealthy("10.0.0.2"))
}

func TestProbeErrorPublishesInstanceAsUnhealthy(t *testing.T) {
	p := newMockProber(map[string][]string{"primary.local": {"10.0.0.1"}}, "10.0.0.1")
	w := newTestWorker(p)
	updates := w.Subscribe("primary.local", false, "mydb", nil, nil)
	poolHealth := receivePoolHealth(t, updates)
	assert.True(t, poolHealth.IsHealthy("10.0.0.1"))

	p.mu.Lock()
	p.probeErrors["10.0.0.1"] = errors.New("probe failed")
	p.mu.Unlock()
	w.Refresh()

	assert.Same(t, poolHealth, receivePoolHealth(t, updates))
	assert.False(t, poolHealth.IsHealthy("10.0.0.1"), "probe errors must override the boolean result")
}

func newTestPoolHealth(ips ...string) *PoolHealth {
	state := NewHealthState()
	state.Track(ips)
	patroni := NewHealthState()
	patroni.Track(ips)
	return &PoolHealth{state: state, patroniHealthState: patroni}
}

func TestTrackNewIPs_AddsNewAndDedupes(t *testing.T) {
	s := newTestPoolHealth()

	got := s.trackNewIPs([]string{"10.0.0.1", "10.0.0.2"})
	assert.ElementsMatch(t, []string{"10.0.0.1", "10.0.0.2"}, got)

	got = s.trackNewIPs([]string{"10.0.0.2", "10.0.0.3"})
	assert.ElementsMatch(t, []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}, got, "existing IPs not duplicated")
}

func TestRoutingSnapshotPreservesIndependentPatroniDiscoveryAndComparisons(t *testing.T) {
	const primary, standby = "10.0.0.1", "10.0.0.2"
	prober := newMockProber(map[string][]string{"primary.local": {primary, standby}}, primary)
	w := newTestWorker(prober)
	sqlProber := &fakeSQLHealthProber{
		responses: map[string][]fakeSQLHealthProbeResponse{
			primary: {{inRecovery: true}},
			standby: {{inRecovery: false}},
		},
		calls: make(map[string]int),
	}
	var comparisonMu sync.Mutex
	comparedPatroniHealth := make(map[string]bool)
	w.newSQLChecker = func(context.Context) *SQLHealthChecker {
		return newTestSQLHealthChecker(t, sqlProber, 1, SQLHealthCheckHooks{
			ProbeFinished: func(result SQLHealthCheckResult, patroniHealthy bool, _ time.Duration, _ []string) {
				comparisonMu.Lock()
				defer comparisonMu.Unlock()
				comparedPatroniHealth[result.IP] = patroniHealthy
			},
		})
	}
	updates := w.Subscribe("primary.local", false, "db", nil, &SQLHealthConfig{
		ConnStringForIP: func(ip string) string { return ip },
	})
	health := receivePoolHealth(t, updates)
	t.Cleanup(func() { w.Unsubscribe(updates) })
	require.NotSame(t, health.state, health.patroniHealthState)

	// Simulate a different published route without modifying Patroni's source snapshot.
	health.routingMu.Lock()
	health.publishRoutingSnapshot(map[string]bool{standby: true}, []string{standby})
	health.routingMu.Unlock()
	assert.ElementsMatch(t, []string{primary, standby}, health.patroniHealthState.Snapshot().Resolved)
	assert.True(t, health.patroniHealthState.IsHealthy(primary))
	w.RefreshSQL()
	assert.Equal(t, map[string]bool{primary: true, standby: false}, comparedPatroniHealth)

	// Patroni retains and probes its known primary even when it disappears from DNS
	// and from the client-visible route, then publishes its independent result again.
	prober.mu.Lock()
	prober.ipsByHost["primary.local"] = []string{standby}
	prober.mu.Unlock()
	w.Refresh()
	assert.True(t, health.IsHealthy(primary))
	assert.False(t, health.IsHealthy(standby))
	assert.ElementsMatch(t, []string{primary, standby}, health.Snapshot().Resolved)
}

func TestFirstSQLSubscriptionDoesNotPublishRejectedPatroniObservation(t *testing.T) {
	for _, routingEnabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("routing_enabled_%t", routingEnabled), func(t *testing.T) {
			const ip = "10.0.0.1"
			w := newTestWorker(newMockProber(map[string][]string{"primary.local": {ip}}, ip))
			patroniUpdates := w.Subscribe("primary.local", false, "db", nil, nil)
			health := receivePoolHealth(t, patroniUpdates)
			t.Cleanup(func() { w.Unsubscribe(patroniUpdates) })
			w.newSQLChecker = func(context.Context) *SQLHealthChecker {
				return newTestSQLHealthChecker(t, &fakeSQLHealthProber{}, 1, SQLHealthCheckHooks{})
			}

			// Pause after the check captures observedAt, before it applies its result.
			entered, resume, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(resume) }) }
			w.hooks.CheckFinished = func(time.Duration, []string) { close(entered); <-resume }
			go func() { w.Refresh(); close(done) }()
			t.Cleanup(func() { release(); <-done })
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("Patroni check did not reach publication")
			}

			// The first SQL subscription seeds the policy with a newer timestamp.
			sqlUpdates := w.Subscribe("primary.local", false, "db", nil, &SQLHealthConfig{
				ConnStringForIP: func(string) string { return "conn" },
				RoutingEnabled: func(context.Context) SQLHealthFeatureFlagEvaluation {
					return validSQLHealthFeatureFlag(routingEnabled)
				},
			})
			require.Same(t, health, receivePoolHealth(t, sqlUpdates))
			t.Cleanup(func() { w.Unsubscribe(sqlUpdates) })
			release()
			<-done
			require.True(t, health.IsHealthy(ip), "a rejected decision must preserve the published route")
			select {
			case <-patroniUpdates:
				t.Fatal("a rejected decision must not notify routing subscribers")
			default:
			}

			// A fresh, accepted unhealthy result must still remove the route.
			w.applyPatroniHealth(health, map[string]bool{ip: false}, nil, []string{ip}, time.Now())
			assert.False(t, health.IsHealthy(ip))
		})
	}
}

func TestUpdateHealthAndCleanup_PublishesHealthAndEvictsExpired(t *testing.T) {
	w := newTestWorker(nil)
	s := newTestPoolHealth("10.0.0.1", "10.0.0.2")

	// both IPs healthy, both in DNS
	w.applyPatroniHealth(s, map[string]bool{"10.0.0.1": true, "10.0.0.2": true}, nil, []string{"10.0.0.1", "10.0.0.2"}, time.Now())
	assert.True(t, s.IsHealthy("10.0.0.1"))
	assert.True(t, s.IsHealthy("10.0.0.2"))
	assert.ElementsMatch(t, []string{"10.0.0.1", "10.0.0.2"}, s.Snapshot().Resolved)

	// 10.0.0.2 leaves DNS but stays healthy — grace window, not evicted
	for i := 0; i < maxFailedHostLookups-1; i++ {
		w.applyPatroniHealth(s, map[string]bool{"10.0.0.1": true, "10.0.0.2": true}, nil, []string{"10.0.0.1"}, time.Now())
	}
	assert.ElementsMatch(t, []string{"10.0.0.1", "10.0.0.2"}, s.Snapshot().Resolved, "still in grace window")

	// 10.0.0.2 now unhealthy and absent from DNS — eligible for eviction
	w.applyPatroniHealth(s, map[string]bool{"10.0.0.1": true, "10.0.0.2": false}, nil, []string{"10.0.0.1"}, time.Now())
	assert.ElementsMatch(t, []string{"10.0.0.1"}, s.Snapshot().Resolved, "evicted after grace window and unhealthy")
	assert.False(t, s.IsHealthy("10.0.0.2"), "evicted IP must not stay healthy")

	// verify lastKnownPgHealth protects an IP that is still healthy
	s2 := newTestPoolHealth("10.0.0.1", "10.0.0.2")
	w.applyPatroniHealth(s2, map[string]bool{"10.0.0.1": true, "10.0.0.2": true}, nil, []string{"10.0.0.1", "10.0.0.2"}, time.Now())
	for i := 0; i < maxFailedHostLookups+1; i++ {
		w.applyPatroniHealth(s2, map[string]bool{"10.0.0.1": true, "10.0.0.2": true}, nil, []string{"10.0.0.1"}, time.Now())
	}
	assert.ElementsMatch(t, []string{"10.0.0.1", "10.0.0.2"}, s2.Snapshot().Resolved, "last known healthy IP never evicted")
}

func TestPatroniRefreshDoesNotWaitForBlockedSQLCheckToCleanUpEvictedPool(t *testing.T) {
	const (
		ip            = "10.0.0.1"
		replacementIP = "10.0.0.2"
	)
	patroniProber := newMockProber(map[string][]string{"primary.local": {ip}}, ip)
	w := newTestWorker(patroniProber)
	w.absenceThreshold = 1

	pools := newTestSQLHealthCheckPools(t)
	sqlProber := &blockingPoolCreatingSQLHealthProber{
		pools:   pools,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	var releaseOnce sync.Once
	releaseSQL := func() { releaseOnce.Do(func() { close(sqlProber.release) }) }
	t.Cleanup(releaseSQL)
	checker := NewSQLHealthChecker(context.Background(), SQLHealthCheckPoolsConfig{}, SQLHealthCheckerConfig{
		Pools:  pools,
		Prober: sqlProber,
	})
	w.newSQLChecker = func(context.Context) *SQLHealthChecker { return checker }
	healthUpdates := w.Subscribe("primary.local", false, "mydb", nil, &SQLHealthConfig{
		ConnStringForIP: func(string) string { return testSQLHealthCheckConnString },
	})
	health := receivePoolHealth(t, healthUpdates)

	sqlRefreshDone := make(chan struct{})
	go func() {
		w.RefreshSQL()
		close(sqlRefreshDone)
	}()
	select {
	case <-sqlProber.started:
	case <-time.After(time.Second):
		t.Fatal("SQL health check did not start")
	}
	require.Equal(t, 1, pools.Len())

	patroniProber.mu.Lock()
	patroniProber.ipsByHost["primary.local"] = []string{replacementIP}
	patroniProber.healthyIPs[ip] = false
	patroniProber.healthyIPs[replacementIP] = true
	patroniProber.mu.Unlock()
	patroniRefreshDone := make(chan struct{})
	go func() {
		w.Refresh()
		close(patroniRefreshDone)
	}()

	select {
	case <-patroniRefreshDone:
	case <-time.After(time.Second):
		t.Fatal("Patroni refresh waited for the blocked SQL health check")
	}
	assert.Same(t, health, receivePoolHealth(t, healthUpdates))
	assert.Equal(t, []string{replacementIP}, health.Snapshot().Resolved)
	assert.Equal(t, 1, pools.Len(), "the pool must remain open while its SQL probe is using it")

	releaseSQL()
	select {
	case <-sqlRefreshDone:
	case <-time.After(time.Second):
		t.Fatal("SQL health check did not finish after it was released")
	}
	assert.Equal(t, 1, pools.Len(), "the first SQL round must retain the IP that it independently discovered")

	// SQL also observes the old instance's demotion, so it no longer protects that
	// address after discovering the replacement primary. The completed round above
	// is no longer reading the mock when its response changes.
	sqlProber.standbyIP = ip
	for range maxFailedHostLookups {
		w.RefreshSQL()
	}
	assert.Equal(t, 1, pools.Len(), "the SQL loop must replace the stale IP pool with the newly discovered one")
	pools.mu.Lock()
	_, retainedOldIP := pools.pools[ip]
	_, discoveredNewIP := pools.pools[replacementIP]
	pools.mu.Unlock()
	assert.False(t, retainedOldIP)
	assert.True(t, discoveredNewIP)
}

func TestRefreshSQLProtectsRetainedStandbyPromotedDuringEmptyDNS(t *testing.T) {
	const a, b = "10.0.0.1", "10.0.0.2"
	patroniProber := newMockProber(map[string][]string{"primary.local": {a, b}}, a)
	w := newTestWorker(patroniProber)
	sqlProber := &fakeSQLHealthProber{
		responses: map[string][]fakeSQLHealthProbeResponse{
			a: {{inRecovery: true}},
			b: {{inRecovery: true}, {inRecovery: false}},
		},
		calls: make(map[string]int),
	}
	w.newSQLChecker = func(context.Context) *SQLHealthChecker {
		return newTestSQLHealthChecker(t, sqlProber, 1, SQLHealthCheckHooks{})
	}
	updates := w.Subscribe("primary.local", false, "db", nil, &SQLHealthConfig{
		ConnStringForIP: func(ip string) string { return ip },
	})
	health := receivePoolHealth(t, updates)
	t.Cleanup(func() { w.Unsubscribe(updates) })

	// Neither standby initially has last-known-healthy retention protection.
	w.RefreshSQL()
	require.Empty(t, health.sqlHealthState.lastKnownHealthy)
	patroniProber.mu.Lock()
	patroniProber.ipsByHost["primary.local"] = nil
	patroniProber.mu.Unlock()

	// B becomes primary while it is retained but absent from the latest DNS answer.
	w.RefreshSQL()
	require.Contains(t, health.sqlHealthState.lastKnownHealthy, b)
	for range maxFailedHostLookups {
		w.RefreshSQL()
	}
	assert.Equal(t, []string{b}, health.sqlHealthState.Snapshot().Resolved, "the promoted primary must survive the DNS absence threshold")
	assert.True(t, health.sqlHealthState.IsHealthy(b))
	assert.Greater(t, sqlProber.callCount(b), maxFailedHostLookups, "the retained primary must continue being probed")
}

func TestRefreshSQLRetainsIndependentlyDiscoveredPrimaryWhenDNSReturnsNoAddresses(t *testing.T) {
	const (
		patroniIP = "127.0.0.1"
		sqlIP     = "127.0.0.2"
	)
	patroniProber := newMockProber(map[string][]string{"primary.local": {patroniIP}}, patroniIP)
	w := newTestWorker(patroniProber)
	pools := newTestSQLHealthCheckPools(t)
	sqlProber := &poolCreatingSQLHealthProber{pools: pools}
	w.newSQLChecker = func(context.Context) *SQLHealthChecker {
		return NewSQLHealthChecker(context.Background(), SQLHealthCheckPoolsConfig{}, SQLHealthCheckerConfig{
			Pools:  pools,
			Prober: sqlProber,
		})
	}

	healthUpdates := w.Subscribe("primary.local", false, "probedb", nil, &SQLHealthConfig{
		ConnStringForIP: func(string) string { return testSQLHealthCheckConnString },
	})
	health := receivePoolHealth(t, healthUpdates)
	t.Cleanup(func() { w.Unsubscribe(healthUpdates) })
	sqlUpdates, err := health.SubscribeSQLHealth(healthUpdates)
	require.NoError(t, err)
	t.Cleanup(sqlUpdates.Close)

	// SQL discovers the replacement before the slower Patroni loop observes it.
	patroniProber.mu.Lock()
	patroniProber.ipsByHost["primary.local"] = []string{sqlIP}
	patroniProber.mu.Unlock()
	w.RefreshSQL()
	first := receiveSQLHealthObservation(t, sqlUpdates)
	require.True(t, first.Writable)
	require.NoError(t, first.Err)
	require.Equal(t, 1, pools.Len())

	// Empty DNS results retain the independently discovered last-known-writable primary and its warm
	// probe pool, matching the Patroni path's protection during simultaneous discovery disruption.
	patroniProber.mu.Lock()
	patroniProber.ipsByHost["primary.local"] = nil
	patroniProber.mu.Unlock()
	for range maxFailedHostLookups + 1 {
		w.RefreshSQL()
		observation := receiveSQLHealthObservation(t, sqlUpdates)
		assert.True(t, observation.Writable)
		assert.NoError(t, observation.Err)
		assert.Equal(t, 1, pools.Len())
	}
	assert.Equal(t, []string{sqlIP}, health.sqlHealthState.Snapshot().Resolved)
	assert.Equal(t, []string{patroniIP}, health.Snapshot().Resolved)
	pools.mu.Lock()
	_, retainedSQLIP := pools.pools[sqlIP]
	pools.mu.Unlock()
	assert.True(t, retainedSQLIP)
}
func TestTick_IpRotation(t *testing.T) {
	// only 10.0.0.1 is healthy; 10.0.0.2 is not
	p := newMockProber(map[string][]string{"primary.local": {"10.0.0.1"}}, "10.0.0.1")
	w := newTestWorker(p)

	updates := w.Subscribe("primary.local", false, "mydb", nil, nil)
	poolHealth := receivePoolHealth(t, updates)
	assert.ElementsMatch(t, []string{"10.0.0.1"}, poolHealth.Snapshot().Resolved)

	p.mu.Lock()
	p.ipsByHost["primary.local"] = []string{"10.0.0.1", "10.0.0.2"}
	p.mu.Unlock()
	w.Refresh()
	assert.Same(t, poolHealth, receivePoolHealth(t, updates))
	assert.ElementsMatch(t, []string{"10.0.0.1", "10.0.0.2"}, poolHealth.Snapshot().Resolved)

	// Both IPs leave DNS. 10.0.0.1 remains healthy so lastKnownPgHealth
	// protects it from eviction. 10.0.0.2 is unhealthy and gets evicted.
	p.mu.Lock()
	p.ipsByHost["primary.local"] = nil
	p.mu.Unlock()

	for i := 0; i < maxFailedHostLookups-1; i++ {
		w.Refresh()
		assert.ElementsMatch(t, []string{"10.0.0.1", "10.0.0.2"}, poolHealth.Snapshot().Resolved, "tick %d: IPs should still be tracked", i+1)
	}
	w.Refresh()
	assert.ElementsMatch(t, []string{"10.0.0.1"}, poolHealth.Snapshot().Resolved, "10.0.0.2 evicted; 10.0.0.1 protected by lastKnownPgHealth")
}

func TestHealthWorkerPreservesSubscriptionMetadataAndHooks(t *testing.T) {
	p := newMockProber(map[string][]string{"primary.local": {"10.0.0.1"}})
	w := newTestWorker(p)
	w.absenceThreshold = 1
	var counts []int
	var subscriptionTags [][]string
	var events []string
	hostTags := []string{"db:first", "postgres_host:primary.local"}
	sharedTags := []string{"postgres_host:primary.local", "pool_role:replica"}
	w.hooks = HealthWorkerHooks{
		Subscriptions: func(count int, tags []string) {
			counts = append(counts, count)
			subscriptionTags = append(subscriptionTags, append([]string(nil), tags...))
		},
		CheckStarted: func(tags []string) {
			assert.Equal(t, sharedTags, tags)
			events = append(events, "start")
		},
		CheckFinished: func(elapsed time.Duration, tags []string) {
			assert.Equal(t, sharedTags, tags)
			assert.GreaterOrEqual(t, elapsed, time.Duration(0))
			events = append(events, "finish")
		},
		Evicted: func(ip string, tags []string) {
			assert.Equal(t, "10.0.0.1", ip)
			assert.Equal(t, hostTags, tags)
			events = append(events, "evict")
		},
	}
	p.recordCall = func(ip, host, dbName string, isReadReplica bool, tags []string) {
		assert.Equal(t, "10.0.0.1", ip)
		assert.Equal(t, "primary.local", host)
		assert.Equal(t, "first", dbName)
		assert.True(t, isReadReplica)
		assert.Equal(t, sharedTags, tags)
	}
	tags := []string{"db:first"}
	firstUpdates := w.Subscribe("primary.local", true, "first", tags, nil)
	first := receivePoolHealth(t, firstUpdates)
	tags[0] = "changed"
	secondUpdates := w.Subscribe("primary.local", true, "second", []string{"db:second"}, nil)
	second := receivePoolHealth(t, secondUpdates)
	assert.Same(t, first, second, "host alone is the subscription key")
	assert.Equal(t, 1, p.probeCalls, "only the first subscriber refreshes inline")
	assert.Equal(t, []string{"start", "finish"}, events)

	p.ipsByHost["primary.local"] = nil
	events = nil
	w.Refresh()
	assert.Equal(t, []string{"start", "finish", "evict"}, events, "timing is reported before cleanup")
	assert.Empty(t, first.Snapshot().Resolved)

	w.Unsubscribe(firstUpdates)
	w.Unsubscribe(secondUpdates)
	w.Unsubscribe(nil)
	assert.Equal(t, []int{1, 2, 1, 0}, counts)
	assert.Equal(t, [][]string{{"db:first"}, {"db:second"}, hostTags, hostTags}, subscriptionTags)
}

func TestUnsubscribeOldSnapshotDoesNotRemoveReplacement(t *testing.T) {
	w := newTestWorker(newMockProber(nil))
	oldUpdates := w.Subscribe("primary.local", false, "mydb", nil, nil)
	old := receivePoolHealth(t, oldUpdates)
	w.Unsubscribe(oldUpdates)
	currentUpdates := w.Subscribe("primary.local", false, "mydb", nil, nil)
	current := receivePoolHealth(t, currentUpdates)
	w.Unsubscribe(oldUpdates)
	assert.Same(t, current, w.hosts["primary.local"])
	assert.NotSame(t, old, current)
}

func TestHealthWorkerChecksHostsAndIPsConcurrentlyBeforePublishing(t *testing.T) {
	p := newMockProber(map[string][]string{
		"a.local": {"10.0.0.1", "10.0.0.2"},
		"b.local": {"10.0.0.3", "10.0.0.4"},
	}, "10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4")
	w := newTestWorker(p)
	a := receivePoolHealth(t, w.Subscribe("a.local", false, "mydb", nil, nil))
	b := receivePoolHealth(t, w.Subscribe("b.local", false, "mydb", nil, nil))
	started := make(chan string, 4)
	release := make(chan struct{})
	done := make(chan struct{})
	p.mockHealthPing = func(ip string) (bool, error) {
		started <- ip
		<-release
		return false, nil
	}
	go func() { w.Refresh(); close(done) }()
	// Always release the probes, even if the concurrency assertion fails.
	defer func() {
		close(release)
		<-done
		assert.False(t, a.IsHealthy("10.0.0.1"))
		assert.False(t, b.IsHealthy("10.0.0.3"))
	}()
	var ips []string
	for range 4 {
		select {
		case ip := <-started:
			ips = append(ips, ip)
		case <-time.After(5 * time.Second):
			t.Fatal("all hosts and IPs must be probed concurrently")
		}
	}
	assert.ElementsMatch(t, []string{"10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4"}, ips)
	assert.True(t, a.IsHealthy("10.0.0.1"), "old health remains visible until probes finish")
	assert.True(t, b.IsHealthy("10.0.0.3"))
	select {
	case <-done:
		t.Fatal("refresh returned before probes finished")
	default:
	}
}

func TestSubscribePreservesSubscriptionTags(t *testing.T) {
	for _, hasHost := range []bool{false, true} {
		tags := make([]string, 2, 8)
		copy(tags, []string{"database:configured_pool", "database_cluster:cluster-a"})
		if hasHost {
			tags = append(tags, "postgres_host:primary.local")
		}
		w := newTestWorker(newMockProber(nil))
		health := receivePoolHealth(t, w.Subscribe("primary.local", false, "physical_db", tags, nil))
		assert.ElementsMatch(t, []string{"database:configured_pool", "database_cluster:cluster-a", "postgres_host:primary.local"}, health.tags)
		for _, tag := range tags[len(tags):cap(tags)] {
			assert.Empty(t, tag)
		}
		tags[0] = "database:changed"
		assert.Contains(t, health.tags, "database:configured_pool")
	}
}

func TestSQLPatroniHealthSnapshotReportsAllFindings(t *testing.T) {
	probeErr := errors.New("database is shutting down")
	now := time.Now()
	snapshot := SQLPatroniHealthSnapshot{
		SQLHealth: map[string]bool{
			"10.0.0.1": true,
			"10.0.0.3": true,
		},
		SQLResults: []SQLHealthResultsSnapshot{{
			ScheduledAt:  now,
			ObservedAt:   now.Add(100 * time.Millisecond),
			CandidateIPs: []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"},
			Tags:         []string{"database:orders"},
			Results: []SQLHealthCheckResult{
				{IP: "10.0.0.1"},
				{IP: "10.0.0.2", Err: probeErr, Stage: SQLHealthCheckAcquireStage},
				{IP: "10.0.0.3"},
			},
		}},
		PatroniHealth: map[string]bool{
			"10.0.0.1": true,
			"10.0.0.2": true,
			"10.0.0.4": false,
		},
	}

	assert.False(t, snapshot.Matches())
	require.Len(t, snapshot.SQLResults, 1)
	assert.Equal(t, []string{"database:orders"}, snapshot.SQLResults[0].Tags)
	assert.ErrorIs(t, snapshot.SQLResults[0].Results[1].Err, probeErr)
	assert.Equal(t, SQLHealthCheckAcquireStage, snapshot.SQLResults[0].Results[1].Stage)
}

func TestSQLPatroniHealthSnapshotMatchesIndependentMapOrder(t *testing.T) {
	now := time.Now()
	snapshot := SQLPatroniHealthSnapshot{
		SQLHealth: map[string]bool{"10.0.0.1": true, "10.0.0.2": false},
		SQLResults: []SQLHealthResultsSnapshot{{
			ScheduledAt:  now,
			ObservedAt:   now,
			CandidateIPs: []string{"10.0.0.2", "10.0.0.1"},
			Results: []SQLHealthCheckResult{
				{IP: "10.0.0.2", InRecovery: true},
				{IP: "10.0.0.1"},
			},
		}},
		PatroniHealth: map[string]bool{"10.0.0.1": true, "10.0.0.2": false},
	}

	assert.True(t, snapshot.Matches())
}

func TestSQLPatroniHealthSnapshotIgnoresUnknownSQLIP(t *testing.T) {
	now := time.Now()
	snapshot := SQLPatroniHealthSnapshot{
		SQLHealth:      map[string]bool{},
		SQLRetainedIPs: []string{"10.0.0.1"},
		SQLResults: []SQLHealthResultsSnapshot{{
			ScheduledAt:  now,
			ObservedAt:   now,
			CandidateIPs: []string{"10.0.0.1"},
			Results: []SQLHealthCheckResult{{
				IP:    "10.0.0.1",
				Err:   context.DeadlineExceeded,
				Stage: SQLHealthCheckAcquireStage,
			}},
		}},
		PatroniHealth: map[string]bool{"10.0.0.1": true},
	}

	assert.True(t, snapshot.Matches())
	assert.ErrorIs(t, snapshot.SQLResults[0].Results[0].Err, context.DeadlineExceeded)
}

func TestSQLPatroniHealthSnapshotDoesNotIgnoreEvictedCandidateIP(t *testing.T) {
	const (
		oldIP = "10.0.0.1"
		newIP = "10.0.0.2"
	)
	snapshot := SQLPatroniHealthSnapshot{
		SQLHealth:      map[string]bool{newIP: true},
		SQLRetainedIPs: []string{newIP},
		SQLResults: []SQLHealthResultsSnapshot{{
			CandidateIPs: []string{oldIP, newIP},
			Results: []SQLHealthCheckResult{
				{IP: oldIP, InRecovery: true},
				{IP: newIP},
			},
		}},
		PatroniHealth: map[string]bool{oldIP: false, newIP: true},
	}

	assert.False(t, snapshot.Matches(),
		"an evicted IP in a historical raw snapshot must still count as a membership disagreement")
}

func TestSQLComparisonDerivesConflictsIndependentOfCompletionOrder(t *testing.T) {
	const ip = "10.0.0.1"
	for _, primaryFirst := range []bool{true, false} {
		t.Run(fmt.Sprintf("primaryFirst=%t", primaryFirst), func(t *testing.T) {
			state := NewHealthState()
			state.Track([]string{ip})
			poolHealth := &PoolHealth{sqlHealthState: state, state: NewHealthState(), patroniHealthState: NewHealthState()}
			primaryTarget := &sqlHealthTarget{tags: []string{"database:orders"}}
			standbyTarget := &sqlHealthTarget{tags: []string{"database:billing"}}
			targets := []*sqlHealthTarget{primaryTarget, standbyTarget}
			if !primaryFirst {
				slices.Reverse(targets)
			}
			now := time.Now()
			var snapshot *sqlHealthComparisonSnapshot
			for i, target := range targets {
				snapshot = poolHealth.recordSQLComparisonPoll(false, target, 0, now, now.Add(time.Duration(i)*time.Second),
					[]string{ip}, SQLHealthCheckOutcome{Results: []SQLHealthCheckResult{{
						IP: ip, InRecovery: target == standbyTarget,
					}}})
				require.NotNil(t, snapshot)
				assert.Len(t, snapshot.results, i+1, "each credential completion is published independently")
			}
			assert.Empty(t, snapshot.health)
			assert.Equal(t, []string{ip}, snapshot.conflictingIPs)
			w := &HealthWorker{hooks: HealthWorkerHooks{
				SQLPatroniCompared: func(comparison SQLPatroniHealthSnapshot, _ []string) {
					assert.Equal(t, []string{ip}, comparison.SQLConflictingIPs)
					for _, patroniRole := range []bool{true, false} {
						comparison.PatroniHealth = map[string]bool{ip: patroniRole}
						assert.False(t, comparison.Matches(), "a conflict cannot match either Patroni role")
					}
				},
			}}
			w.reportSQLPatroniComparison(poolHealth, snapshot)

			// The standby credential's next poll agrees: only its latest observations contribute.
			snapshot = poolHealth.recordSQLComparisonPoll(false, standbyTarget, 0, now.Add(time.Second), now.Add(2*time.Second),
				[]string{ip}, SQLHealthCheckOutcome{Results: []SQLHealthCheckResult{{IP: ip}}})
			assert.Equal(t, map[string]bool{ip: true}, snapshot.health)
			assert.Empty(t, snapshot.conflictingIPs)
			assert.Len(t, snapshot.results, 2)
		})
	}
}

func TestSQLComparisonInconclusivePollReplacesPreviousRoleWithUnknown(t *testing.T) {
	const ip = "10.0.0.1"
	for _, inRecovery := range []bool{false, true} {
		for _, probeErr := range []error{context.DeadlineExceeded, errors.New("database is shutting down")} {
			t.Run(fmt.Sprintf("inRecovery=%t/%s", inRecovery, probeErr), func(t *testing.T) {
				state := NewHealthState()
				state.Track([]string{ip})
				poolHealth := &PoolHealth{sqlHealthState: state}
				target := &sqlHealthTarget{tags: []string{"database:orders"}}
				now := time.Now()
				poolHealth.recordSQLComparisonPoll(false, target, 0, now, now, []string{ip},
					SQLHealthCheckOutcome{Results: []SQLHealthCheckResult{{IP: ip, InRecovery: inRecovery}}})

				snapshot := poolHealth.recordSQLComparisonPoll(false, target, 0, now.Add(time.Second), now.Add(time.Second), []string{ip},
					SQLHealthCheckOutcome{Results: []SQLHealthCheckResult{{IP: ip, Err: probeErr, Stage: SQLHealthCheckQueryStage}}})
				assert.Empty(t, snapshot.health, "an error must not resurrect this target's previous successful role")
				assert.Empty(t, snapshot.conflictingIPs)
				require.Len(t, snapshot.results, 1)
				assert.ErrorIs(t, snapshot.results[0].Results[0].Err, probeErr)
				assert.Equal(t, SQLHealthCheckQueryStage, snapshot.results[0].Results[0].Stage)
				assert.True(t, SQLPatroniHealthSnapshot{
					SQLHealth: snapshot.health, SQLRetainedIPs: snapshot.retainedIPs, PatroniHealth: map[string]bool{ip: true},
				}.Matches(), "unknown remains neutral under the comparison policy")

				// A different credential's current successful result can supply the unknown role.
				otherTarget := &sqlHealthTarget{tags: []string{"database:billing"}}
				snapshot = poolHealth.recordSQLComparisonPoll(false, otherTarget, 0, now.Add(time.Second), now.Add(time.Second), []string{ip},
					SQLHealthCheckOutcome{Results: []SQLHealthCheckResult{{IP: ip, InRecovery: true}}})
				assert.Equal(t, map[string]bool{ip: false}, snapshot.health)
				assert.Empty(t, snapshot.conflictingIPs)
			})
		}
	}
}

func TestSQLComparisonEmptyPollReplacesPreviousResults(t *testing.T) {
	const ip = "10.0.0.1"
	state := NewHealthState()
	state.Track([]string{ip})
	poolHealth := &PoolHealth{sqlHealthState: state}
	target := &sqlHealthTarget{}
	now := time.Now()
	poolHealth.recordSQLComparisonPoll(false, target, 0, now, now, []string{ip},
		SQLHealthCheckOutcome{Results: []SQLHealthCheckResult{{IP: ip}}})
	snapshot := poolHealth.recordSQLComparisonPoll(false, target, 0, now, now, []string{ip}, SQLHealthCheckOutcome{})
	assert.Empty(t, snapshot.health)
	require.Len(t, snapshot.results, 1)
	assert.Empty(t, snapshot.results[0].Results)
}

func TestSQLComparisonDoesNotRestoreRetiredTarget(t *testing.T) {
	const ip = "10.0.0.1"
	state := NewHealthState()
	state.Track([]string{ip})
	poolHealth := &PoolHealth{sqlHealthState: state}
	target := &sqlHealthTarget{tags: []string{"database:orders"}}
	now := time.Now()

	initial := poolHealth.recordSQLComparisonPoll(false, target, 0, now, now, []string{ip},
		SQLHealthCheckOutcome{Results: []SQLHealthCheckResult{{IP: ip}}})
	require.NotNil(t, initial)
	poolHealth.retireSQLComparisonTarget(target)
	late := poolHealth.recordSQLComparisonPoll(false, target, 0, now, now, []string{ip},
		SQLHealthCheckOutcome{Results: []SQLHealthCheckResult{{IP: ip, InRecovery: true}}})
	assert.Nil(t, late, "a completion after the last unsubscribe must not restore the target")
	assert.NotContains(t, poolHealth.sqlComparisonLatest, target)
	_, started, _ := poolHealth.reserveSQLComparisonPoll(target, now, poolHealth.currentSQLComparisonGeneration(target), true)
	assert.False(t, started, "a retired target cannot start another poll")

	// A new subscription with the same tags has a distinct identity; the old role is gone.
	replacement := &sqlHealthTarget{tags: slices.Clone(target.tags)}
	snapshot := poolHealth.recordSQLComparisonPoll(false, replacement, 0, now, now, []string{ip},
		SQLHealthCheckOutcome{Results: []SQLHealthCheckResult{{IP: ip, Err: context.DeadlineExceeded}}})
	assert.Empty(t, snapshot.health)
	require.Len(t, snapshot.results, 1)
}

func TestSQLComparisonRejectsPreDisableGenerationAfterReenable(t *testing.T) {
	const ip = "10.0.0.1"
	state := NewHealthState()
	state.Track([]string{ip})
	poolHealth := &PoolHealth{sqlHealthState: state}
	target := &sqlHealthTarget{}
	oldRound := time.Now()
	generation, started, _ := poolHealth.reserveSQLComparisonPoll(target, oldRound, poolHealth.currentSQLComparisonGeneration(target), true)
	require.True(t, started)
	poolHealth.recordSQLComparisonPoll(false, target, generation, oldRound, oldRound, []string{ip},
		SQLHealthCheckOutcome{Results: []SQLHealthCheckResult{{IP: ip}}})
	poolHealth.disableSQLComparisonTarget(target)
	poolHealth.disableSQLComparisonTarget(target)
	assert.NotContains(t, poolHealth.sqlComparisonLatest, target)
	poolHealth.sqlComparisonMu.Lock()
	current := buildSQLHealthComparisonSnapshot(map[string]struct{}{ip: {}}, poolHealth.sqlComparisonLatest)
	poolHealth.sqlComparisonMu.Unlock()
	assert.Empty(t, current.health, "disabling removes this credential's contribution to the aggregate")

	// Re-enabling while the old check is busy cannot start a competing check.
	_, started, _ = poolHealth.reserveSQLComparisonPoll(target, oldRound.Add(time.Second), poolHealth.currentSQLComparisonGeneration(target), true)
	assert.False(t, started)
	late := poolHealth.recordSQLComparisonPoll(false, target, generation, oldRound, oldRound.Add(time.Second), []string{ip},
		SQLHealthCheckOutcome{Results: []SQLHealthCheckResult{{IP: ip}}})
	assert.Nil(t, late, "a pre-disable completion must not restore comparison state after re-enable")
	target.finishSQLRound(oldRound)

	// Even a regressed wall clock is valid in the new lifecycle: no timestamp cutoff applies.
	newRound := oldRound.Add(-time.Second)
	freshGeneration, started, _ := poolHealth.reserveSQLComparisonPoll(target, newRound, poolHealth.currentSQLComparisonGeneration(target), true)
	require.True(t, started)
	require.NotEqual(t, generation, freshGeneration)
	fresh := poolHealth.recordSQLComparisonPoll(false, target, freshGeneration, newRound, newRound, []string{ip},
		SQLHealthCheckOutcome{Results: []SQLHealthCheckResult{{IP: ip, InRecovery: true}}})
	require.NotNil(t, fresh)
	assert.Equal(t, map[string]bool{ip: false}, fresh.health)
	assert.Contains(t, poolHealth.sqlComparisonLatest, target)
	target.finishSQLRound(newRound)

	poolHealth.retireSQLComparisonTarget(target)
	_, started, _ = poolHealth.reserveSQLComparisonPoll(target, newRound.Add(time.Second), freshGeneration, true)
	assert.False(t, started, "a retired target cannot start another poll")
}

func TestHybridEvaluationIncludesPrimaryRestoredByLateSuccess(t *testing.T) {
	const oldIP, newIP = "10.0.0.1", "10.0.0.2"
	now := time.Now()
	state := NewHealthState()
	state.RefreshResolved([]string{oldIP, newIP}, 1, nil)
	state.RecordHealthAt(now.Add(-time.Second), map[string]bool{oldIP: true})
	// The new IP disappears from DNS before its first successful probe finishes.
	state.RefreshResolved(nil, 1, nil)
	require.ElementsMatch(t, []string{oldIP}, state.Snapshot().Resolved)
	health := &PoolHealth{
		state: NewHealthState(), patroniHealthState: NewHealthState(), sqlHealthState: state,
		hybridPolicy:     newHybridHealthPolicy(true),
		routingEnabledFn: func(context.Context) SQLHealthFeatureFlagEvaluation { return validSQLHealthFeatureFlag(true) },
	}
	var comparison SQLPatroniHealthSnapshot
	var iterations []SQLIteration
	w := &HealthWorker{
		hooks:    HealthWorkerHooks{SQLPatroniCompared: func(snapshot SQLPatroniHealthSnapshot, _ []string) { comparison = snapshot }},
		sqlHooks: SQLHealthWorkerHooks{Iteration: func(iteration SQLIteration, _ []string) { iterations = append(iterations, iteration) }},
	}
	w.completeSQLHealthCheck(&sqlHealthTarget{poolHealth: health}, 0, now,
		[]string{oldIP, newIP}, map[string]struct{}{oldIP: {}, newIP: {}}, SQLHealthCheckOutcome{
			Results: []SQLHealthCheckResult{{IP: oldIP, InRecovery: true}, {IP: newIP}},
		})
	require.Len(t, iterations, 1)
	assert.Equal(t, SQLRoutingResultConclusive, iterations[0].RoutingResult)
	assert.ElementsMatch(t, []string{oldIP, newIP}, comparison.SQLRetainedIPs)
	assert.Equal(t, map[string]bool{oldIP: false, newIP: true}, comparison.SQLHealth)
	assert.Equal(t, map[string]bool{newIP: true}, health.hybridPolicy.routingTiming.candidate.lastNonempty,
		"the same completion must include the restored primary in the hybrid decision")
}

func TestSQLDiscoverySerializesRetentionWithEvaluation(t *testing.T) {
	const ip = "10.0.0.1"
	state := NewHealthState()
	health := &PoolHealth{host: "primary.local", sqlHealthState: state}
	lookup := make(chan bool, 1)
	prober := newMockProber(map[string][]string{"primary.local": {ip}}, ip)
	prober.recordLookup = func(string, []string) {
		// DNS may block: it must not hold up completed polls or invalidation.
		unlocked := health.sqlComparisonMu.TryLock()
		if unlocked {
			health.sqlComparisonMu.Unlock()
		}
		lookup <- unlocked
	}
	w := newTestWorker(prober)
	// Pause the retention mutation so we can verify the evaluation lock covers it.
	state.mu.Lock()
	var release sync.Once
	unlock := func() { release.Do(state.mu.Unlock) }
	defer unlock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.runSQLHealthGroup(sqlHealthGroup{poolHealth: health, targets: []*sqlHealthTarget{{poolHealth: health}}}, time.Now(), true)
	}()
	select {
	case unlocked := <-lookup:
		assert.True(t, unlocked, "DNS lookup must run outside the evaluation lock")
	case <-time.After(time.Second):
		t.Fatal("DNS lookup did not start")
	}
	assert.Eventually(t, func() bool {
		if health.sqlComparisonMu.TryLock() {
			health.sqlComparisonMu.Unlock()
			return false
		}
		return true
	}, time.Second, time.Millisecond, "DNS retention mutation must exclude snapshot evaluation")
	unlock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("SQL refresh did not finish")
	}
	assert.ElementsMatch(t, []string{ip}, state.Snapshot().Resolved)
}

func TestHybridTelemetryUsesDecisionRoutingMode(t *testing.T) {
	const ip = "10.0.0.1"
	for _, tc := range []struct {
		name    string
		enabled bool
		err     error
		result  string
		wantTag string
	}{
		{"conclusive before disable", true, &pgconn.PgError{Code: "57P03", Message: "the database system is starting up"}, SQLRoutingResultConclusive, "sql_health_routing_enabled:true"},
		{"fallback before disable", true, context.DeadlineExceeded, SQLRoutingResultCachedFallback, "sql_health_routing_enabled:true"},
		{"shadow before enable", false, nil, SQLRoutingResultShadow, "sql_health_routing_enabled:false"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := NewHealthState()
			state.Track([]string{ip})
			health := &PoolHealth{
				state: NewHealthState(), patroniHealthState: NewHealthState(), sqlHealthState: state,
				hybridPolicy:     newHybridHealthPolicy(tc.enabled),
				routingEnabledFn: func(context.Context) SQLHealthFeatureFlagEvaluation { return validSQLHealthFeatureFlag(tc.enabled) },
			}
			var iterationCount, errorCount, fallbackCount int
			w := &HealthWorker{
				hooks: HealthWorkerHooks{SQLPatroniCompared: func(SQLPatroniHealthSnapshot, []string) {
					// Model another observer changing mode after evaluation, before reporting.
					health.hybridPolicy.setRoutingEnabled(!tc.enabled)
				}},
				sqlHooks: SQLHealthWorkerHooks{
					Iteration: func(iteration SQLIteration, tags []string) {
						iterationCount++
						assert.Equal(t, tc.result, iteration.RoutingResult)
						assert.Contains(t, tags, tc.wantTag)
					},
					ConclusiveError: func(_ SQLHealthCheckResult, tags []string) {
						errorCount++
						assert.Contains(t, tags, tc.wantTag)
					},
					CachedFallback: func(_ SQLFallback, tags []string) {
						fallbackCount++
						assert.Contains(t, tags, tc.wantTag)
					},
				},
			}
			w.completeSQLHealthCheck(&sqlHealthTarget{poolHealth: health}, 0, time.Now(), []string{ip}, map[string]struct{}{ip: {}},
				SQLHealthCheckOutcome{Results: []SQLHealthCheckResult{{IP: ip, Err: tc.err}}})
			assert.Equal(t, 1, iterationCount)
			if tc.result == SQLRoutingResultConclusive {
				assert.Equal(t, 1, errorCount)
			} else {
				assert.Zero(t, errorCount)
			}
			if tc.result == SQLRoutingResultCachedFallback {
				assert.Equal(t, 1, fallbackCount)
			} else {
				assert.Zero(t, fallbackCount)
			}
		})
	}
}

func TestPatroniPublishesNewLeaderWhileSQLUsesCachedFallback(t *testing.T) {
	const oldPrimary, newPrimary = "10.0.0.1", "10.0.0.2"
	patroniProber := newMockProber(
		map[string][]string{"primary.local": {oldPrimary, newPrimary}},
		oldPrimary,
	)
	w := newTestWorker(patroniProber)
	sqlProber := &credentialSQLHealthProber{
		responses: map[string]fakeSQLHealthProbeResponse{
			"timeout": {err: context.DeadlineExceeded},
		},
		started:   map[string]chan struct{}{},
		release:   map[string]chan struct{}{},
		startOnce: map[string]*sync.Once{},
	}
	w.newSQLChecker = func(context.Context) *SQLHealthChecker {
		return newTestSQLHealthChecker(t, sqlProber, 1, SQLHealthCheckHooks{})
	}
	subscription := w.Subscribe("primary.local", false, "db", nil, &SQLHealthConfig{
		ConnStringForIP: func(string) string { return "timeout" },
		RoutingEnabled:  func(context.Context) SQLHealthFeatureFlagEvaluation { return validSQLHealthFeatureFlag(true) },
	})
	health := receivePoolHealth(t, subscription)
	t.Cleanup(func() { w.Unsubscribe(subscription) })

	var comparisons []LeaderRoutingComparison
	w.sqlHooks.LeaderRoutingCompared = func(c LeaderRoutingComparison, _ []string) { comparisons = append(comparisons, c) }
	w.RefreshSQL()
	require.True(t, health.IsHealthy(oldPrimary))
	require.False(t, health.sqlRoutingActive, "timeout fallback must not suppress Patroni publication")

	w.applyPatroniHealth(
		health,
		map[string]bool{oldPrimary: false, newPrimary: true},
		nil,
		[]string{oldPrimary, newPrimary},
		time.Now(),
	)
	assert.False(t, health.IsHealthy(oldPrimary))
	assert.True(t, health.IsHealthy(newPrimary), "new Patroni leader must publish during SQL fallback")

	require.Len(t, comparisons, 1, "Patroni-owned hybrid routing must be timed on publication, without waiting for SQL")
	assert.Negative(t, comparisons[0].Improvement, "publication follows the Patroni observation")
	assert.Equal(t, SQLRoutingResultCachedFallback, comparisons[0].HybridSource)

	// A subsequent timeout must copy the latest Patroni snapshot rather than restoring the old
	// primary retained by the earlier fallback decision.
	w.RefreshSQL()
	assert.False(t, health.IsHealthy(oldPrimary))
	assert.True(t, health.IsHealthy(newPrimary))
}

func TestHybridRoutingPreservesPatroniWhenSQLDiscoveryHasNoCandidates(t *testing.T) {
	const ip = "10.0.0.1"
	patroniProber := newMockProber(map[string][]string{"primary.local": {ip}}, ip)
	w := newTestWorker(patroniProber)
	w.newSQLChecker = func(context.Context) *SQLHealthChecker {
		return newTestSQLHealthChecker(t, &fakeSQLHealthProber{}, 1, SQLHealthCheckHooks{})
	}
	subscription := w.Subscribe("primary.local", false, "db", nil, &SQLHealthConfig{
		ConnStringForIP: func(string) string { return "conn" },
		RoutingEnabled:  func(context.Context) SQLHealthFeatureFlagEvaluation { return validSQLHealthFeatureFlag(true) },
	})
	health := receivePoolHealth(t, subscription)
	t.Cleanup(func() { w.Unsubscribe(subscription) })
	require.True(t, health.IsHealthy(ip))

	patroniProber.mu.Lock()
	patroniProber.ipsByHost["primary.local"] = nil
	patroniProber.mu.Unlock()
	w.RefreshSQL()

	assert.True(t, health.IsHealthy(ip), "empty SQL discovery must preserve the Patroni route")
	assert.ElementsMatch(t, []string{ip}, health.Snapshot().Resolved)
}

func TestEmptySQLDiscoveryClearsRoutingBans(t *testing.T) {
	const a, b = "10.0.0.1", "10.0.0.2"
	for _, fallback := range []bool{false, true} {
		t.Run(fmt.Sprintf("cached_fallback_%t", fallback), func(t *testing.T) {
			prober := newMockProber(map[string][]string{"primary.local": {a, b}}, a)
			w := newTestWorker(prober)
			other := fakeSQLHealthProbeResponse{inRecovery: true}
			if fallback {
				other.err = context.DeadlineExceeded
			}
			sqlProber := &credentialSQLHealthProber{responses: map[string]fakeSQLHealthProbeResponse{
				a: {err: &pgconn.PgError{Code: "57P02"}}, b: other,
			}}
			w.newSQLChecker = func(context.Context) *SQLHealthChecker {
				return newTestSQLHealthChecker(t, sqlProber, 1, SQLHealthCheckHooks{})
			}
			updates := w.Subscribe("primary.local", false, "db", nil, &SQLHealthConfig{
				ConnStringForIP: func(ip string) string { return ip },
				RoutingEnabled:  func(context.Context) SQLHealthFeatureFlagEvaluation { return validSQLHealthFeatureFlag(true) },
			})
			health := receivePoolHealth(t, updates)
			t.Cleanup(func() { w.Unsubscribe(updates) })
			var changes []SQLRoutingBanChange
			w.sqlHooks.RoutingBanChanged = func(change SQLRoutingBanChange, _ []string) {
				if assert.True(t, health.sqlComparisonMu.TryLock(), "hooks must run outside snapshot locks") {
					health.sqlComparisonMu.Unlock()
				}
				if assert.True(t, health.routingMu.TryLock(), "hooks must run outside routing locks") {
					health.routingMu.Unlock()
				}
				changes = append(changes, change)
			}
			w.RefreshSQL()
			require.Contains(t, health.hybridPolicy.routingBannedIPs, a)
			require.Empty(t, health.sqlHealthState.lastKnownHealthy, "SQL has never confirmed a primary")
			require.Equal(t, !fallback, health.sqlRoutingActive)
			prober.mu.Lock()
			prober.ipsByHost["primary.local"] = nil
			prober.mu.Unlock()
			for range maxFailedHostLookups - 1 {
				w.RefreshSQL()
				require.Contains(t, health.hybridPolicy.routingBannedIPs, a, "raw DNS misses must not clear retained bans")
			}
			w.RefreshSQL()
			require.Empty(t, health.sqlHealthState.Snapshot().Resolved)
			require.Empty(t, health.hybridPolicy.routingBannedIPs)
			assert.False(t, health.sqlRoutingActive)
			assert.True(t, health.IsHealthy(a), "clearing the final ban must restore cached Patroni immediately")
			assert.Equal(t, []SQLRoutingBanChange{
				{IP: a, Banned: true, Reason: SQLRoutingBanReasonPostgresShutdown},
				{IP: a, Reason: SQLRoutingUnbanReasonDNSRemoved},
			}, changes)
			w.RefreshSQL()
			assert.Len(t, changes, 2, "empty discovery must not repeat removal events")
			w.applyPatroniHealth(health, map[string]bool{a: true, b: false}, nil, []string{a, b}, time.Now())
			assert.True(t, health.IsHealthy(a), "an obsolete ban must not filter later Patroni updates")
		})
	}
}

func TestHybridRoutingRestoresPatroniWhenAllSQLCandidatesAreEvicted(t *testing.T) {
	const primary, standby = "10.0.0.1", "10.0.0.2"
	patroniProber := newMockProber(map[string][]string{"primary.local": {primary, standby}}, primary)
	w := newTestWorker(patroniProber)
	sqlProber := &credentialSQLHealthProber{
		responses: map[string]fakeSQLHealthProbeResponse{
			primary: {inRecovery: true},
			standby: {inRecovery: true},
		},
	}
	w.newSQLChecker = func(context.Context) *SQLHealthChecker {
		return newTestSQLHealthChecker(t, sqlProber, 1, SQLHealthCheckHooks{})
	}
	subscription := w.Subscribe("primary.local", false, "db", nil, &SQLHealthConfig{
		ConnStringForIP: func(ip string) string { return ip },
		RoutingEnabled:  func(context.Context) SQLHealthFeatureFlagEvaluation { return validSQLHealthFeatureFlag(true) },
	})
	health := receivePoolHealth(t, subscription)
	t.Cleanup(func() { w.Unsubscribe(subscription) })

	// An all-standby SQL result owns routing but protects no IP from DNS eviction.
	w.RefreshSQL()
	require.True(t, health.sqlRoutingActive)
	require.False(t, health.IsHealthy(primary))
	require.False(t, health.IsHealthy(standby))

	var comparisons []LeaderRoutingComparison
	w.sqlHooks.LeaderRoutingCompared = func(c LeaderRoutingComparison, _ []string) {
		if assert.True(t, health.sqlComparisonMu.TryLock(), "comparison hooks must run outside the SQL snapshot lock") {
			health.sqlComparisonMu.Unlock()
		}
		if assert.True(t, health.routingMu.TryLock(), "comparison hooks must run outside the routing lock") {
			health.routingMu.Unlock()
		}
		comparisons = append(comparisons, c)
	}
	// A newer Patroni leader recovers the all-standby route immediately.
	patroniObservedAt := time.Now()
	w.applyPatroniHealth(health, map[string]bool{primary: false, standby: true}, nil, []string{primary, standby}, patroniObservedAt)
	require.True(t, health.IsHealthy(standby))
	require.Len(t, comparisons, 1)
	assert.Equal(t, SQLRoutingResultPatroniRecovery, comparisons[0].HybridSource)
	assert.Negative(t, comparisons[0].Improvement, "publication follows the Patroni observation")

	patroniProber.mu.Lock()
	patroniProber.ipsByHost["primary.local"] = nil
	patroniProber.mu.Unlock()
	for range maxFailedHostLookups - 1 {
		w.RefreshSQL()
		require.True(t, health.sqlRoutingActive, "empty DNS must not restore Patroni while SQL candidates are retained")
		require.False(t, health.IsHealthy(primary))
	}
	w.RefreshSQL()
	require.Empty(t, health.sqlHealthState.Snapshot().Resolved)
	require.False(t, health.sqlRoutingActive, "evicting every SQL candidate must restore Patroni ownership")
	require.True(t, health.IsHealthy(standby))
	assert.ElementsMatch(t, []string{primary, standby}, health.Snapshot().Resolved)
	assert.False(t, health.sqlRoutingDisabled, "empty discovery must not latch the routing kill switch")
	require.Len(t, comparisons, 1, "restoring the same leader must not repeat its recovery comparison")
	assert.Equal(t, standby, comparisons[0].NewLeaderIP)

	// Empty SQL rounds and a later Patroni refresh must not repeat the comparison.
	w.RefreshSQL()
	w.applyPatroniHealth(health, map[string]bool{primary: false, standby: true}, nil, []string{primary, standby}, time.Now())
	assert.Len(t, comparisons, 1)

	// Patroni continues publishing leader changes while SQL discovery is empty.
	w.applyPatroniHealth(health, map[string]bool{primary: true, standby: false}, nil, []string{primary, standby}, time.Now())
	require.True(t, health.IsHealthy(primary))
	require.False(t, health.IsHealthy(standby))

	// SQL can take ownership again after discovery recovers.
	patroniProber.mu.Lock()
	patroniProber.ipsByHost["primary.local"] = []string{primary, standby}
	patroniProber.mu.Unlock()
	sqlProber.mu.Lock()
	sqlProber.responses[standby] = fakeSQLHealthProbeResponse{inRecovery: false}
	sqlProber.mu.Unlock()
	w.RefreshSQL()
	assert.True(t, health.sqlRoutingActive)
	assert.False(t, health.IsHealthy(primary))
	assert.True(t, health.IsHealthy(standby))
}

func TestSQLRoutingDoesNotReplacePatroniDiscoveryState(t *testing.T) {
	const (
		patroniPrimary = "10.0.0.1"
		standby        = "10.0.0.2"
	)
	patroniProber := newMockProber(
		map[string][]string{"primary.local": {patroniPrimary, standby}},
		patroniPrimary,
	)
	w := newTestWorker(patroniProber)
	sqlProber := &credentialSQLHealthProber{
		responses: map[string]fakeSQLHealthProbeResponse{
			standby: {inRecovery: true},
		},
	}
	w.newSQLChecker = func(context.Context) *SQLHealthChecker {
		return newTestSQLHealthChecker(t, sqlProber, 1, SQLHealthCheckHooks{})
	}
	subscription := w.Subscribe("primary.local", false, "db", nil, &SQLHealthConfig{
		ConnStringForIP: func(ip string) string { return ip },
		RoutingEnabled:  func(context.Context) SQLHealthFeatureFlagEvaluation { return validSQLHealthFeatureFlag(true) },
	})
	health := receivePoolHealth(t, subscription)
	t.Cleanup(func() { w.Unsubscribe(subscription) })
	require.True(t, health.IsHealthy(patroniPrimary))

	// SQL's first discovery sees only the standby and publishes a B-only routing snapshot.
	patroniProber.mu.Lock()
	patroniProber.ipsByHost["primary.local"] = []string{standby}
	patroniProber.mu.Unlock()
	w.RefreshSQL()
	require.ElementsMatch(t, []string{standby}, health.Snapshot().Resolved)
	require.False(t, health.IsHealthy(patroniPrimary))
	assert.ElementsMatch(t, []string{patroniPrimary, standby}, health.patroniHealthState.Snapshot().Resolved)

	// Patroni must continue probing its independently retained primary even though SQL removed it
	// from the client-visible routing snapshot.
	var patroniProbeCalls []string
	patroniProber.mu.Lock()
	patroniProber.recordCall = func(ip, _ string, _ string, _ bool, _ []string) {
		patroniProbeCalls = append(patroniProbeCalls, ip)
	}
	patroniProber.mu.Unlock()
	w.Refresh()
	assert.ElementsMatch(t, []string{patroniPrimary, standby}, patroniProbeCalls)

	// A later SQL timeout falls back to the Patroni snapshot, which still contains A.
	sqlProber.mu.Lock()
	sqlProber.responses[standby] = fakeSQLHealthProbeResponse{err: context.DeadlineExceeded}
	sqlProber.mu.Unlock()
	w.RefreshSQL()
	assert.True(t, health.IsHealthy(patroniPrimary))
	assert.ElementsMatch(t, []string{patroniPrimary, standby}, health.Snapshot().Resolved)
}

func TestSQLChecksDisableRoutingUntilHostStateIsRecreated(t *testing.T) {
	const patroniPrimary, sqlPrimary = "10.0.0.1", "10.0.0.2"
	patroniProber := newMockProber(
		map[string][]string{"primary.local": {patroniPrimary, sqlPrimary}},
		patroniPrimary,
	)
	w := newTestWorker(patroniProber)
	sqlProber := &credentialSQLHealthProber{
		responses: map[string]fakeSQLHealthProbeResponse{
			patroniPrimary: {inRecovery: true},
			sqlPrimary:     {inRecovery: false},
		},
		started:   map[string]chan struct{}{},
		release:   map[string]chan struct{}{},
		startOnce: map[string]*sync.Once{},
	}
	w.newSQLChecker = func(context.Context) *SQLHealthChecker {
		return newTestSQLHealthChecker(t, sqlProber, 1, SQLHealthCheckHooks{})
	}
	var enabled atomic.Bool
	enabled.Store(true)
	subscription := w.Subscribe("primary.local", false, "db", nil, &SQLHealthConfig{
		ConnStringForIP: func(ip string) string { return ip },
		ChecksEnabled:   func(context.Context) SQLHealthFeatureFlagEvaluation { return validSQLHealthFeatureFlag(enabled.Load()) },
		RoutingEnabled:  func(context.Context) SQLHealthFeatureFlagEvaluation { return validSQLHealthFeatureFlag(true) },
	})
	health := receivePoolHealth(t, subscription)
	w.RefreshSQL()
	require.False(t, health.IsHealthy(patroniPrimary), "complete SQL result should become authoritative")
	require.True(t, health.IsHealthy(sqlPrimary))

	probeStarted := make(chan struct{})
	releaseProbe := make(chan struct{})
	var releaseOnce sync.Once
	sqlProber.mu.Lock()
	sqlProber.started[sqlPrimary] = probeStarted
	sqlProber.release[sqlPrimary] = releaseProbe
	sqlProber.startOnce[sqlPrimary] = &sync.Once{}
	sqlProber.mu.Unlock()
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(releaseProbe) })
		w.Unsubscribe(subscription)
	})

	refreshDone := make(chan struct{})
	go func() {
		w.RefreshSQL()
		close(refreshDone)
	}()
	select {
	case <-probeStarted:
	case <-time.After(time.Second):
		t.Fatal("SQL health check did not block")
	}

	enabled.Store(false)
	w.checkSQLHealthGroup(
		sqlHealthGroup{poolHealth: health, targets: health.sqlTargetValues()},
		time.Now(),
	)
	assert.True(t, health.IsHealthy(patroniPrimary), "disabling checks must immediately restore cached Patroni")

	enabled.Store(true)
	releaseOnce.Do(func() { close(releaseProbe) })
	select {
	case <-refreshDone:
	case <-time.After(time.Second):
		t.Fatal("blocked SQL health check did not finish")
	}
	assert.True(t, health.IsHealthy(patroniPrimary), "the pre-disable round must not reactivate SQL routing")

	w.RefreshSQL()
	assert.True(t, health.IsHealthy(patroniPrimary), "re-enabling checks must not reactivate SQL routing without recreating host state")
}

func TestSQLRoutingFlagEvaluationFailureDoesNotLatchDisable(t *testing.T) {
	const patroniPrimary, sqlPrimary = "10.0.0.1", "10.0.0.2"
	patroniProber := newMockProber(
		map[string][]string{"primary.local": {patroniPrimary, sqlPrimary}},
		patroniPrimary,
	)
	w := newTestWorker(patroniProber)
	sqlProber := &fakeSQLHealthProber{
		responses: map[string][]fakeSQLHealthProbeResponse{
			patroniPrimary: {{inRecovery: true}},
			sqlPrimary:     {{inRecovery: false}},
		},
		calls: make(map[string]int),
	}
	w.newSQLChecker = func(context.Context) *SQLHealthChecker {
		return newTestSQLHealthChecker(t, sqlProber, 1, SQLHealthCheckHooks{})
	}
	var evaluation atomic.Value
	evaluation.Store(SQLHealthFeatureFlagEvaluation{})
	subscription := w.Subscribe("primary.local", false, "db", nil, &SQLHealthConfig{
		ConnStringForIP: func(ip string) string { return ip },
		RoutingEnabled: func(context.Context) SQLHealthFeatureFlagEvaluation {
			return evaluation.Load().(SQLHealthFeatureFlagEvaluation)
		},
	})
	health := receivePoolHealth(t, subscription)
	t.Cleanup(func() { w.Unsubscribe(subscription) })

	w.RefreshSQL()
	assert.True(t, health.IsHealthy(patroniPrimary), "an initial evaluation error must preserve Patroni routing")

	evaluation.Store(validSQLHealthFeatureFlag(true))
	w.RefreshSQL()
	assert.True(t, health.IsHealthy(patroniPrimary), "a poll scheduled before the mode transition must not publish")
	w.RefreshSQL()
	assert.False(t, health.IsHealthy(patroniPrimary), "a fresh poll may enable SQL after an earlier error")
	assert.True(t, health.IsHealthy(sqlPrimary))

	evaluation.Store(SQLHealthFeatureFlagEvaluation{Enabled: true, Valid: false})
	w.RefreshSQL()
	assert.True(t, health.IsHealthy(sqlPrimary), "a later evaluation error must preserve the last-known enabled state")

	evaluation.Store(validSQLHealthFeatureFlag(false))
	w.RefreshSQL()
	assert.True(t, health.IsHealthy(patroniPrimary), "an intentional disable must restore Patroni")

	evaluation.Store(validSQLHealthFeatureFlag(true))
	w.RefreshSQL()
	assert.True(t, health.IsHealthy(patroniPrimary), "the intentional disable remains sticky until host recreation")
}

func TestSQLRoutingFlagDisableRejectsBlockedRoundAndStaysDisabled(t *testing.T) {
	const patroniPrimary, sqlPrimary = "10.0.0.1", "10.0.0.2"
	patroniProber := newMockProber(
		map[string][]string{"primary.local": {patroniPrimary, sqlPrimary}},
		patroniPrimary,
	)
	w := newTestWorker(patroniProber)
	sqlProber := &credentialSQLHealthProber{
		responses: map[string]fakeSQLHealthProbeResponse{
			patroniPrimary: {inRecovery: true},
			sqlPrimary:     {inRecovery: false},
		},
		started:   map[string]chan struct{}{},
		release:   map[string]chan struct{}{},
		startOnce: map[string]*sync.Once{},
	}
	w.newSQLChecker = func(context.Context) *SQLHealthChecker {
		return newTestSQLHealthChecker(t, sqlProber, 1, SQLHealthCheckHooks{})
	}
	var routingEnabled atomic.Bool
	routingEnabled.Store(true)
	subscription := w.Subscribe("primary.local", false, "db", nil, &SQLHealthConfig{
		ConnStringForIP: func(ip string) string { return ip },
		RoutingEnabled: func(context.Context) SQLHealthFeatureFlagEvaluation {
			return validSQLHealthFeatureFlag(routingEnabled.Load())
		},
	})
	health := receivePoolHealth(t, subscription)
	t.Cleanup(func() { w.Unsubscribe(subscription) })
	w.RefreshSQL()
	require.False(t, health.IsHealthy(patroniPrimary), "SQL should be authoritative before the transition")
	require.True(t, health.IsHealthy(sqlPrimary))

	blocked := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	sqlProber.mu.Lock()
	sqlProber.started[sqlPrimary] = blocked
	sqlProber.release[sqlPrimary] = release
	sqlProber.startOnce[sqlPrimary] = &sync.Once{}
	sqlProber.mu.Unlock()

	refreshDone := make(chan struct{})
	go func() {
		w.RefreshSQL()
		close(refreshDone)
	}()
	select {
	case <-blocked:
	case <-time.After(time.Second):
		t.Fatal("SQL health check did not block")
	}
	targets := health.sqlTargetValues()

	routingEnabled.Store(false)
	w.checkSQLHealthGroup(sqlHealthGroup{poolHealth: health, targets: targets}, time.Now())
	assert.True(t, health.IsHealthy(patroniPrimary), "disabling routing must restore cached Patroni immediately")

	routingEnabled.Store(true)
	w.checkSQLHealthGroup(sqlHealthGroup{poolHealth: health, targets: targets}, time.Now())
	assert.True(t, health.IsHealthy(patroniPrimary), "a missed re-enabled round must preserve Patroni routing")

	releaseOnce.Do(func() { close(release) })
	select {
	case <-refreshDone:
	case <-time.After(time.Second):
		t.Fatal("blocked SQL health check did not finish")
	}
	assert.True(t, health.IsHealthy(patroniPrimary), "the pre-transition round must not reactivate SQL routing")

	w.RefreshSQL()
	assert.True(t, health.IsHealthy(patroniPrimary), "a fresh round cannot reactivate SQL routing until host state is recreated")
}

func TestRemovingLastSQLTargetRestoresPatroniRouting(t *testing.T) {
	const patroniPrimary, sqlPrimary = "10.0.0.1", "10.0.0.2"
	patroniProber := newMockProber(
		map[string][]string{"primary.local": {patroniPrimary, sqlPrimary}},
		patroniPrimary,
	)
	w := newTestWorker(patroniProber)
	w.hooks.SQLPotentialCompared = func(SQLPotentialComparison, []string) {}
	sqlProber := &fakeSQLHealthProber{
		responses: map[string][]fakeSQLHealthProbeResponse{
			patroniPrimary: {{inRecovery: true}},
			sqlPrimary:     {{inRecovery: false}},
		},
		calls: make(map[string]int),
	}
	w.newSQLChecker = func(context.Context) *SQLHealthChecker {
		return newTestSQLHealthChecker(t, sqlProber, 1, SQLHealthCheckHooks{})
	}
	patroniOnly := w.Subscribe("primary.local", false, "db", nil, nil)
	health := receivePoolHealth(t, patroniOnly)
	sqlSubscription := w.Subscribe("primary.local", false, "db", nil, &SQLHealthConfig{
		ConnStringForIP: func(ip string) string { return ip },
		RoutingEnabled:  func(context.Context) SQLHealthFeatureFlagEvaluation { return validSQLHealthFeatureFlag(true) },
	})
	require.Same(t, health, receivePoolHealth(t, sqlSubscription))
	t.Cleanup(func() { w.Unsubscribe(patroniOnly) })

	w.RefreshSQL()
	require.False(t, health.IsHealthy(patroniPrimary), "SQL should be authoritative before its target is removed")
	require.True(t, health.IsHealthy(sqlPrimary))
	require.NotEmpty(t, health.potentialTiming.leaderComparisons.sqlLeaders)
	w.Unsubscribe(sqlSubscription)
	assert.Empty(t, health.potentialTiming.leaderComparisons.sqlLeaders, "retirement must clear snapshot timing evidence")
	assert.True(t, health.potentialTiming.waitingForSQLSnapshot, "timing must wait for fresh SQL after retirement")
	assert.True(t, health.IsHealthy(patroniPrimary), "the remaining Patroni-only subscription must not keep a stale SQL route")
}

type credentialSQLHealthProber struct {
	mu        sync.Mutex
	responses map[string]fakeSQLHealthProbeResponse
	started   map[string]chan struct{}
	release   map[string]chan struct{}
	startOnce map[string]*sync.Once
	calls     map[string]int
}

func (p *credentialSQLHealthProber) Probe(
	_ context.Context,
	_ string,
	connString string,
	_ []string,
) (bool, error) {
	p.mu.Lock()
	if p.calls == nil {
		p.calls = make(map[string]int)
	}
	p.calls[connString]++
	response := p.responses[connString]
	started := p.started[connString]
	release := p.release[connString]
	once := p.startOnce[connString]
	p.mu.Unlock()
	if started != nil {
		once.Do(func() { close(started) })
	}
	if release != nil {
		<-release
	}
	return response.inRecovery, response.err
}

func (p *credentialSQLHealthProber) callCount(connString string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls[connString]
}

func TestSQLRoutingWaitsForEveryIPDespiteEarlyWritableAvailability(t *testing.T) {
	const oldIP, newIP = "10.0.0.1", "10.0.0.2"
	p := newMockProber(map[string][]string{"primary.local": {oldIP, newIP}}, oldIP)
	w := newTestWorker(p)
	probe := &partiallyBlockingSQLHealthProber{
		blockingIP: oldIP, blockingRecovery: true,
		blocked: make(chan struct{}), release: make(chan struct{}),
	}
	w.newSQLChecker = func(context.Context) *SQLHealthChecker {
		return newTestSQLHealthChecker(t, probe, 1, SQLHealthCheckHooks{})
	}
	updates := w.Subscribe("primary.local", false, "db", nil, &SQLHealthConfig{
		ConnStringForIP: func(ip string) string { return ip },
		RoutingEnabled:  func(context.Context) SQLHealthFeatureFlagEvaluation { return validSQLHealthFeatureFlag(true) },
	})
	health := receivePoolHealth(t, updates)
	availability, err := health.SubscribeSQLHealth(updates)
	require.NoError(t, err)
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(probe.release) }); availability.Close(); w.Unsubscribe(updates) })
	done := make(chan struct{})
	go func() { w.RefreshSQL(); close(done) }()
	observation := receiveSQLHealthObservation(t, availability)
	require.True(t, observation.Writable, "availability can publish before all IPs finish")
	assert.True(t, health.IsHealthy(oldIP), "the incomplete SQL poll must preserve Patroni routing")
	assert.False(t, health.IsHealthy(newIP))
	release.Do(func() { close(probe.release) })
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("SQL poll did not finish")
	}
	assert.False(t, health.IsHealthy(oldIP))
	assert.True(t, health.IsHealthy(newIP), "the complete poll may publish SQL")
	// Patroni keeps its independent leader and cannot overwrite conclusive SQL.
	var comparison SQLPatroniHealthSnapshot
	w.hooks.SQLPatroniCompared = func(snapshot SQLPatroniHealthSnapshot, _ []string) { comparison = snapshot }
	w.Refresh()
	assert.True(t, health.IsHealthy(newIP))
	w.RefreshSQL()
	assert.Equal(t, map[string]bool{oldIP: true, newIP: false}, comparison.PatroniHealth)
	assert.False(t, comparison.Matches(), "comparison must use Patroni's own snapshot, not SQL-owned client routing")
}

func TestPatroniPromotionDuringFallbackTelemetryCannotBeOverwritten(t *testing.T) {
	const oldIP, newIP = "10.0.0.1", "10.0.0.2"
	w := newTestWorker(newMockProber(map[string][]string{"primary.local": {oldIP, newIP}}, oldIP))
	w.newSQLChecker = func(context.Context) *SQLHealthChecker {
		return newTestSQLHealthChecker(t, &fakeSQLHealthProber{responses: map[string][]fakeSQLHealthProbeResponse{
			oldIP: {{err: context.DeadlineExceeded}}, newIP: {{inRecovery: true}},
		}, calls: make(map[string]int)}, 1, SQLHealthCheckHooks{})
	}
	updates := w.Subscribe("primary.local", false, "db", nil, &SQLHealthConfig{
		ConnStringForIP: func(ip string) string { return ip },
		RoutingEnabled:  func(context.Context) SQLHealthFeatureFlagEvaluation { return validSQLHealthFeatureFlag(true) },
	})
	health := receivePoolHealth(t, updates)
	entered, resume, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(resume) }); w.Unsubscribe(updates) })
	w.sqlHooks.CachedFallback = func(SQLFallback, []string) { close(entered); <-resume }
	go func() { w.RefreshSQL(); close(done) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("fallback was not reported")
	}
	assert.True(t, health.IsHealthy(oldIP))
	w.applyPatroniHealth(health, map[string]bool{oldIP: false, newIP: true}, nil, []string{oldIP, newIP}, time.Now())
	assert.True(t, health.IsHealthy(newIP), "hooks must not block Patroni publication")
	release.Do(func() { close(resume) })
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("SQL refresh did not finish")
	}
	assert.True(t, health.IsHealthy(newIP), "the earlier fallback must not overwrite Patroni after its hook returns")
	assert.False(t, health.IsHealthy(oldIP))
}

func TestRoutingDisableSurvivesSQLTargetRecreation(t *testing.T) {
	const patroniIP, sqlIP = "10.0.0.1", "10.0.0.2"
	w := newTestWorker(newMockProber(map[string][]string{"primary.local": {patroniIP, sqlIP}}, patroniIP))
	w.newSQLChecker = func(context.Context) *SQLHealthChecker {
		return newTestSQLHealthChecker(t, &fakeSQLHealthProber{
			responses: map[string][]fakeSQLHealthProbeResponse{patroniIP: {{inRecovery: true}}, sqlIP: {{}}}, calls: make(map[string]int),
		}, 1, SQLHealthCheckHooks{})
	}
	patroniOnly := w.Subscribe("primary.local", false, "db", nil, nil)
	health := receivePoolHealth(t, patroniOnly)
	cfg := &SQLHealthConfig{ConnStringForIP: func(ip string) string { return ip }, RoutingEnabled: func(context.Context) SQLHealthFeatureFlagEvaluation { return validSQLHealthFeatureFlag(true) }}
	sql := w.Subscribe("primary.local", false, "db", nil, cfg)
	w.RefreshSQL()
	require.True(t, health.IsHealthy(sqlIP))
	w.Unsubscribe(sql)
	require.True(t, health.IsHealthy(patroniIP))
	replacement := w.Subscribe("primary.local", false, "db", nil, cfg)
	t.Cleanup(func() { w.Unsubscribe(replacement); w.Unsubscribe(patroniOnly) })
	w.RefreshSQL()
	assert.True(t, health.IsHealthy(patroniIP), "recreating a target must not reset the host's latched disable")
	assert.False(t, health.IsHealthy(sqlIP))
}

func TestDisableRestoresRawPatroniFromFilteredFallback(t *testing.T) {
	for _, removeLastTarget := range []bool{false, true} {
		t.Run(fmt.Sprintf("remove_last_target_%t", removeLastTarget), func(t *testing.T) {
			const a, b = "10.0.0.1", "10.0.0.2"
			w := newTestWorker(newMockProber(map[string][]string{"primary.local": {a, b}}, a))
			w.newSQLChecker = func(context.Context) *SQLHealthChecker {
				return newTestSQLHealthChecker(t, &fakeSQLHealthProber{
					responses: map[string][]fakeSQLHealthProbeResponse{
						a: {{inRecovery: true}, {err: context.DeadlineExceeded}},
						b: {{inRecovery: false}, {inRecovery: true}},
					},
					calls: make(map[string]int),
				}, 1, SQLHealthCheckHooks{})
			}
			patroniUpdates := w.Subscribe("primary.local", false, "db", nil, nil)
			health := receivePoolHealth(t, patroniUpdates)
			t.Cleanup(func() { w.Unsubscribe(patroniUpdates) })
			var enabled atomic.Bool
			enabled.Store(true)
			sqlUpdates := w.Subscribe("primary.local", false, "db", nil, &SQLHealthConfig{
				ConnStringForIP: func(ip string) string { return ip },
				RoutingEnabled: func(context.Context) SQLHealthFeatureFlagEvaluation {
					return validSQLHealthFeatureFlag(enabled.Load())
				},
			})
			t.Cleanup(func() { w.Unsubscribe(sqlUpdates) })
			w.RefreshSQL()
			require.True(t, health.IsHealthy(b))
			w.RefreshSQL()
			require.False(t, health.sqlRoutingActive)
			require.Empty(t, healthyIPs(health.Snapshot().Health))
			require.True(t, health.patroniHealthState.IsHealthy(a))
			receivePoolHealth(t, patroniUpdates)

			if removeLastTarget {
				w.Unsubscribe(sqlUpdates)
			} else {
				enabled.Store(false)
				w.syncSQLRoutingKillSwitch(health)
			}
			require.True(t, health.IsHealthy(a), "disable must restore raw Patroni without another probe")
			assert.False(t, health.IsHealthy(b))
			assert.False(t, health.hybridPolicy.isRoutingEnabled())
			require.Same(t, health, receivePoolHealth(t, patroniUpdates))
			w.syncSQLRoutingKillSwitch(health)
			select {
			case <-patroniUpdates:
				t.Fatal("an already-disabled policy must not republish routing")
			default:
			}
		})
	}
}

func TestAggregatedSQLDecisionsControlPublishedRouting(t *testing.T) {
	const a, b = "10.0.0.1", "10.0.0.2"
	for _, tc := range []struct {
		name           string
		results, other []SQLHealthCheckResult
		wantPrimary    string
		wantSQL        bool
	}{
		{name: "unique primary", results: []SQLHealthCheckResult{{IP: a, InRecovery: true}, {IP: b}}, wantPrimary: b, wantSQL: true},
		{name: "determinate no primary", results: []SQLHealthCheckResult{{IP: a, InRecovery: true}, {IP: b, InRecovery: true}}, wantSQL: true},
		{name: "unknown other IP", results: []SQLHealthCheckResult{{IP: a, Err: context.DeadlineExceeded}, {IP: b}}, wantPrimary: a},
		{name: "multiple primaries", results: []SQLHealthCheckResult{{IP: a}, {IP: b}}, wantPrimary: a},
		{name: "role resolves same IP timeout", results: []SQLHealthCheckResult{{IP: a, InRecovery: true}, {IP: b}}, other: []SQLHealthCheckResult{{IP: b, Err: context.DeadlineExceeded}}, wantPrimary: b, wantSQL: true},
		{name: "conflicting same IP roles", results: []SQLHealthCheckResult{{IP: a, InRecovery: true}, {IP: b}}, other: []SQLHealthCheckResult{{IP: b, InRecovery: true}}, wantPrimary: a},
	} {
		t.Run(tc.name, func(t *testing.T) {
			health := newTestPoolHealth(a, b)
			health.sqlHealthState = NewHealthState()
			health.sqlHealthState.Track([]string{a, b})
			health.hybridPolicy = newHybridHealthPolicy(true)
			health.routingEnabledFn = func(context.Context) SQLHealthFeatureFlagEvaluation { return validSQLHealthFeatureFlag(true) }
			w := newTestWorker(nil)
			w.applyPatroniHealth(health, map[string]bool{a: true, b: false}, nil, []string{a, b}, time.Now())
			now := time.Now()
			if len(tc.other) > 0 {
				health.recordSQLComparisonPoll(false, &sqlHealthTarget{poolHealth: health}, 0, now, now, []string{a, b}, SQLHealthCheckOutcome{Results: tc.other})
			}
			w.completeSQLHealthCheck(&sqlHealthTarget{poolHealth: health}, 0, now, []string{a, b}, map[string]struct{}{a: {}, b: {}}, SQLHealthCheckOutcome{Results: tc.results})
			assert.Equal(t, tc.wantSQL, health.sqlRoutingActive)
			assert.Equal(t, tc.wantPrimary == a, health.IsHealthy(a))
			assert.Equal(t, tc.wantPrimary == b, health.IsHealthy(b))
			assert.ElementsMatch(t, []string{a, b}, health.Snapshot().Resolved)
		})
	}
}

func TestPublishedRoutingPreservesRetentionExclusionsAndPatroniRecovery(t *testing.T) {
	const a, b, c = "10.0.0.1", "10.0.0.2", "10.0.0.3"
	candidates := []string{a, b, c}
	health := newTestPoolHealth(candidates...)
	health.sqlHealthState = NewHealthState()
	health.sqlHealthState.Track(candidates)
	health.hybridPolicy = newHybridHealthPolicy(true)
	health.routingEnabledFn = func(context.Context) SQLHealthFeatureFlagEvaluation { return validSQLHealthFeatureFlag(true) }
	w := newTestWorker(nil)
	patroni := func(leader string) {
		w.applyPatroniHealth(health, map[string]bool{a: leader == a, b: leader == b, c: leader == c}, nil, candidates, time.Now())
	}
	target := &sqlHealthTarget{poolHealth: health}
	sql := func(results ...SQLHealthCheckResult) {
		now := time.Now()
		w.completeSQLHealthCheck(target, 0, now, candidates, map[string]struct{}{a: {}, b: {}, c: {}}, SQLHealthCheckOutcome{
			StartedAt: now, Results: results,
		})
	}

	patroni(a)
	sql(SQLHealthCheckResult{IP: a, InRecovery: true}, SQLHealthCheckResult{IP: b}, SQLHealthCheckResult{IP: c, InRecovery: true})
	require.True(t, health.IsHealthy(b))

	// Uncertainty retains SQL B and must keep stale Patroni A from taking ownership.
	sql(SQLHealthCheckResult{IP: a, InRecovery: true}, SQLHealthCheckResult{IP: b, Err: context.DeadlineExceeded}, SQLHealthCheckResult{IP: c, InRecovery: true})
	require.True(t, health.sqlRoutingActive)
	patroni(a)
	require.True(t, health.IsHealthy(b))
	require.False(t, health.IsHealthy(a))

	// Losing B falls back with A excluded, including on subsequent Patroni polls.
	sql(SQLHealthCheckResult{IP: a, InRecovery: true}, SQLHealthCheckResult{IP: b, InRecovery: true}, SQLHealthCheckResult{IP: c, Err: context.DeadlineExceeded})
	require.False(t, health.sqlRoutingActive)
	patroni(a)
	require.Empty(t, healthyIPs(health.Snapshot().Health))

	// A newer eligible Patroni C publishes immediately without another SQL completion.
	patroni(c)
	require.True(t, health.IsHealthy(c))
	require.False(t, health.IsHealthy(a))
	require.False(t, health.IsHealthy(b))
	patroni("")
	assert.Empty(t, healthyIPs(health.Snapshot().Health), "Patroni recovery must also publish loss of its leader")
}

func TestSQLComparisonPrunesEvictedResultsAndRejectsLateCompletion(t *testing.T) {
	for _, trackPotential := range []bool{false, true} {
		t.Run(fmt.Sprintf("timing=%t", trackPotential), func(t *testing.T) {
			s := &PoolHealth{sqlHealthState: NewHealthState()}
			s.refreshSQLResolved(trackPotential, []string{"A", "B"}, 2)
			first, second := &sqlHealthTarget{}, &sqlHealthTarget{}
			at := time.Now()
			candidates := []string{"A", "B"}
			results := []SQLHealthCheckResult{{IP: "A", InRecovery: true}, {IP: "B"}}
			var published *sqlHealthComparisonSnapshot
			for _, target := range []*sqlHealthTarget{first, second} {
				published = s.recordSQLComparisonPoll(trackPotential, target, 0, at, at, candidates, SQLHealthCheckOutcome{Results: results})
			}
			// A single missing DNS response must not bypass the normal retention threshold.
			require.ElementsMatch(t, candidates, s.refreshSQLResolved(trackPotential, []string{"A"}, 2))
			require.Len(t, s.sqlComparisonLatest[first].Results, 2)
			require.Equal(t, []string{"A"}, s.refreshSQLResolved(trackPotential, []string{"A"}, 2))
			for _, target := range []*sqlHealthTarget{first, second} {
				cached := s.sqlComparisonLatest[target]
				require.Equal(t, []string{"A"}, cached.CandidateIPs)
				require.Equal(t, results[:1], cached.Results)
				require.Equal(t, at, cached.ObservedAt)
			}
			// Snapshots already handed to hooks must remain independent of cache pruning.
			for _, snapshot := range published.results {
				require.Equal(t, results, snapshot.Results)
			}
			late := s.recordSQLComparisonPoll(trackPotential, first, 0, at, time.Now(), candidates, SQLHealthCheckOutcome{Results: results})
			require.NotContains(t, late.health, "B")
			require.Equal(t, results[:1], s.sqlComparisonLatest[first].Results)
			require.Equal(t, []string{"A"}, s.sqlComparisonLatest[first].CandidateIPs)
			require.Equal(t, []string{"A", "B"}, candidates, "filtering must not mutate caller-owned slices")
			require.Equal(t, []SQLHealthCheckResult{{IP: "A", InRecovery: true}, {IP: "B"}}, results)

			// Rediscovery alone cannot make the removed B result reappear in aggregation.
			s.refreshSQLResolved(trackPotential, candidates, 2)
			aOnly := s.recordSQLComparisonPoll(trackPotential, second, 0, at, time.Now(), []string{"A"}, SQLHealthCheckOutcome{Results: results[:1]})
			require.NotContains(t, aOnly.health, "B")
			fresh := s.recordSQLComparisonPoll(trackPotential, first, 0, time.Now(), time.Now(), candidates, SQLHealthCheckOutcome{Results: results})
			require.True(t, fresh.health["B"], "rediscovered IPs can supply fresh evidence")
		})
	}
}

func TestSQLComparisonIgnoresEvictedResultsButPreservesDiagnostics(t *testing.T) {
	oldIP, newIP := "10.0.0.1", "10.0.0.2"
	target := &sqlHealthTarget{}
	snapshot := buildSQLHealthComparisonSnapshot(map[string]struct{}{newIP: {}}, map[*sqlHealthTarget]SQLHealthResultsSnapshot{
		target: {CandidateIPs: []string{oldIP, newIP}, Results: []SQLHealthCheckResult{{IP: oldIP}, {IP: newIP}}},
	})
	assert.Equal(t, map[string]bool{newIP: true}, snapshot.health)
	assert.Equal(t, []string{newIP}, snapshot.retainedIPs)
	require.Len(t, snapshot.results, 1)
	assert.Len(t, snapshot.results[0].Results, 2)
	assert.False(t, SQLPatroniHealthSnapshot{
		SQLHealth: snapshot.health, SQLRetainedIPs: snapshot.retainedIPs,
		PatroniHealth: map[string]bool{oldIP: false, newIP: true},
	}.Matches())
}

func TestSQLRefreshRejectsFlagEvaluationFromBeforeDisable(t *testing.T) {
	const ip = "10.0.0.1"
	w := newTestWorker(newMockProber(map[string][]string{"primary.local": {ip}}, ip))
	prober := &fakeSQLHealthProber{
		responses: map[string][]fakeSQLHealthProbeResponse{"conn": {{}}},
		calls:     make(map[string]int),
	}
	checker := newTestSQLHealthChecker(t, prober, 1, SQLHealthCheckHooks{})
	w.newSQLChecker = func(context.Context) *SQLHealthChecker { return checker }
	var enabled atomic.Bool
	enabled.Store(true)
	var evaluations atomic.Int32
	flagRead := make(chan struct{})
	releaseFlag := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseFlag) }) }
	defer release()
	healthUpdates := w.Subscribe("primary.local", false, "mydb", nil, &SQLHealthConfig{
		ConnStringForIP: func(string) string { return "conn" },
		ChecksEnabled: func(context.Context) SQLHealthFeatureFlagEvaluation {
			value := enabled.Load()
			if evaluations.Add(1) == 1 {
				close(flagRead)
				<-releaseFlag
			}
			return validSQLHealthFeatureFlag(value)
		},
	})
	health := receivePoolHealth(t, healthUpdates)
	t.Cleanup(func() { w.Unsubscribe(healthUpdates) })
	sqlUpdates, err := health.SubscribeSQLHealth()
	require.NoError(t, err)
	t.Cleanup(sqlUpdates.Close)
	var comparisons, missed atomic.Int32
	w.hooks.SQLPatroniCompared = func(SQLPatroniHealthSnapshot, []string) { comparisons.Add(1) }
	w.hooks.SQLCheckMissed = func([]string) { missed.Add(1) }
	w.hooks.SQLPotentialCompared = func(SQLPotentialComparison, []string) { comparisons.Add(1) }
	group := w.sqlHealthGroups()[0]
	oldAt := time.Now()
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.runSQLHealthGroup(group, oldAt, true)
	}()
	select {
	case <-flagRead:
	case <-time.After(time.Second):
		t.Fatal("older tick did not evaluate the check flag")
	}
	enabled.Store(false)
	w.runSQLHealthGroup(group, oldAt.Add(time.Second), true)
	require.True(t, receiveSQLHealthObservation(t, sqlUpdates).Disabled)
	release()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("older tick did not finish")
	}
	assert.Zero(t, prober.callCount("conn"), "stale enabled flag must not start a SQL check")
	assert.Empty(t, health.sqlComparisonLatest, "stale tick must not restore SQL evidence")
	assert.Empty(t, health.potentialTiming.leaderComparisons.sqlLeaders, "stale tick must not restore timing evidence")
	assert.Zero(t, comparisons.Load(), "stale tick must not publish comparison telemetry")
	assert.Zero(t, missed.Load(), "a stale flag evaluation is not a missed observation")
	select {
	case observation := <-sqlUpdates.Updates():
		t.Fatalf("stale tick published an availability observation: %+v", observation)
	default:
	}
	enabled.Store(true)
	w.RefreshSQL()
	assert.Equal(t, 1, prober.callCount("conn"), "a fresh enabled tick must still run")
	assert.Equal(t, int32(1), comparisons.Load())
}

// Per-credential caching must preserve the oldest probe start time, including unknown
// timestamps, so a fresh credential cannot admit SQL evidence from before recovery.
func TestHybridEvaluationPreservesCredentialStartTimesAfterPatroniRecovery(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		t.Run(fmt.Sprintf("unknown_start_%t", unknown), func(t *testing.T) {
			const a, b = "10.0.0.1", "10.0.0.2"
			state := NewHealthState()
			state.Track([]string{a, b})
			health := &PoolHealth{
				state:              NewHealthState(),
				patroniHealthState: NewHealthState(),
				sqlHealthState:     state,
				hybridPolicy:       newHybridHealthPolicy(true),
				routingEnabledFn:   func(context.Context) SQLHealthFeatureFlagEvaluation { return validSQLHealthFeatureFlag(true) },
			}
			policy := health.hybridPolicy
			before := time.Now().Add(-time.Second)
			policy.observePatroni(map[string]bool{a: true}, before)
			policy.observeSQL([]string{a, b}, SQLHealthCheckOutcome{}, before.Add(time.Millisecond))
			policy.observePatroni(map[string]bool{b: true}, before.Add(2*time.Millisecond))
			var iterations []SQLIteration
			w := &HealthWorker{sqlHooks: SQLHealthWorkerHooks{Iteration: func(i SQLIteration, _ []string) { iterations = append(iterations, i) }}}
			first, second := &sqlHealthTarget{poolHealth: health}, &sqlHealthTarget{poolHealth: health}
			complete := func(target *sqlHealthTarget, started time.Time) {
				w.completeSQLHealthCheck(target, 0, time.Now(), []string{a, b}, map[string]struct{}{a: {}, b: {}}, SQLHealthCheckOutcome{
					StartedAt: started,
					Results:   []SQLHealthCheckResult{{IP: a, InRecovery: true}, {IP: b}},
				})
			}
			oldStart := before
			if unknown {
				oldStart = time.Time{}
			}
			complete(first, oldStart)
			assert.Equal(t, oldStart, health.sqlComparisonLatest[first].StartedAt)
			complete(second, time.Now())
			require.Len(t, iterations, 2)
			assert.Equal(t, SQLRoutingResultPatroniRecovery, iterations[0].RoutingResult)
			assert.Equal(t, SQLRoutingResultPatroniRecovery, iterations[1].RoutingResult)
			complete(first, time.Now())
			require.Len(t, iterations, 3)
			assert.Equal(t, SQLRoutingResultConclusive, iterations[2].RoutingResult)
		})
	}
}

func TestHybridEvaluationRejectsCachedSQLBeforePatroniLeaderChange(t *testing.T) {
	for _, unknownStart := range []bool{false, true} {
		t.Run(fmt.Sprintf("unknown_start_%t", unknownStart), func(t *testing.T) {
			const a, b = "10.0.0.1", "10.0.0.2"
			state := NewHealthState()
			state.Track([]string{a, b})
			policy := newHybridHealthPolicy(true)
			health := newTestPoolHealth(a, b)
			health.sqlHealthState = state
			health.hybridPolicy = policy
			health.routingEnabledFn = func(context.Context) SQLHealthFeatureFlagEvaluation { return validSQLHealthFeatureFlag(true) }
			var iterations []SQLIteration
			var selections []LeaderRoutingComparison
			w := &HealthWorker{absenceThreshold: 1, sqlHooks: SQLHealthWorkerHooks{
				Iteration:             func(i SQLIteration, _ []string) { iterations = append(iterations, i) },
				LeaderRoutingCompared: func(c LeaderRoutingComparison, _ []string) { selections = append(selections, c) },
			}}
			first, second := &sqlHealthTarget{poolHealth: health}, &sqlHealthTarget{poolHealth: health}
			complete := func(target *sqlHealthTarget, started time.Time, results []SQLHealthCheckResult) {
				w.completeSQLHealthCheck(target, 0, time.Now(), []string{a, b}, map[string]struct{}{a: {}, b: {}}, SQLHealthCheckOutcome{
					StartedAt: started, Results: results,
				})
			}
			before := time.Now().Add(-time.Second)
			w.applyPatroniHealth(health, map[string]bool{a: true, b: false}, nil, []string{a, b}, before)
			oldStart := before
			if unknownStart {
				oldStart = time.Time{}
			}
			oldResults := []SQLHealthCheckResult{{IP: a}, {IP: b, InRecovery: true}}
			complete(first, oldStart, oldResults)
			complete(second, oldStart, oldResults)
			require.Equal(t, SQLRoutingResultConclusive, iterations[len(iterations)-1].RoutingResult)

			// Patroni advances without a preceding SQL no-primary/recovery decision.
			w.applyPatroniHealth(health, map[string]bool{a: false, b: true}, nil, []string{a, b}, time.Now())
			require.True(t, policy.patroniRecoveryAt.IsZero())
			freshStart := time.Now()
			timeouts := []SQLHealthCheckResult{{IP: a, Err: context.DeadlineExceeded}, {IP: b, Err: context.DeadlineExceeded}}
			complete(first, freshStart, timeouts) // The second target still contributes its old A success.
			require.Equal(t, SQLRoutingResultCachedFallback, iterations[len(iterations)-1].RoutingResult)
			require.NotEmpty(t, selections)
			assert.Equal(t, b, selections[len(selections)-1].NewLeaderIP)
			assert.Equal(t, SQLRoutingResultCachedFallback, selections[len(selections)-1].HybridSource)
			assert.Empty(t, policy.awaitingPatroniLeader)
			assert.Empty(t, policy.supersededPatroniLeader)
			assert.False(t, health.IsHealthy(a))
			assert.True(t, health.IsHealthy(b), "stale SQL must publish the current Patroni leader")

			complete(second, freshStart, timeouts)
			assert.Equal(t, SQLRoutingResultCachedFallback, iterations[len(iterations)-1].RoutingResult)
			assert.Empty(t, policy.awaitingPatroniLeader, "subsequent timeouts must not retain old A")
			// Fresh SQL confirmation of B can resume conclusive routing.
			complete(first, time.Now(), []SQLHealthCheckResult{{IP: a, InRecovery: true}, {IP: b}})
			assert.Equal(t, SQLRoutingResultConclusive, iterations[len(iterations)-1].RoutingResult)
			assert.Empty(t, policy.supersededPatroniLeader)
		})
	}
}

func TestSQLPatroniMismatchReason(t *testing.T) {
	cases := []struct {
		name      string
		sql       map[string]bool
		patroni   map[string]bool
		retained  []string
		conflicts []string
		want      SQLPatroniMismatchReason
	}{
		{name: "agreement", sql: map[string]bool{"B": true, "C": false}, patroni: map[string]bool{"B": true, "C": false}, want: SQLPatroniMatch},
		{name: "old unhealthy Patroni IP", sql: map[string]bool{"B": true}, patroni: map[string]bool{"A": false, "B": true}, want: SQLPatroniMembershipDifference},
		{name: "extra SQL standby", sql: map[string]bool{"B": true, "C": false}, patroni: map[string]bool{"B": true}, want: SQLPatroniMembershipDifference},
		{name: "different leaders", sql: map[string]bool{"A": false, "B": true}, patroni: map[string]bool{"A": true, "B": false}, want: SQLPatroniPrimaryDisagreement},
		{name: "Patroni timeout on leader", sql: map[string]bool{"B": true}, patroni: map[string]bool{"B": false}, want: SQLPatroniPrimaryDisagreement},
		{name: "SQL leader missing from Patroni", sql: map[string]bool{"B": true}, want: SQLPatroniPrimaryDisagreement},
		{name: "Patroni leader missing from SQL candidates", patroni: map[string]bool{"B": true}, want: SQLPatroniPrimaryDisagreement},
		{name: "additional Patroni primary", sql: map[string]bool{"B": true}, patroni: map[string]bool{"A": true, "B": true}, want: SQLPatroniPrimaryDisagreement},
		{name: "additional SQL primary", sql: map[string]bool{"A": true, "B": true}, patroni: map[string]bool{"B": true}, want: SQLPatroniPrimaryDisagreement},
		{name: "role conflict despite common leader", sql: map[string]bool{"B": true}, patroni: map[string]bool{"A": false, "B": true}, conflicts: []string{"A"}, want: SQLPatroniSQLRoleConflict},
		{name: "unknown retained SQL IP is neutral", sql: map[string]bool{"B": true}, patroni: map[string]bool{"A": false, "B": true}, retained: []string{"A", "B"}, want: SQLPatroniMatch},
		{name: "unknown SQL leader is neutral", patroni: map[string]bool{"B": true}, retained: []string{"B"}, want: SQLPatroniMatch},
		{name: "membership without a primary", sql: map[string]bool{"A": false}, want: SQLPatroniMembershipDifference},
		{name: "primary disagreement precedes membership", sql: map[string]bool{"A": false, "B": true}, patroni: map[string]bool{"B": false, "C": false}, want: SQLPatroniPrimaryDisagreement},
		{name: "empty snapshots", want: SQLPatroniMatch},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := SQLPatroniHealthSnapshot{SQLHealth: tc.sql, PatroniHealth: tc.patroni, SQLRetainedIPs: tc.retained, SQLConflictingIPs: tc.conflicts}
			assert.Equal(t, tc.want, snapshot.MismatchReason())
			assert.Equal(t, tc.want == SQLPatroniMatch, snapshot.Matches())
		})
	}
}

func TestPartialSQLPollRecordsShutdownBan(t *testing.T) {
	const a, b = "10.0.0.1", "10.0.0.2"
	for _, tc := range []struct {
		name    string
		err     error
		wantBan bool
		shadow  bool
	}{
		{"crash shutdown", &pgconn.PgError{Code: "57P02"}, true, false},
		{"shutting down", &pgconn.PgError{Code: "57P03", Message: "the database system is shutting down"}, true, false},
		{"admin termination", &pgconn.PgError{Code: "57P01"}, true, false},
		{"startup", &pgconn.PgError{Code: "57P03", Message: "the database system is starting up"}, false, false},
		{"shadow shutdown", &pgconn.PgError{Code: "57P02"}, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			health := newTestPoolHealth(a)
			health.sqlHealthState = NewHealthState()
			health.sqlHealthState.Track([]string{a})
			health.hybridPolicy = newHybridHealthPolicy(!tc.shadow)
			updates := make(chan *PoolHealth, 1)
			health.subscribers = map[HealthUpdates]chan *PoolHealth{HealthUpdates(updates): updates}
			var iterations []SQLIteration
			var changes []SQLRoutingBanChange
			w := &HealthWorker{absenceThreshold: 5, sqlHooks: SQLHealthWorkerHooks{
				Iteration: func(i SQLIteration, _ []string) { iterations = append(iterations, i) },
				RoutingBanChanged: func(c SQLRoutingBanChange, _ []string) {
					if assert.True(t, health.routingMu.TryLock(), "hooks must run outside the routing lock") {
						health.routingMu.Unlock()
					}
					changes = append(changes, c)
				},
			}}
			old := &sqlHealthTarget{poolHealth: health}
			current := &sqlHealthTarget{poolHealth: health}
			w.applyPatroniHealth(health, map[string]bool{a: true}, nil, []string{a}, time.Now())
			w.completeSQLHealthCheck(old, 0, time.Now(), []string{a}, map[string]struct{}{a: {}}, SQLHealthCheckOutcome{
				Results: []SQLHealthCheckResult{{IP: a}},
			})
			require.True(t, health.IsHealthy(a))
			<-updates
			policy := health.hybridPolicy
			recoveryAt, noPrimaryAt := policy.patroniRecoveryAt, policy.sqlNoPrimaryAt
			ownedBySQL := health.sqlRoutingActive

			// B was discovered while the current credential was checking only A.
			health.sqlHealthState.Track([]string{a, b})
			w.completeSQLHealthCheck(current, 0, time.Now(), []string{a}, map[string]struct{}{a: {}}, SQLHealthCheckOutcome{
				Results: []SQLHealthCheckResult{{IP: a, Err: tc.err}},
			})
			require.Len(t, iterations, 1, "partial coverage must not choose a SQL route")
			_, banned := health.hybridPolicy.routingBannedIPs[a]
			require.Equal(t, tc.wantBan, banned)
			assert.NotContains(t, health.hybridPolicy.routingBannedIPs, b)
			wantHealthy := !tc.wantBan || tc.shadow
			require.Equal(t, wantHealthy, health.IsHealthy(a), "a published shutdown leader must be excluded immediately outside shadow mode")
			assert.False(t, health.IsHealthy(b), "partial evidence must not select newly discovered B")
			assert.Equal(t, []string{a}, health.Snapshot().Resolved, "exclusions must preserve published membership")
			assert.ElementsMatch(t, []string{a, b}, health.sqlHealthState.Snapshot().Resolved, "partial exclusions must preserve SQL candidates")
			assert.Equal(t, ownedBySQL, health.sqlRoutingActive)
			assert.Equal(t, recoveryAt, policy.patroniRecoveryAt)
			assert.Equal(t, noPrimaryAt, policy.sqlNoPrimaryAt)
			select {
			case <-updates:
				assert.False(t, wantHealthy, "only a routing exclusion should notify the pool")
			default:
				assert.True(t, wantHealthy, "the pool must be notified immediately when A is excluded")
			}
			w.applyPatroniHealth(health, map[string]bool{a: true}, nil, []string{a}, time.Now())
			assert.Equal(t, wantHealthy, health.IsHealthy(a), "an ordinary Patroni refresh must not revive banned A")
			if tc.wantBan {
				require.Equal(t, []SQLRoutingBanChange{{IP: a, Banned: true, Reason: SQLRoutingBanReasonPostgresShutdown}}, changes)
				w.completeSQLHealthCheck(current, 0, time.Now(), []string{a}, map[string]struct{}{a: {}}, SQLHealthCheckOutcome{
					Results: []SQLHealthCheckResult{{IP: a}},
				})
				require.Contains(t, health.hybridPolicy.routingBannedIPs, a, "partial successes cannot establish recovery")
				require.Len(t, iterations, 1)
			} else {
				require.Empty(t, changes)
			}

			// The next poll overwrites this credential's shutdown with timeouts.
			// The other credential still has its old success, and Patroni still says A.
			w.completeSQLHealthCheck(current, 0, time.Now(), []string{a, b}, map[string]struct{}{a: {}, b: {}}, SQLHealthCheckOutcome{
				Results: []SQLHealthCheckResult{{IP: a, Err: context.DeadlineExceeded}, {IP: b, Err: context.DeadlineExceeded}},
			})
			require.Len(t, iterations, 2)
			wantResult := SQLRoutingResultCachedFallback
			if tc.shadow {
				wantResult = SQLRoutingResultShadow
			}
			assert.Equal(t, wantResult, iterations[1].RoutingResult)
			assert.Equal(t, wantHealthy, health.IsHealthy(a), "cached success and Patroni must not undo a shutdown ban")
			assert.False(t, health.IsHealthy(b))
			if tc.wantBan {
				assert.Len(t, changes, 1, "the same ban must not be reported again")
			}
		})
	}
}

func TestShutdownBanFiltersPublishedPatroniRecovery(t *testing.T) {
	const a, b, c = "10.0.0.1", "10.0.0.2", "10.0.0.3"
	for _, shutdownIP := range []string{b, a} {
		t.Run(shutdownIP, func(t *testing.T) {
			ips := []string{a, b, c}
			health := newTestPoolHealth(ips...)
			health.sqlHealthState = NewHealthState()
			health.sqlHealthState.Track(ips)
			health.hybridPolicy = newHybridHealthPolicy(true)
			updates := make(chan *PoolHealth, 1)
			health.subscribers = map[HealthUpdates]chan *PoolHealth{HealthUpdates(updates): updates}
			var iterations []SQLIteration
			w := &HealthWorker{absenceThreshold: 5, sqlHooks: SQLHealthWorkerHooks{
				Iteration: func(i SQLIteration, _ []string) { iterations = append(iterations, i) },
			}}
			old, fast := &sqlHealthTarget{poolHealth: health}, &sqlHealthTarget{poolHealth: health}
			before := time.Now().Add(-time.Second)
			w.applyPatroniHealth(health, map[string]bool{a: true, b: false, c: false}, nil, ips, before)
			complete := func(target *sqlHealthTarget, outcome SQLHealthCheckOutcome) {
				w.completeSQLHealthCheck(target, 0, time.Now(), ips, map[string]struct{}{a: {}, b: {}, c: {}}, outcome)
			}
			standbys := SQLHealthCheckOutcome{StartedAt: before, Results: []SQLHealthCheckResult{
				{IP: a, InRecovery: true}, {IP: b, InRecovery: true}, {IP: c, InRecovery: true},
			}}
			complete(old, standbys)
			complete(fast, standbys)

			// Patroni finds B before SQL, while the old credential still says all standby.
			w.applyPatroniHealth(health, map[string]bool{a: false, b: true, c: false}, nil, ips, time.Now())
			require.True(t, health.IsHealthy(b))
			policy := health.hybridPolicy
			recoveryAt, noPrimaryAt := policy.patroniRecoveryAt, policy.sqlNoPrimaryAt
			ownedBySQL := health.sqlRoutingActive
			<-updates

			shutdown := SQLHealthCheckOutcome{StartedAt: time.Now(), Results: []SQLHealthCheckResult{
				{IP: a, InRecovery: true}, {IP: b, InRecovery: true}, {IP: c, InRecovery: true},
			}}
			for i := range shutdown.Results {
				if shutdown.Results[i].IP == shutdownIP {
					shutdown.Results[i].Err = &pgconn.PgError{Code: "57P02"}
				}
			}
			complete(fast, shutdown)
			require.Contains(t, policy.routingBannedIPs, shutdownIP)
			require.Equal(t, SQLRoutingResultPatroniRecovery, iterations[len(iterations)-1].RoutingResult)
			assert.Equal(t, shutdownIP != b, health.IsHealthy(b))
			assert.False(t, health.IsHealthy(a))
			assert.False(t, health.IsHealthy(c), "excluding B must not invent a replacement")
			assert.ElementsMatch(t, ips, health.Snapshot().Resolved, "bans must not remove discovery candidates")
			assert.Equal(t, recoveryAt, policy.patroniRecoveryAt)
			assert.True(t, policy.patroniRecoveryActive)
			assert.Equal(t, noPrimaryAt, policy.sqlNoPrimaryAt)
			assert.Empty(t, policy.awaitingPatroniLeader)
			assert.Empty(t, policy.supersededPatroniLeader)
			assert.Empty(t, policy.supersededSQLLeaders)
			assert.Equal(t, ownedBySQL, health.sqlRoutingActive)
			assert.Equal(t, shutdownIP != b, policy.routingTiming.candidate.current[b], "routing timing must follow publication")
			select {
			case <-updates:
				assert.Equal(t, b, shutdownIP, "a ban on an unhealthy standby must not republish routing")
			default:
				assert.NotEqual(t, b, shutdownIP, "the pool must be notified when its leader is excluded")
			}

			complete(fast, shutdown)
			select {
			case <-updates:
				t.Error("an already-excluded IP must not cause repeated publication")
			default:
			}
			// Patroni can still publish C immediately after the next switchover.
			w.applyPatroniHealth(health, map[string]bool{a: false, b: false, c: true}, nil, ips, time.Now())
			assert.True(t, health.IsHealthy(c))
			assert.False(t, health.IsHealthy(b))
		})
	}
}

func TestPartialSQLShutdownRejectsInvalidPoll(t *testing.T) {
	for _, invalid := range []string{"disabled", "retired", "routing mode changed"} {
		t.Run(invalid, func(t *testing.T) {
			health := newTestPoolHealth("A", "B")
			health.sqlHealthState = NewHealthState()
			health.sqlHealthState.Track([]string{"A", "B"})
			health.hybridPolicy = newHybridHealthPolicy(true)
			target := &sqlHealthTarget{poolHealth: health}
			scheduledAt := time.Now()
			switch invalid {
			case "disabled":
				health.disableSQLComparisonTarget(target)
			case "retired":
				health.retireSQLComparisonTarget(target)
			case "routing mode changed":
				health.routingChangedAt = scheduledAt.Add(time.Second)
			}
			w := &HealthWorker{sqlHooks: SQLHealthWorkerHooks{
				RoutingBanChanged: func(SQLRoutingBanChange, []string) { t.Error("invalid poll reported a ban") },
			}}
			w.completeSQLHealthCheck(target, 0, scheduledAt, []string{"A"}, map[string]struct{}{"A": {}}, SQLHealthCheckOutcome{
				Results: []SQLHealthCheckResult{{IP: "A", Err: &pgconn.PgError{Code: "57P02"}}},
			})
			assert.Empty(t, health.hybridPolicy.routingBannedIPs)
		})
	}
}

func TestReportConclusiveErrorsSkipsRoutingBannedIPs(t *testing.T) {
	const (
		bannedIP   = "10.0.0.1"
		eligibleIP = "10.0.0.2"
	)
	w := &HealthWorker{}
	var reported []string
	w.sqlHooks.ConclusiveError = func(result SQLHealthCheckResult, _ []string) {
		reported = append(reported, result.IP)
	}
	checks := []completedSQLHealthCheck{{
		target: &sqlHealthTarget{tags: []string{"database:test"}},
		outcome: SQLHealthCheckOutcome{Results: []SQLHealthCheckResult{
			{IP: bannedIP, Err: &pgconn.PgError{Code: "57P03", Message: "the database system is starting up"}},
			{IP: eligibleIP, Err: &pgconn.PgError{Code: "57P01"}},
		}},
	}}
	decision := hybridRoutingDecision{
		routingResult:    SQLRoutingResultConclusive,
		routingBannedIPs: map[string]time.Time{bannedIP: time.Now()},
	}

	w.reportConclusiveErrors(checks, decision)

	assert.Equal(t, []string{eligibleIP}, reported)
}

func TestReportHybridDecisionReportsBanTransitionsOnce(t *testing.T) {
	const ip = "10.0.0.1"
	at := time.Now()
	policy := newHybridHealthPolicy(true)
	var changes []SQLRoutingBanChange
	w := &HealthWorker{sqlHooks: SQLHealthWorkerHooks{
		RoutingBanChanged: func(change SQLRoutingBanChange, tags []string) {
			changes = append(changes, change)
			assert.Contains(t, tags, "cluster:test")
			assert.Contains(t, tags, "sql_health_routing_enabled:true")
		},
	}}
	poolHealth := &PoolHealth{patroniTags: []string{"cluster:test"}}
	for i, result := range []SQLHealthCheckResult{
		{IP: ip, Err: &pgconn.PgError{Code: sqlHealthCheckCrashShutdown}},
		{IP: ip, Err: &pgconn.PgError{Code: sqlHealthCheckCrashShutdown}},
		{IP: ip, Err: context.DeadlineExceeded},
		{IP: ip, InRecovery: true},
		{IP: ip, InRecovery: true},
	} {
		decision := policy.observeSQL([]string{ip}, aggregateSQLHealthCheckResults([]SQLHealthCheckResult{result}).Outcome, at.Add(time.Duration(i)*time.Second))
		w.reportHybridDecision(poolHealth, decision, true)
	}
	assert.Equal(t, []SQLRoutingBanChange{
		{IP: ip, Banned: true, Reason: SQLRoutingBanReasonPostgresShutdown},
		{IP: ip, Banned: false, Reason: SQLRoutingUnbanReasonConclusiveResult},
	}, changes)
}

// Mirror a promotion between two credentials' scheduled polls. The old role
// stays in raw diagnostics, but cannot force fallback after a later role query.
func TestHybridEvaluationReconcilesOrderedCredentialRoles(t *testing.T) {
	const ip = "10.0.0.1"
	sqlState := NewHealthState()
	sqlState.Track([]string{ip})
	health := &PoolHealth{
		state: NewHealthState(), patroniHealthState: NewHealthState(), sqlHealthState: sqlState,
		hybridPolicy:     newHybridHealthPolicy(true),
		routingEnabledFn: func(context.Context) SQLHealthFeatureFlagEvaluation { return validSQLHealthFeatureFlag(true) },
	}
	var comparisons []SQLPatroniHealthSnapshot
	var iterations []SQLIteration
	w := &HealthWorker{
		hooks:    HealthWorkerHooks{SQLPatroniCompared: func(snapshot SQLPatroniHealthSnapshot, _ []string) { comparisons = append(comparisons, snapshot) }},
		sqlHooks: SQLHealthWorkerHooks{Iteration: func(i SQLIteration, _ []string) { iterations = append(iterations, i) }},
	}
	before := time.Now().Add(-time.Second)
	first, second := &sqlHealthTarget{poolHealth: health}, &sqlHealthTarget{poolHealth: health}
	complete := func(target *sqlHealthTarget, standby bool, start time.Time) {
		w.completeSQLHealthCheck(target, 0, start, []string{ip}, map[string]struct{}{ip: {}}, SQLHealthCheckOutcome{
			StartedAt: start,
			Results:   []SQLHealthCheckResult{{IP: ip, InRecovery: standby, StartedAt: start, CompletedAt: start.Add(time.Millisecond)}},
		})
	}
	complete(first, true, before)
	complete(second, false, before.Add(2*time.Millisecond))
	require.Len(t, iterations, 2)
	assert.Equal(t, SQLRoutingResultConclusive, iterations[1].RoutingResult)
	assert.Empty(t, iterations[1].FallbackReason)
	assert.True(t, health.IsHealthy(ip))
	require.Len(t, comparisons, 2)
	assert.Equal(t, map[string]bool{ip: true}, comparisons[1].SQLHealth)
	assert.Empty(t, comparisons[1].SQLConflictingIPs)
	require.Len(t, comparisons[1].SQLResults, 2)
	assert.True(t, comparisons[1].SQLResults[0].Results[0].InRecovery, "keep raw old standby result for diagnosis")
	// A later demotion must also replace the cached primary, without inventing
	// a conflict or retaining a leader that SQL has explicitly disqualified.
	complete(first, true, before.Add(4*time.Millisecond))
	assert.False(t, health.IsHealthy(ip))
	assert.Equal(t, SQLRoutingResultConclusive, iterations[2].RoutingResult)
	assert.Empty(t, iterations[2].FallbackReason)
}

func TestSQLRoutingTimestampFollowsFlagEvaluation(t *testing.T) {
	for _, replacement := range []bool{false, true} {
		t.Run(fmt.Sprintf("replacement_%t", replacement), func(t *testing.T) {
			const a, b = "10.0.0.1", "10.0.0.2"
			ips := []string{a, b}
			s := &PoolHealth{
				state: NewHealthState(), patroniHealthState: NewHealthState(), sqlHealthState: NewHealthState(),
				hybridPolicy: newHybridHealthPolicy(true),
			}
			s.refreshSQLResolved(true, ips, 1)
			target := &sqlHealthTarget{poolHealth: s}
			w := &HealthWorker{}
			complete := func(primary string) {
				at := time.Now()
				w.completeSQLHealthCheck(target, 0, at, ips, map[string]struct{}{a: {}, b: {}}, SQLHealthCheckOutcome{
					StartedAt: at, Results: []SQLHealthCheckResult{
						{IP: a, InRecovery: primary != a, StartedAt: at, CompletedAt: at},
						{IP: b, InRecovery: primary != b, StartedAt: at, CompletedAt: at},
					},
				})
			}
			w.applyPatroniHealth(s, map[string]bool{a: true, b: false}, nil, ips, time.Now())
			complete(a)
			entered, resume, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once sync.Once
			release := func() { once.Do(func() { close(resume) }) }
			t.Cleanup(release)
			s.routingEnabledFn = func(context.Context) SQLHealthFeatureFlagEvaluation {
				close(entered)
				<-resume
				return validSQLHealthFeatureFlag(true)
			}
			go func() {
				defer close(done)
				primary := ""
				if replacement {
					primary = b
				}
				complete(primary)
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("flag evaluation did not start")
			}
			require.True(t, s.IsHealthy(a), "the old route remains published while flag evaluation waits")
			boundary := time.Now()
			release()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("SQL completion did not finish")
			}
			completedAt := s.sqlComparisonLatest[target].ObservedAt
			assert.True(t, completedAt.Before(boundary), "raw SQL keeps probe completion time")
			if replacement {
				assert.False(t, s.hybridPolicy.routingTiming.pending[b].candidateAt.Before(boundary), "hybrid timing starts at actual publication")
			} else {
				assert.False(t, s.hybridPolicy.sqlNoPrimaryAt.Before(boundary), "routing history starts after evaluation waits")
				assert.False(t, s.hybridPolicy.routingDowntimeStartedAt.Before(boundary), "hybrid downtime starts at actual publication")
			}
		})
	}
}
