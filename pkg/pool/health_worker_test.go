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

	"github.com/jackc/puddle/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const maxFailedHostLookups = 5

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
	pools   *SQLHealthCheckPools
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (p *blockingPoolCreatingSQLHealthProber) Probe(_ context.Context, ip, connString string, _ []string) (bool, error) {
	if _, err := p.pools.GetOrAdd(ip, connString); err != nil {
		return false, err
	}
	p.once.Do(func() { close(p.started) })
	<-p.release
	return false, nil
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

	assert.ElementsMatch(t, []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}, poolHealth.Resolved())
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
	assert.ElementsMatch(t, []string{"10.0.0.1", "10.0.0.2"}, poolHealth.Resolved())
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

	assert.ElementsMatch(t, []string{"10.0.0.1"}, a.Resolved())
	assert.ElementsMatch(t, []string{"10.0.0.2"}, b.Resolved())
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
			secondConnString: {{}},
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
		assert.Equal(t, sharedTags, tags)
		comparisonsMu.Lock()
		defer comparisonsMu.Unlock()
		comparisons = append(comparisons, comparison)
	}
	w.RefreshSQL()
	p.mu.Lock()
	assert.Equal(t, 1, p.lookupCalls, "SQL DNS lookup must be shared across credential targets")
	p.mu.Unlock()
	assert.Equal(t, 1, sqlProber.callCount(firstConnString))
	assert.Equal(t, 1, sqlProber.callCount(secondConnString))
	comparisonsMu.Lock()
	comparisons = append([]SQLPatroniHealthSnapshot(nil), comparisons...)
	comparisonsMu.Unlock()
	require.Len(t, comparisons, 2, "each independently completed credential target must report")
	var completeComparison SQLPatroniHealthSnapshot
	for _, comparison := range comparisons {
		assert.True(t, comparison.Matches())
		if len(comparison.SQLResults) == 2 {
			completeComparison = comparison
		}
	}
	require.Len(t, completeComparison.SQLResults, 2)
	for _, resultSet := range completeComparison.SQLResults {
		require.Len(t, resultSet.Results, 1)
		assert.True(t, resultSet.Results[0].HealthyPrimary())
	}

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

	releaseSlow()
	select {
	case <-refreshDone:
	case <-time.After(time.Second):
		t.Fatal("host-wide SQL round did not finish")
	}
	for _, target := range health.sqlTargetValues() {
		assert.False(t, target.sqlRoundRunning())
	}

	w.RefreshSQL()
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
	assert.Equal(t, []string{patroniIP}, health.Resolved(), "SQL discovery must not mutate Patroni routing state")
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
		missed <- slices.Clone(tags)
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
		ChecksEnabled:   func(context.Context) bool { return checksEnabled.Load() },
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
		ChecksEnabled:   func(context.Context) bool { return enabled },
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

	assert.Empty(t, poolHealth.Resolved())
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

	assert.ElementsMatch(t, []string{"10.0.0.1", "10.0.0.2"}, poolHealth.Resolved())
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
	return &PoolHealth{state: state}
}

func TestTrackNewIPs_AddsNewAndDedupes(t *testing.T) {
	s := newTestPoolHealth()

	got := s.trackNewIPs([]string{"10.0.0.1", "10.0.0.2"})
	assert.ElementsMatch(t, []string{"10.0.0.1", "10.0.0.2"}, got)

	got = s.trackNewIPs([]string{"10.0.0.2", "10.0.0.3"})
	assert.ElementsMatch(t, []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}, got, "existing IPs not duplicated")
}

func TestUpdateHealthAndCleanup_PublishesHealthAndEvictsExpired(t *testing.T) {
	w := newTestWorker(nil)
	s := newTestPoolHealth("10.0.0.1", "10.0.0.2")

	// both IPs healthy, both in DNS
	w.updateHealthAndCleanup(s, map[string]bool{"10.0.0.1": true, "10.0.0.2": true}, nil, []string{"10.0.0.1", "10.0.0.2"})
	assert.True(t, s.IsHealthy("10.0.0.1"))
	assert.True(t, s.IsHealthy("10.0.0.2"))
	assert.ElementsMatch(t, []string{"10.0.0.1", "10.0.0.2"}, s.Resolved())

	// 10.0.0.2 leaves DNS but stays healthy — grace window, not evicted
	for i := 0; i < maxFailedHostLookups-1; i++ {
		w.updateHealthAndCleanup(s, map[string]bool{"10.0.0.1": true, "10.0.0.2": true}, nil, []string{"10.0.0.1"})
	}
	assert.ElementsMatch(t, []string{"10.0.0.1", "10.0.0.2"}, s.Resolved(), "still in grace window")

	// 10.0.0.2 now unhealthy and absent from DNS — eligible for eviction
	w.updateHealthAndCleanup(s, map[string]bool{"10.0.0.1": true, "10.0.0.2": false}, nil, []string{"10.0.0.1"})
	assert.ElementsMatch(t, []string{"10.0.0.1"}, s.Resolved(), "evicted after grace window and unhealthy")
	assert.False(t, s.IsHealthy("10.0.0.2"), "evicted IP must not stay healthy")

	// verify lastKnownPgHealth protects an IP that is still healthy
	s2 := newTestPoolHealth("10.0.0.1", "10.0.0.2")
	w.updateHealthAndCleanup(s2, map[string]bool{"10.0.0.1": true, "10.0.0.2": true}, nil, []string{"10.0.0.1", "10.0.0.2"})
	for i := 0; i < maxFailedHostLookups+1; i++ {
		w.updateHealthAndCleanup(s2, map[string]bool{"10.0.0.1": true, "10.0.0.2": true}, nil, []string{"10.0.0.1"})
	}
	assert.ElementsMatch(t, []string{"10.0.0.1", "10.0.0.2"}, s2.Resolved(), "last known healthy IP never evicted")
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
	assert.Equal(t, []string{replacementIP}, health.Resolved())
	assert.Equal(t, 1, pools.Len(), "the pool must remain open while its SQL probe is using it")

	releaseSQL()
	select {
	case <-sqlRefreshDone:
	case <-time.After(time.Second):
		t.Fatal("SQL health check did not finish after it was released")
	}
	assert.Equal(t, 1, pools.Len(), "the first SQL round must retain the IP that it independently discovered")

	// The SQL loop observes the DNS replacement without involving the Patroni refresh path. It keeps
	// the absent pool only through the configured DNS grace window.
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
	assert.Equal(t, []string{sqlIP}, health.sqlHealthState.Resolved())
	assert.Equal(t, []string{patroniIP}, health.Resolved())
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
	assert.ElementsMatch(t, []string{"10.0.0.1"}, poolHealth.Resolved())

	p.mu.Lock()
	p.ipsByHost["primary.local"] = []string{"10.0.0.1", "10.0.0.2"}
	p.mu.Unlock()
	w.Refresh()
	assert.Same(t, poolHealth, receivePoolHealth(t, updates))
	assert.ElementsMatch(t, []string{"10.0.0.1", "10.0.0.2"}, poolHealth.Resolved())

	// Both IPs leave DNS. 10.0.0.1 remains healthy so lastKnownPgHealth
	// protects it from eviction. 10.0.0.2 is unhealthy and gets evicted.
	p.mu.Lock()
	p.ipsByHost["primary.local"] = nil
	p.mu.Unlock()

	for i := 0; i < maxFailedHostLookups-1; i++ {
		w.Refresh()
		assert.ElementsMatch(t, []string{"10.0.0.1", "10.0.0.2"}, poolHealth.Resolved(), "tick %d: IPs should still be tracked", i+1)
	}
	w.Refresh()
	assert.ElementsMatch(t, []string{"10.0.0.1"}, poolHealth.Resolved(), "10.0.0.2 evicted; 10.0.0.1 protected by lastKnownPgHealth")
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
	assert.Empty(t, first.Resolved())

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
			poolHealth := &PoolHealth{sqlHealthState: state, state: NewHealthState()}
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
	target := &sqlHealthTarget{tags: []string{"database:orders"}}
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
		ChecksEnabled: func(context.Context) bool {
			value := enabled.Load()
			if evaluations.Add(1) == 1 {
				close(flagRead)
				<-releaseFlag
			}
			return value
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
