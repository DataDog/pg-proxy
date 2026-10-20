package pool

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const maxFailedHostLookups = 5

type mockProber struct {
	mu sync.Mutex

	ipsByHost      map[string][]string
	healthyIPs     map[string]bool
	probeErrors    map[string]error
	probeCalls     int
	recordCall     func(ip, host, dbName string, isReadReplica bool, tags []string)
	mockHealthPing func(ip string) (bool, error)
}

func (p *mockProber) Lookup(host string, _ []string) []string {
	p.mu.Lock()
	defer p.mu.Unlock()
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

	poolHealth := receivePoolHealth(t, w.Subscribe("primary.local", false, "mydb", []string{"tag:value"}))

	assert.ElementsMatch(t, []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}, poolHealth.Resolved())
	assert.True(t, poolHealth.IsHealthy("10.0.0.1"))
	assert.False(t, poolHealth.IsHealthy("10.0.0.2"))
	assert.True(t, poolHealth.IsHealthy("10.0.0.3"))
	assert.False(t, poolHealth.IsHealthy("10.0.0.99"), "unknown IPs report false")
}

func TestSubscribe_RegisterNewPoolReusesPoolHealth(t *testing.T) {
	p := newMockProber(map[string][]string{"primary.local": {"10.0.0.1"}}, "10.0.0.1")
	w := newTestWorker(p)

	u1 := w.Subscribe("primary.local", false, "mydb", nil)
	s1 := receivePoolHealth(t, u1)
	u2 := w.Subscribe("primary.local", false, "mydb", nil)
	s2 := receivePoolHealth(t, u2)
	assert.Same(t, s1, s2, "same key returns same poolHealth")

	s1.mu.RLock()
	assert.Equal(t, 2, s1.refcount, "refcount incremented")
	s1.mu.RUnlock()
}

func TestSubscribe_BroadcastsUpdatesToEverySubscriber(t *testing.T) {
	p := newMockProber(map[string][]string{"primary.local": {"10.0.0.1"}}, "10.0.0.1")
	w := newTestWorker(p)

	u1 := w.Subscribe("primary.local", false, "mydb", nil)
	u2 := w.Subscribe("primary.local", false, "mydb", nil)
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

	a := receivePoolHealth(t, w.Subscribe("primary-a.local", false, "db_a", nil))
	b := receivePoolHealth(t, w.Subscribe("primary-b.local", false, "db_b", nil))

	assert.ElementsMatch(t, []string{"10.0.0.1"}, a.Resolved())
	assert.ElementsMatch(t, []string{"10.0.0.2"}, b.Resolved())
}

func TestUnsubscribe_RefcountAboveZero_KeepsPoolHealth(t *testing.T) {
	p := newMockProber(map[string][]string{"primary.local": {"10.0.0.1"}}, "10.0.0.1")
	w := newTestWorker(p)

	u1 := w.Subscribe("primary.local", false, "mydb", nil)
	s := receivePoolHealth(t, u1)
	u2 := w.Subscribe("primary.local", false, "mydb", nil)
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

	updates := w.Subscribe("primary.local", false, "mydb", nil)
	s := receivePoolHealth(t, updates)
	w.Unsubscribe(updates)

	w.mu.Lock()
	_, present := w.hosts[s.host]
	w.mu.Unlock()
	assert.False(t, present, "poolHealth removed from map at refcount 0")
	_, ok := <-updates
	assert.False(t, ok, "subscriber channel should close on unsubscribe")
}

func TestTickOne_EmptyDNS_LeavesResolvedEmpty(t *testing.T) {
	p := newMockProber(nil) // every host resolves to nothing
	w := newTestWorker(p)

	poolHealth := receivePoolHealth(t, w.Subscribe("nowhere.local", false, "mydb", nil))

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

	poolHealth := receivePoolHealth(t, w.Subscribe("primary.local", false, "mydb", nil))

	assert.ElementsMatch(t, []string{"10.0.0.1", "10.0.0.2"}, poolHealth.Resolved())
	assert.False(t, poolHealth.IsHealthy("10.0.0.1"))
	assert.False(t, poolHealth.IsHealthy("10.0.0.2"))
}

func TestProbeErrorPublishesInstanceAsUnhealthy(t *testing.T) {
	p := newMockProber(map[string][]string{"primary.local": {"10.0.0.1"}}, "10.0.0.1")
	w := newTestWorker(p)
	updates := w.Subscribe("primary.local", false, "mydb", nil)
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

func TestTick_IpRotation(t *testing.T) {
	// only 10.0.0.1 is healthy; 10.0.0.2 is not
	p := newMockProber(map[string][]string{"primary.local": {"10.0.0.1"}}, "10.0.0.1")
	w := newTestWorker(p)

	updates := w.Subscribe("primary.local", false, "mydb", nil)
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
	w.hooks = HealthWorkerHooks{
		Subscriptions: func(count int, tags []string) {
			counts = append(counts, count)
			subscriptionTags = append(subscriptionTags, append([]string(nil), tags...))
		},
		CheckStarted: func(tags []string) {
			assert.Equal(t, hostTags, tags)
			events = append(events, "start")
		},
		CheckFinished: func(elapsed time.Duration, tags []string) {
			assert.Equal(t, hostTags, tags)
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
		assert.Equal(t, hostTags, tags)
	}
	tags := []string{"db:first"}
	firstUpdates := w.Subscribe("primary.local", true, "first", tags)
	first := receivePoolHealth(t, firstUpdates)
	tags[0] = "changed"
	secondUpdates := w.Subscribe("primary.local", false, "second", []string{"db:second"})
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
	oldUpdates := w.Subscribe("primary.local", false, "mydb", nil)
	old := receivePoolHealth(t, oldUpdates)
	w.Unsubscribe(oldUpdates)
	currentUpdates := w.Subscribe("primary.local", false, "mydb", nil)
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
	a := receivePoolHealth(t, w.Subscribe("a.local", false, "mydb", nil))
	b := receivePoolHealth(t, w.Subscribe("b.local", false, "mydb", nil))
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

func TestSubscribePreservesIdentityTags(t *testing.T) {
	for _, hasHost := range []bool{false, true} {
		tags := make([]string, 2, 8)
		copy(tags, []string{"database:configured_pool", "database_cluster:cluster-a"})
		if hasHost {
			tags = append(tags, "postgres_host:primary.local")
		}
		w := newTestWorker(newMockProber(nil))
		health := receivePoolHealth(t, w.Subscribe("primary.local", false, "physical_db", tags))
		assert.ElementsMatch(t, []string{"database:configured_pool", "database_cluster:cluster-a", "postgres_host:primary.local"}, health.tags)
		for _, tag := range tags[len(tags):cap(tags)] {
			assert.Empty(t, tag)
		}
		tags[0] = "database:changed"
		assert.Contains(t, health.tags, "database:configured_pool")
	}
}
