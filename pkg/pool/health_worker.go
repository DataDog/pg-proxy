package pool

import (
	"slices"
	"sync"
	"time"
)

// HealthChecker centralizes health probing across pools.
// Pools targeting the same host share discovery, probes, and a snapshot.
// The first subscriber supplies the role, database, and tags for that host.
type HealthChecker interface {
	// Subscribe registers host for periodic probing and returns a channel that
	// receives the shared snapshot after each completed health check. The first
	// Subscribe for a new key runs an inline DNS + probe pass before
	// returning, so an initial snapshot is ready for the caller to receive.
	// Each subscriber gets its own channel so every subscriber observes updates.
	Subscribe(host string, isReadReplica bool, dbName string, tags []string) HealthUpdates
	// Unsubscribe drops a subscription. When the last subscriber for a key
	// unsubscribes the entry is removed and the worker stops probing it.
	Unsubscribe(HealthUpdates)
}

// HealthUpdates delivers the latest shared health snapshot for a subscribed
// host. Updates are coalesced while a consumer is still processing the previous
// notification.
type HealthUpdates <-chan *PoolHealth

// HealthProber supplies discovery and health policy. Its methods may run concurrently.
type HealthProber interface {
	Lookup(host string, tags []string) []string
	Probe(ip, host, dbName string, isReadReplica bool, tags []string) (healthy bool, systemID string, err error)
}

// HealthWorkerHooks reports subscription counts and refresh activity.
// Callbacks may run concurrently across hosts. Evicted must not read the snapshot.
type HealthWorkerHooks struct {
	Subscriptions func(count int, tags []string)
	CheckStarted  func(tags []string)
	CheckFinished func(elapsed time.Duration, tags []string)
	Evicted       func(ip string, tags []string)
}

// HealthWorkerConfig supplies the application policy for a shared worker.
type HealthWorkerConfig struct {
	Interval         time.Duration
	AbsenceThreshold int
	Prober           HealthProber
	Hooks            HealthWorkerHooks
}

// NewHealthWorker constructs a worker without starting periodic refreshes.
// Prober is required. Interval must be positive when Run is used.
func NewHealthWorker(cfg HealthWorkerConfig) *HealthWorker {
	return &HealthWorker{
		interval:         cfg.Interval,
		absenceThreshold: cfg.AbsenceThreshold,
		prober:           cfg.Prober,
		hooks:            cfg.Hooks,
		hosts:            make(map[string]*PoolHealth),
		subscriptions:    make(map[HealthUpdates]*PoolHealth),
	}
}

// HealthWorker shares health checks and snapshots among pools targeting the same host.
type HealthWorker struct {
	interval         time.Duration
	prober           HealthProber
	absenceThreshold int
	hooks            HealthWorkerHooks

	mu            sync.Mutex
	hosts         map[string]*PoolHealth
	subscriptions map[HealthUpdates]*PoolHealth
}

// PoolHealth is a shared snapshot for a subscribed host.
type PoolHealth struct {
	host          string
	isReadReplica bool
	dbName        string
	tags          []string

	mu sync.RWMutex
	// Keep probing until the last pool unsubscribes.
	refcount    int
	subscribers map[HealthUpdates]chan *PoolHealth
	// ready closes after the first inline health check finishes. Concurrent
	// subscribers wait for it instead of observing an empty snapshot.
	ready chan struct{}
	state *HealthState
}

func (w *HealthWorker) Subscribe(host string, isReadReplica bool, dbName string, tags []string) HealthUpdates {
	updates := make(chan *PoolHealth, 1)
	subscription := HealthUpdates(updates)

	w.mu.Lock()
	poolHealth, existed := w.hosts[host]
	if !existed {
		healthTags := slices.Clone(tags)
		if hostTag := "postgres_host:" + host; !slices.Contains(healthTags, hostTag) {
			healthTags = append(healthTags, hostTag)
		}
		poolHealth = &PoolHealth{
			host:          host,
			isReadReplica: isReadReplica,
			dbName:        dbName,
			tags:          healthTags,
			state:         NewHealthState(),
			subscribers:   make(map[HealthUpdates]chan *PoolHealth),
			ready:         make(chan struct{}),
		}
		w.hosts[host] = poolHealth
	}
	poolHealth.mu.Lock()
	poolHealth.refcount++
	poolHealth.subscribers[subscription] = updates
	subscriptionCount := poolHealth.refcount
	poolHealth.mu.Unlock()
	w.subscriptions[subscription] = poolHealth
	w.mu.Unlock()

	if w.hooks.Subscriptions != nil {
		w.hooks.Subscriptions(subscriptionCount, tags)
	}

	if !existed {
		w.checkPoolHealth(poolHealth)
		close(poolHealth.ready)
		return subscription
	}

	<-poolHealth.ready
	poolHealth.mu.Lock()
	select {
	case updates <- poolHealth:
	default:
	}
	poolHealth.mu.Unlock()
	return subscription
}

func (w *HealthWorker) Unsubscribe(subscription HealthUpdates) {
	if subscription == nil {
		return
	}

	w.mu.Lock()
	poolHealth, ok := w.subscriptions[subscription]
	if !ok {
		w.mu.Unlock()
		return
	}
	delete(w.subscriptions, subscription)

	poolHealth.mu.Lock()
	updates := poolHealth.subscribers[subscription]
	delete(poolHealth.subscribers, subscription)
	poolHealth.refcount--
	if poolHealth.refcount <= 0 {
		if current, ok := w.hosts[poolHealth.host]; ok && current == poolHealth {
			delete(w.hosts, poolHealth.host)
		}
	}
	subscriptionCount := poolHealth.refcount
	poolHealth.mu.Unlock()
	close(updates)
	w.mu.Unlock()
	if w.hooks.Subscriptions != nil {
		w.hooks.Subscriptions(subscriptionCount, poolHealth.tags)
	}
}

// Run refreshes subscribed hosts for the lifetime of the process.
func (w *HealthWorker) Run() {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for range ticker.C {
		w.Refresh()
	}
}

func (s *PoolHealth) trackNewIPs(ips []string) []string {
	return s.state.Track(ips)
}

func (w *HealthWorker) updateHealthAndCleanup(s *PoolHealth, pghealth map[string]bool, systemIDs map[string]string, ips []string) {
	s.state.Update(pghealth, systemIDs, ips, w.absenceThreshold, func(ip string) {
		if w.hooks.Evicted != nil {
			w.hooks.Evicted(ip, s.tags)
		}
	})
	s.notifySubscribers()
}

func (s *PoolHealth) notifySubscribers() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, updates := range s.subscribers {
		select {
		case updates <- s:
		default:
		}
	}
}

func (w *HealthWorker) checkPoolHealth(poolHealth *PoolHealth) {
	start := time.Now()
	if w.hooks.CheckStarted != nil {
		w.hooks.CheckStarted(poolHealth.tags)
	}

	ips := w.prober.Lookup(poolHealth.host, poolHealth.tags)
	toProbe := poolHealth.trackNewIPs(ips)

	pghealth := make(map[string]bool, len(toProbe))
	systemIDs := make(map[string]string, len(toProbe))
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, ip := range toProbe {
		wg.Add(1)
		go func(ip string) {
			defer wg.Done()
			ok, systemID, err := w.prober.Probe(ip, poolHealth.host, poolHealth.dbName, poolHealth.isReadReplica, poolHealth.tags)
			mu.Lock()
			pghealth[ip] = err == nil && ok
			systemIDs[ip] = systemID
			mu.Unlock()
		}(ip)
	}
	wg.Wait()

	if w.hooks.CheckFinished != nil {
		w.hooks.CheckFinished(time.Since(start), poolHealth.tags)
	}

	w.updateHealthAndCleanup(poolHealth, pghealth, systemIDs, ips)
}

// Refresh checks all subscribed hosts in parallel and waits for their snapshots.
// Call it serially; Run already does this on each tick.
func (w *HealthWorker) Refresh() {
	w.mu.Lock()
	allPoolHealths := make([]*PoolHealth, 0, len(w.hosts))
	for _, s := range w.hosts {
		allPoolHealths = append(allPoolHealths, s)
	}
	w.mu.Unlock()

	var wg sync.WaitGroup
	for _, s := range allPoolHealths {
		wg.Add(1)
		go func(poolHealth *PoolHealth) {
			defer wg.Done()
			w.checkPoolHealth(poolHealth)
		}(s)
	}
	wg.Wait()
}

// Resolved returns a copy of the currently tracked IPs for the host.
func (s *PoolHealth) Resolved() []string {
	return s.state.Resolved()
}

// IsHealthy returns whether ip passed its most recent probe. Unknown
// IPs report false.
func (s *PoolHealth) IsHealthy(ip string) bool {
	return s.state.IsHealthy(ip)
}
