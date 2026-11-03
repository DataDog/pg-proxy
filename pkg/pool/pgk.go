package pool

import (
	"errors"
	"fmt"
	"slices"
	"time"
)

// PGKPoolConfig configures a dynamically discovered direct pool.
type PGKPoolConfig struct {
	Direct *DirectRuntime
	// Discover returns retained addresses after the discovery source has applied
	// its policy for transient lookup failures. HealthState supplies that policy
	// for the application's shared health worker.
	Discover    func() []string
	NewInstance func(string) PostgresInstance
	// RegisterInstance optionally prepares and publishes an instance atomically with resizing.
	RegisterInstance  func(string, PostgresInstance) error
	HealthUpdates     HealthUpdates
	ReadReplica       bool
	RunSetupWithRetry func(func() error)
	Resolved          func(string)
	// SetupFinished observes each periodic setup attempt, including retries.
	// Initial construction does not emit this event.
	SetupFinished   func(time.Duration)
	LeaderChanged   func(string, string)
	LeaderUnchanged func(string)
	Close           func()
	Initialize      bool
	Start           bool
}

// PGKPool manages a dynamically discovered set of PostgreSQL instances.
//
// TODO: Consider renaming this to DiscoveredDirectPool in a follow-up API RFC.
// PGK is one discovery-policy adapter; the runtime itself is generic.
type PGKPool struct {
	*DirectRuntime
	discover          func() []string
	newInstance       func(string) PostgresInstance
	registerInstance  func(string, PostgresInstance) error
	healthUpdates     HealthUpdates
	readReplica       bool
	runSetupWithRetry func(func() error)
	resolved          func(string)
	setupFinished     func(time.Duration)
	leaderChanged     func(string, string)
	leaderUnchanged   func(string)
	closeAdapter      func()

	lastLeader  string
	lastHealthy []string
}

// NewPGKPool creates and initializes a discovered direct pool.
func NewPGKPool(cfg PGKPoolConfig) *PGKPool {
	pool := &PGKPool{
		DirectRuntime:     cfg.Direct,
		discover:          cfg.Discover,
		newInstance:       cfg.NewInstance,
		registerInstance:  cfg.RegisterInstance,
		healthUpdates:     cfg.HealthUpdates,
		readReplica:       cfg.ReadReplica,
		runSetupWithRetry: cfg.RunSetupWithRetry,
		resolved:          cfg.Resolved,
		setupFinished:     cfg.SetupFinished,
		leaderChanged:     cfg.LeaderChanged,
		leaderUnchanged:   cfg.LeaderUnchanged,
		closeAdapter:      cfg.Close,
	}
	if cfg.Initialize {
		_ = pool.Setup()
	}
	if cfg.Start {
		go pool.run()
	}
	return pool
}

// Setup reconciles discovery, health, selection state, and stale instances.
func (p *PGKPool) Setup() error {
	addresses := p.discover()
	current := make(map[string]bool, len(addresses))
	for _, address := range addresses {
		current[address] = true
		if p.resolved != nil {
			p.resolved(address)
		}
		if _, ok := p.State().Load(address); !ok {
			instance := p.newInstance(address)
			if p.registerInstance != nil {
				if err := p.registerInstance(address, instance); err != nil {
					instance.Close()
					return fmt.Errorf("register instance %s: %w", address, err)
				}
			} else {
				p.State().Store(address, instance)
			}
		}
	}

	p.CheckHealth()
	healthy := p.RefreshHealthy()
	p.reportLeader(healthy)
	p.cleanup(current, healthy)
	if len(healthy) == 0 {
		return errors.New("no valid ips found")
	}
	return nil
}

// SetupWithRetry applies the host retry adapter around Setup.
func (p *PGKPool) SetupWithRetry() {
	setup := func() error {
		start := time.Now()
		err := p.Setup()
		if p.setupFinished != nil {
			p.setupFinished(time.Since(start))
		}
		return err
	}
	if p.runSetupWithRetry == nil {
		_ = setup()
		return
	}
	p.runSetupWithRetry(setup)
}

// cleanup removes instances that are no longer accessible.
// An instance is removed if it is absent from the discovery source's retained
// addresses and is no longer healthy. If there are no healthy addresses, the last
// known healthy addresses are retained. This handles Fabric removing an IP from
// DNS while that instance can still serve queries. In a leader cluster, where
// at most one instance is healthy, it prevents prematurely removing the leader
// when DNS no longer returns it and Patroni is temporarily unresponsive.
func (p *PGKPool) cleanup(current map[string]bool, healthy []string) {
	if len(healthy) > 0 {
		p.lastHealthy = slices.Clone(healthy)
	}
	p.State().Range(func(address string, instance PostgresInstance) bool {
		if !current[address] && !slices.Contains(p.lastHealthy, address) {
			instance.Close()
			p.State().Delete(address)
		}
		return true
	})
}

func (p *PGKPool) reportLeader(healthy []string) {
	if p.readReplica || len(healthy) == 0 {
		return
	}
	next := healthy[0]
	if p.lastLeader != "" {
		if p.lastLeader != next && p.leaderChanged != nil {
			p.leaderChanged(p.lastLeader, next)
		} else if p.lastLeader == next && p.leaderUnchanged != nil {
			p.leaderUnchanged(next)
		}
	}
	p.lastLeader = next
}

func (p *PGKPool) run() {
	for {
		select {
		case _, ok := <-p.healthUpdates:
			if !ok {
				return
			}
			p.SetupWithRetry()
		case <-p.Done():
			return
		}
	}
}

// ClosePools closes instances and releases the discovery adapter.
func (p *PGKPool) ClosePools() {
	p.DirectRuntime.ClosePools()
	if p.closeAdapter != nil {
		p.closeAdapter()
	}
}
