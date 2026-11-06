package warden

import (
	"context"
	"fmt"
	"sync"
	"time"

	pgproxy "github.com/DataDog/pg-proxy"
)

// Normalize converts an embedder-owned configuration into the portable
// reconciliation snapshot consumed by Warden.
type Normalize[C, D any] func(C) (Snapshot[D], error)

// RefreshInterval returns a new polling interval and whether it should replace
// the current interval after a configuration has been accepted.
type RefreshInterval[C any] func(C) (time.Duration, bool)

// ControllerHooks adapt configuration acceptance to embedder-owned inventory
// and policy.
type ControllerHooks[C any] struct {
	BeforeAccept func(C) error
	Accepted     func(C)
}

// ControllerConfig configures the complete portable Warden control plane.
// C remains embedder-owned configuration and D remains embedder-owned database
// data; Normalize is the boundary that removes those types from the runtime.
type ControllerConfig[C, D any] struct {
	Context         context.Context
	Runtime         *Warden
	Interval        time.Duration
	Source          Source[C]
	Normalize       Normalize[C, D]
	RefreshInterval RefreshInterval[C]
	Factory         func(context.Context, string, D, []D) (ManagedBackend, error)
	Retry           Retry[ManagedBackend]
	DatabaseUpdate  DatabaseUpdateHandler[D]
	BackendReady    BackendReadyHandler
	Hooks           ControllerHooks[C]
}

// Controller composes versioned configuration refresh with backend and
// logical-database reconciliation.
type Controller[C, D any] struct {
	runtime         *Warden
	reconciler      *Reconciler[D]
	refresher       *Refresher[C]
	normalize       Normalize[C, D]
	refreshInterval RefreshInterval[C]
	hooks           ControllerHooks[C]
	reconcileMu     sync.Mutex
}

// NewController creates a stopped portable Warden control plane.
func NewController[C, D any](cfg ControllerConfig[C, D]) *Controller[C, D] {
	runtime := cfg.Runtime
	if runtime == nil {
		runtime = New()
	}
	controller := &Controller[C, D]{
		runtime:         runtime,
		normalize:       cfg.Normalize,
		refreshInterval: cfg.RefreshInterval,
		hooks:           cfg.Hooks,
	}
	controller.reconciler = NewReconciler(ReconcilerConfig[D]{
		Context:        cfg.Context,
		Runtime:        runtime,
		Factory:        cfg.Factory,
		Retry:          cfg.Retry,
		DatabaseUpdate: cfg.DatabaseUpdate,
		BackendReady:   cfg.BackendReady,
	})
	controller.refresher = NewRefresher(RefresherConfig[C]{
		Context:  cfg.Context,
		Interval: cfg.Interval,
		Source:   cfg.Source,
		Apply:    controller.Accept,
	})
	return controller
}

// Initialize loads the initial configuration and starts periodic refresh.
func (c *Controller[C, D]) Initialize() error {
	return c.refresher.Initialize()
}

// Refresh immediately fetches and conditionally applies configuration.
func (c *Controller[C, D]) Refresh() error {
	return c.refresher.Refresh()
}

// Accept normalizes and reconciles an embedder-owned configuration.
func (c *Controller[C, D]) Accept(cfg C) (err error) {
	c.reconcileMu.Lock()
	defer c.reconcileMu.Unlock()
	start := time.Now()
	defer func() {
		if err != nil {
			pgproxy.Metrics().Count("dd.orgstore.pg_proxy.warden.accept.count", 1, []string{"outcome:error"}, 1)
			return
		}
		pgproxy.Metrics().Timing("dd.orgstore.pg_proxy.warden.accept.duration", time.Since(start), nil, 1)
		pgproxy.Metrics().Count("dd.orgstore.pg_proxy.warden.accept.count", 1, []string{"outcome:success"}, 1)
	}()
	if c.hooks.BeforeAccept != nil {
		if beforeErr := c.hooks.BeforeAccept(cfg); beforeErr != nil {
			return fmt.Errorf("before accepting configuration: %w", beforeErr)
		}
	}
	if c.normalize == nil {
		return fmt.Errorf("warden controller normalize callback is required")
	}
	snapshot, normalizeErr := c.normalize(cfg)
	if normalizeErr != nil {
		return fmt.Errorf("normalize configuration: %w", normalizeErr)
	}
	if reconcileErr := c.reconciler.Accept(snapshot); reconcileErr != nil {
		return reconcileErr
	}
	if c.hooks.Accepted != nil {
		c.hooks.Accepted(cfg)
	}
	if c.refreshInterval != nil {
		if interval, ok := c.refreshInterval(cfg); ok {
			c.refresher.SetInterval(interval)
			pgproxy.Logger().Info(fmt.Sprintf("Updated refresh period: %s", interval))
		}
	}
	return nil
}

// Runtime returns the live backend, topology, and session inventory.
func (c *Controller[C, D]) Runtime() *Warden {
	return c.runtime
}

// Reconciler returns the managed backend inventory.
func (c *Controller[C, D]) Reconciler() *Reconciler[D] {
	return c.reconciler
}

// SetInterval changes the configuration polling interval.
func (c *Controller[C, D]) SetInterval(interval time.Duration) {
	c.refresher.SetInterval(interval)
}
