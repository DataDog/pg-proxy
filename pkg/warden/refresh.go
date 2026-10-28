package warden

import (
	"context"
	"fmt"
	"sync"
	"time"

	pgproxy "github.com/DataDog/pg-proxy"
)

// Source loads configuration and a source-defined, monotonically increasing revision.
// The revision identifies changes for this refresher; it need not be the version
// of a persisted object. A source combining multiple inputs may use a local counter.
type Source[C any] func(context.Context) (C, uint64, error)

// RefresherConfig configures portable polling and source revision handling.
type RefresherConfig[C any] struct {
	Context  context.Context
	Interval time.Duration
	Source   Source[C]
	Apply    func(C) error
}

// Refresher polls configuration and applies newer source revisions.
type Refresher[C any] struct {
	ctx    context.Context
	source Source[C]
	apply  func(C) error
	ticker *time.Ticker

	mu                          sync.Mutex
	latestAppliedSourceRevision uint64
	hasAppliedSourceRevision    bool
}

// NewRefresher creates a stopped configuration refresher.
func NewRefresher[C any](cfg RefresherConfig[C]) *Refresher[C] {
	ctx := cfg.Context
	if ctx == nil {
		ctx = context.Background()
	}
	return &Refresher[C]{
		ctx:    ctx,
		source: cfg.Source,
		apply:  cfg.Apply,
		ticker: time.NewTicker(cfg.Interval),
	}
}

// Initialize loads the initial configuration and starts polling.
func (r *Refresher[C]) Initialize() error {
	if err := r.Refresh(); err != nil {
		return fmt.Errorf("failed to initialize warden: %w", err)
	}
	go r.run()
	return nil
}

// Refresh fetches and applies the latest configuration when its source revision is new.
func (r *Refresher[C]) Refresh() error {
	cfg, sourceRevision, err := r.source(r.ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch configuration: %w", err)
	}

	// Keep the source revision check and application together so concurrent refreshes
	// cannot apply stale configuration or call Apply concurrently.
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.hasAppliedSourceRevision && sourceRevision <= r.latestAppliedSourceRevision {
		pgproxy.Logger().Debug(fmt.Sprintf("skipping configuration update based on source change counters: source_revision=%d latest_applied_source_revision=%d", sourceRevision, r.latestAppliedSourceRevision))
		pgproxy.Metrics().Count("dd.orgstore.pg_proxy.warden.accept.count", 1, []string{"outcome:skipped"}, 1)
		return nil
	}
	pgproxy.Logger().Info(fmt.Sprintf("applying configuration using source change counter: source_revision=%d configuration=%#v", sourceRevision, &cfg))
	if err := r.apply(cfg); err != nil {
		return fmt.Errorf("failed to accept configuration: %w", err)
	}

	r.latestAppliedSourceRevision = sourceRevision
	r.hasAppliedSourceRevision = true
	return nil
}

// SetInterval changes the polling interval.
func (r *Refresher[C]) SetInterval(interval time.Duration) {
	r.ticker.Reset(interval)
}

func (r *Refresher[C]) run() {
	for {
		select {
		case <-r.ticker.C:
			if err := r.Refresh(); err != nil {
				pgproxy.Logger().Error(fmt.Sprintf("configuration refresh failed: %v", err))
			}
		case <-r.ctx.Done():
			r.ticker.Stop()
			return
		}
	}
}
