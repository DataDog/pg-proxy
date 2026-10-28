package resizable

import (
	"context"
	"fmt"
	"sync"
	"time"
)

const acquireRetryInterval = 10 * time.Millisecond

// resizableGate is a dynamically resizable semaphore for all physical
// connections belonging to one physical pgx instance. A slot remains reserved while
// a connection is being constructed, is idle or acquired, and is cleaning up.
type resizableGate struct {
	mu             sync.Mutex
	limit          int32
	totalOpenConns int32
	// numConnsRetiring prevents concurrent releases from selecting more than the excess.
	numConnsRetiring int32
	// availabilitySignal is the channel each blocked acquisition waits on.
	availabilitySignal chan struct{}
}

func newResizableGate(limit int32) (*resizableGate, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("connection limit must be positive: %d", limit)
	}
	return &resizableGate{
		limit:              limit,
		availabilitySignal: make(chan struct{}),
	}, nil
}

// tryReserve reserves capacity for a physical connection before construction
// begins. Holding the lock across the check and increment prevents concurrent
// constructors from exceeding the limit.
func (g *resizableGate) tryReserve() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.totalOpenConns >= g.limit {
		return false
	}
	g.totalOpenConns++
	return true
}

// connConstructionFailed removes capacity reserved for a connection
// that failed to finish construction.
func (g *resizableGate) connConstructionFailed() {
	g.decrementTotalOpenConns(false)
}

// connClosed records that a physical connection has finished closing.
func (g *resizableGate) connClosed() {
	g.decrementTotalOpenConns(false)
}

// retiringConnClosed records that a connection selected for retirement
// has finished closing.
func (g *resizableGate) retiringConnClosed() {
	g.decrementTotalOpenConns(true)
}

func (g *resizableGate) decrementTotalOpenConns(retiring bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.totalOpenConns == 0 {
		return
	}
	g.totalOpenConns--
	if retiring && g.numConnsRetiring > 0 {
		g.numConnsRetiring--
	}
	if g.totalOpenConns < g.limit {
		g.wakeWaiters()
	}
}

// trackClosingConnection records a closure selected by pgx or a caller hook
// so downsizing excludes it while physical cleanup is pending.
func (g *resizableGate) trackClosingConnection() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.numConnsRetiring++
}

// tryRetireExcessConnection selects one released connection for closure when
// the connections not already retiring still exceed the current limit.
func (g *resizableGate) tryRetireExcessConnection() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.totalOpenConns-g.numConnsRetiring <= g.limit {
		return false
	}
	g.numConnsRetiring++
	return true
}

// needsRetirement excludes connections whose cleanup is already pending.
func (g *resizableGate) needsRetirement() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.totalOpenConns-g.numConnsRetiring > g.limit
}

// resize changes the maximum number of physical connections. Existing
// connections above a smaller limit retain their slots until they close.
func (g *resizableGate) resize(limit int32) error {
	if limit <= 0 {
		return fmt.Errorf("connection limit must be positive: %d", limit)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	previousLimit := g.limit
	g.limit = limit
	if g.limit > previousLimit {
		g.wakeWaiters()
	}
	return nil
}

// notifyIdleConnAvailability wakes acquisitions when an existing connection
// becomes idle even though the number of physical connections did not change.
func (g *resizableGate) notifyIdleConnAvailability() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.wakeWaiters()
}

// wait returns on an availability hint or a bounded retry. The retry covers
// pgxpool publishing an idle connection after AfterRelease and constructor
// paths that publish idle connections without calling AfterRelease.
func (g *resizableGate) wait(ctx context.Context, availability <-chan struct{}) error {
	timer := time.NewTimer(acquireRetryInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-availability:
		return nil
	case <-timer.C:
		return nil
	}
}

// wakeWaiters closes the channel held by blocked Acquire calls, then creates a
// channel for later blocked calls. The caller must hold g.mu to prevent
// concurrent reads or closes.
func (g *resizableGate) wakeWaiters() {
	close(g.availabilitySignal)
	g.availabilitySignal = make(chan struct{})
}

func (g *resizableGate) getLimit() int32 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.limit
}

func (g *resizableGate) getTotalOpenConns() int32 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.totalOpenConns
}

func (g *resizableGate) getNumConnsRetiring() int32 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.numConnsRetiring
}

// getAvailabilitySignal returns the channel a blocked Acquire call should wait on.
// Callers capture it before trying pgxpool.Acquire so they cannot miss a
// connection becoming idle between those two operations.
func (g *resizableGate) getAvailabilitySignal() <-chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.availabilitySignal
}
