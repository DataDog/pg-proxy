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

// release frees a physical slot after connection cleanup finishes.
func (g *resizableGate) release() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.totalOpenConns == 0 {
		return
	}
	g.totalOpenConns--
	if g.totalOpenConns < g.limit {
		g.wakeWaiters()
	}
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

// getAvailabilitySignal returns the channel a blocked Acquire call should wait on.
// Callers capture it before trying pgxpool.Acquire so they cannot miss a
// connection becoming idle between those two operations.
func (g *resizableGate) getAvailabilitySignal() <-chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.availabilitySignal
}
