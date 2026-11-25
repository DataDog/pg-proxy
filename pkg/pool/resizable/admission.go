// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package resizable

import (
	"container/list"
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/puddle/v2"
)

var errPoolClosed = puddle.ErrClosedPool

type admissionKey struct{}
type admission struct {
	context.Context
	gate    *resizableGate
	ticket  uint64
	elem    *list.Element
	ready   chan struct{}
	pending bool
}

// puddle may continue constructing after Acquire returns. Tag that constructor
// with its exact grant so a late dial never consumes another caller's grant.
func (a *admission) Value(key any) any {
	if key == (admissionKey{}) {
		return a
	}
	return a.Context.Value(key)
}

func (g *resizableGate) acquire(ctx context.Context, acquire func(context.Context) (*pgxpool.Conn, error)) (*pgxpool.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return nil, errPoolClosed
	}
	g.sequence++
	a := &admission{Context: ctx, gate: g, ticket: g.sequence}
	if g.waiting.Len() == 0 && g.available(1) > 0 {
		g.grant(a)
	} else {
		g.enqueue(a)
	}
	for {
		if err := g.awaitGrantLocked(ctx, a); err != nil {
			if a.elem != nil {
				g.waiting.Remove(a.elem)
				a.elem = nil
				if g.waiting.Len() == 0 {
					g.timer.Stop()
				}
			}
			g.consumeGrant(a)
			g.hint()
			g.mu.Unlock()
			return nil, err
		}
		g.mu.Unlock()
		conn, err := acquire(a)
		retry := onlyVirtualLimit(err)
		g.mu.Lock()
		g.consumeGrant(a)
		if !retry {
			g.hint()
			g.mu.Unlock()
			return conn, err
		}
		if g.closed {
			g.mu.Unlock()
			return nil, errPoolClosed
		}
		// Reinsert with the original ticket. A grant is permission to try, not a connection.
		g.enqueue(a)
	}
}

// awaitGrantLocked releases mu while parked and returns with it held.
func (g *resizableGate) awaitGrantLocked(ctx context.Context, a *admission) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if g.closed {
			return errPoolClosed
		}
		if a.elem == nil {
			return nil
		}
		tick := g.timer.C
		g.mu.Unlock()
		elapsed := false
		select {
		case <-ctx.Done():
		case <-g.stop:
		case <-a.ready:
		case <-tick:
			elapsed = true
		}
		g.mu.Lock()
		if !g.closed && (elapsed || (a.elem != nil && ctx.Err() == nil)) {
			g.dispatch(a, elapsed)
		}
	}
}

// available accounts for grants whose caller may not yet have entered pgx.
// Once a dial reserves its slot it no longer competes with idle checkouts.
func (g *resizableGate) available(want int32) int32 {
	n := max(0, g.limit-g.totalOpenConns) - g.pending
	if n < want {
		n += g.idle()
	}
	return n
}

func (g *resizableGate) grant(a *admission) {
	a.pending = true
	g.pending++
}

func (g *resizableGate) consumeGrant(a *admission) {
	if a != nil && a.gate == g && a.pending {
		a.pending = false
		g.pending--
	}
}

func (g *resizableGate) enqueue(a *admission) {
	if a.ready == nil {
		a.ready = make(chan struct{}, 1)
	}
	if g.waiting.Len() == 0 {
		a.elem = g.waiting.PushBack(a)
		g.delay = time.Millisecond
		if g.timer == nil {
			g.timer = time.NewTimer(g.delay)
		} else {
			g.timer.Reset(g.delay)
		}
		g.hint()
		return
	}
	for e := g.waiting.Back(); e != nil; e = e.Prev() {
		if e.Value.(*admission).ticket < a.ticket {
			a.elem = g.waiting.InsertAfter(a, e)
			return
		}
	}
	a.elem = g.waiting.PushFront(a)
	g.hint()
}

// A release nudges only the head. A single shared timer covers pgx publishing
// idle after that hint (or without any hint). Whichever waiter receives the
// tick checks availability for the whole queue; only granted callers enter pgx.
func (g *resizableGate) dispatch(checker *admission, elapsed bool) {
	size := g.waiting.Len()
	if size == 0 {
		return
	}
	available := g.available(int32(size))
	for n := available; n > 0 && g.waiting.Len() > 0; n-- {
		a := g.waiting.Remove(g.waiting.Front()).(*admission)
		a.elem = nil
		g.grant(a)
		if a != checker {
			a.signal()
		}
	}
	switch {
	case g.waiting.Len() == 0:
		g.timer.Stop()
	case available > 0:
		g.delay = time.Millisecond
		g.timer.Reset(g.delay)
	case elapsed:
		g.delay = min(2*g.delay, 10*time.Millisecond)
		g.timer.Reset(g.delay)
	}
}

func (a *admission) signal() {
	select {
	case a.ready <- struct{}{}:
	default:
	}
}
func (g *resizableGate) hint() {
	if head := g.waiting.Front(); head != nil {
		head.Value.(*admission).signal()
	}
}
func (g *resizableGate) close() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.closed = true
	close(g.stop)
	if g.timer != nil {
		g.timer.Stop()
	}
}

// pgconn joins per-address failures. A capacity miss must not mask another
// address's dial, TLS, or authentication error.
func onlyVirtualLimit(err error) bool {
	for err != nil {
		if err == errVirtualConnectionLimit {
			return true
		}
		switch e := err.(type) {
		case interface{ Unwrap() []error }:
			causes := e.Unwrap()
			for _, cause := range causes {
				if !onlyVirtualLimit(cause) {
					return false
				}
			}
			return len(causes) > 0
		case interface{ Unwrap() error }:
			err = e.Unwrap()
		default:
			return false
		}
	}
	return false
}
