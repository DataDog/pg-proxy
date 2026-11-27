// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package resizable

import (
	"container/list"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// One gate per pgxpool, shared by checkout admission and backend dialers.
type resizableGate struct {
	mu               sync.Mutex
	limit            int32
	totalOpenConns   int32
	numConnsRetiring int32
	sockets          map[*pgconn.PgConn]*budgetConn

	closed   bool
	waiting  list.List // *admission, in arrival order
	sequence uint64
	pending  int32 // grants not yet consumed by a physical dial or completed checkout
	timer    *time.Timer
	delay    time.Duration
	stop     chan struct{}
	idle     func() int32
}

func newResizableGate(limit int32) (*resizableGate, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("connection limit must be positive: %d", limit)
	}
	return &resizableGate{
		limit: limit, sockets: make(map[*pgconn.PgConn]*budgetConn),
		stop: make(chan struct{}),
	}, nil
}

func (g *resizableGate) reserve(a *admission) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return errPoolClosed
	}
	if g.totalOpenConns >= g.limit {
		return errVirtualConnectionLimit
	}
	g.totalOpenConns++
	g.consumeGrant(a)
	return nil
}

func (g *resizableGate) resize(limit int32) error {
	if limit <= 0 {
		return fmt.Errorf("connection limit must be positive: %d", limit)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return errPoolClosed
	}
	if limit > g.limit {
		g.hint()
	}
	g.limit = limit
	return nil
}

func (g *resizableGate) needsRetirement() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.totalOpenConns-g.numConnsRetiring > g.limit
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
