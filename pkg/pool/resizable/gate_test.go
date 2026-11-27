// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package resizable

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func testGate(t *testing.T, limit int32, idle func() int32) *resizableGate {
	t.Helper()
	g, err := newResizableGate(limit)
	require.NoError(t, err)
	if idle == nil {
		idle = func() int32 { return 0 }
	}
	g.idle = idle
	t.Cleanup(g.close)
	return g
}
func testSocket(t *testing.T, g *resizableGate) *budgetConn {
	t.Helper()
	require.NoError(t, g.reserve(nil))
	a, b := net.Pipe()
	c := &budgetConn{Conn: a, gate: g}
	t.Cleanup(func() { _ = c.Close(); _ = b.Close() })
	return c
}
func (g *resizableGate) getNumConnsRetiring() int32 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.numConnsRetiring
}
func channelClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}
func signalGate(g *resizableGate) { g.mu.Lock(); defer g.mu.Unlock(); g.hint() }
func waitQueue(t *testing.T, g *resizableGate, n int) {
	t.Helper()
	require.Eventually(t, func() bool { g.mu.Lock(); defer g.mu.Unlock(); return g.waiting.Len() == n }, time.Second, time.Millisecond)
}

func TestConcurrentBudgetAndRetirement(t *testing.T) {
	g := testGate(t, 5, nil)
	var wg sync.WaitGroup
	var held []*budgetConn
	var mu sync.Mutex
	for range 100 {
		wg.Go(func() {
			if g.reserve(nil) == nil {
				mu.Lock()
				held = append(held, &budgetConn{gate: g})
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	require.Len(t, held, 5)
	require.NoError(t, g.resize(2))
	for _, c := range held {
		wg.Go(func() { c.retire(false) })
	}
	wg.Wait()
	require.EqualValues(t, 3, g.getNumConnsRetiring())
	require.EqualValues(t, 5, g.getTotalOpenConns())
	for _, c := range held {
		if c.retiring {
			c.refund()
		}
	}
	require.EqualValues(t, 2, g.getTotalOpenConns())
	require.ErrorIs(t, g.reserve(nil), errVirtualConnectionLimit)
}

func TestFIFOAfterCancellationAndLostGrant(t *testing.T) {
	var idle atomic.Int32
	g := testGate(t, 1, idle.Load)
	testSocket(t, g)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	head, cancelHead := context.WithCancel(ctx)
	type result struct {
		id  int
		err error
	}
	results := make(chan result, 3)
	var attempts atomic.Int32
	for i := 0; i < 3; i++ {
		c := ctx
		if i == 0 {
			c = head
		}
		go func() {
			_, err := g.acquire(c, func(context.Context) (*pgxpool.Conn, error) {
				idle.Store(0)
				if i == 1 && attempts.Add(1) == 1 {
					return nil, errVirtualConnectionLimit
				}
				return nil, nil
			})
			results <- result{i, err}
		}()
		waitQueue(t, g, i+1)
	}
	cancelHead()
	require.Equal(t, result{0, context.Canceled}, <-results)
	idle.Store(1)
	signalGate(g)
	require.Eventually(t, func() bool { return attempts.Load() == 1 }, time.Second, time.Millisecond)
	waitQueue(t, g, 2)
	// Silent publication: no hint when a detached constructor releases to puddle.
	idle.Store(1)
	require.Equal(t, result{id: 1}, <-results)
	idle.Store(1)
	signalGate(g)
	require.Equal(t, result{id: 2}, <-results)
}

func TestRetryOnlyWhenEveryCauseIsCapacity(t *testing.T) {
	capacity := fmt.Errorf("dial: %w", errVirtualConnectionLimit)
	require.True(t, onlyVirtualLimit(fmt.Errorf("connect: %w", errors.Join(capacity, capacity))))
	require.False(t, onlyVirtualLimit(errors.Join(capacity, errors.New("auth rejected"))))
	require.False(t, onlyVirtualLimit(context.DeadlineExceeded))
}

func TestPoolWideBackoff(t *testing.T) {
	for _, n := range []int{1, 200} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var checks, idle, acquired atomic.Int32
				g, err := newResizableGate(1)
				require.NoError(t, err)
				g.idle = func() int32 { checks.Add(1); return idle.Load() }
				defer g.close()
				require.NoError(t, g.reserve(nil))
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				for range n {
					go g.acquire(ctx, func(context.Context) (*pgxpool.Conn, error) { idle.Store(0); acquired.Add(1); return nil, nil })
				}
				synctest.Wait()
				initial := checks.Load()
				for i, d := range []time.Duration{1, 2, 4, 8, 10, 10} {
					time.Sleep(d * time.Millisecond)
					synctest.Wait()
					require.EqualValues(t, i+1, checks.Load()-initial)
				}
				idle.Store(1)
				signalGate(g)
				synctest.Wait()
				require.EqualValues(t, 1, acquired.Load(), "hint must not wait for the 10 ms tick")
				if n > 1 {
					afterGrant := checks.Load()
					time.Sleep(time.Millisecond)
					synctest.Wait()
					require.EqualValues(t, afterGrant+1, checks.Load(), "progress resets backoff")
				}
				cancel()
				synctest.Wait()
				before := checks.Load()
				time.Sleep(time.Second)
				synctest.Wait()
				require.Equal(t, before, checks.Load())
			})
		})
	}
}
