// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package pool

import (
	"sync/atomic"

	pgproxy "github.com/DataDog/pg-proxy"
)

// RoundRobinCounter provides a lock-free, high-throughput, and low-latency way
// to cycle through a sequence of integer indexes (e.g., 0 to N-1) in round-robin order.
// It avoids the overhead of mutexes and is suitable for use across multiple goroutines.
type RoundRobinCounter struct {
	counter atomic.Uint32
}

// InvalidIndex is the value returned by Next when the size is not greater than 0.
const InvalidIndex = -1

// NewRoundRobinCounterWithStart returns a RoundRobinCounter initialized with a specific starting value.
func NewRoundRobinCounterWithStart(start uint32) *RoundRobinCounter {
	rr := &RoundRobinCounter{}
	rr.counter.Store(start)
	return rr
}

// Next returns the next index in round-robin order for a given pool size.
// It atomically increments the counter and wraps using modulo to stay in bounds,
// even after uint32 overflow.
// It will return -1 if size is not greater than 0.
// Safe to call concurrently.
func (rr *RoundRobinCounter) Next(size int) int {
	if size <= 0 {
		pgproxy.Logger().Warn("RoundRobinCounter.Next: size must be > 0", "size", size)
		return InvalidIndex
	}

	next := rr.counter.Add(1)
	return int(next % uint32(size))
}

// Current returns the internal counter value.
// Safe to call concurrently.
func (rr *RoundRobinCounter) Current() int {
	return int(rr.counter.Load())
}
