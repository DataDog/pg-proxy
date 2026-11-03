package pool

import (
	"math"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestNext_Simple checks that RoundRobinCounter cycles through indexes
// in proper round-robin order under sequential access.
func TestNext_Simple(t *testing.T) {
	assert := assert.New(t)
	rr := &RoundRobinCounter{}
	size := 4

	for i := 1; i <= 5; i++ {
		assert.Equal(i%size, rr.Next(size))
	}
}

// TestNext_ReturnsMinusOneOnZeroSize verifies that calling Next with size <= 0 returns -1.
func TestNext_ReturnsMinusOneOnZeroSize(t *testing.T) {
	assert := assert.New(t)
	rr := &RoundRobinCounter{}

	assert.Equal(-1, rr.Next(0))
	assert.Equal(-1, rr.Next(-1))
}

// TestNext_OverflowWrapsCorrectly verifies that the internal counter wraps
// from MaxUint32 back to 0 cleanly without panicking or corrupting state.
// It checks both the returned index and the raw counter value.
func TestNext_OverflowWrapsCorrectly(t *testing.T) {
	assert := assert.New(t)

	const start = math.MaxUint32 - 2 // 4294967293
	const size = 10

	rr := NewRoundRobinCounterWithStart(start)

	// counter: 4294967294, index: 4294967294 % 10 = 4
	assert.Equal(4, rr.Next(size))
	assert.Equal(start+1, rr.Current())

	// counter: 4294967295, index: 4294967295 % 10 = 5
	assert.Equal(5, rr.Next(size))
	assert.Equal(start+2, rr.Current())

	// counter wraps to 0, index: 0 % 10 = 0
	assert.Equal(0, rr.Next(size))
	assert.Equal(0, rr.Current())

	// counter: 1, index: 1 % 10 = 1
	assert.Equal(1, rr.Next(size))
	assert.Equal(1, rr.Current())
}

// TestNext_ConcurrentDistributionIsExact verifies that under concurrent access,
// the counter still increments correctly and distributes calls in perfect round-robin order.
// This works because the counter uses atomic Add(1), so each call gets a unique sequential value.
// Since modulo ensures bounded values, and total is divisible by size, we expect an exact frequency.
func TestNext_ConcurrentDistributionIsExact(t *testing.T) {
	assert := assert.New(t)

	rr := &RoundRobinCounter{}
	size := 5
	total := 50 // must be divisible by size

	results := make([]int, total)

	var wg sync.WaitGroup
	var start sync.WaitGroup
	start.Add(1) // signal for goroutines to start together

	for i := 0; i < total; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			start.Wait()
			// Each goroutine stores its result in its own slice slot (no contention)
			results[i] = rr.Next(size)
		}(i)
	}

	start.Done()
	wg.Wait()

	// Count frequency of each returned index
	counts := make([]int, size)
	for _, idx := range results {
		counts[idx]++
	}

	// Each index should appear exactly total/size times
	for i := 0; i < size; i++ {
		assert.Equal(total/size, counts[i], "index %d appeared %d times, expected %d", i, counts[i], total/size)
	}
}
