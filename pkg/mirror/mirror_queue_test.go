package mirror

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMirrorQueue_FIFOOrder(t *testing.T) {
	q := newMirrorQueue(1024, nil)
	queries := []string{"one", "two", "three"}
	for _, s := range queries {
		assert.True(t, q.tryEnqueue(mirrorItem{msg: &pgproto3.Query{String: s}, size: 10}), "enqueue %q dropped unexpectedly", s)
	}
	require.Len(t, q.items, len(queries))
	for i, s := range queries {
		gotMsg, ok := q.items[i].msg.(*pgproto3.Query)
		require.True(t, ok, "item %d is not a *Query", i)
		assert.Equal(t, s, gotMsg.String, "item %d out of order", i)
	}
}

func TestMirrorQueue_DropNewestOnByteCap(t *testing.T) {
	const item = 10
	cost := item + mirrorItemStructOverhead // each entry is charged size + object overhead
	q := newMirrorQueue(2*cost, nil)        // fits two items, not a third
	for i := 0; i < 2; i++ {
		assert.True(t, q.tryEnqueue(mirrorItem{msg: &pgproto3.Query{String: "q"}, size: item}), "enqueue %d dropped unexpectedly", i)
	}
	assert.False(t, q.tryEnqueue(mirrorItem{msg: &pgproto3.Query{String: "q"}, size: item}), "third enqueue should have been dropped (over byte cap)")

	bytes, dropped := q.stats()
	assert.Equal(t, 2*cost, bytes, "dropped item must not count toward queued bytes")
	assert.Equal(t, uint64(1), dropped)
	assert.Len(t, q.items, 2)
}

// TestMirrorQueue_GlobalBudgetSharedAcrossQueues verifies the shared budget bounds
// the aggregate: a session with room under its own cap is still dropped (reason
// global) once the shared budget is exhausted, and close() returns the bytes so
// another session can proceed.
func TestMirrorQueue_GlobalBudgetSharedAcrossQueues(t *testing.T) {
	const size = 10
	cost := size + mirrorItemStructOverhead
	budget := newMirrorBudget(int64(cost)) // room for exactly one item across all queues
	build := func() mirrorItem { return mirrorItem{msg: &pgproto3.Query{String: "q"}, size: size} }

	// Two sessions sharing the budget, each with ample per-session cap.
	q1 := newMirrorQueue(100*cost, budget)
	q2 := newMirrorQueue(100*cost, budget)

	assert.Equal(t, mirrorEnqueued, q1.tryEnqueueFunc(size, build), "first item fits the shared budget")
	assert.Equal(t, mirrorDroppedGlobalBudget, q2.tryEnqueueFunc(size, build), "shared budget exhausted despite q2's own cap having room")

	q1.close() // returns q1's reservation to the shared budget and frees its clones
	assert.EqualValues(t, 0, budget.UsedBytes(), "close must release the queue's reservation")
	assert.Nil(t, q1.items, "close must drop the queued clones so freed budget doesn't outlive live payloads")
	assert.Equal(t, mirrorEnqueued, q2.tryEnqueueFunc(size, build), "budget freed by close, q2 now fits")
}

func TestMirrorQueue_CloseRejectsAndIsIdempotent(t *testing.T) {
	q := newMirrorQueue(1024, nil)
	q.close()
	assert.False(t, q.tryEnqueue(mirrorItem{msg: &pgproto3.Query{String: "q"}, size: 10}), "enqueue after close should be rejected")
	assert.NotPanics(t, q.close, "close must be idempotent")
	_, dropped := q.stats()
	assert.Equal(t, uint64(1), dropped)
}

// TestMirrorQueue_EnqueueSentinelBypassesCapAndBudget verifies control sentinels
// are admitted even when both the per-session cap and the shared budget are full,
// so a saturated queue can never drop a batch boundary — while a closed queue still
// rejects them.
func TestMirrorQueue_EnqueueSentinelBypassesCapAndBudget(t *testing.T) {
	sentinel := mirrorItemOverhead + mirrorItemStructOverhead
	budget := newMirrorBudget(1)   // already effectively full
	q := newMirrorQueue(1, budget) // cap smaller than a single sentinel

	assert.True(t, q.enqueueSentinel(mirrorItem{typ: mirrorRelease, size: mirrorItemOverhead}), "sentinel must be admitted despite a full cap and budget")
	bytes, dropped := q.stats()
	assert.Equal(t, sentinel, bytes, "sentinel bytes must be charged so the worker returns them on drain")
	assert.EqualValues(t, sentinel, budget.UsedBytes(), "sentinel must be force-reserved against the shared budget")
	assert.Zero(t, dropped, "a guaranteed sentinel is never a drop")

	q.close()
	assert.False(t, q.enqueueSentinel(mirrorItem{typ: mirrorRelease, size: mirrorItemOverhead}), "a closed queue rejects sentinels")
}

// TestMirrorQueue_InFlightCountsAgainstCap verifies dequeued-but-unreleased bytes
// still count against the per-session cap, so a session pipelining a long batch
// without a flush can't keep admitting past its cap once the worker has drained the
// queue into the shadow buffer.
func TestMirrorQueue_InFlightCountsAgainstCap(t *testing.T) {
	const itemSize = 10
	cost := itemSize + mirrorItemStructOverhead
	q := newMirrorQueue(2*cost, nil) // room for two items
	build := func() mirrorItem { return mirrorItem{msg: &pgproto3.Query{String: "q"}, size: itemSize} }

	assert.Equal(t, mirrorEnqueued, q.tryEnqueueFunc(itemSize, build))
	assert.Equal(t, mirrorEnqueued, q.tryEnqueueFunc(itemSize, build))

	it, ok := q.dequeue() // frees byteCount but the item is now in flight
	require.True(t, ok)
	bytes, _ := q.stats()
	assert.Equal(t, cost, bytes, "one item still queued after dequeuing the other")

	assert.Equal(t, mirrorDroppedSessionCap, q.tryEnqueueFunc(itemSize, build),
		"in-flight + queued already fill the cap, so a third item must be dropped")

	q.releaseBudget(it.size) // consumer done with the in-flight item
	assert.Equal(t, mirrorEnqueued, q.tryEnqueueFunc(itemSize, build),
		"releasing the in-flight item frees cap room again")
}

// The TestCloneFrontendMessage_* tests verify a queued copy is unaffected when the
// original message's backing arrays are mutated (as clientConn.Receive reuses its
// buffer for the next message).

func TestCloneFrontendMessage_BindParametersOwned(t *testing.T) {
	orig := &pgproto3.Bind{Parameters: [][]byte{[]byte("abc")}}
	cloned, ok := cloneFrontendMessage(orig).(*pgproto3.Bind)
	require.True(t, ok, "clone did not return a *Bind")
	orig.Parameters[0][0] = 'X' // mutate the original's backing array
	assert.Equal(t, "abc", string(cloned.Parameters[0]), "clone must own its bytes")
}

func TestCloneFrontendMessage_EmptyBindParameterStaysNonNil(t *testing.T) {
	// A non-NULL empty parameter (e.g. '') must not become nil: pgproto3 encodes
	// nil as NULL(-1) but an empty slice as length 0, so collapsing it to nil would
	// replay an empty value as NULL on the shadow.
	orig := &pgproto3.Bind{Parameters: [][]byte{{}, nil}}
	cloned, ok := cloneFrontendMessage(orig).(*pgproto3.Bind)
	require.True(t, ok, "clone did not return a *Bind")
	assert.NotNil(t, cloned.Parameters[0], "empty parameter must stay non-nil zero-length (empty != NULL)")
	assert.Nil(t, cloned.Parameters[1], "NULL parameter must stay nil")
}

func TestCloneFrontendMessage_ParseOIDsOwned(t *testing.T) {
	orig := &pgproto3.Parse{ParameterOIDs: []uint32{1, 2, 3}}
	cloned, ok := cloneFrontendMessage(orig).(*pgproto3.Parse)
	require.True(t, ok, "clone did not return a *Parse")
	orig.ParameterOIDs[0] = 99
	assert.Equal(t, uint32(1), cloned.ParameterOIDs[0], "clone must own its OIDs")
}

func TestCloneFrontendMessage_CopyDataOwned(t *testing.T) {
	orig := &pgproto3.CopyData{Data: []byte("abc")}
	cloned, ok := cloneFrontendMessage(orig).(*pgproto3.CopyData)
	require.True(t, ok, "clone did not return a *CopyData")
	orig.Data[0] = 'X' // mutate the original's backing array
	assert.Equal(t, "abc", string(cloned.Data), "clone must own its bytes")
}

func TestCloneFrontendMessage_UnsupportedReturnsNil(t *testing.T) {
	assert.Nil(t, cloneFrontendMessage(&pgproto3.Terminate{}), "clone of unsupported message should be nil")
	assert.False(t, canMirror(&pgproto3.Terminate{}), "canMirror(Terminate) should be false")
}

func TestSizeOfFrontendMessage(t *testing.T) {
	tests := []struct {
		name string
		msg  pgproto3.FrontendMessage
		want int
	}{
		{"query", &pgproto3.Query{String: "select 1"}, mirrorItemOverhead + len("select 1")},
		{"empty query", &pgproto3.Query{}, mirrorItemOverhead},
		{"sync sentinel", &pgproto3.Sync{}, mirrorItemOverhead},
		{"flush sentinel", &pgproto3.Flush{}, mirrorItemOverhead},
		{
			"bind with params",
			&pgproto3.Bind{Parameters: [][]byte{[]byte("ab"), []byte("cde")}},
			mirrorItemOverhead +
				(len("ab") + paramLenPrefixSize + mirrorSliceHeaderOverhead) +
				(len("cde") + paramLenPrefixSize + mirrorSliceHeaderOverhead),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, sizeOfFrontendMessage(tc.msg))
		})
	}

	// A larger payload must never estimate smaller than a shorter one.
	small := sizeOfFrontendMessage(&pgproto3.Query{String: "a"})
	large := sizeOfFrontendMessage(&pgproto3.Query{String: "aaaaaaaaaa"})
	assert.Greater(t, large, small, "larger message must not estimate smaller than a shorter one")
}

// TestMirrorQueue_ConcurrentEnqueue exercises the lock under the race detector
// and checks the byte accounting stays consistent with what was accepted.
func TestMirrorQueue_ConcurrentEnqueue(t *testing.T) {
	const goroutines, perG, itemSize = 8, 200, 10
	cost := itemSize + mirrorItemStructOverhead    // charged per accepted entry
	q := newMirrorQueue(goroutines*perG*cost, nil) // large enough to accept all

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				q.tryEnqueue(mirrorItem{msg: &pgproto3.Query{String: "q"}, size: itemSize})
			}
		}()
	}
	wg.Wait()

	bytes, dropped := q.stats()
	assert.Equal(t, uint64(0), dropped, "cap was large enough, nothing should drop")
	assert.Equal(t, goroutines*perG*cost, bytes)
	assert.Len(t, q.items, goroutines*perG)
}

// TestMirrorQueue_DequeueFIFOAndByteAccounting checks dequeue pops in order and
// returns the exact budget (size + struct overhead) to byteCount, so a fully
// drained queue reads zero bytes.
func TestMirrorQueue_DequeueFIFOAndByteAccounting(t *testing.T) {
	const itemSize = 10
	q := newMirrorQueue(100*(itemSize+mirrorItemStructOverhead), nil)
	queries := []string{"one", "two", "three"}
	for _, s := range queries {
		require.True(t, q.tryEnqueue(mirrorItem{msg: &pgproto3.Query{String: s}, size: itemSize}), "enqueue %q dropped", s)
	}
	for i, s := range queries {
		it, ok := q.dequeue()
		require.True(t, ok, "dequeue %d returned ok=false", i)
		gotMsg, isQuery := it.msg.(*pgproto3.Query)
		require.True(t, isQuery, "dequeue %d not a *Query", i)
		assert.Equal(t, s, gotMsg.String, "dequeue %d out of order", i)
	}
	bytes, _ := q.stats()
	assert.Equal(t, 0, bytes, "queued bytes must return to 0 after draining all")
}

// TestMirrorQueue_ReleaseBudgetReturnsBytes is the regression test for the
// consumer/global-budget interaction: dequeue alone must NOT return bytes to the
// shared budget (the caller still holds the clone); releaseBudget does, once the
// caller is done. Otherwise the process-wide cap leaks and eventually rejects all
// mirroring.
func TestMirrorQueue_ReleaseBudgetReturnsBytes(t *testing.T) {
	const itemSize, n = 10, 5
	cost := itemSize + mirrorItemStructOverhead
	budget := newMirrorBudget(int64(n * cost))
	q := newMirrorQueue(100*cost, budget)
	for i := 0; i < n; i++ {
		require.True(t, q.tryEnqueue(mirrorItem{msg: &pgproto3.Query{String: "q"}, size: itemSize}), "enqueue %d dropped", i)
	}
	assert.EqualValues(t, n*cost, budget.UsedBytes(), "budget should be fully reserved before draining")

	items := make([]mirrorItem, 0, n)
	for i := 0; i < n; i++ {
		it, ok := q.dequeue()
		require.True(t, ok)
		items = append(items, it)
	}
	assert.EqualValues(t, n*cost, budget.UsedBytes(), "dequeue must NOT release the budget while the caller holds the clones")

	for _, it := range items {
		q.releaseBudget(it.size)
	}
	assert.EqualValues(t, 0, budget.UsedBytes(), "releaseBudget must return every finished item's bytes to the shared budget")
}

// TestMirrorQueue_DequeueBlocksUntilEnqueue verifies a consumer blocks on an empty
// queue and wakes when an item is enqueued.
func TestMirrorQueue_DequeueBlocksUntilEnqueue(t *testing.T) {
	q := newMirrorQueue(1024, nil)
	got := make(chan mirrorItem, 1)
	go func() {
		if it, ok := q.dequeue(); ok {
			got <- it
		}
	}()

	select {
	case <-got:
		t.Fatal("dequeue returned before any enqueue")
	case <-time.After(20 * time.Millisecond):
	}

	q.tryEnqueue(mirrorItem{msg: &pgproto3.Query{String: "q"}, size: 10})
	select {
	case it := <-got:
		m, ok := it.msg.(*pgproto3.Query)
		require.True(t, ok)
		assert.Equal(t, "q", m.String)
	case <-time.After(time.Second):
		t.Fatal("dequeue did not wake after enqueue")
	}
}

// TestMirrorQueue_CloseWakesConsumerAndReleasesBudget verifies close wakes a
// blocked consumer so it exits, and returns the queue's outstanding bytes to the
// shared budget (any undrained tail is dropped, not replayed).
func TestMirrorQueue_CloseWakesConsumerAndReleasesBudget(t *testing.T) {
	const itemSize = 10
	cost := itemSize + mirrorItemStructOverhead
	budget := newMirrorBudget(100 * int64(cost))
	q := newMirrorQueue(100*cost, budget)
	for i := 0; i < 3; i++ {
		require.True(t, q.tryEnqueue(mirrorItem{msg: &pgproto3.Query{String: "q"}, size: itemSize}))
	}

	done := make(chan bool, 1)
	go func() {
		for {
			it, ok := q.dequeue()
			if !ok {
				break
			}
			q.releaseBudget(it.size)
		}
		done <- true
	}()

	q.close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("close did not wake the consumer to exit")
	}
	assert.EqualValues(t, 0, budget.UsedBytes(), "close must return outstanding bytes to the shared budget")
}

// TestMirrorQueue_ConcurrentDrainPreservesOrder runs a producer and consumer
// concurrently (under -race), checking strict FIFO and that both byte accounts
// return to zero after a full drain. It drains a known count rather than relying
// on close, because close drops the undrained tail (see
// TestMirrorQueue_CloseWakesConsumerAndReleasesBudget).
func TestMirrorQueue_ConcurrentDrainPreservesOrder(t *testing.T) {
	const total, itemSize = 5000, 10
	cost := itemSize + mirrorItemStructOverhead
	budget := newMirrorBudget(int64((total + 10) * cost))
	q := newMirrorQueue((total+10)*cost, budget)

	go func() {
		for i := 0; i < total; i++ {
			for !q.tryEnqueue(mirrorItem{msg: &pgproto3.Query{String: strconv.Itoa(i)}, size: itemSize}) {
			}
		}
	}()

	prev := -1
	for count := 0; count < total; count++ {
		it, ok := q.dequeue()
		require.True(t, ok, "dequeue returned false before draining all items")
		n, err := strconv.Atoi(it.msg.(*pgproto3.Query).String)
		require.NoError(t, err)
		require.Equal(t, prev+1, n, "out of order")
		prev = n
		q.releaseBudget(it.size)
	}
	bytes, _ := q.stats()
	assert.Equal(t, 0, bytes, "queued bytes must return to 0")
	assert.EqualValues(t, 0, budget.UsedBytes(), "shared budget must return to 0 after full drain")
}

// TestMirrorQueue_DrainToEmptyReleasesBackingArray: draining to empty must drop the
// backing array, not leave a zero-length slice pinning it.
func TestMirrorQueue_DrainToEmptyReleasesBackingArray(t *testing.T) {
	q := newMirrorQueue(1024, nil)
	for i := 0; i < 3; i++ {
		require.True(t, q.tryEnqueue(mirrorItem{msg: &pgproto3.Query{String: "q"}, size: 10}))
	}
	for i := 0; i < 3; i++ {
		_, ok := q.dequeue()
		require.True(t, ok)
	}
	assert.Nil(t, q.items, "draining to empty must release the backing array")
}
