// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package mirror

import (
	"sync"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/DataDog/pg-proxy/pkg/link"
	"github.com/DataDog/pg-proxy/pkg/query"
)

// mirrorItemOverhead approximates the pgproto3 wire header (type byte + length);
// also the full cost of a body-less Sync/Flush.
const mirrorItemOverhead = 5

// mirrorItemStructOverhead is charged per item on top of its wire size so the byte
// budget bounds real memory even under a flood of tiny messages.
const mirrorItemStructOverhead = 64

// mirrorSliceHeaderOverhead approximates a slice header (ptr+len+cap) on 64-bit,
// charged per Bind parameter since the clone keeps a [][]byte slot for each.
const mirrorSliceHeaderOverhead = 24

// pgproto3 wire widths, named so the size estimate below and its test share one
// source of truth instead of scattering magic numbers.
const (
	oidWireSize        = 4 // uint32 parameter OID (Parse)
	formatCodeWireSize = 2 // int16 format code (Bind)
	paramLenPrefixSize = 4 // int32 length prefix per Bind parameter (Bind)
	objectTypeByteSize = 1 // 'S'/'P' object-type byte (Describe/Close)
	maxRowsWireSize    = 4 // uint32 max-rows (Execute)
)

// mirrorItemType distinguishes a replayable message from the boundary sentinels
// the shadow worker acts on. Named typ on the item since type is a keyword.
type mirrorItemType uint8

const (
	mirrorMsg     mirrorItemType = iota // a cloned frontend message to replay
	mirrorFlush                         // the source flushed here; worker flushes + drains replies
	mirrorRelease                       // the source released its link; worker returns its shadow link
	mirrorPoison                        // a message in this batch was dropped; worker skips the rest
)

// mirrorItem is one queue entry: a cloned forwarded message, or a boundary
// sentinel. size is its cost against the byte budget.
type mirrorItem struct {
	msg         pgproto3.FrontendMessage // nil for a sentinel
	expectation query.ReplyExpectation
	typ         mirrorItemType
	size        int
	marks       link.SessionMarks // for a release sentinel: the source checkout's session marks
}

// enqueueOutcome reports why an enqueue was accepted or dropped, so the caller can
// emit a reason-tagged metric.
type enqueueOutcome int

const (
	mirrorEnqueued            enqueueOutcome = iota // accepted
	mirrorDroppedSessionCap                         // over this session's byte cap (or closed)
	mirrorDroppedGlobalBudget                       // over the shared process-wide budget
)

// mirrorQueue is a per-session, byte-bounded (config.Mirroring.QueueSize),
// best-effort FIFO of messages forwarded to the source, drained by a future
// shadow worker. Enqueue never blocks; an over-budget item is dropped and counted.
// Bounded by bytes, not message count, because messages span ~5 B to several MB.
// Every accepted item is also charged against the shared, process-wide budget so
// many mirrored sessions can't collectively OOM the proxy.
type mirrorQueue struct {
	mu        sync.Mutex
	cond      *sync.Cond // signalled on enqueue, broadcast on close
	items     []mirrorItem
	byteCount int // bytes of items still queued (not yet dequeued)
	inFlight  int // bytes dequeued by the worker but not yet flushed/released
	byteCap   int
	closed    bool
	dropped   uint64
	budget    *Budget // shared across all sessions; nil means unbounded (tests)
}

func newMirrorQueue(byteCap int, budget *Budget) *mirrorQueue {
	q := &mirrorQueue{byteCap: byteCap, budget: budget}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// Queue is a per-session, byte-bounded, non-blocking mirror queue.
type Queue = mirrorQueue

// NewQueue constructs a queue backed by the process-wide budget.
func NewQueue(byteCap int, budget *Budget) *Queue { return newMirrorQueue(byteCap, budget) }

// Close stops producers and wakes the worker.
func (q *mirrorQueue) Close() { q.close() }

// tryEnqueue appends it if it fits both the session and global budgets; else drops
// and counts. Never blocks. Returns whether it was enqueued.
func (q *mirrorQueue) tryEnqueue(it mirrorItem) bool {
	return q.tryEnqueueFunc(it.size, func() mirrorItem { return it }) == mirrorEnqueued
}

// tryEnqueueFunc reserves budget for an item of the given size and calls build to
// construct it only if it fits, so callers can defer an expensive clone until
// capacity is confirmed. Charges mirrorItemStructOverhead on top, against this
// session's cap (which also counts in-flight bytes, so a no-flush session can't
// admit past it) and the shared budget. Never blocks.
func (q *mirrorQueue) tryEnqueueFunc(size int, build func() mirrorItem) enqueueOutcome {
	cost := size + mirrorItemStructOverhead
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed || q.byteCount+q.inFlight+cost > q.byteCap {
		q.dropped++
		return mirrorDroppedSessionCap
	}
	if q.budget != nil && !q.budget.tryReserve(cost) {
		q.dropped++
		return mirrorDroppedGlobalBudget
	}
	q.items = append(q.items, build())
	q.byteCount += cost
	q.cond.Signal()
	return mirrorEnqueued
}

// enqueueSentinel appends a control sentinel (flush/release), bypassing the cap and
// budget so a full queue can never drop a batch boundary (a dropped release would
// strand the worker's shadow link + budget until session close). Bounded: only
// data-bearing batches enqueue one. Returns false only if the queue is closed.
func (q *mirrorQueue) enqueueSentinel(it mirrorItem) bool {
	cost := it.size + mirrorItemStructOverhead
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return false
	}
	if q.budget != nil {
		q.budget.forceReserve(cost)
	}
	q.items = append(q.items, it)
	q.byteCount += cost
	q.cond.Signal()
	return true
}

// dequeue blocks for the next item, FIFO; ok is false once the queue is closed and
// drained. It moves the item's bytes from byteCount to inFlight (still counted until
// the consumer releases them, since the clone stays buffered on the shadow link).
func (q *mirrorQueue) dequeue() (mirrorItem, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.items) == 0 && !q.closed {
		q.cond.Wait()
	}
	if len(q.items) == 0 {
		return mirrorItem{}, false // closed and drained
	}
	it := q.items[0]
	q.items[0] = mirrorItem{} // drop the clone reference so it can be GC'd
	q.items = q.items[1:]
	cost := it.size + mirrorItemStructOverhead
	q.byteCount -= cost
	q.inFlight += cost
	if len(q.items) == 0 {
		q.items = nil // don't pin the backing array on an idle queue
	}
	return it, true
}

// tryReserveRaw reserves n bytes the worker produced itself (a Parse it injected),
// which never passed through tryEnqueueFunc. Without it the cap stops bounding the
// shadow buffer. Released with the batch, like any dequeued item's reservation.
func (q *mirrorQueue) tryReserveRaw(n int) bool {
	if n <= 0 {
		return true
	}
	q.mu.Lock()
	if q.byteCount+q.inFlight+n > q.byteCap {
		q.mu.Unlock()
		return false
	}
	q.inFlight += n
	q.mu.Unlock()
	if q.budget != nil && !q.budget.tryReserve(n) {
		q.mu.Lock()
		q.inFlight -= n
		q.mu.Unlock()
		return false
	}
	return true
}

// releaseBudget releases one finished item's bytes (size + overhead) from in-flight and the budget.
func (q *mirrorQueue) releaseBudget(size int) {
	q.releaseInFlight(size + mirrorItemStructOverhead)
}

// releaseBudgetRaw returns n bytes (a batch's held reservation) to the shared budget.
func (q *mirrorQueue) releaseBudgetRaw(n int) {
	q.releaseInFlight(n)
}

// releaseInFlight returns n in-flight bytes (dequeued but not yet flushed) to the
// per-session reservation and the shared budget. Pairs with the charge dequeue adds.
func (q *mirrorQueue) releaseInFlight(n int) {
	if n <= 0 {
		return
	}
	q.mu.Lock()
	q.inFlight -= n
	q.mu.Unlock()
	if q.budget != nil {
		q.budget.release(n)
	}
}

// close rejects further enqueues, wakes any blocked consumer, and drops the
// undrained tail — returning its bytes to the shared budget and freeing the clones
// together (freeing one without the other would unbound the cap). Idempotent.
func (q *mirrorQueue) close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.closed = true
	if q.budget != nil {
		q.budget.release(q.byteCount)
	}
	q.byteCount = 0
	q.items = nil // free the cloned payloads for GC in lockstep with the budget release
	q.cond.Broadcast()
}

// stats returns the queued byte count and the cumulative dropped count.
func (q *mirrorQueue) stats() (queuedBytes int, dropped uint64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.byteCount, q.dropped
}

// cloneFrontendMessage deep-copies msg so the queued copy survives the next
// clientConn.Receive(), which reuses one buffer and clobbers slice fields. Only
// slice fields need copying (strings are immutable); unsupported types return nil.
func cloneFrontendMessage(msg pgproto3.FrontendMessage) pgproto3.FrontendMessage {
	switch m := msg.(type) {
	case *pgproto3.Query:
		c := *m
		return &c
	case *pgproto3.Parse:
		c := *m
		c.ParameterOIDs = append([]uint32(nil), m.ParameterOIDs...)
		return &c
	case *pgproto3.Bind:
		c := *m
		c.ParameterFormatCodes = append([]int16(nil), m.ParameterFormatCodes...)
		c.ResultFormatCodes = append([]int16(nil), m.ResultFormatCodes...)
		if m.Parameters != nil {
			c.Parameters = make([][]byte, len(m.Parameters))
			for i, p := range m.Parameters {
				if p != nil {
					// make (not append-to-nil) so a non-NULL empty parameter stays a
					// non-nil zero-length slice: pgproto3 encodes nil as NULL(-1) but
					// an empty slice as length 0, and the shadow replay must preserve that.
					c.Parameters[i] = make([]byte, len(p))
					copy(c.Parameters[i], p)
				}
			}
		}
		return &c
	case *pgproto3.Describe:
		c := *m
		return &c
	case *pgproto3.Execute:
		c := *m
		return &c
	case *pgproto3.Close:
		c := *m
		return &c
	case *pgproto3.Sync:
		return &pgproto3.Sync{}
	case *pgproto3.Flush:
		return &pgproto3.Flush{}
	case *pgproto3.CopyData:
		c := *m
		c.Data = append([]byte(nil), m.Data...)
		return &c
	case *pgproto3.CopyDone:
		return &pgproto3.CopyDone{}
	case *pgproto3.CopyFail:
		c := *m // Message is a string, immutable
		return &c
	default:
		return nil
	}
}

// canMirror reports whether the queue records msg's type — the set
// cloneFrontendMessage copies — so the enqueue path can skip others early.
func canMirror(msg pgproto3.FrontendMessage) bool {
	switch msg.(type) {
	case *pgproto3.Query, *pgproto3.Parse, *pgproto3.Bind, *pgproto3.Describe,
		*pgproto3.Execute, *pgproto3.Close, *pgproto3.Sync, *pgproto3.Flush,
		*pgproto3.CopyData, *pgproto3.CopyDone, *pgproto3.CopyFail:
		return true
	default:
		return false
	}
}

// sizeOfFrontendMessage cheaply estimates msg's encoded byte size for byte
// accounting — an estimate, not exact: encoding to measure would cost the hot path.
func sizeOfFrontendMessage(msg pgproto3.FrontendMessage) int {
	switch m := msg.(type) {
	case *pgproto3.Query:
		return mirrorItemOverhead + len(m.String)
	case *pgproto3.Parse:
		return mirrorItemOverhead + len(m.Name) + len(m.Query) + oidWireSize*len(m.ParameterOIDs)
	case *pgproto3.Bind:
		n := mirrorItemOverhead + len(m.DestinationPortal) + len(m.PreparedStatement)
		n += formatCodeWireSize * len(m.ParameterFormatCodes)
		n += formatCodeWireSize * len(m.ResultFormatCodes)
		for _, p := range m.Parameters {
			// wire length prefix + the []byte header the clone retains per parameter
			n += len(p) + paramLenPrefixSize + mirrorSliceHeaderOverhead
		}
		return n
	case *pgproto3.Describe:
		return mirrorItemOverhead + objectTypeByteSize + len(m.Name)
	case *pgproto3.Execute:
		return mirrorItemOverhead + len(m.Portal) + maxRowsWireSize
	case *pgproto3.Close:
		return mirrorItemOverhead + objectTypeByteSize + len(m.Name)
	case *pgproto3.CopyData:
		return mirrorItemOverhead + len(m.Data)
	case *pgproto3.CopyFail:
		return mirrorItemOverhead + len(m.Message)
	default:
		// Sync, Flush, CopyDone, and any other body-less message.
		return mirrorItemOverhead
	}
}
