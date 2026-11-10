package mirror

import (
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"

	"github.com/DataDog/pg-proxy/pkg/query"
)

// queuedTypes snapshots the item types currently queued, in FIFO order, without
// draining (dequeue would block on an empty queue and close would drop the tail).
func queuedTypes(q *mirrorQueue) []mirrorItemType {
	q.mu.Lock()
	defer q.mu.Unlock()
	types := make([]mirrorItemType, len(q.items))
	for i, it := range q.items {
		types[i] = it.typ
	}
	return types
}

func mirrorLinkOver(q *mirrorQueue) *mirroringLink {
	return newMirroringLink(&fakeShadowLink{}, q, nil)
}

// TestMirroringLink_DataBatchEnqueuesBoundaries: a batch that admits data enqueues
// its flush and release sentinels in order.
func TestMirroringLink_DataBatchEnqueuesBoundaries(t *testing.T) {
	q := newMirrorQueue(1<<20, nil)
	l := mirrorLinkOver(q)

	l.Send(&pgproto3.Query{String: "x"}, query.QueryReply)
	assert.NoError(t, l.Flush())
	assert.NoError(t, l.Release())

	assert.Equal(t, []mirrorItemType{mirrorMsg, mirrorFlush, mirrorRelease}, queuedTypes(q))
}

// TestMirroringLink_CoalescesEmptyFlush: a flush with no data buffered since the
// last one is skipped (an empty flush is a no-op on the shadow), but a flush after
// fresh data is enqueued again.
func TestMirroringLink_CoalescesEmptyFlush(t *testing.T) {
	q := newMirrorQueue(1<<20, nil)
	l := mirrorLinkOver(q)

	l.Send(&pgproto3.Query{String: "x"}, query.QueryReply)
	assert.NoError(t, l.Flush()) // has data -> enqueued
	assert.NoError(t, l.Flush()) // no new data -> coalesced away
	l.Send(&pgproto3.Query{String: "y"}, query.QueryReply)
	assert.NoError(t, l.Flush()) // fresh data -> enqueued again

	assert.Equal(t, []mirrorItemType{mirrorMsg, mirrorFlush, mirrorMsg, mirrorFlush}, queuedTypes(q))
}

// TestMirroringLink_EmptyBatchEnqueuesNothing: a batch that sends no mirrorable data
// and drops nothing (so it isn't poisoned) never makes the worker acquire a link, so
// its flush and release sentinels are skipped and nothing is queued.
func TestMirroringLink_EmptyBatchEnqueuesNothing(t *testing.T) {
	q := newMirrorQueue(1<<20, nil)
	l := mirrorLinkOver(q)

	assert.NoError(t, l.Flush()) // no data buffered
	assert.NoError(t, l.Release())

	assert.Empty(t, queuedTypes(q), "an empty, unpoisoned batch must not queue sentinels")
}

// TestMirroringLink_ReleaseGuaranteedWhenFull: once a batch has admitted data, its
// release sentinel is admitted even though the queue is full — this is the fix for
// the stranded-shadow-link boomerang. The queue is intentionally left over its cap.
func TestMirroringLink_ReleaseGuaranteedWhenFull(t *testing.T) {
	dataCost := sizeOfFrontendMessage(&pgproto3.Query{String: "x"}) + mirrorItemStructOverhead
	q := newMirrorQueue(dataCost, nil) // room for the data message, none for the sentinel
	l := mirrorLinkOver(q)

	l.Send(&pgproto3.Query{String: "x"}, query.QueryReply)
	assert.NoError(t, l.Release())

	assert.Equal(t, []mirrorItemType{mirrorMsg, mirrorRelease}, queuedTypes(q))
	bytes, dropped := q.stats()
	assert.Greater(t, bytes, dataCost, "the guaranteed sentinel pushes the queue over its cap")
	assert.Zero(t, dropped, "the sentinel is admitted, not dropped")
}

// TestMirroringLink_DropPoisonsBatch: a dropped message enqueues a guaranteed poison
// sentinel (once per batch) so the worker skips the whole batch, and the release
// still follows so the skip resets.
func TestMirroringLink_DropPoisonsBatch(t *testing.T) {
	dataCost := sizeOfFrontendMessage(&pgproto3.Query{String: "x"}) + mirrorItemStructOverhead
	q := newMirrorQueue(dataCost, nil) // room for one data message only
	l := mirrorLinkOver(q)

	l.Send(&pgproto3.Query{String: "x"}, query.QueryReply) // admitted
	l.Send(&pgproto3.Query{String: "y"}, query.QueryReply) // dropped -> poison
	l.Send(&pgproto3.Query{String: "z"}, query.QueryReply) // dropped -> already poisoned, no second poison
	assert.NoError(t, l.Release())

	assert.Equal(t, []mirrorItemType{mirrorMsg, mirrorPoison, mirrorRelease}, queuedTypes(q))
}

// TestMirroringLink_DataLessPoisonedBatchEnqueuesNothing: a batch whose messages were
// all dropped never made the worker acquire a link, so there is nothing to skip or
// release. It queues no control items, keeping guaranteed sentinels bounded by the
// byte cap even under a flood of dropped batches.
func TestMirroringLink_DataLessPoisonedBatchEnqueuesNothing(t *testing.T) {
	q := newMirrorQueue(1, nil) // smaller than any item, so every data message drops
	l := mirrorLinkOver(q)

	l.Send(&pgproto3.Query{String: "x"}, query.QueryReply) // dropped -> poisoned, nothing queued
	l.Send(&pgproto3.Query{String: "y"}, query.QueryReply) // poisoned -> not even cloned
	assert.NoError(t, l.Release())

	assert.Empty(t, queuedTypes(q), "a fully-dropped batch must not force control items past the cap")
}

// A Parse the session did not forward reaches the queue with the ParseReply
// expectation the worker's drain needs. A plain link offers no such entry point.
func TestMirroringLink_MirrorParseEnqueuesWithParseReply(t *testing.T) {
	q := newMirrorQueue(1<<20, nil)
	l := mirrorLinkOver(q)

	m, ok := any(l).(StmtDefMirror)
	assert.True(t, ok, "mirroringLink must satisfy StmtDefMirror")
	_, plainIsMirror := any(&fakeShadowLink{}).(StmtDefMirror)
	assert.False(t, plainIsMirror, "a plain link must not satisfy StmtDefMirror")

	m.MirrorParse(&pgproto3.Parse{Name: "s1", Query: "select 1"})
	assert.NoError(t, l.Flush())

	assert.Equal(t, []mirrorItemType{mirrorMsg, mirrorFlush}, queuedTypes(q))
	it, ok := q.dequeue()
	assert.True(t, ok)
	assert.Equal(t, query.ParseReply, it.expectation)
	assert.IsType(t, &pgproto3.Parse{}, it.msg)
}
