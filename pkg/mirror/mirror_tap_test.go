// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package mirror

import (
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/DataDog/pg-proxy/pkg/link"
	"github.com/DataDog/pg-proxy/pkg/query"
)

// queueCapRoomy is a session byte cap large enough that nothing is dropped, for the
// tests whose subject is not admission control.
const queueCapRoomy = 1 << 20 // 1 MiB

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

// TestTap_DataBatchEnqueuesBoundaries: a batch that admits data enqueues
// its flush and release sentinels in order.
func TestTap_DataBatchEnqueuesBoundaries(t *testing.T) {
	q := newMirrorQueue(queueCapRoomy, nil)
	tap := NewTap(q, nil)

	tap.OnSend(&pgproto3.Query{String: "x"}, query.QueryReply)
	tap.OnFlushSucceeded()
	tap.OnRelease()

	assert.Equal(t, []mirrorItemType{mirrorMsg, mirrorFlush, mirrorRelease}, queuedTypes(q))
}

// TestTap_CoalescesEmptyFlush: a flush with no data buffered since the
// last one is skipped (an empty flush is a no-op on the shadow), but a flush after
// fresh data is enqueued again.
func TestTap_CoalescesEmptyFlush(t *testing.T) {
	q := newMirrorQueue(queueCapRoomy, nil)
	tap := NewTap(q, nil)

	tap.OnSend(&pgproto3.Query{String: "x"}, query.QueryReply)
	tap.OnFlushSucceeded() // has data -> enqueued
	tap.OnFlushSucceeded() // no new data -> coalesced away
	tap.OnSend(&pgproto3.Query{String: "y"}, query.QueryReply)
	tap.OnFlushSucceeded() // fresh data -> enqueued again

	assert.Equal(t, []mirrorItemType{mirrorMsg, mirrorFlush, mirrorMsg, mirrorFlush}, queuedTypes(q))
}

// TestTap_EmptyBatchEnqueuesNothing: a batch that sends no mirrorable data
// and drops nothing (so it isn't poisoned) never makes the worker acquire a link, so
// its flush and release sentinels are skipped and nothing is queued.
func TestTap_EmptyBatchEnqueuesNothing(t *testing.T) {
	q := newMirrorQueue(queueCapRoomy, nil)
	tap := NewTap(q, nil)

	tap.OnFlushSucceeded() // no data buffered
	tap.OnRelease()

	assert.Empty(t, queuedTypes(q), "an empty, unpoisoned batch must not queue sentinels")
}

// TestTap_ReleaseGuaranteedWhenFull: once a batch has admitted data, its
// release sentinel is admitted even though the queue is full — this is the fix for
// the stranded-shadow-link boomerang. The queue is intentionally left over its cap.
func TestTap_ReleaseGuaranteedWhenFull(t *testing.T) {
	dataCost := sizeOfFrontendMessage(&pgproto3.Query{String: "x"}) + mirrorItemStructOverhead
	q := newMirrorQueue(dataCost, nil) // room for the data message, none for the sentinel
	tap := NewTap(q, nil)

	tap.OnSend(&pgproto3.Query{String: "x"}, query.QueryReply)
	tap.OnRelease()

	assert.Equal(t, []mirrorItemType{mirrorMsg, mirrorRelease}, queuedTypes(q))
	bytes, dropped := q.stats()
	assert.Greater(t, bytes, dataCost, "the guaranteed sentinel pushes the queue over its cap")
	assert.Zero(t, dropped, "the sentinel is admitted, not dropped")
}

// TestTap_DropPoisonsBatch: a dropped message enqueues a guaranteed poison
// sentinel (once per batch) so the worker skips the whole batch, and the release
// still follows so the skip resets.
func TestTap_DropPoisonsBatch(t *testing.T) {
	dataCost := sizeOfFrontendMessage(&pgproto3.Query{String: "x"}) + mirrorItemStructOverhead
	q := newMirrorQueue(dataCost, nil) // room for one data message only
	tap := NewTap(q, nil)

	tap.OnSend(&pgproto3.Query{String: "x"}, query.QueryReply) // admitted
	tap.OnSend(&pgproto3.Query{String: "y"}, query.QueryReply) // dropped -> poison
	tap.OnSend(&pgproto3.Query{String: "z"}, query.QueryReply) // dropped -> already poisoned, no second poison
	tap.OnRelease()

	assert.Equal(t, []mirrorItemType{mirrorMsg, mirrorPoison, mirrorRelease}, queuedTypes(q))
}

// TestTap_DataLessPoisonedBatchEnqueuesNothing: a batch whose messages were
// all dropped never made the worker acquire a link, so there is nothing to skip or
// release. It queues no control items, keeping guaranteed sentinels bounded by the
// byte cap even under a flood of dropped batches.
func TestTap_DataLessPoisonedBatchEnqueuesNothing(t *testing.T) {
	q := newMirrorQueue(1, nil) // smaller than any item, so every data message drops
	tap := NewTap(q, nil)

	tap.OnSend(&pgproto3.Query{String: "x"}, query.QueryReply) // dropped -> poisoned, nothing queued
	tap.OnSend(&pgproto3.Query{String: "y"}, query.QueryReply) // poisoned -> not even cloned
	tap.OnRelease()

	assert.Empty(t, queuedTypes(q), "a fully-dropped batch must not force control items past the cap")
}

// TestTap_SuppressedParseEnqueuesWithParseReply verifies that a Parse the
// session did not forward reaches the queue with the ParseReply expectation
// the worker's drain needs.
func TestTap_SuppressedParseEnqueuesWithParseReply(t *testing.T) {
	q := newMirrorQueue(queueCapRoomy, nil)
	tap := NewTap(q, nil)

	tap.OnSuppressedParse(&pgproto3.Parse{Name: "s1", Query: "select 1"})
	tap.OnFlushSucceeded()

	assert.Equal(t, []mirrorItemType{mirrorMsg, mirrorFlush}, queuedTypes(q))
	it, ok := q.dequeue()
	assert.True(t, ok)
	assert.Equal(t, query.ParseReply, it.expectation)
	assert.IsType(t, &pgproto3.Parse{}, it.msg)
}

func TestTap_NilIsNoOp(t *testing.T) {
	var tap *Tap
	assert.NotPanics(t, func() {
		tap.OnMarkSession(link.MarkClientSQL)
		tap.OnSend(&pgproto3.Query{String: "x"}, query.QueryReply)
		tap.OnSuppressedParse(&pgproto3.Parse{Name: "s1", Query: "select 1"})
		tap.OnFlushSucceeded()
		tap.OnRelease()
	})
}

// TestTap_ReleaseCarriesSessionMarks: marks ride the release sentinel, so the
// worker can mark the shadow link that replayed them.
func TestTap_ReleaseCarriesSessionMarks(t *testing.T) {
	q := newMirrorQueue(queueCapRoomy, nil)
	tap := NewTap(q, nil)

	tap.OnMarkSession(link.MarkClientSQL)
	tap.OnSend(&pgproto3.Query{String: "SET search_path = app"}, query.QueryReply)
	tap.OnFlushSucceeded()
	tap.OnRelease()

	require.Equal(t, []mirrorItemType{mirrorMsg, mirrorFlush, mirrorRelease}, queuedTypes(q))
	assert.Equal(t, link.MarkClientSQL, q.items[2].marks)
}
