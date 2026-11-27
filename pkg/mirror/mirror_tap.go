// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package mirror

import (
	"github.com/jackc/pgx/v5/pgproto3"

	pgproxy "github.com/DataDog/pg-proxy"
	"github.com/DataDog/pg-proxy/pkg/query"
)

// Tap observes messages forwarded by a session on one backend checkout and
// queues clones for asynchronous shadow replay.
//
// The session owns the queue — Tap only holds a reference to enqueue into.
// One Tap per checkout (acquire to release), mutated only on the session
// goroutine. A nil *Tap is a safe no-op.
type Tap struct {
	q    *mirrorQueue
	tags []string

	batchHasData bool // any data admitted since acquire; gates the release sentinel
	flushHasData bool // data admitted since the last flush; gates the flush sentinel
	poisoned     bool // a message in this batch was dropped; worker must skip it whole
}

// NewTap creates a per-checkout mirroring tap backed by q.
func NewTap(q *Queue, tags []string) *Tap {
	return &Tap{q: q, tags: tags}
}

// OnSend queues a clone of a message forwarded to the source backend. Never blocks.
func (t *Tap) OnSend(msg pgproto3.FrontendMessage, expectation query.ReplyExpectation) {
	if t == nil {
		return
	}
	t.enqueue(msg, expectation)
}

// OnSuppressedParse queues a Parse the session satisfied locally without
// forwarding to the source backend.
func (t *Tap) OnSuppressedParse(msg *pgproto3.Parse) {
	if t == nil {
		return
	}
	t.enqueue(msg, query.ParseReply) // Postgres replies ParseComplete; the drain must expect it
}

// enqueue queues a clone of msg for the shadow. Never blocks.
func (t *Tap) enqueue(msg pgproto3.FrontendMessage, expectation query.ReplyExpectation) {
	// A poisoned batch is already doomed (the worker skips it), so stop cloning.
	if t.poisoned || !canMirror(msg) {
		return
	}
	size := sizeOfFrontendMessage(msg) // size the original; clone only if it fits
	// message_mirrored counts only queued messages, so a drop isn't double-counted.
	// A drop is tagged by reason so a saturated global budget is distinguishable from
	// a single session outrunning its own cap.
	switch t.q.tryEnqueueFunc(size, func() mirrorItem {
		return mirrorItem{msg: cloneFrontendMessage(msg), expectation: expectation, size: size}
	}) {
	case mirrorEnqueued:
		// batch admitted data -> its flush/release sentinels are worth admitting
		t.batchHasData = true
		t.flushHasData = true
		_ = pgproxy.Metrics().Count("pg_proxy.session.message_mirrored", 1, t.tags, 1)
	case mirrorDroppedSessionCap:
		// A dropped Parse costs the worker its definition. It recovers on the next
		// transaction whose source conn lacks the statement, which re-sends one — but
		// not once every conn is warm.
		t.poison()
		_ = pgproxy.Metrics().Count("pg_proxy.session.message_mirror_dropped", 1, t.dropTag("reason:session"), 1)
	case mirrorDroppedGlobalBudget:
		t.poison()
		_ = pgproxy.Metrics().Count("pg_proxy.session.message_mirror_dropped", 1, t.dropTag("reason:global"), 1)
	}
}

// poison marks the batch so the worker skips it whole (a dropped message would
// otherwise replay a partial transaction on the shadow). The sentinel is only
// enqueued once data was admitted; a fully-dropped batch has nothing queued to skip,
// so it stays empty and its control items don't escape the byte cap.
func (t *Tap) poison() {
	if t.poisoned {
		return
	}
	t.poisoned = true
	if t.batchHasData {
		t.q.enqueueSentinel(mirrorItem{typ: mirrorPoison, size: mirrorItemOverhead})
	}
}

// dropTag returns t.tags with reason appended, copied so repeated calls never
// clobber the shared backing array.
func (t *Tap) dropTag(reason string) []string {
	return append(append([]string(nil), t.tags...), reason)
}

// OnFlushSucceeded marks the flush boundary (via enqueueSentinel) after a
// successful source flush, but only when data was buffered since the last one:
// an empty flush is a no-op on the shadow, which also coalesces repeats.
func (t *Tap) OnFlushSucceeded() {
	if t == nil {
		return
	}
	if t.flushHasData {
		t.q.enqueueSentinel(mirrorItem{typ: mirrorFlush, size: mirrorItemOverhead})
		t.flushHasData = false
	}
}

// OnRelease marks the batch boundary so the worker returns its shadow link at
// the same ready-for-query point. Guaranteed (enqueueSentinel) so a full queue
// can't strand the link, but only for a batch that admitted data (no data means
// no link was acquired).
func (t *Tap) OnRelease() {
	if t == nil {
		return
	}
	if t.batchHasData {
		t.q.enqueueSentinel(mirrorItem{typ: mirrorRelease, size: mirrorItemOverhead})
	}
}
