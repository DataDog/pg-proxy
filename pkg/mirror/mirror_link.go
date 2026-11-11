// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package mirror

import (
	"github.com/jackc/pgx/v5/pgproto3"

	pgproxy "github.com/DataDog/pg-proxy"
	"github.com/DataDog/pg-proxy/pkg/link"
	"github.com/DataDog/pg-proxy/pkg/query"
)

// mirroringLink wraps a Link so everything the session forwards to the source is
// also queued for shadow replay. The mirror decision is made once,
// when the link is created; from then on Send/Flush are the single choke point
// for mirroring, so the session can't forget to mirror a message it sends.
//
// The session owns the queue — the link only holds a reference to enqueue into.
// The wrapped Link is embedded, so every method that doesn't concern mirroring
// (Receive, expectations, prepared-statement bookkeeping, ...) forwards to the
// real link unchanged.
// One mirroringLink per batch (acquire to release), so batchHasData/flushHasData are
// batch-scoped, mutated only on the session goroutine.
type mirroringLink struct {
	link.Link
	q    *mirrorQueue
	tags []string

	batchHasData bool // any data admitted since acquire; gates the release sentinel
	flushHasData bool // data admitted since the last flush; gates the flush sentinel
	poisoned     bool // a message in this batch was dropped; worker must skip it whole
}

func newMirroringLink(backend link.Link, q *mirrorQueue, tags []string) *mirroringLink {
	return &mirroringLink{Link: backend, q: q, tags: tags}
}

// NewLink wraps a source link with asynchronous shadow mirroring.
func NewLink(backend link.Link, q *Queue, tags []string) link.Link {
	return newMirroringLink(backend, q, tags)
}

// StmtDefMirror lets a session hand over a Parse it did not forward to the source.
type StmtDefMirror interface {
	MirrorParse(msg *pgproto3.Parse)
}

// Send forwards msg to the source, then queues a clone for the shadow.
func (l *mirroringLink) Send(msg pgproto3.FrontendMessage, expectation query.ReplyExpectation) {
	l.Link.Send(msg, expectation)
	l.enqueue(msg, expectation)
}

// MirrorParse queues a Parse the session did not forward to the source.
func (l *mirroringLink) MirrorParse(msg *pgproto3.Parse) {
	l.enqueue(msg, query.ParseReply) // Postgres replies ParseComplete; the drain must expect it
}

// enqueue queues a clone of msg for the shadow. Never blocks.
func (l *mirroringLink) enqueue(msg pgproto3.FrontendMessage, expectation query.ReplyExpectation) {
	// A poisoned batch is already doomed (the worker skips it), so stop cloning.
	if l.poisoned || !canMirror(msg) {
		return
	}
	size := sizeOfFrontendMessage(msg) // size the original; clone only if it fits
	// message_mirrored counts only queued messages, so a drop isn't double-counted.
	// A drop is tagged by reason so a saturated global budget is distinguishable from
	// a single session outrunning its own cap.
	switch l.q.tryEnqueueFunc(size, func() mirrorItem {
		return mirrorItem{msg: cloneFrontendMessage(msg), expectation: expectation, size: size}
	}) {
	case mirrorEnqueued:
		// batch admitted data -> its flush/release sentinels are worth admitting
		l.batchHasData = true
		l.flushHasData = true
		_ = pgproxy.Metrics().Count("pg_proxy.session.message_mirrored", 1, l.tags, 1)
	case mirrorDroppedSessionCap:
		// A dropped Parse costs the worker its definition. It recovers on the next
		// transaction whose source conn lacks the statement, which re-sends one — but
		// not once every conn is warm.
		l.poison()
		_ = pgproxy.Metrics().Count("pg_proxy.session.message_mirror_dropped", 1, l.dropTag("reason:session"), 1)
	case mirrorDroppedGlobalBudget:
		l.poison()
		_ = pgproxy.Metrics().Count("pg_proxy.session.message_mirror_dropped", 1, l.dropTag("reason:global"), 1)
	}
}

// poison marks the batch so the worker skips it whole (a dropped message would
// otherwise replay a partial transaction on the shadow). The sentinel is only
// enqueued once data was admitted; a fully-dropped batch has nothing queued to skip,
// so it stays empty and its control items don't escape the byte cap.
func (l *mirroringLink) poison() {
	if l.poisoned {
		return
	}
	l.poisoned = true
	if l.batchHasData {
		l.q.enqueueSentinel(mirrorItem{typ: mirrorPoison, size: mirrorItemOverhead})
	}
}

// dropTag returns l.tags with reason appended, copied so repeated calls never
// clobber the shared backing array.
func (l *mirroringLink) dropTag(reason string) []string {
	return append(append([]string(nil), l.tags...), reason)
}

// Flush marks the flush boundary (via enqueueSentinel) after a successful source
// flush, but only when data was buffered since the last one: an empty flush is a
// no-op on the shadow, which also coalesces repeats. Nothing on source failure.
func (l *mirroringLink) Flush() error {
	if err := l.Link.Flush(); err != nil {
		return err
	}
	if l.flushHasData {
		l.q.enqueueSentinel(mirrorItem{typ: mirrorFlush, size: mirrorItemOverhead})
		l.flushHasData = false
	}
	return nil
}

// Release marks the batch boundary so the worker returns its shadow link at the same
// ready-for-query point. Guaranteed (enqueueSentinel) so a full queue can't strand the
// link, but only for a batch that admitted data (no data means no link was acquired).
func (l *mirroringLink) Release() error {
	if l.batchHasData {
		l.q.enqueueSentinel(mirrorItem{typ: mirrorRelease, size: mirrorItemOverhead})
	}
	return l.Link.Release()
}
