// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package mirror

import (
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"

	pgproxy "github.com/DataDog/pg-proxy"
	"github.com/DataDog/pg-proxy/pkg/link"
	"github.com/DataDog/pg-proxy/pkg/prepared"
	"github.com/DataDog/pg-proxy/pkg/query"
)

type Link = link.Link

// mirrorWorkerDrainTimeout bounds how long session teardown waits for the worker
// to drain the closed queue and release its shadow link before force-closing it.
const mirrorWorkerDrainTimeout = time.Second

// mirrorShadowDrainTimeout bounds a single stalled shadow read. var so tests can shorten it.
var mirrorShadowDrainTimeout = 5 * time.Second

// mirrorWorker drains a session's mirrorQueue and replays it onto a shadow
// database, in FIFO order, discarding all responses. It runs on its own
// goroutine so it never blocks or slows the primary client->source path.
//
// It borrows a shadow link on the first message of a batch and returns it at the
// batch's release sentinel — the same borrow/release rhythm the source session
// uses, so live shadow connections track concurrently-active mirrored sessions
// rather than total sessions.
type mirrorWorker struct {
	q       *mirrorQueue
	acquire func() (link.Link, error) // borrow a fresh shadow link from the shadow pool
	tags    []string
	// seed pre-fills the replay catalog. A client that prepared its statements before
	// mirroring was enabled never sends Parse again, so without this the catalog
	// stays empty for the session's life and every Bind reaches the shadow as 26000.
	seed []prepared.PreparedStatement

	done chan struct{}

	mu      sync.Mutex // guards curLink so stop() can force-close a hung link
	curLink link.Link
}

func newMirrorWorker(q *mirrorQueue, acquire func() (link.Link, error), tags []string, seed []prepared.PreparedStatement) *mirrorWorker {
	return &mirrorWorker{q: q, acquire: acquire, tags: tags, seed: seed, done: make(chan struct{})}
}

// Worker asynchronously replays queued messages onto a shadow link.
type Worker = mirrorWorker

// NewWorker constructs a mirror replay worker. seed is the set of named prepared
// statements the session's source connection already holds.
func NewWorker(q *Queue, acquire func() (link.Link, error), tags []string, seed []prepared.PreparedStatement) *Worker {
	return newMirrorWorker(q, acquire, tags, seed)
}

// Run drains the queue until it is closed.
func (w *mirrorWorker) Run() { w.run() }

// Stop bounds teardown of the worker and any held shadow link.
func (w *mirrorWorker) Stop(timeout time.Duration) { w.stop(timeout) }

// WorkerDrainTimeout preserves the production session teardown bound.
const WorkerDrainTimeout = mirrorWorkerDrainTimeout

// TeardownDrainTimeout bounds a teardown that is not racing session close. It exceeds the
// acquire bound the caller gives this worker, so a worker parked in acquire fails on its own
// deadline first instead of tripping stop_timeout. It guarantees nothing for the other
// blocking steps: flushAndDrain re-arms its timer per read, and Release runs its role reset
// and prepared-statement deallocation on unbounded contexts (RESET ALL itself is capped at
// 5s), so a slow-but-healthy shadow can still trip stop_timeout here. This stop runs off the
// client path, so that does not delay client queries.
const TeardownDrainTimeout = 2 * mirrorWorkerDrainTimeout

// cleanup is run's deferred exit: force-close the link (a clean RESET could stall
// teardown on a slow shadow), signal done, emit the stopped metric.
func (w *mirrorWorker) cleanup() {
	w.discardLink()
	close(w.done)
	w.count("mirror.worker.stopped")
}

// run drains and replays until the queue is closed and empty. It owns curLink
// for the goroutine's lifetime and always releases it on exit.
func (w *mirrorWorker) run() {
	// skipping drops the rest of a poisoned batch (a replay error, a link that could
	// not be acquired, or a producer-side drop) until the next release sentinel, so
	// we never replay a partial transaction or a Bind whose Parse was lost.
	skipping := false
	// Local to run(), so it is plainly single-goroutine and dies with the goroutine.
	cat := newStmtCatalog()
	for _, st := range w.seed {
		if st.IsProxyNamedPreparedStmt() {
			cat.put(st)
		}
	}
	w.seed = nil // the catalog owns these now; the snapshot would outlive its use
	// pending is budget for messages buffered on the shadow link but not yet flushed,
	// so the cap bounds live data (queue + buffer). Released at flush/batch-end/exit.
	pending := 0
	// recorded tracks whether this batch claimed new statements on the shadow
	// connection. The claim is written before Postgres answers, and a link exposes
	// no way to retract one, so an error in such a batch costs us the connection.
	recorded := false
	release := func() {
		w.q.releaseBudgetRaw(pending)
		pending = 0
	}

	w.count("mirror.worker.started")
	// Single deferred exit: recover from a panic (best-effort; close the queue so the
	// producer stops filling one no worker will drain), free held budget, then clean up.
	defer func() {
		if r := recover(); r != nil {
			w.count("mirror.worker.panic")
			pgproxy.Logger().Error("mirror worker recovered from panic; dropping replay", "panic", r)
			w.q.close()
		}
		release()
		w.cleanup()
	}()

	for {
		it, ok := w.q.dequeue()
		if !ok {
			return // queue closed and drained; deferred release frees the tail's budget
		}
		pending += it.size + mirrorItemStructOverhead
		switch it.typ {
		case mirrorMsg:
			// Catalog before anything can skip the message: the client never re-sends a
			// Parse, so a definition lost here is lost for the rest of the session.
			var parsed prepared.PreparedStatement
			if m, ok := it.msg.(*pgproto3.Parse); ok {
				parsed = prepared.CreatePreparedStatement("", m.Name, m.Query, m.ParameterOIDs)
				if parsed.IsProxyNamedPreparedStmt() {
					cat.put(parsed)
				}
			}
			if skipping {
				break
			}
			link := w.getLink()
			if link == nil {
				var err error
				if link, err = w.acquireLink(); err != nil {
					w.count("mirror.replay_errors", "op:acquire")
					skipping = true
					break
				}
			}
			// Shadow links are borrowed per batch, so a Bind can land on a connection that never saw its Parse.
			switch m := it.msg.(type) {
			case *pgproto3.Parse:
				if link.HasExistingNamedPreparedStmt(parsed) {
					_ = link.RecordExistingNamedPreparedStmtUsed(parsed)
					w.count("mirror.replayed", "kind:parse_existing")
					break // this connection already has it; sending again is 42P05
				}
				link.Send(m, it.expectation)
				// While Postgres discards until Sync it ignores this Parse, so claiming it
				// would hand the pool a statement the connection does not have.
				if !link.DiscardingUntilSync() && link.RecordNewNamedPreparedStmt(parsed) == nil && parsed.IsProxyNamedPreparedStmt() {
					recorded = true
				}
			case *pgproto3.Bind:
				n, rec, admitted := w.reparseIfMissing(link, cat, m.PreparedStatement)
				if !admitted {
					skipping = true
					break
				}
				pending += n
				recorded = rec || recorded
				link.Send(m, it.expectation)
			case *pgproto3.Describe:
				if m.ObjectType == 'S' { // 'S' = statement, 'P' = portal
					n, rec, admitted := w.reparseIfMissing(link, cat, m.Name)
					if !admitted {
						skipping = true
						break
					}
					pending += n
					recorded = rec || recorded
				}
				link.Send(m, it.expectation)
			default:
				link.Send(it.msg, it.expectation) // buffered; hits the network at the flush sentinel
			}
			if skipping {
				break // the budget refused an injected Parse; nothing was replayed
			}
			w.count("mirror.replayed", "kind:msg")
		case mirrorFlush:
			if link := w.getLink(); !skipping && link != nil {
				sawError, err := w.flushAndDrain(link)
				switch {
				case err != nil:
					w.count("mirror.replay_errors", "op:flush")
					w.reconnect()
					skipping = true
				case sawError && recorded:
					// One of those statements may have been rejected. Discard the connection
					// rather than hand a false claim back to the pool for another session to hit.
					w.count("mirror.replay_errors", "op:stmt_rejected")
					w.reconnect()
					skipping = true
				default:
					w.count("mirror.replayed", "kind:flush")
				}
			}
			recorded = false
			release() // buffer flushed or batch dropped
		case mirrorPoison:
			skipping = true // a producer drop poisoned the batch; skip to release, RESET rolls back
		case mirrorRelease:
			recorded = false
			release() // free budget before the (possibly slow) shadow RESET
			w.releaseLink(it.marks)
			skipping = false // next batch starts clean
		}
	}
}

// reparseIfMissing sends the Parse when the shadow connection lacks the statement.
// It reports the bytes injected, whether the connection was recorded as now holding
// the statement, and whether the injection fit. A false admitted means the caller
// must skip the batch, the same answer a dropped enqueue gives the producer.
func (w *mirrorWorker) reparseIfMissing(link link.Link, cat *stmtCatalog, name string) (injected int, recorded, admitted bool) {
	if !prepared.IsNamedPreparedStmt(name) {
		if link.MayHoldForeignUnnamedStmt() && !link.DiscardingUntilSync() {
			// The shadow connection skipped its last reset and may still hold another
			// session's unnamed statement, which this Bind or Describe would reach. Drop
			// it, so the shadow answers as the source did. A Close is a few bytes, so it
			// is not charged to the budget. While Postgres discards until Sync, it would
			// drop the Close and this Bind or Describe alike.
			link.Send(&pgproto3.Close{ObjectType: 'S'}, query.CloseReply)
			w.count("mirror.shadow_unnamed_close")
		}
		return 0, false, true // otherwise the client re-parses the unnamed statement every time
	}
	st, ok := cat.get(name)
	if !ok {
		w.count("mirror.catalog_miss")
		return 0, false, true // continuing is never worse than skipping: a miss costs one batch either way
	}
	if link.HasExistingNamedPreparedStmt(st) {
		// Refresh usage so Release's LFU/LRU eviction sees this statement as hot. A
		// mirrored statement is normally only ever reached through Bind, so without
		// this it looks unused and is deallocated ahead of genuinely cold ones.
		_ = link.RecordExistingNamedPreparedStmtUsed(st)
		return 0, false, true
	}
	if link.DiscardingUntilSync() {
		return 0, false, true // Postgres drops it until Sync; injecting buys nothing and claims a lie
	}
	parse := &pgproto3.Parse{
		Name: st.GetProxyName(), Query: st.GetSQLQuery(), ParameterOIDs: st.GetParamOIDs(),
	}
	// This Parse never passed the enqueue door, so charge it here or the cap stops
	// bounding live data.
	size := sizeOfFrontendMessage(parse)
	if !w.q.tryReserveRaw(size) {
		w.count("mirror.replay_errors", "op:reparse_budget")
		return 0, false, false
	}
	// The caller only folds size into the batch's reservation on a normal return, so a
	// panic between here and there would strand these bytes in the shared budget for
	// the life of the process. Hand ownership over at the last moment.
	owned := true
	defer func() {
		if owned {
			w.q.releaseBudgetRaw(size)
		}
	}()
	link.Send(parse, query.ParseReply)
	w.count("mirror.shadow_reparse")
	recorded = link.RecordNewNamedPreparedStmt(st) == nil
	owned = false
	return size, recorded, true
}

// flushAndDrain flushes to the shadow, then reads and drops every reply until idle,
// via DecidePgMessageAction (one expectation spans many wire messages). An error
// (FATAL or a diverging reply) tells the caller to force-close and skip the batch.
// sawError reports a non-FATAL ErrorResponse, which the drain itself tolerates.
func (w *mirrorWorker) flushAndDrain(link link.Link) (sawError bool, err error) {
	// Force-close if the flush or a read stalls, so a slow shadow can't pin this
	// goroutine/pool conn. Re-armed per step, so it guards one stall, not a big drain.
	timer := time.AfterFunc(mirrorShadowDrainTimeout, func() { link.ForceClose() })
	defer timer.Stop()
	if err := link.Flush(); err != nil {
		return sawError, err
	}
	timer.Reset(mirrorShadowDrainTimeout) // flush done; re-arm for the drain
	traceState := &query.TraceState{}     // no root trace is started, so no spans are emitted
	for {
		expectation := link.NextExpectation()
		if expectation == query.NoReply {
			return sawError, nil
		}
		msg, err := link.Receive()
		if err != nil {
			return sawError, err
		}
		timer.Reset(mirrorShadowDrainTimeout) // re-arm: this read made progress
		if e, ok := msg.(*pgproto3.ErrorResponse); ok {
			// Non-FATAL errors return no error from DecidePgMessageAction, so replay
			// failures are otherwise invisible.
			w.count("mirror.replay_error_response", "code:"+e.Code)
			sawError = true
		}

		action, err := query.DecidePgMessageAction(msg, expectation, traceState)
		if err != nil {
			return sawError, err
		}
		if action.ResolveReplyExpectation {
			link.ResolveNextExpectation()
		}
		if action.DiscardUntilSync {
			link.DiscardUntilSync()
		}
		if action.SetTxStatus != query.NoTxStatus {
			link.HandleReadyForQuery(action.SetTxStatus) // keep Idle() accurate so Release picks the right path
		}
	}
}

func (w *mirrorWorker) acquireLink() (link.Link, error) {
	link, err := w.acquire()
	if err != nil {
		return nil, err
	}
	w.mu.Lock()
	w.curLink = link
	w.mu.Unlock()
	w.count("mirror.shadow_link.acquired")
	return link, nil
}

func (w *mirrorWorker) getLink() link.Link {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.curLink
}

// take clears and returns curLink under the mutex, so stop() can't force-close one being released.
func (w *mirrorWorker) take() link.Link {
	w.mu.Lock()
	defer w.mu.Unlock()
	link := w.curLink
	w.curLink = nil
	return link
}

// discardLink force-closes the link on teardown (a clean RESET could stall session close).
func (w *mirrorWorker) discardLink() {
	link := w.take()
	if link == nil {
		return
	}
	link.ForceClose()
	_ = link.Release()
	w.count("mirror.shadow_link.discarded")
}

// releaseLink returns the shadow link to its pool, marked first with the source
// checkout's session marks: the shadow replayed what that checkout ran. No-op if
// none is held.
func (w *mirrorWorker) releaseLink(marks link.SessionMarks) {
	link := w.take()
	if link == nil {
		return
	}
	link.MarkSession(marks)
	if err := link.Release(); err != nil {
		pgproxy.Logger().Warn("mirror worker failed to release shadow link", "err", err)
	}
	w.count("mirror.shadow_link.released")
}

// reconnect discards a broken shadow link: force-close it, then Release so the
// pgxpool conn returns to the pool (to be discarded) rather than leaking.
func (w *mirrorWorker) reconnect() {
	link := w.take()
	if link == nil {
		return
	}
	link.ForceClose()
	_ = link.Release()
	w.count("mirror.shadow_link.reconnect")
}

// stop waits for the worker to drain the closed queue; on timeout it force-closes
// the in-use link and waits once more (also bounded) so a wedged worker can't pin
// session close. Callers must close the queue first.
func (w *mirrorWorker) stop(timeout time.Duration) {
	select {
	case <-w.done:
		return
	case <-time.After(timeout):
	}
	w.count("mirror.worker.stop_timeout")
	w.mu.Lock()
	if w.curLink != nil {
		w.curLink.ForceClose() // unblock a stuck Receive; run() releases the link
	}
	w.mu.Unlock()
	select {
	case <-w.done:
	case <-time.After(timeout):
		w.count("mirror.worker.stop_abandoned")
	}
}

func (w *mirrorWorker) count(metric string, extraTags ...string) {
	tags := w.tags
	if len(extraTags) > 0 {
		tags = append(append([]string(nil), w.tags...), extraTags...)
	}
	_ = pgproxy.Metrics().Count("pg_proxy.session."+metric, 1, tags, 1)
}
