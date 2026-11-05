package mirror

import (
	"sync"
	"time"

	pgproxy "github.com/DataDog/pg-proxy"
	"github.com/DataDog/pg-proxy/pkg/link"
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

	done chan struct{}

	mu      sync.Mutex // guards curLink so stop() can force-close a hung link
	curLink link.Link
}

func newMirrorWorker(q *mirrorQueue, acquire func() (link.Link, error), tags []string) *mirrorWorker {
	return &mirrorWorker{q: q, acquire: acquire, tags: tags, done: make(chan struct{})}
}

// Worker asynchronously replays queued messages onto a shadow link.
type Worker = mirrorWorker

// NewWorker constructs a mirror replay worker.
func NewWorker(q *Queue, acquire func() (link.Link, error), tags []string) *Worker {
	return newMirrorWorker(q, acquire, tags)
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
	// pending is budget for messages buffered on the shadow link but not yet flushed,
	// so the cap bounds live data (queue + buffer). Released at flush/batch-end/exit.
	pending := 0
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
			link.Send(it.msg, it.expectation) // buffered; hits the network at the flush sentinel
			w.count("mirror.replayed", "kind:msg")
		case mirrorFlush:
			if link := w.getLink(); !skipping && link != nil {
				if err := w.flushAndDrain(link); err != nil {
					w.count("mirror.replay_errors", "op:flush")
					w.reconnect()
					skipping = true
				} else {
					w.count("mirror.replayed", "kind:flush")
				}
			}
			release() // buffer flushed or batch dropped
		case mirrorPoison:
			skipping = true // a producer drop poisoned the batch; skip to release, RESET rolls back
		case mirrorRelease:
			release() // free budget before the (possibly slow) shadow RESET
			w.releaseLink()
			skipping = false // next batch starts clean
		}
	}
}

// flushAndDrain flushes to the shadow, then reads and drops every reply until idle,
// via DecidePgMessageAction (one expectation spans many wire messages). An error
// (FATAL or a diverging reply) tells the caller to force-close and skip the batch.
func (w *mirrorWorker) flushAndDrain(link link.Link) error {
	// Force-close if the flush or a read stalls, so a slow shadow can't pin this
	// goroutine/pool conn. Re-armed per step, so it guards one stall, not a big drain.
	timer := time.AfterFunc(mirrorShadowDrainTimeout, func() { link.ForceClose() })
	defer timer.Stop()
	if err := link.Flush(); err != nil {
		return err
	}
	timer.Reset(mirrorShadowDrainTimeout) // flush done; re-arm for the drain
	traceState := &query.TraceState{}     // no root trace is started, so no spans are emitted
	for {
		expectation := link.NextExpectation()
		if expectation == query.NoReply {
			return nil
		}
		msg, err := link.Receive()
		if err != nil {
			return err
		}
		timer.Reset(mirrorShadowDrainTimeout) // re-arm: this read made progress

		action, err := query.DecidePgMessageAction(msg, expectation, traceState)
		if err != nil {
			return err
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

// releaseLink returns the borrowed shadow link to the pool. No-op if none is held.
func (w *mirrorWorker) releaseLink() {
	link := w.take()
	if link == nil {
		return
	}
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
	_ = pgproxy.Metrics().Count("dd.orgstore.pg_proxy.session."+metric, 1, tags, 1)
}
