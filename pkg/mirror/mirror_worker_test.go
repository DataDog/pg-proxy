package mirror

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/DataDog/pg-proxy/pkg/prepared"
	"github.com/DataDog/pg-proxy/pkg/query"
)

// fakeShadowLink is a hand-rolled Link that models the parts the worker uses:
// it records sent messages, tracks the reply-expectation queue exactly as a real
// SessionLink does (append on Send unless NoReply), and hands out canned replies.
type fakeShadowLink struct {
	sent         []pgproto3.FrontendMessage
	expectations []query.ReplyExpectation
	replies      []pgproto3.BackendMessage
	flushErr     error
	receiveErr   error

	panicOnSend bool // when set, Send panics — exercises the worker's recover()

	receiveDelay time.Duration // per-Receive delay — models a slow-but-progressing shadow

	flushes     int
	releases    int
	forceCloses int

	// blockUntilClose, when set, makes Receive block until ForceClose is called —
	// used to exercise the stop() force-close path.
	blockUntilClose chan struct{}
	closeOnce       sync.Once

	stmts        map[string]bool // named statements live on this connection, as SessionLink tracks
	discarding   bool            // extended-query error state, as SessionLink tracks
	existingUsed []string        // proxy names passed to RecordExistingNamedPreparedStmtUsed
}

func (l *fakeShadowLink) Send(msg pgproto3.FrontendMessage, e query.ReplyExpectation) {
	if l.panicOnSend {
		panic("shadow send blew up")
	}
	l.sent = append(l.sent, msg)
	if e != query.NoReply {
		// Mirror SessionLink: a Sync clears the error state, and while it is set
		// the message still goes out but no reply is expected for it.
		if e == query.SyncReply {
			l.discarding = false
		}
		if !l.discarding {
			l.expectations = append(l.expectations, e)
		}
	}
}
func (l *fakeShadowLink) Flush() error { l.flushes++; return l.flushErr }
func (l *fakeShadowLink) Receive() (pgproto3.BackendMessage, error) {
	if l.receiveDelay > 0 {
		time.Sleep(l.receiveDelay)
	}
	if l.blockUntilClose != nil {
		<-l.blockUntilClose // unblocked by ForceClose
		return nil, errors.New("connection force-closed")
	}
	if l.receiveErr != nil {
		return nil, l.receiveErr
	}
	if len(l.replies) == 0 {
		return &pgproto3.ReadyForQuery{TxStatus: 'I'}, nil // safe idle terminator so tests never hang
	}
	r := l.replies[0]
	l.replies = l.replies[1:]
	return r, nil
}
func (l *fakeShadowLink) NextExpectation() query.ReplyExpectation {
	if len(l.expectations) == 0 {
		return query.NoReply
	}
	return l.expectations[0]
}
func (l *fakeShadowLink) ResolveNextExpectation() {
	if len(l.expectations) > 0 {
		l.expectations = l.expectations[1:]
	}
}
func (l *fakeShadowLink) DiscardUntilSync() {
	// Mirror SessionLink: drop queued expectations up to (but keeping) the next
	// SyncReply, whose ReadyForQuery still resolves it.
	n := 0
	for n < len(l.expectations) && l.expectations[n] != query.SyncReply {
		n++
	}
	l.expectations = l.expectations[n:]
	l.discarding = len(l.expectations) == 0
}
func (l *fakeShadowLink) DiscardingUntilSync() bool { return l.discarding }
func (l *fakeShadowLink) Idle() bool                { return len(l.expectations) == 0 }
func (l *fakeShadowLink) Cancel() error             { return nil }
func (l *fakeShadowLink) Release() error            { l.releases++; return nil }
func (l *fakeShadowLink) ForceClose() bool {
	l.forceCloses++
	if l.blockUntilClose != nil {
		l.closeOnce.Do(func() { close(l.blockUntilClose) })
	}
	return true
}
func (l *fakeShadowLink) DebugExpectationString() string     { return "" }
func (l *fakeShadowLink) TxStatus() query.TxStatus           { return 0 }
func (l *fakeShadowLink) SetTxStatus(query.TxStatus)         {}
func (l *fakeShadowLink) HandleReadyForQuery(query.TxStatus) {}
func (l *fakeShadowLink) RecordNewNamedPreparedStmt(st prepared.PreparedStatement) error {
	if !st.IsProxyNamedPreparedStmt() {
		return nil // SessionLink no-ops for the unnamed statement
	}
	if l.stmts == nil {
		l.stmts = map[string]bool{}
	}
	l.stmts[st.GetProxyName()] = true
	return nil
}
func (l *fakeShadowLink) RecordExistingNamedPreparedStmtUsed(st prepared.PreparedStatement) error {
	if !st.IsProxyNamedPreparedStmt() {
		return nil
	}
	l.existingUsed = append(l.existingUsed, st.GetProxyName())
	return nil
}
func (l *fakeShadowLink) HasExistingNamedPreparedStmt(st prepared.PreparedStatement) bool {
	return st.IsProxyNamedPreparedStmt() && l.stmts[st.GetProxyName()]
}

// linkFactory hands out a fresh fakeShadowLink per acquire and records them.
type linkFactory struct {
	links   []*fakeShadowLink
	acquire func() (Link, error)
}

func newLinkFactory(prep func(*fakeShadowLink)) *linkFactory {
	f := &linkFactory{}
	f.acquire = func() (Link, error) {
		l := &fakeShadowLink{}
		if prep != nil {
			prep(l)
		}
		f.links = append(f.links, l)
		return l, nil
	}
	return f
}

// drainClosed runs the worker against a queue already loaded with items, waits
// until it has drained them all, then closes the queue so the worker exits. It
// must not close before the drain completes: close drops the undrained tail (the
// worker replays only what it dequeued while the queue was open), so the items
// have to be consumed first for the test's assertions to hold.
func drainClosed(q *mirrorQueue, acquire func() (Link, error)) {
	drainClosedWithSeed(q, acquire, nil)
}

// drainClosedWithSeed is drainClosed for a worker whose catalog is pre-seeded.
func drainClosedWithSeed(q *mirrorQueue, acquire func() (Link, error), seed []prepared.PreparedStatement) {
	w := newMirrorWorker(q, acquire, nil, seed)
	done := make(chan struct{})
	go func() { w.run(); close(done) }()
	for { // items were enqueued before this call, so bytes>0 until fully drained
		if b, _ := q.stats(); b == 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	q.close()
	<-done
}

func TestMirrorWorker_ReplaysBatchAndDropsReplies(t *testing.T) {
	q := newMirrorQueue(1<<20, nil)
	q.tryEnqueue(mirrorItem{typ: mirrorMsg, msg: &pgproto3.Parse{Name: "p"}, expectation: query.ParseReply, size: 10})
	q.tryEnqueue(mirrorItem{typ: mirrorMsg, msg: &pgproto3.Bind{}, expectation: query.BindReply, size: 10})
	q.tryEnqueue(mirrorItem{typ: mirrorFlush, size: mirrorItemOverhead})
	q.tryEnqueue(mirrorItem{typ: mirrorRelease, size: mirrorItemOverhead})

	f := newLinkFactory(func(l *fakeShadowLink) {
		l.replies = []pgproto3.BackendMessage{&pgproto3.ParseComplete{}, &pgproto3.BindComplete{}}
	})
	drainClosed(q, f.acquire)

	if len(f.links) != 1 {
		t.Fatalf("acquired %d links, want 1 (one batch)", len(f.links))
	}
	l := f.links[0]
	if len(l.sent) != 2 {
		t.Fatalf("sent %d messages, want 2", len(l.sent))
	}
	if _, ok := l.sent[0].(*pgproto3.Parse); !ok {
		t.Errorf("first sent = %T, want *Parse", l.sent[0])
	}
	if _, ok := l.sent[1].(*pgproto3.Bind); !ok {
		t.Errorf("second sent = %T, want *Bind", l.sent[1])
	}
	if l.flushes != 1 {
		t.Errorf("flushes = %d, want 1", l.flushes)
	}
	if len(l.expectations) != 0 {
		t.Errorf("undrained expectations = %d, want 0 (all replies read and dropped)", len(l.expectations))
	}
	if l.releases != 1 {
		t.Errorf("releases = %d, want 1 (release sentinel)", l.releases)
	}
}

func TestMirrorWorker_SeparateBatchesReacquire(t *testing.T) {
	q := newMirrorQueue(1<<20, nil)
	// Two batches, each ending in release.
	q.tryEnqueue(mirrorItem{typ: mirrorMsg, msg: &pgproto3.Query{String: "a"}, expectation: query.QueryReply, size: 10})
	q.tryEnqueue(mirrorItem{typ: mirrorFlush, size: mirrorItemOverhead})
	q.tryEnqueue(mirrorItem{typ: mirrorRelease, size: mirrorItemOverhead})
	q.tryEnqueue(mirrorItem{typ: mirrorMsg, msg: &pgproto3.Query{String: "b"}, expectation: query.QueryReply, size: 10})
	q.tryEnqueue(mirrorItem{typ: mirrorFlush, size: mirrorItemOverhead})
	q.tryEnqueue(mirrorItem{typ: mirrorRelease, size: mirrorItemOverhead})

	f := newLinkFactory(nil)
	drainClosed(q, f.acquire)

	if len(f.links) != 2 {
		t.Fatalf("acquired %d links, want 2 (one per batch)", len(f.links))
	}
	for i, l := range f.links {
		if len(l.sent) != 1 || l.releases != 1 {
			t.Errorf("batch %d: sent=%d releases=%d, want 1/1", i, len(l.sent), l.releases)
		}
	}
}

func TestMirrorWorker_FlushErrorSkipsToNextBatch(t *testing.T) {
	q := newMirrorQueue(1<<20, nil)
	// Batch 1 flush fails; its trailing Execute must be skipped.
	q.tryEnqueue(mirrorItem{typ: mirrorMsg, msg: &pgproto3.Parse{}, expectation: query.ParseReply, size: 10})
	q.tryEnqueue(mirrorItem{typ: mirrorFlush, size: mirrorItemOverhead})
	q.tryEnqueue(mirrorItem{typ: mirrorMsg, msg: &pgproto3.Execute{}, expectation: query.ExecuteReply, size: 10})
	q.tryEnqueue(mirrorItem{typ: mirrorRelease, size: mirrorItemOverhead})
	// Batch 2 is healthy and must replay on a fresh link.
	q.tryEnqueue(mirrorItem{typ: mirrorMsg, msg: &pgproto3.Query{String: "ok"}, expectation: query.QueryReply, size: 10})
	q.tryEnqueue(mirrorItem{typ: mirrorFlush, size: mirrorItemOverhead})
	q.tryEnqueue(mirrorItem{typ: mirrorRelease, size: mirrorItemOverhead})

	first := true
	f := &linkFactory{}
	f.acquire = func() (Link, error) {
		l := &fakeShadowLink{}
		if first {
			l.flushErr = errors.New("shadow flush failed")
			first = false
		}
		f.links = append(f.links, l)
		return l, nil
	}
	drainClosed(q, f.acquire)

	if len(f.links) != 2 {
		t.Fatalf("acquired %d links, want 2", len(f.links))
	}
	if f.links[0].forceCloses != 1 {
		t.Errorf("batch 1 forceCloses = %d, want 1 (flush error force-closes)", f.links[0].forceCloses)
	}
	if len(f.links[0].sent) != 1 {
		t.Errorf("batch 1 sent = %d, want 1 (Execute after the error must be skipped)", len(f.links[0].sent))
	}
	if len(f.links[1].sent) != 1 || f.links[1].flushes != 1 || f.links[1].releases != 1 {
		t.Errorf("batch 2 sent=%d flushes=%d releases=%d, want 1/1/1", len(f.links[1].sent), f.links[1].flushes, f.links[1].releases)
	}
}

func TestMirrorWorker_AcquireErrorSkipsBatch(t *testing.T) {
	q := newMirrorQueue(1<<20, nil)
	q.tryEnqueue(mirrorItem{typ: mirrorMsg, msg: &pgproto3.Query{String: "a"}, expectation: query.QueryReply, size: 10})
	q.tryEnqueue(mirrorItem{typ: mirrorFlush, size: mirrorItemOverhead})
	q.tryEnqueue(mirrorItem{typ: mirrorRelease, size: mirrorItemOverhead})
	q.tryEnqueue(mirrorItem{typ: mirrorMsg, msg: &pgproto3.Query{String: "b"}, expectation: query.QueryReply, size: 10})
	q.tryEnqueue(mirrorItem{typ: mirrorFlush, size: mirrorItemOverhead})
	q.tryEnqueue(mirrorItem{typ: mirrorRelease, size: mirrorItemOverhead})

	calls := 0
	f := &linkFactory{}
	f.acquire = func() (Link, error) {
		calls++
		if calls == 1 {
			return nil, errors.New("no shadow state")
		}
		l := &fakeShadowLink{}
		f.links = append(f.links, l)
		return l, nil
	}
	drainClosed(q, f.acquire)

	if calls != 2 {
		t.Fatalf("acquire called %d times, want 2 (batch 1 fails, batch 2 retries)", calls)
	}
	if len(f.links) != 1 || len(f.links[0].sent) != 1 {
		t.Errorf("second batch did not replay cleanly: links=%d", len(f.links))
	}
}

// TestMirrorWorker_DrainsFullResultSet is the regression test for the drain bug:
// one QueryReply expectation spans many wire messages (RowDescription, DataRows,
// CommandComplete, ReadyForQuery). The worker must consume all of them, not one
// per expectation, or leftover rows break RESET ALL when the link is released.
func TestMirrorWorker_DrainsFullResultSet(t *testing.T) {
	q := newMirrorQueue(1<<20, nil)
	q.tryEnqueue(mirrorItem{typ: mirrorMsg, msg: &pgproto3.Query{String: "select 1"}, expectation: query.QueryReply, size: 10})
	q.tryEnqueue(mirrorItem{typ: mirrorFlush, size: mirrorItemOverhead})
	q.tryEnqueue(mirrorItem{typ: mirrorRelease, size: mirrorItemOverhead})

	f := newLinkFactory(func(l *fakeShadowLink) {
		l.replies = []pgproto3.BackendMessage{
			&pgproto3.RowDescription{},
			&pgproto3.DataRow{}, &pgproto3.DataRow{},
			&pgproto3.CommandComplete{},
			&pgproto3.ReadyForQuery{TxStatus: 'I'},
		}
	})
	drainClosed(q, f.acquire)

	l := f.links[0]
	if len(l.replies) != 0 {
		t.Errorf("undrained replies = %d, want 0 (whole result set must be read before release)", len(l.replies))
	}
	if len(l.expectations) != 0 {
		t.Errorf("undrained expectations = %d, want 0", len(l.expectations))
	}
	if l.releases != 1 {
		t.Errorf("releases = %d, want 1", l.releases)
	}
}

// TestMirrorWorker_PoisonSkipsBatch verifies a poison sentinel (a producer-side
// drop) makes the worker skip the rest of that batch, so a transaction missing a
// dropped statement is never partially replayed, and that skip resets at release.
func TestMirrorWorker_PoisonSkipsBatch(t *testing.T) {
	q := newMirrorQueue(1<<20, nil)
	// Batch 1: begin replays, then a drop poisons it, so the trailing insert is skipped.
	q.tryEnqueue(mirrorItem{typ: mirrorMsg, msg: &pgproto3.Query{String: "begin"}, expectation: query.QueryReply, size: 10})
	q.tryEnqueue(mirrorItem{typ: mirrorFlush, size: mirrorItemOverhead})
	q.tryEnqueue(mirrorItem{typ: mirrorPoison, size: mirrorItemOverhead})
	q.tryEnqueue(mirrorItem{typ: mirrorMsg, msg: &pgproto3.Query{String: "insert"}, expectation: query.QueryReply, size: 10})
	q.tryEnqueue(mirrorItem{typ: mirrorFlush, size: mirrorItemOverhead})
	q.tryEnqueue(mirrorItem{typ: mirrorRelease, size: mirrorItemOverhead})
	// Batch 2 is healthy and must replay on a fresh link (skip reset at release).
	q.tryEnqueue(mirrorItem{typ: mirrorMsg, msg: &pgproto3.Query{String: "ok"}, expectation: query.QueryReply, size: 10})
	q.tryEnqueue(mirrorItem{typ: mirrorFlush, size: mirrorItemOverhead})
	q.tryEnqueue(mirrorItem{typ: mirrorRelease, size: mirrorItemOverhead})

	f := newLinkFactory(nil)
	drainClosed(q, f.acquire)

	if len(f.links) != 2 {
		t.Fatalf("acquired %d links, want 2 (one per batch)", len(f.links))
	}
	if got := len(f.links[0].sent); got != 1 {
		t.Errorf("batch 1 sent %d messages, want 1 (insert after poison must be skipped)", got)
	}
	if f.links[0].releases != 1 {
		t.Errorf("batch 1 releases = %d, want 1 (release rolls back the partial tx)", f.links[0].releases)
	}
	if got := len(f.links[1].sent); got != 1 {
		t.Errorf("batch 2 sent %d messages, want 1 (skip must reset at release)", got)
	}
}

// TestMirrorWorker_WatchdogResetsOnProgress verifies the drain watchdog guards a
// single stalled read, not the whole drain: a healthy result set whose total drain
// time exceeds the timeout still completes, because the watchdog is re-armed after
// every read that makes progress.
func TestMirrorWorker_WatchdogResetsOnProgress(t *testing.T) {
	orig := mirrorShadowDrainTimeout
	mirrorShadowDrainTimeout = 200 * time.Millisecond
	defer func() { mirrorShadowDrainTimeout = orig }()

	q := newMirrorQueue(1<<20, nil)
	q.tryEnqueue(mirrorItem{typ: mirrorMsg, msg: &pgproto3.Query{String: "select *"}, expectation: query.QueryReply, size: 10})
	q.tryEnqueue(mirrorItem{typ: mirrorFlush, size: mirrorItemOverhead})
	q.tryEnqueue(mirrorItem{typ: mirrorRelease, size: mirrorItemOverhead})

	// 40 rows at 10ms each = ~400ms of drain, well past the 200ms timeout, but no
	// single read stalls — each is far under it, so the watchdog must never fire.
	replies := []pgproto3.BackendMessage{&pgproto3.RowDescription{}}
	for i := 0; i < 40; i++ {
		replies = append(replies, &pgproto3.DataRow{})
	}
	replies = append(replies, &pgproto3.CommandComplete{}, &pgproto3.ReadyForQuery{TxStatus: 'I'})

	f := newLinkFactory(func(l *fakeShadowLink) {
		l.receiveDelay = 10 * time.Millisecond
		l.replies = replies
	})
	drainClosed(q, f.acquire)

	l := f.links[0]
	if l.forceCloses != 0 {
		t.Errorf("forceCloses = %d, want 0 (a progressing drain must not trip the watchdog)", l.forceCloses)
	}
	if len(l.replies) != 0 {
		t.Errorf("undrained replies = %d, want 0 (whole result set must drain)", len(l.replies))
	}
	if l.releases != 1 {
		t.Errorf("releases = %d, want 1", l.releases)
	}
}

// TestMirrorWorker_ErrorResponseDiscardsUntilSync checks that an extended-query
// error response drains cleanly: DiscardUntilSync drops the batch's pending
// expectations up to the Sync, whose ReadyForQuery resolves the rest, so the
// worker never blocks on a reply that will not come and leaves the link idle.
func TestMirrorWorker_ErrorResponseDiscardsUntilSync(t *testing.T) {
	q := newMirrorQueue(1<<20, nil)
	q.tryEnqueue(mirrorItem{typ: mirrorMsg, msg: &pgproto3.Parse{}, expectation: query.ParseReply, size: 10})
	q.tryEnqueue(mirrorItem{typ: mirrorMsg, msg: &pgproto3.Bind{}, expectation: query.BindReply, size: 10})
	q.tryEnqueue(mirrorItem{typ: mirrorMsg, msg: &pgproto3.Sync{}, expectation: query.SyncReply, size: 10})
	q.tryEnqueue(mirrorItem{typ: mirrorFlush, size: mirrorItemOverhead})
	q.tryEnqueue(mirrorItem{typ: mirrorRelease, size: mirrorItemOverhead})

	f := newLinkFactory(func(l *fakeShadowLink) {
		// Shadow errors on Parse: ErrorResponse, then ReadyForQuery for the Sync.
		l.replies = []pgproto3.BackendMessage{&pgproto3.ErrorResponse{}, &pgproto3.ReadyForQuery{TxStatus: 'I'}}
	})
	drainClosed(q, f.acquire)

	l := f.links[0]
	if len(l.expectations) != 0 {
		t.Errorf("undrained expectations = %d, want 0 (DiscardUntilSync + ReadyForQuery must drain the batch)", len(l.expectations))
	}
	if l.releases != 1 {
		t.Errorf("releases = %d, want 1", l.releases)
	}
}

// TestMirrorWorker_StopForceClosesHungLink verifies stop() force-closes a link
// stuck in Receive so the worker exits within the timeout.
func TestMirrorWorker_StopForceClosesHungLink(t *testing.T) {
	q := newMirrorQueue(1<<20, nil)
	q.tryEnqueue(mirrorItem{typ: mirrorMsg, msg: &pgproto3.Query{String: "a"}, expectation: query.QueryReply, size: 10})
	q.tryEnqueue(mirrorItem{typ: mirrorFlush, size: mirrorItemOverhead})
	// No release sentinel: the worker blocks in flushAndDrain's Receive.

	l := &fakeShadowLink{blockUntilClose: make(chan struct{})}
	w := newMirrorWorker(q, func() (Link, error) { return l, nil }, nil, nil)
	go w.run()

	time.Sleep(30 * time.Millisecond) // let the worker reach the blocked Receive
	q.close()                         // signal drain; worker is still stuck in Receive

	done := make(chan struct{})
	go func() { w.stop(50 * time.Millisecond); close(done) }()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not stop after force-close")
	}
	if l.forceCloses == 0 {
		t.Error("stop() did not force-close the hung link")
	}
}

// TestMirrorWorker_ReleasesBudgetAfterHandling verifies the worker returns each
// item's bytes to the shared budget once it has finished with it, so a fully
// drained session leaves the global budget at zero (bytes are held in-flight,
// not released at dequeue).
func TestMirrorWorker_ReleasesBudgetAfterHandling(t *testing.T) {
	const itemSize = 10
	cost := itemSize + mirrorItemStructOverhead
	budget := newMirrorBudget(100 * int64(cost))
	q := newMirrorQueue(100*cost, budget)
	q.tryEnqueue(mirrorItem{typ: mirrorMsg, msg: &pgproto3.Query{String: "q"}, expectation: query.QueryReply, size: itemSize})
	q.tryEnqueue(mirrorItem{typ: mirrorFlush, size: mirrorItemOverhead})
	q.tryEnqueue(mirrorItem{typ: mirrorRelease, size: mirrorItemOverhead})

	drainClosed(q, newLinkFactory(nil).acquire)

	if used := budget.UsedBytes(); used != 0 {
		t.Errorf("shared budget = %d bytes after draining, want 0 (worker must release each handled item)", used)
	}
}

// waitFor polls cond until true or fails the test after a generous timeout.
func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal(msg)
}

// TestMirrorWorker_RecoversFromPanic verifies a panic on the shadow replay path is
// contained to the worker goroutine (not propagated, which would crash the whole
// process) and the worker still exits cleanly, releasing its link.
func TestMirrorWorker_RecoversFromPanic(t *testing.T) {
	q := newMirrorQueue(1<<20, nil)
	q.tryEnqueue(mirrorItem{typ: mirrorMsg, msg: &pgproto3.Query{String: "boom"}, expectation: query.QueryReply, size: 10})
	q.tryEnqueue(mirrorItem{typ: mirrorRelease, size: mirrorItemOverhead})

	l := &fakeShadowLink{panicOnSend: true}
	w := newMirrorWorker(q, func() (Link, error) { return l, nil }, nil, nil)
	done := make(chan struct{})
	go func() { w.run(); close(done) }() // must not crash the test process

	// The panic on Send is recovered, so run() returns and closes done on its own —
	// no q.close() needed. Without recover, the panic would abort the test process.
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not exit after a panic (recover missing?)")
	}
	if l.forceCloses != 1 {
		t.Errorf("forceCloses = %d, want 1 (panic exit must discard the borrowed link)", l.forceCloses)
	}
	// The panic path must close the queue so the producer stops filling one no worker
	// drains: a further enqueue is rejected.
	if q.tryEnqueue(mirrorItem{typ: mirrorMsg, msg: &pgproto3.Query{String: "later"}, size: 10}) {
		t.Error("queue still open after worker panic; enqueue should be rejected")
	}
}

// TestMirrorWorker_HoldsBudgetUntilFlush verifies a buffered-but-unflushed message
// keeps its budget reserved: it is not released at dequeue, so the byte cap bounds
// live mirrored data (queue + shadow write buffer), not just what sits in the queue.
func TestMirrorWorker_HoldsBudgetUntilFlush(t *testing.T) {
	const itemSize = 10
	cost := itemSize + mirrorItemStructOverhead
	budget := newMirrorBudget(100 * int64(cost))
	q := newMirrorQueue(100*cost, budget)
	q.tryEnqueue(mirrorItem{typ: mirrorMsg, msg: &pgproto3.Query{String: "q"}, expectation: query.QueryReply, size: itemSize})

	f := newLinkFactory(nil)
	w := newMirrorWorker(q, f.acquire, nil, nil)
	go w.run()

	// The message is dequeued (queue empties) and Sent to the shadow buffer, but no
	// flush sentinel has arrived, so its budget must stay reserved.
	waitFor(t, func() bool { b, _ := q.stats(); return b == 0 }, "worker never dequeued the message")
	if used := budget.UsedBytes(); used != int64(cost) {
		t.Errorf("budget = %d after Send but before flush, want %d (buffered bytes must stay reserved)", used, cost)
	}

	q.tryEnqueue(mirrorItem{typ: mirrorFlush, size: mirrorItemOverhead})
	q.tryEnqueue(mirrorItem{typ: mirrorRelease, size: mirrorItemOverhead})
	waitFor(t, func() bool { b, _ := q.stats(); return b == 0 }, "worker never drained the flush/release")
	q.close()
	waitFor(t, func() bool { return budget.UsedBytes() == 0 }, "budget not fully released after flush + drain")
}

// sentKinds names each message the shadow link was sent, for readable assertions.
func sentKinds(l *fakeShadowLink) []string {
	kinds := make([]string, len(l.sent))
	for i, msg := range l.sent {
		switch m := msg.(type) {
		case *pgproto3.Parse:
			kinds[i] = "Parse:" + m.Name
		case *pgproto3.Bind:
			kinds[i] = "Bind:" + m.PreparedStatement
		case *pgproto3.Describe:
			kinds[i] = "Describe:" + string(m.ObjectType) + ":" + m.Name
		default:
			kinds[i] = fmt.Sprintf("%T", msg)
		}
	}
	return kinds
}

// perLinkFactory prepares the nth acquired link with preps[n], so one batch's
// connection can be cold and the next one's warm.
func perLinkFactory(preps ...func(*fakeShadowLink)) *linkFactory {
	f := &linkFactory{}
	f.acquire = func() (Link, error) {
		l := &fakeShadowLink{}
		if n := len(f.links); n < len(preps) && preps[n] != nil {
			preps[n](l)
		}
		f.links = append(f.links, l)
		return l, nil
	}
	return f
}

// enqueueBatch loads one batch: the messages, then the flush and release sentinels.
func enqueueBatch(q *mirrorQueue, msgs ...mirrorItem) {
	for _, it := range msgs {
		it.typ = mirrorMsg
		it.size = 10
		q.tryEnqueue(it)
	}
	q.tryEnqueue(mirrorItem{typ: mirrorFlush, size: mirrorItemOverhead})
	q.tryEnqueue(mirrorItem{typ: mirrorRelease, size: mirrorItemOverhead})
}

// 42P05 guard: a Parse for a statement the connection has must not reach the wire.
func TestMirrorWorker_DuplicateParseNotResent(t *testing.T) {
	q := newMirrorQueue(1<<20, nil)
	enqueueBatch(q,
		mirrorItem{msg: &pgproto3.Parse{Name: "s1", Query: "select 1"}, expectation: query.ParseReply},
		mirrorItem{msg: &pgproto3.Parse{Name: "s1", Query: "select 1"}, expectation: query.ParseReply},
	)

	f := newLinkFactory(func(l *fakeShadowLink) {
		l.replies = []pgproto3.BackendMessage{&pgproto3.ParseComplete{}}
	})
	drainClosed(q, f.acquire)

	l := f.links[0]
	if got, want := sentKinds(l), []string{"Parse:s1"}; !slices.Equal(got, want) {
		t.Errorf("sent = %v, want %v (the duplicate Parse would be 42P05)", got, want)
	}
	if got, want := l.existingUsed, []string{"s1"}; !slices.Equal(got, want) {
		t.Errorf("RecordExistingNamedPreparedStmtUsed = %v, want %v", got, want)
	}
	if len(l.expectations) != 0 {
		t.Errorf("undrained expectations = %d, want 0 (the skipped Parse must register none)", len(l.expectations))
	}
}

func TestMirrorWorker_ReparsesOnFreshConnection(t *testing.T) {
	q := newMirrorQueue(1<<20, nil)
	enqueueBatch(q,
		mirrorItem{msg: &pgproto3.Parse{Name: "s1", Query: "select 1"}, expectation: query.ParseReply},
		mirrorItem{msg: &pgproto3.Bind{PreparedStatement: "s1"}, expectation: query.BindReply},
	)
	// Batch 2: only a Bind, as pgx sends it once the source has parsed.
	enqueueBatch(q, mirrorItem{msg: &pgproto3.Bind{PreparedStatement: "s1"}, expectation: query.BindReply})

	replies := func(l *fakeShadowLink) {
		l.replies = []pgproto3.BackendMessage{&pgproto3.ParseComplete{}, &pgproto3.BindComplete{}}
	}
	f := perLinkFactory(replies, replies)
	drainClosed(q, f.acquire)

	if len(f.links) != 2 {
		t.Fatalf("acquired %d links, want 2", len(f.links))
	}
	if got, want := sentKinds(f.links[1]), []string{"Parse:s1", "Bind:s1"}; !slices.Equal(got, want) {
		t.Errorf("batch 2 sent = %v, want %v (the Parse must be re-injected)", got, want)
	}
	if len(f.links[1].expectations) != 0 {
		t.Errorf("undrained expectations = %d, want 0 (the injected Parse must be drained in-batch)", len(f.links[1].expectations))
	}
}

// The catalog has it but so does the connection, so nothing is re-sent.
func TestMirrorWorker_NoReparseWhenConnectionHasStmt(t *testing.T) {
	q := newMirrorQueue(1<<20, nil)
	enqueueBatch(q,
		mirrorItem{msg: &pgproto3.Parse{Name: "s1", Query: "select 1"}, expectation: query.ParseReply},
		mirrorItem{msg: &pgproto3.Bind{PreparedStatement: "s1"}, expectation: query.BindReply},
	)
	enqueueBatch(q, mirrorItem{msg: &pgproto3.Bind{PreparedStatement: "s1"}, expectation: query.BindReply})

	f := perLinkFactory(
		func(l *fakeShadowLink) {
			l.replies = []pgproto3.BackendMessage{&pgproto3.ParseComplete{}, &pgproto3.BindComplete{}}
		},
		func(l *fakeShadowLink) { // batch 2 borrows a connection that kept the statement
			l.stmts = map[string]bool{"s1": true}
			l.replies = []pgproto3.BackendMessage{&pgproto3.BindComplete{}}
		},
	)
	drainClosed(q, f.acquire)

	if got, want := sentKinds(f.links[1]), []string{"Bind:s1"}; !slices.Equal(got, want) {
		t.Errorf("batch 2 sent = %v, want %v (no Parse: the connection has it)", got, want)
	}
}

// A Bind whose definition the worker never saw must not skip the batch.
func TestMirrorWorker_CatalogMissContinuesBatch(t *testing.T) {
	q := newMirrorQueue(1<<20, nil)
	enqueueBatch(q,
		mirrorItem{msg: &pgproto3.Bind{PreparedStatement: "unknown"}, expectation: query.BindReply},
		mirrorItem{msg: &pgproto3.Execute{}, expectation: query.ExecuteReply},
	)

	f := newLinkFactory(func(l *fakeShadowLink) {
		l.replies = []pgproto3.BackendMessage{&pgproto3.BindComplete{}, &pgproto3.CommandComplete{}}
	})
	drainClosed(q, f.acquire)

	l := f.links[0]
	if got, want := sentKinds(l), []string{"Bind:unknown", "*pgproto3.Execute"}; !slices.Equal(got, want) {
		t.Errorf("sent = %v, want %v (a catalog miss must not skip the batch)", got, want)
	}
	if l.releases != 1 {
		t.Errorf("releases = %d, want 1", l.releases)
	}
}

// The oldest entry goes, the newest stays.
func TestStmtCatalog_EvictsOldest(t *testing.T) {
	c := newStmtCatalog()
	name := func(i int) string { return fmt.Sprintf("s%d", i) }
	for i := 0; i <= mirrorStmtCatalogMax; i++ { // one more than fits
		c.put(prepared.CreatePreparedStatement("", name(i), "select 1", nil))
	}

	if len(c.byName) != mirrorStmtCatalogMax {
		t.Errorf("catalog size = %d, want %d", len(c.byName), mirrorStmtCatalogMax)
	}
	if _, ok := c.get(name(0)); ok {
		t.Error("oldest entry still present, want evicted")
	}
	if _, ok := c.get(name(mirrorStmtCatalogMax)); !ok {
		t.Error("newest entry missing, want present")
	}
}

// Forwards a client Describe on a named statement, so a batch can open with one on a cold connection.
func TestMirrorWorker_DescribeStatementReparses(t *testing.T) {
	q := newMirrorQueue(1<<20, nil)
	enqueueBatch(q,
		mirrorItem{msg: &pgproto3.Parse{Name: "s1", Query: "select 1"}, expectation: query.ParseReply},
		mirrorItem{msg: &pgproto3.Bind{PreparedStatement: "s1"}, expectation: query.BindReply},
	)
	enqueueBatch(q,
		mirrorItem{msg: &pgproto3.Describe{ObjectType: 'S', Name: "s1"}, expectation: query.DescribeReply},
		mirrorItem{msg: &pgproto3.Describe{ObjectType: 'P'}, expectation: query.DescribeReply},
	)

	f := perLinkFactory(
		func(l *fakeShadowLink) {
			l.replies = []pgproto3.BackendMessage{&pgproto3.ParseComplete{}, &pgproto3.BindComplete{}}
		},
		func(l *fakeShadowLink) {
			l.replies = []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{}, &pgproto3.RowDescription{}, &pgproto3.RowDescription{},
			}
		},
	)
	drainClosed(q, f.acquire)

	want := []string{"Parse:s1", "Describe:S:s1", "Describe:P:"}
	if got := sentKinds(f.links[1]); !slices.Equal(got, want) {
		t.Errorf("batch 2 sent = %v, want %v", got, want)
	}
}

// A client that prepared before mirroring was enabled never sends Parse again, so
// the seeded catalog is the only thing standing between its Binds and 26000.
func TestMirrorWorker_SeededCatalogReparsesWithoutParse(t *testing.T) {
	q := newMirrorQueue(1<<20, nil)
	enqueueBatch(q, mirrorItem{msg: &pgproto3.Bind{PreparedStatement: "s1"}, expectation: query.BindReply})

	seed := []prepared.PreparedStatement{prepared.CreatePreparedStatement("c1", "s1", "select 1", nil)}
	f := newLinkFactory(func(l *fakeShadowLink) {
		l.replies = []pgproto3.BackendMessage{&pgproto3.ParseComplete{}, &pgproto3.BindComplete{}}
	})
	drainClosedWithSeed(q, f.acquire, seed)

	l := f.links[0]
	if got, want := sentKinds(l), []string{"Parse:s1", "Bind:s1"}; !slices.Equal(got, want) {
		t.Errorf("sent = %v, want %v (the seeded Parse must be injected)", got, want)
	}
	if len(l.expectations) != 0 {
		t.Errorf("undrained expectations = %d, want 0", len(l.expectations))
	}
}

// Without the seed the same batch is exactly the staging failure: a bare Bind.
func TestMirrorWorker_UnseededCatalogSendsBareBind(t *testing.T) {
	q := newMirrorQueue(1<<20, nil)
	enqueueBatch(q, mirrorItem{msg: &pgproto3.Bind{PreparedStatement: "s1"}, expectation: query.BindReply})

	f := newLinkFactory(func(l *fakeShadowLink) {
		l.replies = []pgproto3.BackendMessage{&pgproto3.BindComplete{}}
	})
	drainClosed(q, f.acquire)

	if got, want := sentKinds(f.links[0]), []string{"Bind:s1"}; !slices.Equal(got, want) {
		t.Errorf("sent = %v, want %v", got, want)
	}
}

// A batch that never got a shadow link still carries its Parse definitions, so the
// next batch can repair its Binds instead of missing the catalog forever.
func TestMirrorWorker_CatalogSurvivesAcquireFailure(t *testing.T) {
	q := newMirrorQueue(1<<20, nil)
	enqueueBatch(q,
		mirrorItem{msg: &pgproto3.Parse{Name: "s1", Query: "select 1"}, expectation: query.ParseReply},
		mirrorItem{msg: &pgproto3.Bind{PreparedStatement: "s1"}, expectation: query.BindReply},
	)
	enqueueBatch(q, mirrorItem{msg: &pgproto3.Bind{PreparedStatement: "s1"}, expectation: query.BindReply})

	calls := 0
	f := &linkFactory{}
	f.acquire = func() (Link, error) {
		calls++
		if calls == 1 {
			return nil, errors.New("no shadow state")
		}
		l := &fakeShadowLink{replies: []pgproto3.BackendMessage{&pgproto3.ParseComplete{}, &pgproto3.BindComplete{}}}
		f.links = append(f.links, l)
		return l, nil
	}
	drainClosed(q, f.acquire)

	if len(f.links) != 1 {
		t.Fatalf("acquired %d links, want 1 (batch 1 fails, batch 2 succeeds)", len(f.links))
	}
	if got, want := sentKinds(f.links[0]), []string{"Parse:s1", "Bind:s1"}; !slices.Equal(got, want) {
		t.Errorf("batch 2 sent = %v, want %v (batch 1's Parse must still be catalogued)", got, want)
	}
}

// A statement is recorded on the shadow connection before Postgres answers, and the
// record cannot be retracted. If that batch errored, the connection must not go back
// to the pool claiming a statement it may not have.
func TestMirrorWorker_ErrorAfterRecordingDiscardsConnection(t *testing.T) {
	q := newMirrorQueue(1<<20, nil)
	enqueueBatch(q,
		mirrorItem{msg: &pgproto3.Parse{Name: "s1", Query: "select bogus"}, expectation: query.ParseReply},
		mirrorItem{msg: &pgproto3.Sync{}, expectation: query.SyncReply},
	)

	f := newLinkFactory(func(l *fakeShadowLink) {
		l.replies = []pgproto3.BackendMessage{&pgproto3.ErrorResponse{Code: "42601"}, &pgproto3.ReadyForQuery{TxStatus: 'I'}}
	})
	drainClosed(q, f.acquire)

	if l := f.links[0]; l.forceCloses != 1 {
		t.Errorf("forceCloses = %d, want 1 (a connection with an unconfirmed statement must be discarded)", l.forceCloses)
	}
}

// A non-FATAL error puts Postgres in its discard-until-Sync state, which outlives the
// flush that observed it. A Parse replayed in that window is dropped by Postgres, so
// claiming it on the connection would hand the pool a statement it does not have and
// cost a later session a 26000.
//
// Built by hand rather than with enqueueBatch: this needs a flush sentinel mid-batch,
// and nothing recorded in the erroring window, so the sawError && recorded guard stays
// out of the way and the discard state is what is under test.
func TestMirrorWorker_NoClaimWhileDiscardingUntilSync(t *testing.T) {
	q := newMirrorQueue(1<<20, nil)
	// First window: an Execute fails, so Postgres starts discarding. Nothing recorded.
	q.tryEnqueue(mirrorItem{typ: mirrorMsg, size: 10, msg: &pgproto3.Execute{}, expectation: query.ExecuteReply})
	q.enqueueSentinel(mirrorItem{typ: mirrorFlush, size: mirrorItemOverhead})
	// Same batch, still before the Sync: Postgres ignores this Parse.
	q.tryEnqueue(mirrorItem{typ: mirrorMsg, size: 10, msg: &pgproto3.Parse{Name: "s2", Query: "select 2"}, expectation: query.ParseReply})
	q.tryEnqueue(mirrorItem{typ: mirrorMsg, size: 10, msg: &pgproto3.Sync{}, expectation: query.SyncReply})
	q.enqueueSentinel(mirrorItem{typ: mirrorFlush, size: mirrorItemOverhead})
	q.enqueueSentinel(mirrorItem{typ: mirrorRelease, size: mirrorItemOverhead})

	f := newLinkFactory(func(l *fakeShadowLink) {
		l.replies = []pgproto3.BackendMessage{
			&pgproto3.ErrorResponse{Code: "42703"}, // answers the Execute
			&pgproto3.ReadyForQuery{TxStatus: 'I'}, // answers the Sync
		}
	})
	drainClosed(q, f.acquire)

	l := f.links[0]
	if l.stmts["s2"] {
		t.Error("s2 was claimed on the connection, but Postgres discarded it until the Sync")
	}
	if l.forceCloses != 0 {
		t.Errorf("forceCloses = %d, want 0: nothing was claimed, so the connection is still good", l.forceCloses)
	}
}
