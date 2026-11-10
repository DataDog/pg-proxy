// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package session

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"

	"github.com/DataDog/pg-proxy/pkg/link"
	"github.com/DataDog/pg-proxy/pkg/mirror"
	"github.com/DataDog/pg-proxy/pkg/query"
)

const mirrorTestDatabase = "logical"

// mirrorTestSession builds a session whose mirroring policy is toggled through the
// returned flag, so a test can flip mirroring off the way a configuration change would.
func mirrorTestSession(t *testing.T, budget *mirror.Budget, acquireShadow func(context.Context, string, LinkRequest) (link.Link, error)) (*Session, *atomic.Bool) {
	t.Helper()
	enabled := &atomic.Bool{}
	enabled.Store(true)
	s := New(Config{
		Context: context.Background(), User: "user", Database: "database",
		LogicalDatabase: mirrorTestDatabase, Client: &testClient{},
		Mirroring: Mirroring{
			Policy: func(context.Context) MirrorPolicy {
				return MirrorPolicy{
					Enabled:        enabled.Load(),
					SourceDatabase: mirrorTestDatabase,
					ShadowDatabase: "shadow",
					DisableAt:      time.Now().Add(time.Hour),
					QueueSize:      1 << 20,
				}
			},
			AcquireShadow: acquireShadow,
		},
		MirrorBudget: budget,
	})
	t.Cleanup(func() {
		if s.mirror != nil {
			s.mirror.Close()
		}
		if s.mirrorWorker != nil {
			s.mirrorWorker.Stop(time.Second)
		}
	})
	return s, enabled
}

// TestMirroringLinkFor_RebuildsOnReEnable: turning the policy off drops the queue and
// worker, and turning it back on builds new ones. The rebuild is what re-reads the
// ShadowDatabase and tags, which a worker otherwise pins for its whole lifetime.
func TestMirroringLinkFor_RebuildsOnReEnable(t *testing.T) {
	s, enabled := mirrorTestSession(t, mirror.NewBudget(1<<20), nil)

	s.mirroringLinkFor(&testLink{})
	firstQueue, firstWorker := s.mirror, s.mirrorWorker
	require.NotNil(t, firstQueue)
	require.NotNil(t, firstWorker)

	enabled.Store(false)
	source := &testLink{}
	require.Same(t, link.Link(source), s.mirroringLinkFor(source), "a disabled session must not wrap its link")
	require.Nil(t, s.mirror, "the queue must be torn down when mirroring is disabled")
	require.Nil(t, s.mirrorWorker, "the worker must be torn down when mirroring is disabled")

	// The disable branch runs on every transaction while mirroring is off, so repeated
	// teardowns must not double-close the queue.
	for range 2 {
		s.mirroringLinkFor(&testLink{})
		require.Nil(t, s.mirror)
	}

	enabled.Store(true)
	s.mirroringLinkFor(&testLink{})
	require.NotNil(t, s.mirror)
	require.NotNil(t, s.mirrorWorker)
	require.NotSame(t, firstQueue, s.mirror, "re-enabling must build a fresh queue")
	require.NotSame(t, firstWorker, s.mirrorWorker, "re-enabling must build a fresh worker")
}

// TestMirroringLinkFor_ReleasesBudgetOnDisable: the undrained tail's bytes go back to the
// shared, process-wide budget as soon as mirroring is disabled, instead of trickling back
// batch by batch (or never, while the worker is parked) and starving live mirrored sessions.
func TestMirroringLinkFor_ReleasesBudgetOnDisable(t *testing.T) {
	const queuedMessages = 4
	payload := strings.Repeat("x", 1024)
	inFlightCost := oneSendCost(t, payload)

	acquireStarted := make(chan struct{})
	releaseAcquire := make(chan struct{})
	var releaseOnce sync.Once
	releaseWorker := func() { releaseOnce.Do(func() { close(releaseAcquire) }) }
	// Honour ctx like a real acquire: the session bounds this call, so the worker frees itself
	// on that deadline even if nothing closes releaseAcquire. Without it an assertion that
	// fails below leaves the worker parked and the session's Stop cleanup waits out both of
	// its timeouts, and cleanups run last-in-first-out so releaseWorker cannot rescue it.
	acquireShadow := func(ctx context.Context, _ string, _ LinkRequest) (link.Link, error) {
		close(acquireStarted) // the session's acquire closure gates on mirroringApplicable, so this runs once
		select {
		case <-releaseAcquire:
		case <-ctx.Done():
		}
		return nil, errors.New("shadow unavailable")
	}
	budget := mirror.NewBudget(1 << 20)
	s, enabled := mirrorTestSession(t, budget, acquireShadow)
	t.Cleanup(releaseWorker)

	mirrored := s.mirroringLinkFor(&testLink{})
	for range queuedMessages {
		mirrored.Send(&pgproto3.Query{String: payload}, query.QueryReply)
	}
	select {
	case <-acquireStarted: // the worker holds the first message in flight and is stuck acquiring
	case <-time.After(5 * time.Second):
		t.Fatal("worker never reached acquire; it cannot be holding anything in flight")
	}
	require.Positive(t, budget.UsedBytes())

	enabled.Store(false)
	s.mirroringLinkFor(&testLink{})
	require.LessOrEqual(t, budget.UsedBytes(), inFlightCost,
		"disabling must return the undrained tail at once, leaving only what the worker holds")

	releaseWorker() // let the stuck acquire fail so the worker can exit
	require.Eventually(t, func() bool { return budget.UsedBytes() == 0 }, 2*time.Second, 10*time.Millisecond,
		"the exiting worker must return its in-flight bytes too")
}

// oneSendCost measures what a single Send reserves against a budget, sentinels and
// per-item overhead included, so the assertion above does not assume every queued item
// costs the same.
func oneSendCost(t *testing.T, payload string) int64 {
	t.Helper()
	budget := mirror.NewBudget(1 << 20)
	q := mirror.NewQueue(1<<20, budget)
	t.Cleanup(q.Close)
	mirror.NewLink(&testLink{}, q, nil).Send(&pgproto3.Query{String: payload}, query.QueryReply)
	used := budget.UsedBytes()
	require.Positive(t, used, "a Send must reserve something, or the measurement is meaningless")
	return used
}

// TestMirroringLinkFor_NoPolicyIsNoOp: mirroringLinkFor runs on every link acquire in the
// process, including deployments with no mirroring configured, so it must stay inert there.
func TestMirroringLinkFor_NoPolicyIsNoOp(t *testing.T) {
	s := New(Config{Context: context.Background(), Client: &testClient{}})

	source := &testLink{}
	require.Same(t, link.Link(source), s.mirroringLinkFor(source))
	require.Nil(t, s.mirror)
	require.Nil(t, s.mirrorWorker)
}

// TestMirroringLinkFor_DisableDoesNotWaitOnWedgedShadow: a shadow stuck waiting for a reply
// must not make a real query wait. Disabling hands the source link straight back and the
// wedged shadow is force-closed and released off the client path.
func TestMirroringLinkFor_DisableDoesNotWaitOnWedgedShadow(t *testing.T) {
	shadowBlocked := make(chan struct{})
	var unblockOnce sync.Once
	unblockShadow := func() { unblockOnce.Do(func() { close(shadowBlocked) }) }
	receiveStarted := make(chan struct{})
	var receiveOnce sync.Once
	var forceCloses, releases atomic.Int64

	shadow := &testLink{
		nextExpectation: func() query.ReplyExpectation { return query.QueryReply },
		receive: func() (pgproto3.BackendMessage, error) {
			receiveOnce.Do(func() { close(receiveStarted) })
			<-shadowBlocked // a real wedged read only ends when the link is closed under it
			return nil, errors.New("shadow link closed")
		},
		forceClose: func() bool { forceCloses.Add(1); unblockShadow(); return true },
		release:    func() error { releases.Add(1); return nil },
	}
	acquireShadow := func(context.Context, string, LinkRequest) (link.Link, error) { return shadow, nil }
	s, enabled := mirrorTestSession(t, mirror.NewBudget(1<<20), acquireShadow)
	// Registered after mirrorTestSession so it runs before that cleanup's Stop: cleanups are
	// last-in-first-out, and a Stop against a still-wedged fake would wait out both timeouts.
	t.Cleanup(unblockShadow)

	mirrored := s.mirroringLinkFor(&testLink{})
	mirrored.Send(&pgproto3.Query{String: "select 1"}, query.QueryReply)
	require.NoError(t, mirrored.Flush()) // the flush sentinel is what sends the worker into the shadow read
	select {
	case <-receiveStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("worker never read from the shadow; it cannot be wedged")
	}

	enabled.Store(false)
	start := time.Now()
	source := &testLink{}
	require.Same(t, link.Link(source), s.mirroringLinkFor(source), "a disabled session must hand its source link back unwrapped")
	require.Less(t, time.Since(start), mirror.TeardownDrainTimeout,
		"teardown must not hold the client path while the shadow is wedged")

	// stop force-closes at TeardownDrainTimeout, well inside flushAndDrain's own 5s guard,
	// so this asserts the detached stop did it rather than the drain timer.
	require.Eventually(t, func() bool { return forceCloses.Load() > 0 && releases.Load() > 0 },
		2*mirror.TeardownDrainTimeout, 10*time.Millisecond,
		"the detached stop must force-close and release the wedged shadow link")
}
