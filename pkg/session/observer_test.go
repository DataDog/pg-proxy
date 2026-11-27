// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package session

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pgproxy "github.com/DataDog/pg-proxy"
	"github.com/DataDog/pg-proxy/pkg/link"
	"github.com/DataDog/pg-proxy/pkg/mirror"
)

type recordingObserver struct {
	events []string
	metas  []CheckoutMeta
	closed int
}

func (o *recordingObserver) OnCheckout(meta CheckoutMeta) {
	o.events = append(o.events, "checkout")
	o.metas = append(o.metas, meta)
}
func (o *recordingObserver) OnRelease() { o.events = append(o.events, "release") }
func (o *recordingObserver) OnFrontendMessage(pgproto3.FrontendMessage) {
	o.events = append(o.events, "frontend")
}
func (o *recordingObserver) OnBackendMessage(pgproto3.BackendMessage) {
	o.events = append(o.events, "backend")
}
func (o *recordingObserver) OnClientFlushSucceeded() { o.events = append(o.events, "flush") }
func (o *recordingObserver) OnClosed() {
	o.events = append(o.events, "closed")
	o.closed++
}

type panickingObserver struct {
	panicOn string
	closed  int
	calls   int
}

func (o *panickingObserver) OnCheckout(CheckoutMeta) {
	o.calls++
	if o.panicOn == "checkout" {
		panic("boom checkout")
	}
}
func (o *panickingObserver) OnRelease() {
	o.calls++
	if o.panicOn == "release" {
		panic("boom release")
	}
}
func (o *panickingObserver) OnFrontendMessage(pgproto3.FrontendMessage) {
	o.calls++
	if o.panicOn == "frontend_message" {
		panic("boom frontend")
	}
}
func (o *panickingObserver) OnBackendMessage(pgproto3.BackendMessage) {
	o.calls++
	if o.panicOn == "backend_message" {
		panic("boom backend")
	}
}
func (o *panickingObserver) OnClientFlushSucceeded() {
	o.calls++
	if o.panicOn == "client_flush_succeeded" {
		panic("boom flush")
	}
}
func (o *panickingObserver) OnClosed() {
	o.closed++
	if o.panicOn == "closed" {
		panic("boom closed")
	}
}

type checkoutTestBackend struct {
	meta    CheckoutMeta
	acquire func(context.Context, LinkRequest) (link.Link, error)
}

func (b *checkoutTestBackend) Acquire(ctx context.Context, req LinkRequest) (link.Link, error) {
	return b.acquire(ctx, req)
}

func (b *checkoutTestBackend) Checkout(ctx context.Context, req LinkRequest) (AcquiredLink, error) {
	l, err := b.acquire(ctx, req)
	if err != nil {
		return AcquiredLink{}, err
	}
	return AcquiredLink{Link: l, Meta: b.meta}, nil
}

var sharedPassthroughLoadShedder LoadShedder = &passthroughLoadShedder{}

func (b *checkoutTestBackend) LoadShedder() LoadShedder { return sharedPassthroughLoadShedder }

func hasObserverPanicMetric(m *recordingMetrics, hook string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, call := range m.counts {
		if call.name == pgproxy.DefaultMetricNamespace+observerPanicMetric && slices.Contains(call.tags, "hook:"+hook) {
			return true
		}
	}
	return false
}

func TestObserverPanicDisablesOnlyPanickingObserver(t *testing.T) {
	hooks := []string{
		"checkout",
		"frontend_message",
		"backend_message",
		"release",
		"client_flush_succeeded",
		"closed",
	}

	for _, hookName := range hooks {
		t.Run(hookName, func(t *testing.T) {
			metrics := &recordingMetrics{}
			restore := pgproxy.Bootstrap(pgproxy.Dependencies{Metrics: metrics})
			defer restore()

			h := newCycleHarness(t, false)
			panicker := &panickingObserver{panicOn: hookName}
			healthy := &recordingObserver{}

			client := h.newClient(func(cfg *Config) {
				cfg.Observers = []Observer{panicker, healthy}
			})
			script := protocolCycles[1].cycle(t, h) // simple_query

			require.NoError(t, client.run(script), "first cycle must succeed despite observer panic")
			callsAfterFirst := panicker.calls

			// Second cycle proves the session stayed healthy, panicker stayed
			// disabled (except when panicking only on close), and healthy kept
			// receiving every event.
			require.NoError(t, client.run(script), "second cycle must succeed")
			client.session.Close(nil)
			client.session.Close(nil) // idempotent

			assert.True(t, hasObserverPanicMetric(metrics, hookName), "expected panic metric for hook %q", hookName)
			assert.Equal(t, 1, panicker.closed, "panicking observer OnClosed must run exactly once")
			assert.Equal(t, 1, healthy.closed, "healthy observer OnClosed must run exactly once")
			if hookName != "closed" {
				assert.Equal(t, callsAfterFirst, panicker.calls, "disabled observer must receive no further callbacks")
			}
			// Healthy observer must see both cycles plus final close.
			assert.Equal(t, []string{
				"frontend", "checkout", "backend", "backend", "backend", "backend", "release", "flush",
				"frontend", "checkout", "backend", "backend", "backend", "backend", "release", "flush",
				"closed",
			}, healthy.events)
		})
	}
}

func TestObserverLifecycleOrderingWithMirroring(t *testing.T) {
	h := newCycleHarness(t, false)
	meta := CheckoutMeta{
		ClusterName:   "example-cluster",
		EffectiveRole: "app_user",
		Rerouted:      true,
		ForReadQuery:  true,
		Annotation:    "route-token",
	}
	origAcquire := h.backend.acquire
	wrappedBackend := &checkoutTestBackend{
		meta:    meta,
		acquire: origAcquire,
	}

	obs := &recordingObserver{}
	budget := mirror.NewBudget(1 << 20)
	client := h.newClient(func(cfg *Config) {
		cfg.Backend = wrappedBackend
		cfg.Observers = []Observer{obs}
		cfg.MirrorBudget = budget
		cfg.Mirroring = Mirroring{
			Policy: func(context.Context) MirrorPolicy {
				return MirrorPolicy{
					Enabled:        true,
					SourceDatabase: "app_db",
					ShadowDatabase: "shadow_db",
					DisableAt:      time.Now().Add(time.Hour),
					QueueSize:      1 << 20,
				}
			},
		}
	})
	script := protocolCycles[1].cycle(t, h) // simple_query

	require.NoError(t, client.run(script))
	client.session.Close(nil)

	require.Len(t, obs.metas, 1)
	assert.Equal(t, meta, obs.metas[0])
	assert.Equal(t, []string{
		"frontend", "checkout", "backend", "backend", "backend", "backend", "release", "flush", "closed",
	}, obs.events)
}

type getterCallingObserver struct {
	s *Session
}

func (o *getterCallingObserver) checkAll() {
	_ = o.s.Idle()
	_ = o.s.Cancel()
	_ = o.s.Backend()
	_ = o.s.PendingBackend()
	_ = o.s.ReadStates()
	_ = o.s.CurrentLink()
}

func (o *getterCallingObserver) OnCheckout(CheckoutMeta)                    { o.checkAll() }
func (o *getterCallingObserver) OnRelease()                                 { o.checkAll() }
func (o *getterCallingObserver) OnFrontendMessage(pgproto3.FrontendMessage) { o.checkAll() }
func (o *getterCallingObserver) OnBackendMessage(pgproto3.BackendMessage)   { o.checkAll() }
func (o *getterCallingObserver) OnClientFlushSucceeded()                    { o.checkAll() }
func (o *getterCallingObserver) OnClosed()                                  { o.checkAll() }

// TestObserverCanCallAllSessionGettersWithoutDeadlock verifies under -race that
// every Observer callback runs lock-free on the session goroutine, so calling
// Idle, Cancel, Backend, PendingBackend, ReadStates, and CurrentLink never
// deadlocks or races with concurrent Cancel and Remap* callers.
func TestObserverCanCallAllSessionGettersWithoutDeadlock(t *testing.T) {
	obs := &getterCallingObserver{}
	next := &namedBackend{name: "next"}
	reads := []ReadStateConfig{{Backend: next, Weight: 1}}
	s := newPortableSession(context.Background(), &testClient{}, nil, &namedBackend{name: "initial"},
		func(cfg *Config) { cfg.Observers = []Observer{obs} })
	obs.s = s

	done := make(chan struct{})
	go func() {
		defer close(done)
		runConcurrently(
			func() {
				s.setLink(&testLink{})
				s.notifyCheckout(CheckoutMeta{ClusterName: "c1"})
				s.notifyFrontendMessage(&pgproto3.Query{String: "select 1"})
				s.notifyBackendMessage(&pgproto3.ReadyForQuery{TxStatus: 'I'})
				s.handleStateRemapping()
				s.releaseCurrentLink(false)
				s.notifyClientFlushSucceeded()
			},
			func() {
				_ = s.Cancel()
				s.RemapBackend(next)
				s.RemapReadStates(reads)
				s.RemapBackend(nil)
				s.RemapReadStates(nil)
			},
		)
		s.Close(nil)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("observer deadlocked calling session getters")
	}
}

// TestCancelAndDisconnectDoNotBlockOnReleaseIO verifies that Cancel and
// disconnect ForceClose return immediately while Release is blocked on I/O,
// because releaseCurrentLink fences AbortHandle before calling link.Release.
func TestCancelAndDisconnectDoNotBlockOnReleaseIO(t *testing.T) {
	releaseStarted := make(chan struct{})
	finishRelease := make(chan struct{})
	finishOnce := sync.OnceFunc(func() { close(finishRelease) })
	defer finishOnce()

	var cancelCalls, forceCloseCalls int
	backendLink := &testLink{
		release: func() error {
			close(releaseStarted)
			<-finishRelease
			return nil
		},
		cancel: func() error {
			cancelCalls++
			return nil
		},
		forceClose: func() bool {
			forceCloseCalls++
			return true
		},
	}
	probe := &fakeProbe{}
	s := newMonitorTestSession(true, probe)
	s.setLink(backendLink)

	releaseDone := make(chan struct{})
	go func() {
		defer close(releaseDone)
		s.releaseCurrentLink(false)
	}()
	<-releaseStarted

	cancelDone := make(chan error, 1)
	go func() { cancelDone <- s.Cancel() }()
	select {
	case err := <-cancelDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Session.Cancel blocked on Release I/O")
	}

	handleDone := make(chan struct{})
	go func() {
		defer close(handleDone)
		_, cancel := context.WithCancelCause(context.Background())
		s.handleClientDisconnect(cancel)
	}()
	select {
	case <-handleDone:
	case <-time.After(time.Second):
		t.Fatal("handleClientDisconnect blocked on Release I/O")
	}

	finishOnce()
	<-releaseDone

	assert.Zero(t, cancelCalls, "Cancel must not invoke the link once Release started")
	assert.Zero(t, forceCloseCalls, "ForceClose must not invoke the link once Release started")
}

type embeddedSessionLinkWrapper struct {
	*link.SessionLink
	cancelCalls     int
	forceCloseCalls int
}

func (w *embeddedSessionLinkWrapper) Cancel() error {
	w.cancelCalls++
	return nil
}

func (w *embeddedSessionLinkWrapper) ForceClose() bool {
	w.forceCloseCalls++
	return true
}

// TestWrapperEmbeddingSessionLinkPreservesCancelAndForceClose verifies that a
// Link wrapper embedding *link.SessionLink still has its own Cancel and
// ForceClose methods called rather than the embedded link's handle.
func TestWrapperEmbeddingSessionLinkPreservesCancelAndForceClose(t *testing.T) {
	wrapper := &embeddedSessionLinkWrapper{
		SessionLink: link.NewFrontendLink(nil),
	}
	s := newMonitorTestSession(true, &fakeProbe{})
	s.setLink(wrapper)

	require.NoError(t, s.Cancel())
	assert.Equal(t, 1, wrapper.cancelCalls, "Session.Cancel must invoke wrapper's Cancel override")

	_, cancel := context.WithCancelCause(context.Background())
	s.handleClientDisconnect(cancel)
	assert.Equal(t, 1, wrapper.forceCloseCalls, "client disconnect must invoke wrapper's ForceClose override")
}

func TestInitialLinkEmitsPairedCheckoutAndRelease(t *testing.T) {
	obs := &recordingObserver{}
	meta := CheckoutMeta{
		ClusterName:   "initial-cluster",
		EffectiveRole: "app_user",
		Annotation:    "initial-route",
	}
	s := New(Config{
		Client:              &testClient{},
		InitialLink:         &testLink{},
		InitialCheckoutMeta: meta,
		Observers:           []Observer{obs},
	})
	s.Close(nil)

	require.Len(t, obs.metas, 1)
	assert.Equal(t, meta, obs.metas[0])
	assert.Equal(t, []string{"checkout", "release", "closed"}, obs.events)
}

type panicNotifyingObserver struct {
	NoopObserver
	panicInNotifier bool
	notifiedHook    string
	notifiedValue   any
	closed          int
}

func (o *panicNotifyingObserver) OnFrontendMessage(pgproto3.FrontendMessage) {
	panic("frontend boom")
}

func (o *panicNotifyingObserver) OnPanic(hook string, recovered any) {
	o.notifiedHook = hook
	o.notifiedValue = recovered
	if o.panicInNotifier {
		panic("notifier boom")
	}
}

func (o *panicNotifyingObserver) OnClosed() { o.closed++ }

func TestObserverPanicNotifierReceivesHookAndValue(t *testing.T) {
	for _, panicInNotifier := range []bool{false, true} {
		t.Run(map[bool]string{false: "normal", true: "notifier panics"}[panicInNotifier], func(t *testing.T) {
			obs := &panicNotifyingObserver{panicInNotifier: panicInNotifier}
			s := New(Config{
				Client:    &testClient{},
				Observers: []Observer{obs},
			})
			assert.NotPanics(t, func() {
				s.notifyFrontendMessage(&pgproto3.Sync{})
			})
			assert.Equal(t, "frontend_message", obs.notifiedHook)
			assert.Equal(t, "frontend boom", obs.notifiedValue)
			assert.Equal(t, 1, obs.closed)
		})
	}
}
