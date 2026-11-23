// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package session

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeProbe is a ClientLivenessProbe test double that reports disconnected
// once triggerDisconnect is called.
type fakeProbe struct{ disconnected atomic.Bool }

func (p *fakeProbe) IsDisconnected() bool { return p.disconnected.Load() }
func (p *fakeProbe) triggerDisconnect()   { p.disconnected.Store(true) }

// blockingProbe reports a disconnect once release is closed. entered is closed
// when the first poll calls it.
type blockingProbe struct {
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func newBlockingProbe() *blockingProbe {
	return &blockingProbe{entered: make(chan struct{}), release: make(chan struct{})}
}

func (p *blockingProbe) IsDisconnected() bool {
	p.once.Do(func() { close(p.entered) })
	<-p.release
	return true
}

// forEachDisconnectMonitor runs test once with each client-disconnect monitor:
// the goroutine for each checkout (the default) and the session timer.
func forEachDisconnectMonitor(t *testing.T, test func(t *testing.T, timer bool)) {
	t.Helper()
	for _, mode := range []struct {
		name  string
		timer bool
	}{{"goroutine", false}, {"timer", true}} {
		t.Run(mode.name, func(t *testing.T) { test(t, mode.timer) })
	}
}

// withDisconnectTimer sets Config.ClientDisconnectTimer.
func withDisconnectTimer(timer bool) func(*Config) {
	return func(cfg *Config) { cfg.ClientDisconnectTimer = timer }
}

func newMonitorTestSession(timer bool, probe ClientLivenessProbe) *Session {
	s := New(Config{
		Context:               context.Background(),
		User:                  "user",
		Database:              "database",
		PID:                   42,
		SecretKey:             []byte{1, 2, 3, 4},
		Service:               "service",
		Client:                &testClient{},
		ClientDisconnectTimer: timer,
	})
	s.livenessProbe = probe
	return s
}

// TestClientMonitorCancellationCause verifies that the monitor cancels the
// acquire context with ErrClientDisconnected as the cause, so consumers can
// classify client-driven acquire cancellations distinctly from other context
// causes (pool timeouts, deadlines, upstream cancellation).
func TestClientMonitorCancellationCause(t *testing.T) {
	forEachDisconnectMonitor(t, func(t *testing.T, timer bool) {
		probe := &fakeProbe{}
		s := newMonitorTestSession(timer, probe)

		monitor, acquireCtx := s.startClientMonitor()
		defer monitor.stop()

		probe.triggerDisconnect()

		assert.Eventually(t, func() bool { return acquireCtx.Err() != nil }, 2*time.Second, 10*time.Millisecond)
		assert.ErrorIs(t, context.Cause(acquireCtx), ErrClientDisconnected)
		assert.True(t, s.ClientDisconnected())
	})
}

// TestClientMonitorAcquireContextLifetime verifies the difference between the
// modes: the goroutine monitor ends its context with each checkout, and the
// timer monitor keeps one context for the session.
func TestClientMonitorAcquireContextLifetime(t *testing.T) {
	forEachDisconnectMonitor(t, func(t *testing.T, timer bool) {
		s := newMonitorTestSession(timer, &fakeProbe{})

		first, firstCtx := s.startClientMonitor()
		first.stop()
		second, secondCtx := s.startClientMonitor()
		second.stop()

		if timer {
			assert.True(t, firstCtx == secondCtx, "the timer monitor must reuse the session's context")
			assert.NoError(t, firstCtx.Err(), "stop must not cancel the session's context")
		} else {
			assert.False(t, firstCtx == secondCtx)
			assert.ErrorIs(t, firstCtx.Err(), context.Canceled)
		}
		assert.False(t, s.ClientDisconnected())
	})
}

// TestClientMonitorStopWaitsForRunningPoll verifies that in both modes stop
// waits for a poll that is running, so the monitor does nothing after stop
// returns.
func TestClientMonitorStopWaitsForRunningPoll(t *testing.T) {
	forEachDisconnectMonitor(t, func(t *testing.T, timer bool) {
		probe := newBlockingProbe()
		s := newMonitorTestSession(timer, probe)

		monitor, _ := s.startClientMonitor()
		select {
		case <-probe.entered:
		case <-time.After(2 * time.Second):
			t.Fatal("the monitor did not poll")
		}
		stopped := make(chan struct{})
		go func() {
			monitor.stop()
			close(stopped)
		}()
		select {
		case <-stopped:
			t.Fatal("stop returned while a poll was running")
		case <-time.After(clientMonitorInterval):
		}

		close(probe.release)
		select {
		case <-stopped:
		case <-time.After(2 * time.Second):
			t.Fatal("stop did not return after the poll ended")
		}
		assert.True(t, s.ClientDisconnected(), "the running poll must finish before stop returns")
	})
}

// TestTimerMonitorPollAfterCheckoutDoesNothing covers a timer that fires just
// after its checkout ended.
func TestTimerMonitorPollAfterCheckoutDoesNothing(t *testing.T) {
	probe := &fakeProbe{}
	s := newMonitorTestSession(true, probe)

	monitor, acquireCtx := s.startClientMonitor()
	monitor.stop()
	probe.triggerDisconnect()
	s.timerMonitor.poll()

	assert.NoError(t, acquireCtx.Err())
	assert.False(t, s.ClientDisconnected())
}

// TestTimerMonitorStopPreventsLaterAction verifies that a stopped monitor does
// not poll again.
func TestTimerMonitorStopPreventsLaterAction(t *testing.T) {
	probe := &fakeProbe{}
	s := newMonitorTestSession(true, probe)

	monitor, acquireCtx := s.startClientMonitor()
	monitor.stop()
	probe.triggerDisconnect()
	time.Sleep(3 * clientMonitorInterval)

	assert.NoError(t, acquireCtx.Err())
	assert.False(t, s.ClientDisconnected())
}

// TestTimerMonitorDetectsDisconnectDuringLongCheckout verifies that the timer
// re-arms itself while a checkout lasts longer than one poll interval.
func TestTimerMonitorDetectsDisconnectDuringLongCheckout(t *testing.T) {
	probe := &fakeProbe{}
	s := newMonitorTestSession(true, probe)

	monitor, acquireCtx := s.startClientMonitor()
	defer monitor.stop()
	// At least two polls find the client connected and re-arm the timer.
	time.Sleep(2*clientMonitorInterval + clientMonitorInterval/2)
	assert.NoError(t, acquireCtx.Err())

	probe.triggerDisconnect()

	assert.Eventually(t, func() bool { return acquireCtx.Err() != nil }, 2*time.Second, 10*time.Millisecond)
	assert.ErrorIs(t, context.Cause(acquireCtx), ErrClientDisconnected)
	assert.True(t, s.ClientDisconnected())
}

// TestTimerMonitorCloseReleasesContext verifies that close cancels the session's
// acquire context without a disconnect cause.
func TestTimerMonitorCloseReleasesContext(t *testing.T) {
	s := newMonitorTestSession(true, &fakeProbe{})

	monitor, acquireCtx := s.startClientMonitor()
	monitor.stop()
	s.timerMonitor.close()

	assert.ErrorIs(t, acquireCtx.Err(), context.Canceled)
	assert.NotErrorIs(t, context.Cause(acquireCtx), ErrClientDisconnected)
	assert.False(t, s.ClientDisconnected())
}

// TestSessionCloseReleasesTimerMonitor verifies that the end of a session closes
// its timer monitor.
func TestSessionCloseReleasesTimerMonitor(t *testing.T) {
	s, fakeClient, fakePG, closer := newRunningTestSession(t, withDisconnectTimer(true))
	s.SetLivenessProbe(&fakeLivenessProbe{})

	fakeClient.Send(&pgproto3.Query{String: "SELECT 1"})
	require.NoError(t, fakeClient.Flush())
	_, err := fakePG.Receive()
	require.NoError(t, err)
	fakePG.Send(&pgproto3.EmptyQueryResponse{})
	fakePG.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	require.NoError(t, fakePG.Flush())
	for range 2 {
		_, err = fakeClient.Receive()
		require.NoError(t, err)
	}

	closer() // returns after Run, so after close
	require.NotNil(t, s.timerMonitor)
	assert.ErrorIs(t, s.timerMonitor.ctx.Err(), context.Canceled)
}
