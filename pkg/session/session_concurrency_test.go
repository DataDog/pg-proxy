// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package session

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/DataDog/pg-proxy/pkg/link"
	"github.com/DataDog/pg-proxy/pkg/query"
)

// Run these with -race: each pairs a call from another goroutine with the
// session-goroutine code that touches the same fields.

const concurrencyIterations = 1000

func newConcurrencyTestSession(hooks Hooks) *Session {
	return New(Config{
		Context: context.Background(),
		User:    "user", Database: "database", Service: "service",
		PID:     42,
		Client:  &testClient{},
		Backend: &namedBackend{name: "initial"},
		Hooks:   hooks,
	})
}

// runConcurrently runs session and host on separate goroutines with no
// happens-before edge between them.
func runConcurrently(session, host func()) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range concurrencyIterations {
			session()
		}
	}()
	go func() {
		defer wg.Done()
		for range concurrencyIterations {
			host()
		}
	}()
	wg.Wait()
}

// The warden calls RemapBackend and RemapReadStates while the session goroutine
// applies pending remaps at a message boundary. The hooks read the session's
// current backend and read states on the caller's goroutine.
func TestRemapConcurrentWithMessageBoundary(t *testing.T) {
	next := &namedBackend{name: "next"}
	reads := []ReadStateConfig{{Backend: &namedBackend{name: "read"}, Weight: 1}}
	for name, test := range map[string]struct {
		hooks Hooks
		remap func(*Session)
	}{
		"backend":          {remap: func(s *Session) { s.RemapBackend(next) }},
		"backend hook":     {hooks: Hooks{BackendChanged: func(Backend, Backend) {}}, remap: func(s *Session) { s.RemapBackend(next) }},
		"read states hook": {hooks: Hooks{ReadStatesChanged: func(_, _ []ReadStateConfig) {}}, remap: func(s *Session) { s.RemapReadStates(reads) }},
	} {
		t.Run(name, func(t *testing.T) {
			s := newConcurrencyTestSession(test.hooks)
			runConcurrently(func() { s.handleStateRemapping() }, func() { test.remap(s) })
		})
	}
}

// Hooks run while the session holds remapMu or linkMu and may read the session
// back through its getters.
func TestHooksCanCallGetters(t *testing.T) {
	var s *Session
	s = newConcurrencyTestSession(Hooks{
		BackendChanged:    func(Backend, Backend) { s.Backend(); s.PendingBackend() },
		ReadStatesChanged: func(_, _ []ReadStateConfig) { s.Backend(); s.PendingBackend() },
		LinkChanged:       func(link.Link) { s.CurrentLink() },
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.RemapBackend(&namedBackend{name: "next"})
		s.RemapReadStates(nil)
		s.handleStateRemapping()
		s.linkMu.Lock()
		s.setLink(&testLink{})
		s.linkMu.Unlock()
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a hook deadlocked calling a session getter")
	}
}

// The listener calls Cancel while the session goroutine checks links out and in.
func TestCancelConcurrentWithCheckout(t *testing.T) {
	s := newConcurrencyTestSession(Hooks{})
	backendLink := &testLink{}
	runConcurrently(
		func() {
			s.linkMu.Lock()
			s.setLink(backendLink)
			s.linkMu.Unlock()
			s.linkMu.Lock()
			s.setLink(nil)
			s.linkMu.Unlock()
		},
		func() { _ = s.Cancel() },
	)
}

// The client monitor must not force-close a link that flushPostgres released.
// SessionLink tolerates it, but the Link interface does not require that.
func TestClientMonitorDoesNotForceCloseLinkReleasedByFlush(t *testing.T) {
	releaseStarted := make(chan struct{})
	finishRelease := make(chan struct{})
	expectation := query.SyncReply
	backendLink := &testLink{
		nextExpectation:        func() query.ReplyExpectation { return expectation },
		resolveNextExpectation: func() { expectation = query.NoReply },
		receive: func() (pgproto3.BackendMessage, error) {
			return &pgproto3.ReadyForQuery{TxStatus: 'I'}, nil
		},
		release: func() error {
			close(releaseStarted)
			<-finishRelease
			return nil
		},
	}
	probe := &fakeProbe{}
	s := New(Config{
		Context: context.Background(),
		Client:  &testClient{}, ClientConn: &writeRecordingConn{},
		Backend: &namedBackend{name: "initial"}, InitialLink: backendLink,
		PostgresMsgBufferSize: 8192,
	})
	s.livenessProbe = probe

	monitor, _ := s.startClientMonitor()
	go func() {
		<-releaseStarted
		probe.triggerDisconnect()
		// Give the monitor time to observe the disconnect while Release runs.
		time.Sleep(250 * time.Millisecond)
		close(finishRelease)
	}()

	_, err := s.flushPostgres()
	require.NoError(t, err)
	require.Nil(t, s.CurrentLink())
	monitor.stop()
	assert.Zero(t, backendLink.calls.forceClose, "client monitor force-closed a released link")
}
