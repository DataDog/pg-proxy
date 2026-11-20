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

	"github.com/stretchr/testify/assert"

	"github.com/DataDog/pg-proxy/pkg/link"
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

// Hooks may read the session back through its getters while another goroutine
// queues remaps: all of them on the session goroutine, and Backend and
// PendingBackend in the pending reports on the Remap* caller's goroutine.
func TestHooksCanCallGetters(t *testing.T) {
	var s *Session
	readBack := func(onSessionGoroutine bool) {
		if onSessionGoroutine {
			s.CurrentLink()
		}
		s.Backend()
		s.PendingBackend()
	}
	s = newConcurrencyTestSession(Hooks{
		BackendChanged:    func(_, pending Backend) { readBack(pending == nil) },
		ReadStatesChanged: func(_, pending []ReadStateConfig) { readBack(pending == nil) },
		LinkChanged:       func(link.Link) { readBack(true) },
	})
	next := &namedBackend{name: "next"}
	reads := []ReadStateConfig{{Backend: next, Weight: 1}}
	backendLink := &testLink{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		runConcurrently(
			func() {
				s.handleStateRemapping()
				s.linkMu.Lock()
				s.setLink(backendLink)
				s.linkMu.Unlock()
				s.linkMu.Lock()
				s.setLink(nil)
				s.linkMu.Unlock()
			},
			func() { s.RemapBackend(next); s.RemapReadStates(reads) },
		)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("a hook deadlocked calling a session getter")
	}
}

// A hook that tracks the current and pending backend from BackendChanged reports
// must stay correct while remaps race with the session applying them.
func TestRemapReportsArriveInOrder(t *testing.T) {
	var current, pending Backend
	var outOfOrder int
	s := newConcurrencyTestSession(Hooks{
		BackendChanged: func(reportedCurrent, reportedPending Backend) {
			if reportedPending == nil {
				// Applied: the session switched to the last pending backend.
				if reportedCurrent != pending {
					outOfOrder++
				}
				current, pending = reportedCurrent, nil
				return
			}
			if reportedCurrent != current {
				outOfOrder++
			}
			pending = reportedPending
		},
	})
	current = s.Backend()
	backends := []Backend{&namedBackend{name: "a"}, &namedBackend{name: "b"}}
	remaps := 0
	runConcurrently(
		func() { s.handleStateRemapping() },
		func() { s.RemapBackend(backends[remaps%2]); remaps++ },
	)
	assert.Zero(t, outOfOrder, "remap reports arrived out of order")
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
