// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package session

import (
	"context"
	"sync"
	"testing"

	"github.com/DataDog/pg-proxy/pkg/link"
)

// Run these with -race: each pairs a call from another goroutine with the
// session-goroutine code that touches the same fields.

const concurrencyIterations = 1000

func newConcurrencyTestSession() *Session {
	return newPortableSession(context.Background(), &testClient{}, nil, &namedBackend{name: "initial"}, nil)
}

// checkOut and checkIn set and clear the session's link, as a checkout does.
func checkOut(s *Session, backendLink link.Link) {
	s.setLink(backendLink)
}

func checkIn(s *Session) {
	s.setLink(nil)
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

// The listener calls Cancel while the session goroutine checks links out and in.
func TestCancelConcurrentWithCheckout(t *testing.T) {
	s := newConcurrencyTestSession()
	backendLink := &testLink{}
	runConcurrently(
		func() { checkOut(s, backendLink); checkIn(s) },
		func() { _ = s.Cancel() },
	)
}
