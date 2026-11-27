// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package link

import "sync"

// AbortHandle isolates cross-goroutine cancellation and force-close from the
// session's data-plane Link. Once MarkReleased runs, Cancel and ForceClose
// become no-ops so they can never touch a connection returned to the pool.
type AbortHandle struct {
	mu       sync.Mutex
	released bool
	target   abortTarget
}

// abortTarget runs Cancel and ForceClose while the AbortHandle's lock is held.
type abortTarget interface {
	cancelLocked() error
	forceCloseLocked() bool
}

// AbortHandleFor returns the handle that fences backendLink's Cancel and
// ForceClose against its release, or nil for a nil link. A *SessionLink uses
// its embedded handle. Any other Link, including a wrapper that embeds
// *SessionLink, gets a new handle that calls the wrapper's own methods.
func AbortHandleFor(backendLink Link) *AbortHandle {
	switch l := backendLink.(type) {
	case nil:
		return nil
	case *SessionLink:
		return &l.abort
	}
	adapter := &linkAbortTarget{link: backendLink}
	adapter.handle.target = adapter
	return &adapter.handle
}

// linkAbortTarget shares one allocation between a handle and the Link it fences.
type linkAbortTarget struct {
	handle AbortHandle
	link   Link
}

func (t *linkAbortTarget) cancelLocked() error    { return t.link.Cancel() }
func (t *linkAbortTarget) forceCloseLocked() bool { return t.link.ForceClose() }

// Cancel cancels an in-flight backend request. Returns nil if the handle is nil
// or already released.
func (h *AbortHandle) Cancel() error {
	if h == nil {
		return nil
	}
	return h.cancel(h.target)
}

func (h *AbortHandle) cancel(target abortTarget) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.released || target == nil {
		return nil
	}
	return target.cancelLocked()
}

// ForceClose force-closes the underlying backend connection to unblock a
// pending Receive. Reports whether it closed a live connection. Returns false
// if the handle is nil or already released.
func (h *AbortHandle) ForceClose() bool {
	if h == nil {
		return false
	}
	return h.forceClose(h.target)
}

func (h *AbortHandle) forceClose(target abortTarget) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.released || target == nil {
		return false
	}
	return target.forceCloseLocked()
}

// MarkReleased prevents future Cancel and ForceClose calls from acting on the
// underlying connection. Any in-flight Cancel or ForceClose finishes before
// MarkReleased returns. It is idempotent.
func (h *AbortHandle) MarkReleased() {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.released = true
	h.mu.Unlock()
}
