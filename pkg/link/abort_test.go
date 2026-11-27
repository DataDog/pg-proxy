// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package link

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAbortHandleNilIsNoOp(t *testing.T) {
	var h *AbortHandle
	assert.NoError(t, h.Cancel())
	assert.False(t, h.ForceClose())
	assert.NotPanics(t, h.MarkReleased)
}

// abortStubLink is a Link other than *SessionLink whose Cancel and ForceClose
// run the supplied callbacks.
type abortStubLink struct {
	Link
	cancel     func() error
	forceClose func() bool
}

func (l *abortStubLink) Cancel() error    { return l.cancel() }
func (l *abortStubLink) ForceClose() bool { return l.forceClose() }

func TestAbortHandleForSessionLinkUsesEmbeddedHandle(t *testing.T) {
	assert.Nil(t, AbortHandleFor(nil))
	sl := NewFrontendLink(nil)
	assert.Same(t, &sl.abort, AbortHandleFor(sl))
}

func TestAbortHandleStopsActionsAfterMarkReleased(t *testing.T) {
	var cancels, closes atomic.Int32
	h := AbortHandleFor(&abortStubLink{
		cancel:     func() error { cancels.Add(1); return nil },
		forceClose: func() bool { closes.Add(1); return true },
	})

	assert.NoError(t, h.Cancel())
	assert.True(t, h.ForceClose())
	assert.Equal(t, int32(1), cancels.Load())
	assert.Equal(t, int32(1), closes.Load())

	h.MarkReleased()
	assert.NoError(t, h.Cancel())
	assert.False(t, h.ForceClose())
	assert.Equal(t, int32(1), cancels.Load())
	assert.Equal(t, int32(1), closes.Load())
}

// TestAbortHandleConcurrentCancelForceCloseRelease hammers Cancel, ForceClose,
// and MarkReleased concurrently under -race and asserts neither callback ever
// runs after MarkReleased returns.
func TestAbortHandleConcurrentCancelForceCloseRelease(t *testing.T) {
	for range 200 {
		var released atomic.Bool
		var postReleaseCalls atomic.Int32

		h := AbortHandleFor(&abortStubLink{
			cancel: func() error {
				if released.Load() {
					postReleaseCalls.Add(1)
				}
				return nil
			},
			forceClose: func() bool {
				if released.Load() {
					postReleaseCalls.Add(1)
				}
				return true
			},
		})

		var wg sync.WaitGroup
		start := make(chan struct{})
		for range 4 {
			wg.Add(2)
			go func() {
				defer wg.Done()
				<-start
				_ = h.Cancel()
			}()
			go func() {
				defer wg.Done()
				<-start
				_ = h.ForceClose()
			}()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			h.MarkReleased()
			released.Store(true)
		}()
		close(start)
		wg.Wait()

		assert.Zero(t, postReleaseCalls.Load(), "Cancel or ForceClose ran after MarkReleased returned")
	}
}
