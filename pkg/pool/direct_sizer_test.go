// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package pool

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

type sizingTestInstance struct {
	stateTestInstance
	limit  int32
	resize func(int32) error
}

func (i *sizingTestInstance) ResizeMaxConns(limit int32) error {
	if i.resize != nil {
		if err := i.resize(limit); err != nil {
			return err
		}
	}
	i.limit = limit
	return nil
}

func TestDirectSizerAppliesAcceptedLimitBeforePublication(t *testing.T) {
	state := NewDirectState()
	sizer := NewDirectPoolSizer(state, 50)
	require.NoError(t, sizer.Resize(10))
	instance := &sizingTestInstance{limit: 50}
	require.NoError(t, sizer.Store("new", instance))
	stored, ok := state.Load("new")
	require.True(t, ok)
	assert.Same(t, instance, stored)
	assert.EqualValues(t, 10, instance.limit)
	assert.EqualValues(t, 10, sizer.Limit())
}

func TestDirectSizerSerializesPublicationAndResize(t *testing.T) {
	state := NewDirectState()
	sizer := NewDirectPoolSizer(state, 50)
	entered, stored, resized := make(chan error, 1), make(chan error, 1), make(chan error, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	await := func(result <-chan error) {
		t.Helper()
		select {
		case err := <-result:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("sizing operation did not complete")
		}
	}
	instance := &sizingTestInstance{resize: func(limit int32) error {
		// Verify both operations hold the same lock, without assuming that a
		// goroutine has reached Lock merely because it signaled it has started.
		if sizer.mu.TryLock() {
			sizer.mu.Unlock()
			err := errors.New("instance sizing must hold the coordinator lock")
			if limit == 50 {
				entered <- err
			}
			return err
		}
		if limit == 50 {
			entered <- nil
			<-release
		}
		return nil
	}}
	go func() { stored <- sizer.Store("new", instance) }()
	await(entered)
	_, visible := state.Load("new")
	assert.False(t, visible)
	go func() { resized <- sizer.Resize(10) }()
	unblock()
	await(stored)
	await(resized)
	assert.EqualValues(t, 10, instance.limit)
	assert.EqualValues(t, 10, sizer.Limit())
	// Check Resize's lock ownership with no other operation holding the mutex.
	require.NoError(t, sizer.Resize(15))
	assert.EqualValues(t, 15, instance.limit)
}

func TestDirectSizerDoesNotPublishFailedInstance(t *testing.T) {
	state := NewDirectState()
	sizer := NewDirectPoolSizer(state, 10)
	instance := &sizingTestInstance{resize: func(int32) error { return errors.New("failed") }}
	require.Error(t, sizer.Store("new", instance))
	_, ok := state.Load("new")
	assert.False(t, ok)
	assert.EqualValues(t, 10, sizer.Limit())
}
