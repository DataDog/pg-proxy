// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package resizable

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResizableGateRejectsInvalidLimits(t *testing.T) {
	_, err := newResizableGate(0)
	require.EqualError(t, err, "connection limit must be positive: 0")

	gate, err := newResizableGate(1)
	require.NoError(t, err)
	require.EqualError(t, gate.resize(-1), "connection limit must be positive: -1")
	assert.EqualValues(t, 1, gate.getLimit())
}

func TestResizableGateDoesNotExceedLimitConcurrently(t *testing.T) {
	const (
		limit      = int32(5)
		contenders = 100
	)
	gate, err := newResizableGate(limit)
	require.NoError(t, err)

	start := make(chan struct{})
	results := make(chan bool, contenders)
	var wg sync.WaitGroup
	for range contenders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results <- gate.tryReserve()
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	var reserved int32
	for ok := range results {
		if ok {
			reserved++
		}
	}
	assert.Equal(t, limit, reserved)
	assert.Equal(t, limit, gate.getTotalOpenConns())
}

func TestResizableGateConnCloseWakesWaiterAtAvailableCapacity(t *testing.T) {
	gate, err := newResizableGate(1)
	require.NoError(t, err)
	require.True(t, gate.tryReserve())
	signal := gate.getAvailabilitySignal()

	gate.connClosed()
	require.NoError(t, gate.wait(context.Background(), signal))
	assert.EqualValues(t, 0, gate.getTotalOpenConns())
	assert.True(t, gate.tryReserve())
}

func TestResizableGateResizeUpWakesWaiter(t *testing.T) {
	gate, err := newResizableGate(1)
	require.NoError(t, err)
	require.True(t, gate.tryReserve())
	signal := gate.getAvailabilitySignal()

	require.NoError(t, gate.resize(2))
	require.NoError(t, gate.wait(context.Background(), signal))
	assert.True(t, gate.tryReserve())
	assert.EqualValues(t, 2, gate.getTotalOpenConns())
}

func TestResizableGateResizeDownWaitsForEnoughConnectionsToClose(t *testing.T) {
	gate, err := newResizableGate(3)
	require.NoError(t, err)
	for range 3 {
		require.True(t, gate.tryReserve())
	}
	signal := gate.getAvailabilitySignal()
	require.NoError(t, gate.resize(1))

	gate.connClosed()
	gate.connClosed()
	assert.False(t, channelClosed(signal), "capacity is still at the smaller limit")
	assert.False(t, gate.tryReserve())

	gate.connClosed()
	assert.True(t, channelClosed(signal), "capacity is now below the smaller limit")
	assert.True(t, gate.tryReserve())
}

func TestResizableGateRetiresOnlyConcurrentExcess(t *testing.T) {
	gate, err := newResizableGate(5)
	require.NoError(t, err)
	for range 5 {
		require.True(t, gate.tryReserve())
	}
	require.NoError(t, gate.resize(2))

	const contenders = 100
	start := make(chan struct{})
	results := make(chan bool, contenders)
	var wg sync.WaitGroup
	for range contenders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results <- gate.tryRetireExcessConnection()
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	var retiring int32
	for selected := range results {
		if selected {
			retiring++
		}
	}
	assert.EqualValues(t, 3, retiring)
	assert.EqualValues(t, 3, gate.getNumConnsRetiring())

	for range retiring {
		gate.retiringConnClosed()
	}
	assert.EqualValues(t, 2, gate.getTotalOpenConns())
	assert.EqualValues(t, 0, gate.getNumConnsRetiring())
}

func TestResizableGateIdleNotificationWakesWaiterWithoutFreeingSlot(t *testing.T) {
	gate, err := newResizableGate(1)
	require.NoError(t, err)
	require.True(t, gate.tryReserve())
	signal := gate.getAvailabilitySignal()

	gate.notifyIdleConnAvailability()
	require.NoError(t, gate.wait(context.Background(), signal))
	assert.EqualValues(t, 1, gate.getTotalOpenConns())
	assert.False(t, gate.tryReserve())
}

func TestResizableGateWaitHonorsContext(t *testing.T) {
	gate, err := newResizableGate(1)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = gate.wait(ctx, gate.getAvailabilitySignal())
	require.ErrorIs(t, err, context.Canceled)
}

func TestResizableGateCapturedSignalPreventsLostWake(t *testing.T) {
	gate, err := newResizableGate(1)
	require.NoError(t, err)
	signal := gate.getAvailabilitySignal()

	gate.notifyIdleConnAvailability()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, gate.wait(ctx, signal))
}

func channelClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}
