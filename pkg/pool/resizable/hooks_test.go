// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package resizable

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHooksReserveBeforeConnectionConstruction(t *testing.T) {
	gate, err := newResizableGate(1)
	require.NoError(t, err)
	config, err := pgxpool.ParseConfig("postgres://localhost/test")
	require.NoError(t, err)
	originalTracer := &queryOnlyTracer{}
	config.ConnConfig.Tracer = originalTracer
	installHooks(config, gate)

	connConfig := config.ConnConfig.Copy()
	require.NoError(t, config.BeforeConnect(context.Background(), connConfig))
	assert.EqualValues(t, 1, gate.getTotalOpenConns())
	tracer, ok := connConfig.Tracer.(*connectionTracer)
	require.True(t, ok)
	assert.Same(t, originalTracer, tracer.query)

	tracer.TraceConnectEnd(context.Background(), pgx.TraceConnectEndData{Err: errors.New("failed")})
	assert.Eventually(t, func() bool { return gate.getTotalOpenConns() == 0 }, time.Second, time.Millisecond)
}

func TestHooksPreserveSizingConfiguration(t *testing.T) {
	gate, err := newResizableGate(10)
	require.NoError(t, err)
	config, err := pgxpool.ParseConfig("postgres://localhost/test")
	require.NoError(t, err)
	config.MaxConns = 10
	config.MinConns = 3
	config.MinIdleConns = 2

	installHooks(config, gate)

	assert.EqualValues(t, 10, config.MaxConns)
	assert.EqualValues(t, 3, config.MinConns)
	assert.EqualValues(t, 2, config.MinIdleConns)
}

func TestHooksRejectConstructionAtLimit(t *testing.T) {
	gate, err := newResizableGate(1)
	require.NoError(t, err)
	require.True(t, gate.tryReserve())
	config, err := pgxpool.ParseConfig("postgres://localhost/test")
	require.NoError(t, err)
	beforeConnectCalled := false
	config.BeforeConnect = func(context.Context, *pgx.ConnConfig) error {
		beforeConnectCalled = true
		return nil
	}
	installHooks(config, gate)

	err = config.BeforeConnect(context.Background(), config.ConnConfig.Copy())
	require.ErrorIs(t, err, errVirtualConnectionLimit)
	assert.EqualError(t, err, "virtual connection limit reached")
	assert.False(t, beforeConnectCalled)
	assert.EqualValues(t, 1, gate.getTotalOpenConns())
}

func TestHooksReleaseReservationWhenExistingBeforeConnectFails(t *testing.T) {
	gate, err := newResizableGate(1)
	require.NoError(t, err)
	config, err := pgxpool.ParseConfig("postgres://localhost/test")
	require.NoError(t, err)
	expected := errors.New("existing hook failed")
	config.BeforeConnect = func(context.Context, *pgx.ConnConfig) error { return expected }
	installHooks(config, gate)

	err = config.BeforeConnect(context.Background(), config.ConnConfig.Copy())
	require.ErrorIs(t, err, expected)
	assert.EqualValues(t, 0, gate.getTotalOpenConns())
}

func TestHooksNotifyOnlyWhenConnectionReturnsIdle(t *testing.T) {
	gate, err := newResizableGate(1)
	require.NoError(t, err)
	config, err := pgxpool.ParseConfig("postgres://localhost/test")
	require.NoError(t, err)
	config.AfterRelease = func(*pgx.Conn) bool { return false }
	installHooks(config, gate)

	signal := gate.getAvailabilitySignal()
	assert.False(t, config.AfterRelease(nil))
	assert.False(t, channelClosed(signal))

	config, err = pgxpool.ParseConfig("postgres://localhost/test")
	require.NoError(t, err)
	installHooks(config, gate)
	signal = gate.getAvailabilitySignal()
	assert.True(t, config.AfterRelease(nil))
	assert.True(t, channelClosed(signal))
}

func TestRecordConnClosedAfterCleanupRetainsCountUntilCleanupCompletes(t *testing.T) {
	gate, err := newResizableGate(1)
	require.NoError(t, err)
	require.True(t, gate.tryReserve())
	cleanupDone := make(chan struct{})

	recordConnClosedAfterCleanup(gate, cleanupDone, false)
	assert.Never(t, func() bool { return gate.getTotalOpenConns() == 0 }, 10*time.Millisecond, time.Millisecond)
	close(cleanupDone)
	assert.Eventually(t, func() bool { return gate.getTotalOpenConns() == 0 }, time.Second, time.Millisecond)
}

func TestRecordConnClosedAfterCleanupCompletesRetirement(t *testing.T) {
	gate, err := newResizableGate(1)
	require.NoError(t, err)
	require.True(t, gate.tryReserve())
	gate.trackClosingConnection()
	cleanupDone := make(chan struct{})

	recordConnClosedAfterCleanup(gate, cleanupDone, true)
	assert.EqualValues(t, 1, gate.getTotalOpenConns())
	assert.EqualValues(t, 1, gate.getNumConnsRetiring())
	close(cleanupDone)
	assert.Eventually(t, func() bool {
		return gate.getTotalOpenConns() == 0 && gate.getNumConnsRetiring() == 0
	}, time.Second, time.Millisecond)
}

// Pending pgx closures must count toward a downsize before cleanup finishes.
func TestPendingClosuresPreventExcessRetirement(t *testing.T) {
	for _, resizeFirst := range []bool{false, true} {
		name := "close_before_resize"
		if resizeFirst {
			name = "resize_before_close"
		}
		t.Run(name, func(t *testing.T) {
			gate, err := newResizableGate(5)
			require.NoError(t, err)
			for i := 0; i < 5; i++ {
				require.True(t, gate.tryReserve())
			}
			config, err := pgxpool.ParseConfig("postgres://localhost/test")
			require.NoError(t, err)
			installHooks(config, gate)

			if resizeFirst {
				require.NoError(t, gate.resize(3))
			}
			cleanupDone := make(chan struct{})
			defer func() {
				if !channelClosed(cleanupDone) {
					close(cleanupDone)
				}
			}()
			recordConnClosedAfterCleanup(gate, cleanupDone, false)
			recordConnClosedAfterCleanup(gate, cleanupDone, false)
			if !resizeFirst {
				require.NoError(t, gate.resize(3))
			}

			assert.EqualValues(t, 5, gate.getTotalOpenConns())
			assert.EqualValues(t, 2, gate.getNumConnsRetiring())
			assert.False(t, gate.tryReserve(), "cleanup must still occupy physical capacity")
			assert.False(t, gate.needsRetirement(), "reconciliation must exclude pending closures")
			for i := 0; i < 3; i++ {
				assert.True(t, config.AfterRelease(nil), "keep the remaining healthy connections")
			}

			close(cleanupDone)
			assert.Eventually(t, func() bool {
				return gate.getTotalOpenConns() == 3 && gate.getNumConnsRetiring() == 0
			}, time.Second, time.Millisecond)
			assert.False(t, gate.needsRetirement())
		})
	}
}
