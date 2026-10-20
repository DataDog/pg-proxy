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

func TestReleaseAfterCleanupRetainsSlotUntilCleanupCompletes(t *testing.T) {
	gate, err := newResizableGate(1)
	require.NoError(t, err)
	require.True(t, gate.tryReserve())
	cleanupDone := make(chan struct{})

	releaseAfterCleanup(gate, cleanupDone)
	assert.Never(t, func() bool { return gate.getTotalOpenConns() == 0 }, 10*time.Millisecond, time.Millisecond)
	close(cleanupDone)
	assert.Eventually(t, func() bool { return gate.getTotalOpenConns() == 0 }, time.Second, time.Millisecond)
}
