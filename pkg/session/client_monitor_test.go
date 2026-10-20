package session

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// fakeProbe is a ClientLivenessProbe test double that reports disconnected
// once triggerDisconnect is called.
type fakeProbe struct{ disconnected atomic.Bool }

func (p *fakeProbe) IsDisconnected() bool { return p.disconnected.Load() }
func (p *fakeProbe) triggerDisconnect()   { p.disconnected.Store(true) }

// TestClientMonitorCancellationCause verifies that the monitor cancels the
// acquire context with ErrClientDisconnected as the cause, so consumers can
// classify client-driven acquire cancellations distinctly from other context
// causes (pool timeouts, deadlines, upstream cancellation).
func TestClientMonitorCancellationCause(t *testing.T) {
	probe := &fakeProbe{}
	s := New(Config{
		Context:   context.Background(),
		User:      "user",
		Database:  "database",
		PID:       42,
		SecretKey: []byte{1, 2, 3, 4},
		Service:   "service",
		Client:    &testClient{},
	})

	s.livenessProbe = probe

	monitor, acquireCtx := s.startClientMonitor()
	defer monitor.stop()

	probe.triggerDisconnect()

	assert.Eventually(t, func() bool { return acquireCtx.Err() != nil }, 2*time.Second, 10*time.Millisecond)
	assert.ErrorIs(t, context.Cause(acquireCtx), ErrClientDisconnected)
}
