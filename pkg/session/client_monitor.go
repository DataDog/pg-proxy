// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package session

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	pgproxy "github.com/DataDog/pg-proxy"
)

// ClientLivenessProbe abstracts client disconnection detection. The real
// implementation polls the TCP socket; tests can inject a fake.
type ClientLivenessProbe interface {
	IsDisconnected() bool
}

// pollRDHUP is the Linux-specific POLLRDHUP event flag (peer closed writing
// half of the connection). On non-Linux platforms poll() silently ignores
// unknown event bits, so the probe degrades to only detecting POLLERR/POLLHUP.
// This is acceptable because pg-proxy only runs on Linux in production.
const pollRDHUP = 0x2000

// tcpLivenessProbe checks a TCP socket for disconnection using poll(2) with
// POLLRDHUP on the raw file descriptor.
type tcpLivenessProbe struct {
	rawConn syscall.RawConn
}

// IsDisconnected performs a non-blocking poll on the socket to detect whether
// the peer has closed the connection. Unlike MSG_PEEK-based approaches, POLLRDHUP
// detects FIN at the TCP layer regardless of buffered data in the receive queue,
// so pipelined client messages do not mask a disconnection.
func (p *tcpLivenessProbe) IsDisconnected() bool {
	var disconnected bool
	err := p.rawConn.Control(func(fd uintptr) {
		fds := []unix.PollFd{{
			Fd:     int32(fd),
			Events: pollRDHUP,
		}}
		n, err := unix.Poll(fds, 0) // timeout=0: non-blocking
		if err != nil {
			if errors.Is(err, syscall.EINTR) {
				// Signal interrupted the call, not a disconnect.
				return
			}
			pgproxy.Logger().Warn("unexpected poll error on client socket", "fd", fd, "err", err)
			disconnected = true
			return
		}
		if n > 0 && fds[0].Revents&(pollRDHUP|unix.POLLERR|unix.POLLHUP) != 0 {
			disconnected = true
		}
	})
	if err != nil {
		pgproxy.Logger().Warn("client liveness raw connection control failed", "err", err)
		disconnected = true
	}
	return disconnected
}

// newTCPLivenessProbe creates a probe for the given connection, unwrapping
// TLS if necessary. Returns nil if the connection is not TCP.
//
// For TLS connections, the probe operates on the raw TCP socket below the TLS
// layer. POLLRDHUP detects FIN/RST at the TCP level regardless of buffered
// ciphertext, so TLS close_notify-only disconnects (without TCP shutdown) are
// not detected. In practice this is fine: PostgreSQL clients close the socket
// directly.
func NewTCPLivenessProbe(conn net.Conn) ClientLivenessProbe {
	// Unwrap TLS to poll the underlying TCP socket.
	if tlsConn, ok := conn.(*tls.Conn); ok {
		conn = tlsConn.NetConn()
	}

	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return nil
	}

	rawConn, err := tcpConn.SyscallConn()
	if err != nil {
		pgproxy.Logger().Warn("failed to get syscall connection for client monitor", "err", err)
		return nil
	}

	return &tcpLivenessProbe{rawConn: rawConn}
}

// ErrClientDisconnected is the cancellation cause the client monitor attaches to
// the acquire context when it detects a client disconnect. Consumers that
// classify acquire failures (e.g. pg-proxy's pool-saturation signal) use it to
// distinguish client-driven cancellations from backend-driven ones.
var ErrClientDisconnected = errors.New("client disconnected")

// clientMonitorInterval is how often a monitor polls the client socket while a
// checkout is in flight.
const clientMonitorInterval = 100 * time.Millisecond

// checkoutMonitor watches the client for disconnection during one checkout.
type checkoutMonitor interface {
	// stop ends the watch for the current checkout.
	stop()
}

// startClientMonitor begins polling the client connection for disconnection.
// It returns a checkoutMonitor handle and a context.Context that is cancelled
// when the client disconnects (useful for cancelling AcquireConnection).
//
// When disconnection is detected, the monitor:
//  1. Sets s.clientDisconnected to true
//  2. Cancels the returned context (aborts pending AcquireConnection)
//  3. Calls s.link.ForceClose() if a link exists (unblocks pending Receive)
//
// Config.ClientDisconnectTimer selects the implementation: a goroutine for each
// checkout (clientMonitor) or one timer for the session (timerMonitor).
func (s *Session) startClientMonitor() (checkoutMonitor, context.Context) {
	if s.clientDisconnectTimer {
		return s.startTimerMonitor()
	}
	return s.startGoroutineMonitor()
}

// handleClientDisconnect runs the disconnect actions of both monitors.
func (s *Session) handleClientDisconnect(cancel context.CancelCauseFunc) {
	pgproxy.Logger().Info("client disconnection detected", "session_id", s.pid)
	_ = pgproxy.Metrics().Count("pg_proxy.sessions.client_disconnected", 1, s.obsVals.metricTags, 1)

	s.clientDisconnected.Store(true)
	cancel(ErrClientDisconnected) // abort a pending AcquireConnection with an identifiable cause

	s.linkMu.Lock()
	link := s.link
	s.linkMu.Unlock()
	if link != nil {
		s.forceCloseBackendOnClientDisconnect(link)
	}
}

// clientMonitor polls the client TCP connection to detect disconnection
// without consuming any data from the socket buffer. It starts a goroutine and
// a ticker for each checkout.
type clientMonitor struct {
	cancel context.CancelCauseFunc
	done   chan struct{}
}

func (s *Session) startGoroutineMonitor() (*clientMonitor, context.Context) {
	// Use Background rather than s.ctx: when connection draining is disabled,
	// Run() continues serving traffic after s.ctx is canceled. Deriving from
	// s.ctx would make acquireCtx immediately canceled, failing every link
	// acquisition. The monitor is cleaned up by defer monitor.stop().
	ctx, cancel := context.WithCancelCause(context.Background())
	done := make(chan struct{})

	probe := s.livenessProbe
	if probe == nil {
		close(done)
		return &clientMonitor{cancel: cancel, done: done}, ctx
	}

	go func() {
		defer close(done)
		ticker := time.NewTicker(clientMonitorInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if probe.IsDisconnected() {
					s.handleClientDisconnect(cancel)
					return
				}
			}
		}
	}()

	return &clientMonitor{cancel: cancel, done: done}, ctx
}

// stop cancels the monitor goroutine and waits for it to exit.
func (m *clientMonitor) stop() {
	m.cancel(nil)
	<-m.done
}

// timerMonitor is the client monitor that Config.ClientDisconnectTimer selects.
// A session creates one and arms it for each checkout. Arming resets a timer
// rather than starting a goroutine: most checkouts finish well inside
// clientMonitorInterval, so they cost one timer reset and one stop. A timer that
// fires runs one poll on its own goroutine and re-arms itself while the checkout
// is still in flight.
type timerMonitor struct {
	s     *Session
	probe ClientLivenessProbe
	// ctx is the session's acquire context. A detected disconnect cancels it
	// with ErrClientDisconnected as the cause, and close cancels it when the
	// session ends, so one context serves every checkout of the session.
	ctx    context.Context
	cancel context.CancelCauseFunc

	// mu serializes poll with startTimerMonitor, stop, and close. Because stop
	// waits for a poll that is running, the monitor does nothing after stop
	// returns, as with clientMonitor. Without that wait, a poll could probe the
	// socket after the listener closed it and report a disconnect that did not
	// occur.
	//
	// poll releases mu before it handles a disconnect, because the handling
	// takes s.linkMu and calls code outside the monitor, such as metrics and a
	// link's ForceClose. A ForceClose that wrongly closes the session then
	// behaves as in goroutine mode rather than deadlocking. stop waits for that
	// handling through m.handling, so do not call stop while holding s.linkMu.
	mu    sync.Mutex
	timer *time.Timer
	// active is true while a checkout is in flight. A callback that started
	// for an earlier checkout and gets mu during a later one probes that
	// checkout early, which is harmless.
	active bool
	// disconnected stops further polls once a disconnect has been handled.
	disconnected bool
	// handling counts the disconnect that poll handles after it releases mu.
	handling sync.WaitGroup
}

func (s *Session) startTimerMonitor() (*timerMonitor, context.Context) {
	m := s.timerMonitor
	// The probe is not compared with m.probe: an interface comparison panics
	// when the probe's dynamic type is not comparable.
	if m == nil || s.livenessProbeChanged {
		if m != nil {
			m.close()
		}
		s.livenessProbeChanged = false
		// Use Background rather than s.ctx, for the reason given in
		// startGoroutineMonitor.
		ctx, cancel := context.WithCancelCause(context.Background())
		m = &timerMonitor{s: s, probe: s.livenessProbe, ctx: ctx, cancel: cancel}
		s.timerMonitor = m
	}
	if m.probe == nil {
		return m, m.ctx
	}
	m.mu.Lock()
	m.armLocked()
	m.mu.Unlock()
	return m, m.ctx
}

// armLocked starts polling for a checkout. m.mu must be held.
func (m *timerMonitor) armLocked() {
	m.active = true
	if m.timer == nil {
		m.timer = time.AfterFunc(clientMonitorInterval, m.poll)
	} else {
		m.timer.Reset(clientMonitorInterval)
	}
}

// poll runs on the timer's goroutine.
func (m *timerMonitor) poll() {
	m.mu.Lock()
	found := m.checkLocked()
	if found {
		// Add under mu, so a stop that follows this poll waits for the handling.
		m.handling.Add(1)
	}
	m.mu.Unlock()
	if found {
		m.s.handleClientDisconnect(m.cancel)
		m.handling.Done()
	}
}

// checkLocked probes the client during a checkout and re-arms the timer while
// the client is connected. It reports whether it found a disconnect that poll
// must handle. m.mu must be held.
func (m *timerMonitor) checkLocked() bool {
	if !m.active || m.disconnected {
		return false
	}
	if !m.probe.IsDisconnected() {
		m.timer.Reset(clientMonitorInterval)
		return false
	}
	m.disconnected = true
	return true
}

// stop disarms polling when the checkout ends. It waits for a poll that is
// running, and for the disconnect handling that poll may have started.
func (m *timerMonitor) stop() {
	m.disarm()
	m.handling.Wait()
}

// disarm stops polling for the current checkout. It waits for a poll that holds
// m.mu, but not for disconnect handling.
func (m *timerMonitor) disarm() {
	m.mu.Lock()
	m.stopLocked()
	m.mu.Unlock()
}

// stopLocked disarms polling. m.mu must be held.
func (m *timerMonitor) stopLocked() {
	if m.timer != nil {
		// A callback that already started waits for m.mu and then sees that
		// no checkout is active, or probes the next checkout.
		m.timer.Stop()
	}
	m.active = false
}

// close stops the monitor for good and releases its context. Unlike stop, it
// does not wait for disconnect handling: a ForceClose that wrongly closes the
// session calls close from that handling, on the timer's goroutine. The
// deferred stop at the end of the checkout still waits for it.
func (m *timerMonitor) close() {
	m.disarm()
	m.cancel(nil)
}
