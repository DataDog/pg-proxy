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
			if err == syscall.EINTR {
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

// clientMonitor polls the client TCP connection to detect disconnection
// without consuming any data from the socket buffer.
type clientMonitor struct {
	cancel context.CancelCauseFunc
	done   chan struct{}
}

// startClientMonitor begins polling the client connection for disconnection.
// It returns a clientMonitor handle and a context.Context that is cancelled
// when the client disconnects (useful for cancelling AcquireConnection).
//
// When disconnection is detected, the monitor:
//  1. Sets s.clientDisconnected to true
//  2. Cancels the returned context (aborts pending AcquireConnection)
//  3. Calls s.link.ForceClose() if a link exists (unblocks pending Receive)
func (s *Session) startClientMonitor() (*clientMonitor, context.Context) {
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
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if probe.IsDisconnected() {
					pgproxy.Logger().Info("client disconnection detected", "session_id", s.pid)
					_ = pgproxy.Metrics().Count("dd.orgstore.pg_proxy.sessions.client_disconnected", 1, s.obsVals.metricTags, 1)

					s.clientDisconnected.Store(true)
					cancel(ErrClientDisconnected) // abort a pending AcquireConnection with an identifiable cause

					s.linkMu.Lock()
					link := s.link
					s.linkMu.Unlock()
					if link != nil {
						s.forceCloseBackendOnClientDisconnect(link)
					}
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
