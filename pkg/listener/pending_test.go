package listener

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/DataDog/pg-proxy/pkg/protocol"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

func TestPendingConnectionsLimit(t *testing.T) {
	for _, phase := range []string{"startup", "TLS", "password", "timeout", "load shedding"} {
		t.Run(phase, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			base, admission, shedder, err := newTestListener(ctx)
			require.NoError(t, err)
			require.NoError(t, base.Close())
			cfg := base.cfg
			cfg.MaxPendingConnections, cfg.StartupTimeout = 1, 2*time.Second
			if phase == "TLS" {
				cfg.TLSConfig, _ = testTLSConfigs(t)
			}
			if phase == "timeout" {
				cfg.StartupTimeout = 100 * time.Millisecond
			}
			if phase == "load shedding" {
				shedder.active.Store(true)
			}
			cfg.Hooks.Authenticate = func(_ context.Context, _ StartupRequest, _ StartupResult[struct{}], ex protocol.Exchange) (AuthenticationResult, error) {
				_, err := protocol.GetCleartextPassword(ex)
				return AuthenticationResult{Service: "alice"}, err
			}
			engine, err := New(ctx, cfg)
			require.NoError(t, err)
			done := make(chan error, 1)
			go func() { done <- engine.Run() }()
			t.Cleanup(func() {
				cancel()
				_ = engine.Close()
				<-done
				require.Eventually(t, func() bool {
					return admission.GetGlobalCount() == 0 && len(engine.pending) == 0
				}, time.Second, time.Millisecond, "startup handlers and sessions must finish before the next test")
			})
			dial := func() net.Conn {
				conn, err := net.DialTimeout("tcp", engine.Addr().String(), time.Second)
				require.NoError(t, err)
				t.Cleanup(func() { _ = conn.Close() })
				require.NoError(t, conn.SetDeadline(time.Now().Add(3*time.Second)))
				return conn
			}
			first := dial()
			if phase == "TLS" {
				requestSSL(t, first, 'S')
			}
			if phase == "password" {
				_, err := first.Write(startupPacket(t))
				require.NoError(t, err)
				msg, err := pgproto3.NewFrontend(first, first).Receive()
				require.NoError(t, err)
				require.IsType(t, &pgproto3.AuthenticationCleartextPassword{}, msg)
			}
			require.Eventually(t, func() bool { return len(engine.pending) == 1 }, time.Second, time.Millisecond)
			if phase != "timeout" {
				excess := dial()
				_, err := excess.Read(make([]byte, 1))
				require.Error(t, err)
				var timeout net.Error
				require.False(t, errors.As(err, &timeout) && timeout.Timeout(), "excess startup must close immediately")
				require.Zero(t, admission.GetGlobalCount())
				require.NoError(t, first.Close())
			}
			require.Eventually(t, func() bool { return len(engine.pending) == 0 }, time.Second, time.Millisecond)
			shedder.active.Store(false)
			// Successful startup releases its slot while the session stays open.
			for range 2 {
				conn := dial()
				_, err := conn.Write(startupPacket(t))
				require.NoError(t, err)
				frontend := pgproto3.NewFrontend(conn, conn)
				msg, err := frontend.Receive()
				require.NoError(t, err)
				require.IsType(t, &pgproto3.AuthenticationCleartextPassword{}, msg)
				frontend.Send(&pgproto3.PasswordMessage{Password: "test"})
				require.NoError(t, frontend.Flush())
				for {
					msg, err = frontend.Receive()
					require.NoError(t, err)
					if _, ok := msg.(*pgproto3.ReadyForQuery); ok {
						break
					}
				}
				require.Eventually(t, func() bool { return len(engine.pending) == 0 }, time.Second, time.Millisecond)
			}
			require.EqualValues(t, 2, admission.GetGlobalCount())
		})
	}
}

func TestNegativePendingConnectionsLimit(t *testing.T) {
	_, err := New(t.Context(), Config[struct{}]{MaxPendingConnections: -1})
	require.ErrorContains(t, err, "maximum pending connections")
}
