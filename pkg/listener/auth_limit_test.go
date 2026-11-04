package listener

import (
	"context"
	"crypto/tls"
	"io"
	"testing"
	"time"

	"github.com/DataDog/pg-proxy/pkg/protocol"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

func TestShutdownClosesPendingAuthentication(t *testing.T) {
	for _, useTLS := range []bool{false, true} {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		engine, admission, _, err := newTestListener(ctx)
		require.NoError(t, err)
		defer engine.Close()
		serverTLS, clientTLS := testTLSConfigs(t)
		if useTLS {
			engine.cfg.TLSConfig, engine.cfg.RequireTLS = serverTLS, true
		}
		engine.cfg.Hooks.Authenticate = func(_ context.Context, _ StartupRequest, _ StartupResult[struct{}], ex protocol.Exchange) (AuthenticationResult, error) {
			_, err := protocol.GetCleartextPassword(ex)
			return AuthenticationResult{}, err
		}
		conn := testListenerConn(t, engine)
		if useTLS {
			requestSSL(t, conn, 'S')
			secured := tls.Client(conn, clientTLS)
			require.NoError(t, secured.HandshakeContext(t.Context()))
			conn = secured
		}
		frontend := pgproto3.NewFrontend(conn, conn)
		_, err = conn.Write(startupPacket(t))
		require.NoError(t, err)
		message, err := frontend.Receive()
		require.NoError(t, err)
		require.IsType(t, &pgproto3.AuthenticationCleartextPassword{}, message)
		cancel()
		done := make(chan struct{})
		go func() { _, _ = io.ReadAll(conn); close(done) }()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("shutdown left authentication blocked")
		}
		require.Zero(t, admission.GetGlobalCount())
	}
}

func TestAuthenticationLimitRejectsHeaderBeforeReadingBody(t *testing.T) {
	engine, admission, _, err := newTestListener(t.Context())
	require.NoError(t, err)
	defer engine.Close()
	engine.cfg.AuthenticationMessageLimit = 64
	engine.cfg.Hooks.Authenticate = func(_ context.Context, _ StartupRequest, _ StartupResult[struct{}], ex protocol.Exchange) (AuthenticationResult, error) {
		_, err := protocol.GetCleartextPassword(ex)
		return AuthenticationResult{}, err
	}
	conn := testListenerConn(t, engine)
	frontend := pgproto3.NewFrontend(conn, conn)
	_, err = conn.Write(startupPacket(t))
	require.NoError(t, err)
	message, err := frontend.Receive()
	require.NoError(t, err)
	require.IsType(t, &pgproto3.AuthenticationCleartextPassword{}, message)
	// Declare a 65536-byte body, then supply none. The limit must reject the
	// header immediately rather than allocating or waiting for the body.
	_, err = conn.Write([]byte{'p', 0, 1, 0, 4})
	require.NoError(t, err)
	message, err = frontend.Receive()
	require.NoError(t, err)
	require.IsType(t, &pgproto3.ErrorResponse{}, message)
	_, err = frontend.Receive()
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.Zero(t, admission.GetGlobalCount())
}

func TestNegativeAuthenticationLimitFailsBeforeListen(t *testing.T) {
	_, err := New(t.Context(), Config[struct{}]{AuthenticationMessageLimit: -1})
	require.ErrorContains(t, err, "authentication message limit")
}
