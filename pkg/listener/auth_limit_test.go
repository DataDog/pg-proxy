// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package listener

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
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

func TestStartupTimeout(t *testing.T) {
	for _, phase := range []string{"startup packet", "TLS handshake", "password reply", "provider callback"} {
		t.Run(phase, func(t *testing.T) {
			engine, admission, _, err := newTestListener(t.Context())
			require.NoError(t, err)
			defer engine.Close()
			engine.cfg.StartupTimeout = 100 * time.Millisecond
			if phase == "TLS handshake" {
				engine.cfg.TLSConfig, _ = testTLSConfigs(t)
			}
			providerDone := make(chan error, 1)
			engine.cfg.Hooks.Authenticate = func(ctx context.Context, _ StartupRequest, _ StartupResult[struct{}], ex protocol.Exchange) (AuthenticationResult, error) {
				if phase == "provider callback" {
					<-ctx.Done()
					providerDone <- ctx.Err()
					// Even a provider that returns success after its deadline cannot
					// create a session or consume an admission slot.
					return AuthenticationResult{Service: "alice"}, nil
				}
				_, err := protocol.GetCleartextPassword(ex)
				return AuthenticationResult{}, err
			}
			conn := testListenerConn(t, engine)
			if phase == "TLS handshake" {
				requestSSL(t, conn, 'S')
			} else if phase != "startup packet" {
				_, err = conn.Write(startupPacket(t))
				require.NoError(t, err)
				if phase == "password reply" {
					message, err := pgproto3.NewFrontend(conn, conn).Receive()
					require.NoError(t, err)
					require.IsType(t, &pgproto3.AuthenticationCleartextPassword{}, message)
				}
			}
			require.NoError(t, conn.SetReadDeadline(time.Now().Add(time.Second)))
			_, err = io.ReadAll(conn)
			var networkError net.Error
			require.False(t, errors.As(err, &networkError) && networkError.Timeout(), "server must close before the client's deadline")
			if phase == "provider callback" {
				require.ErrorIs(t, <-providerDone, context.DeadlineExceeded)
			}
			require.Zero(t, admission.GetGlobalCount())
		})
	}
}

func TestStartupTimeoutDoesNotExpireEstablishedSession(t *testing.T) {
	engine, admission, _, err := newTestListener(t.Context())
	require.NoError(t, err)
	defer engine.Close()
	engine.cfg.StartupTimeout = 100 * time.Millisecond
	created := make(chan context.Context, 1)
	newSession := engine.cfg.Hooks.NewSession
	engine.cfg.Hooks.NewSession = func(cfg SessionConfig[struct{}]) (Session, error) {
		created <- cfg.Context
		return newSession(cfg)
	}
	conn := testListenerConn(t, engine)
	_, err = conn.Write(startupPacket(t))
	require.NoError(t, err)
	frontend := pgproto3.NewFrontend(conn, conn)
	for {
		message, err := frontend.Receive()
		require.NoError(t, err)
		if _, ok := message.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
	sessionCtx := <-created
	<-time.After(2 * engine.cfg.StartupTimeout)
	require.NoError(t, sessionCtx.Err())
	require.EqualValues(t, 1, admission.GetGlobalCount())
	frontend.Send(&pgproto3.Terminate{})
	require.NoError(t, frontend.Flush())
	_, err = frontend.Receive()
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.Eventually(t, func() bool { return admission.GetGlobalCount() == 0 }, time.Second, time.Millisecond)
}

func TestNegativeStartupTimeoutFailsBeforeListen(t *testing.T) {
	_, err := New(t.Context(), Config[struct{}]{StartupTimeout: -time.Second})
	require.ErrorContains(t, err, "startup timeout")
}
