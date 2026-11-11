// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package listener

import (
	"context"
	"io"
	"sync/atomic"
	"testing"

	"github.com/DataDog/pg-proxy/pkg/protocol"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

func TestAuthenticationRejectsBeforeAdmission(t *testing.T) {
	for _, tc := range []struct {
		name             string
		parameters       map[string]string
		wantResolution   bool
		wantVerification bool
	}{
		{name: "missing user", parameters: map[string]string{"database": "testdb"}},
		{name: "empty user", parameters: map[string]string{"user": "", "database": "testdb"}},
		{name: "passthrough without identity", parameters: map[string]string{"user": "alice", "database": "testdb", "options": "-c pg_proxy.passthrough_user=true"}, wantResolution: true, wantVerification: true},
		{name: "empty resolved database", parameters: map[string]string{"user": "alice", "database": ""}, wantResolution: true},
		{name: "empty identity", parameters: map[string]string{"user": "alice", "database": "testdb"}, wantResolution: true, wantVerification: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			engine, admission, _, err := newTestListener(t.Context())
			require.NoError(t, err)
			defer engine.Close()
			var resolved, verified atomic.Bool
			engine.cfg.Hooks.ResolveStartup = func(_ context.Context, request StartupRequest) (StartupResult[struct{}], error) {
				resolved.Store(true)
				return StartupResult[struct{}]{LogicalDatabase: request.Database}, nil
			}
			engine.cfg.Hooks.Authenticate = func(context.Context, StartupRequest, StartupResult[struct{}], protocol.Exchange) (AuthenticationResult, error) {
				verified.Store(true)
				return AuthenticationResult{}, nil
			}
			engine.cfg.Hooks.NewSession = func(SessionConfig[struct{}]) (Session, error) {
				panic("rejected login reached session construction")
			}
			conn := testListenerConn(t, engine)
			frontend := pgproto3.NewFrontend(conn, conn)
			frontend.Send(&pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersionNumber, Parameters: tc.parameters})
			require.NoError(t, frontend.Flush())
			response, err := frontend.Receive()
			require.NoError(t, err)
			require.Equal(t, &pgproto3.ErrorResponse{Severity: "FATAL", Code: "08006", Message: startupFailureMessage}, response)
			_, err = frontend.Receive()
			require.ErrorIs(t, err, io.ErrUnexpectedEOF)
			require.Zero(t, admission.GetGlobalCount())
			require.Equal(t, tc.wantResolution, resolved.Load())
			require.Equal(t, tc.wantVerification, verified.Load())
		})
	}
}

func TestCanceledVerificationCannotCreateSession(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	engine, admission, _, err := newTestListener(ctx)
	require.NoError(t, err)
	defer engine.Close()
	engine.cfg.Hooks.Authenticate = func(context.Context, StartupRequest, StartupResult[struct{}], protocol.Exchange) (AuthenticationResult, error) {
		cancel()
		return AuthenticationResult{Service: "verified-service"}, nil
	}
	conn := testListenerConn(t, engine)
	_, err = conn.Write(startupPacket(t))
	require.NoError(t, err)
	frontend := pgproto3.NewFrontend(conn, conn)
	message, err := frontend.Receive()
	require.NoError(t, err)
	_, ok := message.(*pgproto3.ErrorResponse)
	require.True(t, ok)
	_, err = frontend.Receive()
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.Zero(t, admission.GetGlobalCount())
}
