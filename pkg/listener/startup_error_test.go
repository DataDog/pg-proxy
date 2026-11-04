package listener

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pgproxy "github.com/DataDog/pg-proxy"
	"github.com/DataDog/pg-proxy/pkg/protocol"
	pgsession "github.com/DataDog/pg-proxy/pkg/session"
)

type startupLogBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *startupLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}
func (b *startupLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

func listenerCounts(metrics *listenerRecordingMetrics) []listenerMetricCall {
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	return append([]listenerMetricCall(nil), metrics.counts...)
}

func startupPacket(t *testing.T) []byte {
	t.Helper()
	packet, err := (&pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters:      map[string]string{"user": "testuser", "database": "testdb"},
	}).Encode(nil)
	require.NoError(t, err)
	return packet
}

func TestStartupFailureSanitization(t *testing.T) {
	const secret = "password-token-sasl-internal-cluster-sentinel"
	for _, tc := range []struct {
		stage, reason string
		cause         error
	}{
		{"resolve_startup", "startup_failure", errors.New(secret)},
		{"authenticate", "startup_failure", errors.New(secret)},
		{"authenticate", "authentication_failed", ErrAuthenticationFailed},
		{"authenticate", "invalid_credentials", ErrInvalidCredentials},
		{"authenticate", "unsupported_auth_option", ErrUnsupportedAuthOption},
		{"authenticate", "startup_failure", AuthenticationError(secret)},
	} {
		t.Run(fmt.Sprintf("%s/%s/%T", tc.stage, tc.reason, tc.cause), func(t *testing.T) {
			var logs startupLogBuffer
			metrics := &listenerRecordingMetrics{}
			restore := pgproxy.Bootstrap(pgproxy.Dependencies{
				LogHandler: slog.NewTextHandler(&logs, nil), Metrics: metrics,
			})
			defer restore()
			engine, admission, _, err := newTestListener(t.Context())
			require.NoError(t, err)
			defer engine.Close()
			cause := fmt.Errorf("%s: %w", secret, tc.cause)
			if tc.stage == "resolve_startup" {
				engine.cfg.Hooks.ResolveStartup = func(context.Context, StartupRequest) (StartupResult[struct{}], error) {
					return StartupResult[struct{}]{}, cause
				}
			} else {
				engine.cfg.Hooks.Authenticate = func(context.Context, StartupRequest, StartupResult[struct{}], protocol.Exchange) (AuthenticationResult, error) {
					return AuthenticationResult{}, cause
				}
			}
			conn := testListenerConn(t, engine)
			frontend := pgproto3.NewFrontend(conn, conn)
			_, err = conn.Write(startupPacket(t))
			require.NoError(t, err)
			message, err := frontend.Receive()
			require.NoError(t, err)
			require.Equal(t, &pgproto3.ErrorResponse{
				Severity: "FATAL", Code: pgerrcode.ConnectionFailure, Message: startupFailureMessage,
			}, message)
			_, err = frontend.Receive()
			require.ErrorIs(t, err, io.ErrUnexpectedEOF, "one error followed by connection closure")
			assert.NotContains(t, logs.String(), secret)
			assert.Contains(t, logs.String(), "stage="+tc.stage)
			assert.Contains(t, logs.String(), "reason="+tc.reason)
			if tc.reason != "startup_failure" {
				assert.Contains(t, logs.String(), ": "+tc.cause.Error())
			}
			assert.Contains(t, logs.String(), "requested_user=testuser")
			assert.Contains(t, logs.String(), "requested_database=testdb")
			assert.Contains(t, logs.String(), "connection_id=")
			assert.Equal(t, int32(0), admission.GetGlobalCount())
			calls := listenerCounts(metrics)
			require.Len(t, calls, 1)
			assert.Equal(t, []string{"outcome:error", "placement:first_message_failure"}, calls[0].tags)
		})
	}
}

func TestCancelFailureDiagnostics(t *testing.T) {
	for _, tc := range []struct {
		name, reason, diagnostic string
		pid                      uint32
		noLookup                 bool
	}{
		{"no lookup", "session_not_found", "no session found for pid: 999", 999, true},
		{"missing session", "session_not_found", "no session found for pid: 999", 999, false},
		{"incorrect secret", "incorrect_secret", "incorrect cancel request secret key for pid: 2", 2, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs startupLogBuffer
			metrics := &listenerRecordingMetrics{}
			t.Cleanup(pgproxy.Bootstrap(pgproxy.Dependencies{
				LogHandler: slog.NewTextHandler(&logs, nil), Metrics: metrics,
			}))
			engine, _, _, err := newTestListener(t.Context())
			require.NoError(t, err)
			defer engine.Close()
			engine.cfg.Hooks.Lifecycle.Register(pgsession.New(pgsession.Config{PID: 2, SecretKey: []byte("s!k?")}))
			if tc.noLookup {
				engine.cfg.Hooks.Lifecycle.Lookup = nil
			}
			conn := testListenerConn(t, engine)
			frontend := pgproto3.NewFrontend(conn, conn)
			frontend.Send(&pgproto3.CancelRequest{ProcessID: tc.pid, SecretKey: []byte("b!d!")})
			require.NoError(t, frontend.Flush())
			message, err := frontend.Receive()
			require.NoError(t, err)
			require.Equal(t, &pgproto3.ErrorResponse{
				Severity: "FATAL", Code: pgerrcode.ConnectionFailure, Message: startupFailureMessage,
			}, message)
			_, err = frontend.Receive()
			require.ErrorIs(t, err, io.ErrUnexpectedEOF)
			assert.Contains(t, logs.String(), "failed to cancel request: "+tc.diagnostic)
			assert.Contains(t, logs.String(), "stage=cancel reason="+tc.reason)
			assert.NotContains(t, logs.String(), "b!d!")
			assert.NotContains(t, logs.String(), "s!k?")
			assert.NotContains(t, logs.String(), fmt.Sprint([]byte("b!d!")))
			assert.Equal(t, 1, strings.Count(logs.String(), "level=ERROR"))
			calls := listenerCounts(metrics)
			require.Len(t, calls, 1)
			assert.Equal(t, []string{"outcome:error", "placement:first_message_failure"}, calls[0].tags)
		})
	}
}

func TestStartupOptionsDoNotLogPayloads(t *testing.T) {
	const secret = "PASSWORD_TOKEN_SASL_SENTINEL"
	for _, input := range []string{
		secret + "%zz", secret,
		"-c pg_proxy." + secret + "=" + secret,
		"-c pg_proxy.dynamic_routing=" + secret,
		"-c pg_proxy.priority=" + secret,
	} {
		t.Run(input, func(t *testing.T) {
			var logs startupLogBuffer
			restore := pgproxy.Bootstrap(pgproxy.Dependencies{
				LogHandler: slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}),
			})
			defer restore()
			_, err := ParseSessionOptions(input)
			if err != nil {
				assert.NotContains(t, err.Error(), secret)
			}
			assert.NotContains(t, logs.String(), secret)
			assert.Contains(t, logs.String(), "reason=")
		})
	}
}

func TestStartupLogsBoundAndEncodeIdentities(t *testing.T) {
	var logs bytes.Buffer
	restore := pgproxy.Bootstrap(pgproxy.Dependencies{LogHandler: slog.NewJSONHandler(&logs, nil)})
	defer restore()
	engine, _, _, err := newTestListener(t.Context())
	require.NoError(t, err)
	defer engine.Close()
	engine.cfg.Hooks.ResolveStartup = func(context.Context, StartupRequest) (StartupResult[struct{}], error) {
		return StartupResult[struct{}]{}, errors.New("private resolver details")
	}
	requested := "user\n\"reason=forged\"" + strings.Repeat("é", 256)
	packet, err := (&pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters: map[string]string{"user": requested, "database": "customer-db"}}).Encode(nil)
	require.NoError(t, err)
	engine.handleConn(newBufferConn(packet), func() {})
	engine.handleConn(newBufferConn(packet), func() {})
	lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
	require.Len(t, lines, 2, "identity must not create additional log records")
	var first, second map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &first))
	require.NoError(t, json.Unmarshal([]byte(lines[1]), &second))
	assert.LessOrEqual(t, utf8.RuneCountInString(first["requested_user"].(string)), 256)
	assert.True(t, strings.HasPrefix(requested, first["requested_user"].(string)))
	assert.Equal(t, "customer-db", first["requested_database"])
	assert.NotEmpty(t, first["connection_id"])
	assert.NotEqual(t, first["connection_id"], second["connection_id"])
	assert.Equal(t, "startup_failure", second["reason"])
	assert.NotContains(t, first, "authenticated_principal")
}
