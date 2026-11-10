// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	pgproxy "github.com/DataDog/pg-proxy"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

type authLogBuffer struct {
	mu sync.Mutex
	bytes.Buffer
}

func (b *authLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.Write(p)
}
func (b *authLogBuffer) text() string { b.mu.Lock(); defer b.mu.Unlock(); return b.Buffer.String() }

func verifyProxyAuthentication(t *testing.T, backendURL string, observer *pgx.Conn) {
	ctx, stop := context.WithTimeout(t.Context(), 45*time.Second)
	defer stop()
	var logs authLogBuffer
	t.Cleanup(pgproxy.Bootstrap(pgproxy.Dependencies{LogHandler: slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})}))
	_, err := observer.Exec(ctx, "SET password_encryption = 'scram-sha-256'")
	require.NoError(t, err)
	// These fixture roles produce a real PostgreSQL verifier and a second backend.
	// The proxy never creates roles for Alice or Bob.
	_, err = observer.Exec(ctx, "CREATE ROLE auth_fixture NOLOGIN PASSWORD 'bob-client-password'")
	require.NoError(t, err)
	_, err = observer.Exec(ctx, "CREATE ROLE auth_reports LOGIN PASSWORD 'reports-backend-password'")
	require.NoError(t, err)
	_, err = observer.Exec(ctx, "CREATE DATABASE auth_reports OWNER auth_reports")
	require.NoError(t, err)
	var verifier string
	require.NoError(t, observer.QueryRow(ctx, "SELECT rolpassword FROM pg_authid WHERE rolname = 'auth_fixture'").Scan(&verifier))
	backend, err := pgx.ParseConfig(backendURL)
	require.NoError(t, err)
	certFile, keyFile := clientTLSFiles(t)
	document := fmt.Sprintf(`databases:
- {name: primary, host: %q, port: %d, dbname: %q, username: %q, password: %q, sslmode: disable, max_conns: 1}
- {name: reports-primary, host: %q, port: %d, dbname: auth_reports, username: auth_reports, password: reports-backend-password, sslmode: disable, max_conns: 1}
clusters:
  orders: {primary: primary}
  reports: {primary: reports-primary}
logical_databases:
  local_db: {cluster: orders}
  reports_db: {cluster: reports}
proxy:
  listener_address: '0.0.0.0:0'
  password_auth_enabled: true
  tls_required: true
  tls_cert_file: %q
  tls_key_file: %q
users:
- static_credentials: {username: alice, password: alice-client-password}
  databases: [local_db]
- static_credentials: {username: bob, scram_sha_256: %q}
  databases: [local_db, reports_db]
`, backend.Host, backend.Port, backend.Database, backend.User, backend.Password, backend.Host, backend.Port, certFile, keyFile, verifier)
	cfg, err := parseConfig([]string{"-config", writeConfig(t, "yaml", document), "-telemetry", "none"})
	require.NoError(t, err)
	proxy, err := newProxy(ctx, cfg)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- serve(ctx, stop, proxy.server, proxy.sessions, proxy.inventory.Close, 5*time.Second) }()
	t.Cleanup(func() {
		stop()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Error("authenticated proxy did not stop")
		}
	})
	address := net.JoinHostPort("127.0.0.1", fmt.Sprint(proxy.server.Addr().(*net.TCPAddr).Port))
	connect := func(user, password, database, mode, options string) (*pgx.Conn, error) {
		settings, err := pgx.ParseConfig("postgres://" + user + "@" + address + "/" + database + "?sslmode=" + mode + "&sslrootcert=" + url.QueryEscape(certFile))
		if err != nil {
			return nil, err
		}
		settings.Password = password
		if options != "" {
			settings.RuntimeParams["options"] = options
		}
		return pgx.ConnectConfig(ctx, settings)
	}
	alice, err := connect("alice", "alice-client-password", "local_db", "verify-full", "")
	require.NoError(t, err)
	defer alice.Close(context.Background())
	bob, err := connect("bob", "bob-client-password", "local_db", "verify-full", "")
	require.NoError(t, err)
	defer bob.Close(context.Background())
	services := func() map[string]int {
		proxy.sessions.mu.Lock()
		defer proxy.sessions.mu.Unlock()
		result := make(map[string]int)
		for _, session := range proxy.sessions.sessions {
			result[session.(interface{ Service() string }).Service()]++
		}
		return result
	}
	require.Eventually(t, func() bool {
		counts := services()
		return counts["alice"] == 1 && counts["bob"] == 1 && len(counts) == 2
	}, time.Second, 10*time.Millisecond)
	require.Len(t, proxy.inventory.backends, 2, "backend pools are per configured cluster")
	var alicePID, bobPID int
	for client, target := range map[*pgx.Conn]*int{alice: &alicePID, bob: &bobPID} {
		var currentUser string
		require.NoError(t, client.QueryRow(ctx, "select current_user, pg_backend_pid()").Scan(&currentUser, target))
		require.Equal(t, backend.User, currentUser)
	}
	require.Equal(t, alicePID, bobPID, "distinct proxy users share one backend connection")
	var size int
	require.NoError(t, alice.QueryRow(ctx, "select length($1::text)", strings.Repeat("x", 64<<10)).Scan(&size))
	require.Equal(t, 64<<10, size, "authentication message limit must not limit SQL")
	errors := make(chan error, 2)
	for _, client := range []*pgx.Conn{alice, bob} {
		go func() {
			for i := 0; i < 5; i++ {
				var current string
				if err := client.QueryRow(ctx, "select current_user").Scan(&current); err != nil {
					errors <- err
					return
				}
				if current != backend.User {
					errors <- fmt.Errorf("unexpected backend user")
					return
				}
			}
			errors <- nil
		}()
	}
	require.NoError(t, <-errors)
	require.NoError(t, <-errors)
	for _, transport := range []string{"TLS", "plaintext"} {
		t.Run("authenticated cancellation "+transport, func(t *testing.T) { verifyTLSQueryCancellation(t, ctx, alice, observer, transport) })
	}
	reports, err := connect("bob", "bob-client-password", "reports_db", "verify-full", "")
	require.NoError(t, err)
	var current string
	require.NoError(t, reports.QueryRow(ctx, "select current_user").Scan(&current))
	require.Equal(t, "auth_reports", current)
	require.NoError(t, reports.Close(ctx))
	for _, tc := range []struct{ name, user, password, database, mode, options string }{
		{"wrong password", "alice", "wrong-password", "local_db", "verify-full", ""},
		{"Alice as Bob", "bob", "alice-client-password", "local_db", "verify-full", ""},
		{"unknown user", "unknown", "alice-client-password", "local_db", "verify-full", ""},
		{"backend username", backend.User, backend.Password, "local_db", "verify-full", ""},
		{"forbidden database", "alice", "alice-client-password", "reports_db", "verify-full", ""},
		{"unknown database", "alice", "alice-client-password", "unknown", "verify-full", ""},
		{"passthrough", "alice", "alice-client-password", "local_db", "verify-full", "-c pg_proxy.passthrough_user=true"},
		{"plaintext", "alice", "alice-client-password", "local_db", "disable", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, err := connect(tc.user, tc.password, tc.database, tc.mode, tc.options)
			if client != nil {
				client.Close(ctx)
			}
			require.Error(t, err)
			require.NotContains(t, err.Error(), "alice-client-password")
			require.NotContains(t, err.Error(), verifier)
		})
	}
	for _, oversized := range []bool{false, true} {
		verifyRejectedAuthMessages(t, address, alice.Config().TLSConfig, oversized)
	}
	require.NoError(t, alice.Close(ctx))
	require.NoError(t, bob.Close(ctx))
	require.Eventually(t, func() bool { return len(services()) == 0 }, time.Second, 10*time.Millisecond)
	for _, secret := range []string{"alice-client-password", "bob-client-password", "wrong-password", verifier, "AUTH_PAYLOAD_SENTINEL"} {
		require.NotContains(t, logs.text(), secret)
	}
	require.Contains(t, logs.text(), "authenticated_user=alice")
	require.Contains(t, logs.text(), "authenticated_user=bob")
}

func verifyRejectedAuthMessages(t *testing.T, address string, tlsConfig *tls.Config, oversized bool) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", address, 3*time.Second)
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetDeadline(time.Now().Add(3*time.Second)))
	frontend := pgproto3.NewFrontend(conn, conn)
	frontend.Send(&pgproto3.SSLRequest{})
	require.NoError(t, frontend.Flush())
	var reply [1]byte
	_, err = io.ReadFull(conn, reply[:])
	require.NoError(t, err)
	require.Equal(t, byte('S'), reply[0])
	secured := tls.Client(conn, tlsConfig)
	require.NoError(t, secured.Handshake())
	frontend = pgproto3.NewFrontend(secured, secured)
	frontend.Send(&pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersionNumber, Parameters: map[string]string{"user": "alice", "database": "local_db"}})
	require.NoError(t, frontend.Flush())
	message, err := frontend.Receive()
	require.NoError(t, err)
	require.IsType(t, &pgproto3.AuthenticationSASL{}, message)
	data := "n,,n=,r=clientnonce"
	if oversized {
		data += strings.Repeat("AUTH_PAYLOAD_SENTINEL", 1024)
		// The proxy rejects an oversized message from its length header and closes
		// without reading the body. Send only the header and a prefix containing the
		// sentinel: writing the rest could race that close and fail with a broken
		// pipe. If the proxy instead waited for the full body, the deadline would
		// expire before the FATAL response below.
		frame, err := (&pgproto3.SASLInitialResponse{AuthMechanism: "SCRAM-SHA-256", Data: []byte(data)}).Encode(nil)
		require.NoError(t, err)
		prefix := frame[:4096]
		require.Contains(t, string(prefix), "AUTH_PAYLOAD_SENTINEL")
		_, err = secured.Write(prefix)
		require.NoError(t, err)
	} else {
		frontend.Send(&pgproto3.SASLInitialResponse{AuthMechanism: "SCRAM-SHA-256", Data: []byte(data)})
		require.NoError(t, frontend.Flush())
		message, err = frontend.Receive()
		require.NoError(t, err)
		challenge := message.(*pgproto3.AuthenticationSASLContinue)
		nonce := strings.TrimPrefix(strings.Split(string(challenge.Data), ",")[0], "r=")
		frontend.Send(&pgproto3.SASLResponse{Data: []byte("c=biws,r=" + nonce + ",p=" + base64.StdEncoding.EncodeToString(make([]byte, 32)))})
		require.NoError(t, frontend.Flush())
	}
	message, err = frontend.Receive()
	require.NoError(t, err)
	require.Equal(t, &pgproto3.ErrorResponse{Severity: "FATAL", Code: "08006", Message: "could not establish connection"}, message)
	_, err = frontend.Receive()
	require.Error(t, err, "failure closes without successful startup messages")
}
