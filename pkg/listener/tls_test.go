package listener

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/DataDog/pg-proxy/pkg/protocol"
	pgsession "github.com/DataDog/pg-proxy/pkg/session"
)

func testTLSConfigs(t *testing.T) (*tls.Config, *tls.Config) {
	t.Helper()
	fixture := httptest.NewTLSServer(nil)
	t.Cleanup(fixture.Close)
	roots := x509.NewCertPool()
	roots.AddCert(fixture.Certificate())
	return fixture.TLS.Clone(), &tls.Config{RootCAs: roots, ServerName: "example.com"}
}

// Serve one connection through the real negotiation/startup path and join it at cleanup.
func testListenerConn(t *testing.T, engine *Listener[struct{}]) net.Conn {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := engine.ln.Accept()
		if assert.NoError(t, err) {
			_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
			engine.handleConn(conn)
		}
	}()
	conn, err := net.Dial("tcp", engine.Addr().String())
	require.NoError(t, err)
	require.NoError(t, conn.SetDeadline(time.Now().Add(5*time.Second)))
	t.Cleanup(func() {
		_ = conn.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("connection handler did not close")
		}
	})
	return conn
}

func requestSSL(t *testing.T, conn net.Conn, want byte) {
	t.Helper()
	frontend := pgproto3.NewFrontend(conn, conn)
	frontend.Send(&pgproto3.SSLRequest{})
	require.NoError(t, frontend.Flush())
	var reply [1]byte
	_, err := io.ReadFull(conn, reply[:])
	require.NoError(t, err)
	require.Equal(t, want, reply[0])
}

func TestRequireTLSConfiguration(t *testing.T) {
	engine, _, _, err := newTestListener(t.Context())
	require.NoError(t, err)
	defer engine.Close()
	cfg := engine.cfg
	cfg.RequireTLS = true
	cfg.Listen = func(string, string) (net.Listener, error) {
		t.Error("invalid TLS configuration must fail before binding")
		return nil, errors.New("unexpected listen")
	}
	_, err = New(t.Context(), cfg)
	require.EqualError(t, err, "listener requires TLS but TLS configuration is missing")
}

type forbiddenAdmission struct{ Admission }

func (forbiddenAdmission) RegisterSession(string) error { panic("unexpected session admission") }

func forbidStartupHooks(engine *Listener[struct{}]) {
	engine.cfg.Hooks.ResolveStartup = func(context.Context, StartupRequest) (StartupResult[struct{}], error) {
		panic("unexpected startup resolution")
	}
	engine.cfg.Hooks.Authenticate = func(context.Context, StartupRequest, StartupResult[struct{}], protocol.Exchange) (AuthenticationResult, error) {
		panic("unexpected authentication")
	}
	engine.cfg.Hooks.NewSession = func(SessionConfig[struct{}]) (Session, error) {
		panic("unexpected session creation")
	}
	engine.cfg.Admission = forbiddenAdmission{}
}

func TestRequireTLSRejectsPlaintext(t *testing.T) {
	for _, parameters := range []map[string]string{
		{"user": "testuser", "database": "testdb"},
		{"user": "private-user", "database": "private-db", "options": "invalid"},
		{"database": "testdb"},
	} {
		t.Run(parameters["user"], func(t *testing.T) {
			engine, _, _, err := newTestListener(t.Context())
			require.NoError(t, err)
			defer engine.Close()
			engine.cfg.RequireTLS = true
			engine.cfg.TLSConfig, _ = testTLSConfigs(t)
			forbidStartupHooks(engine)
			conn := testListenerConn(t, engine)
			frontend := pgproto3.NewFrontend(conn, conn)
			frontend.Send(&pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersionNumber, Parameters: parameters})
			require.NoError(t, frontend.Flush())
			message, err := frontend.Receive()
			require.NoError(t, err)
			require.Equal(t, &pgproto3.ErrorResponse{Severity: "FATAL", Code: pgerrcode.ConnectionFailure, Message: "TLS is required"}, message)
			_, err = frontend.Receive()
			require.ErrorIs(t, err, io.ErrUnexpectedEOF, "one error followed by connection close")
		})
	}
}

func TestRequireTLSRejectsIncompleteHandshake(t *testing.T) {
	engine, _, _, err := newTestListener(t.Context())
	require.NoError(t, err)
	defer engine.Close()
	engine.cfg.RequireTLS = true
	forbidStartupHooks(engine)
	conn := tls.Server(newBufferConn(nil), &tls.Config{})
	_, err = engine.handleStartup(conn, &pgproto3.StartupMessage{})
	require.EqualError(t, err, "TLS is required")
}

func TestListenerTLS(t *testing.T) {
	for _, mode := range []string{"static", "GetCertificate", "GetConfigForClient", "optional TLS", "optional plaintext", "no TLS"} {
		t.Run(mode, func(t *testing.T) {
			serverTLS, clientTLS := testTLSConfigs(t)
			engine, _, _, err := newTestListener(t.Context())
			require.NoError(t, err)
			defer engine.Close()
			engine.cfg.RequireTLS = mode != "optional TLS" && mode != "optional plaintext" && mode != "no TLS"
			engine.cfg.TLSConfig = serverTLS
			callbackCalled := make(chan struct{}, 1)
			switch mode {
			case "GetCertificate":
				engine.cfg.TLSConfig = &tls.Config{GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
					callbackCalled <- struct{}{}
					return &serverTLS.Certificates[0], nil
				}}
			case "GetConfigForClient":
				engine.cfg.TLSConfig = &tls.Config{GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
					callbackCalled <- struct{}{}
					return serverTLS, nil
				}}
			case "no TLS":
				engine.cfg.TLSConfig = nil
			}
			conn := testListenerConn(t, engine)
			if mode == "no TLS" {
				requestSSL(t, conn, 'N')
			} else if mode != "optional plaintext" {
				requestSSL(t, conn, 'S')
				secured := tls.Client(conn, clientTLS)
				require.NoError(t, secured.HandshakeContext(t.Context()))
				require.NotEmpty(t, secured.ConnectionState().VerifiedChains)
				conn = secured
			}
			frontend := pgproto3.NewFrontend(conn, conn)
			frontend.Send(&pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersionNumber, Parameters: map[string]string{"user": "testuser", "database": "testdb"}})
			require.NoError(t, frontend.Flush())
			for {
				message, err := frontend.Receive()
				require.NoError(t, err)
				_, rejected := message.(*pgproto3.ErrorResponse)
				require.False(t, rejected, "unexpected startup error: %v", message)
				if _, ok := message.(*pgproto3.ReadyForQuery); ok {
					break
				}
			}
			if mode == "GetCertificate" || mode == "GetConfigForClient" {
				require.Len(t, callbackCalled, 1)
			}
			frontend.Send(&pgproto3.Terminate{})
			require.NoError(t, frontend.Flush())
		})
	}
}

func TestListenerTLSHandshakeFailure(t *testing.T) {
	for _, mode := range []string{"untrusted certificate", "wrong hostname", "certificate callback failure", "plaintext after SSL acceptance"} {
		t.Run(mode, func(t *testing.T) {
			serverTLS, clientTLS := testTLSConfigs(t)
			engine, _, _, err := newTestListener(t.Context())
			require.NoError(t, err)
			defer engine.Close()
			engine.cfg.RequireTLS = true
			engine.cfg.TLSConfig = serverTLS
			forbidStartupHooks(engine)
			switch mode {
			case "untrusted certificate":
				clientTLS.RootCAs = x509.NewCertPool()
			case "wrong hostname":
				clientTLS.ServerName = "wrong.example"
			case "certificate callback failure":
				engine.cfg.TLSConfig = &tls.Config{GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
					return nil, errors.New("private certificate provider failure")
				}}
			}
			conn := testListenerConn(t, engine)
			requestSSL(t, conn, 'S')
			if mode == "plaintext after SSL acceptance" {
				_, err = conn.Write([]byte("not TLS"))
				require.NoError(t, err)
			} else {
				err = tls.Client(conn, clientTLS).HandshakeContext(t.Context())
				require.Error(t, err)
				if mode == "untrusted certificate" || mode == "wrong hostname" {
					var verificationError *tls.CertificateVerificationError
					require.ErrorAs(t, err, &verificationError)
				}
			}
			rest, err := io.ReadAll(conn)
			require.NoError(t, err)
			require.Empty(t, rest, "failed TLS must close without a plaintext PostgreSQL error")
		})
	}
}

type tlsCancelSession struct {
	pgsession.SessionHandle
	cancelled chan struct{}
}

func (s tlsCancelSession) Cancel() error { s.cancelled <- struct{}{}; return nil }

func TestRequireTLSAllowsCancel(t *testing.T) {
	for _, useTLS := range []bool{false, true} {
		t.Run(map[bool]string{false: "plaintext", true: "TLS"}[useTLS], func(t *testing.T) {
			engine, _, _, err := newTestListener(t.Context())
			require.NoError(t, err)
			defer engine.Close()
			serverTLS, clientTLS := testTLSConfigs(t)
			engine.cfg.RequireTLS, engine.cfg.TLSConfig = true, serverTLS
			forbidStartupHooks(engine)
			cancelled := make(chan struct{}, 1)
			secret := secretKeyFromUint32(42)
			engine.cfg.Hooks.Lifecycle.Lookup = func(pid uint32) (pgsession.SessionHandle, bool) {
				return tlsCancelSession{SessionHandle: pgsession.New(pgsession.Config{SecretKey: secret}), cancelled: cancelled}, pid == 7
			}
			conn := testListenerConn(t, engine)
			if useTLS {
				requestSSL(t, conn, 'S')
				secured := tls.Client(conn, clientTLS)
				require.NoError(t, secured.HandshakeContext(t.Context()))
				conn = secured
			}
			frontend := pgproto3.NewFrontend(conn, conn)
			frontend.Send(&pgproto3.CancelRequest{ProcessID: 7, SecretKey: secret})
			require.NoError(t, frontend.Flush())
			reply, err := io.ReadAll(conn)
			require.NoError(t, err)
			require.Empty(t, reply)
			require.Len(t, cancelled, 1)
		})
	}
}
