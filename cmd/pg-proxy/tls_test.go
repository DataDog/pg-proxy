// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	pgconfig "github.com/DataDog/pg-proxy/pkg/config"
)

func clientTLSFiles(t *testing.T) (string, string) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	certificate := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"},
		DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, certificate, certificate, public, private)
	require.NoError(t, err)
	key, err := x509.MarshalPKCS8PrivateKey(private)
	require.NoError(t, err)
	directory := t.TempDir()
	certFile, keyFile := filepath.Join(directory, "cert.pem"), filepath.Join(directory, "key.pem")
	require.NoError(t, os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600))
	require.NoError(t, os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}), 0600))
	return certFile, keyFile
}

func TestClientTLSConfiguration(t *testing.T) {
	certFile, keyFile := clientTLSFiles(t)
	for _, mode := range []string{"disabled", "optional", "required"} {
		for _, extension := range []string{"json", "yaml"} {
			t.Run(mode+"/"+extension, func(t *testing.T) {
				shared := ossConfig()
				if mode != "disabled" {
					shared.Proxy.TLSCertFile, shared.Proxy.TLSKeyFile = certFile, keyFile
				}
				shared.Proxy.TLSRequired = mode == "required"
				marshal := json.Marshal
				if extension == "yaml" {
					marshal = yaml.Marshal
				}
				data, err := marshal(shared)
				require.NoError(t, err)
				cfg, err := parseConfig([]string{"-config", writeConfig(t, extension, string(data)), "-telemetry", "none"})
				require.NoError(t, err)
				require.Equal(t, mode == "required", cfg.requireTLS)
				if mode == "disabled" {
					require.Nil(t, cfg.tlsConfig)
					return
				}
				require.NotNil(t, cfg.tlsConfig)
				require.Equal(t, uint16(tls.VersionTLS12), cfg.tlsConfig.MinVersion)
				require.Len(t, cfg.tlsConfig.Certificates, 1)
				require.NotNil(t, cfg.tlsConfig.Certificates[0].PrivateKey)
				shared.Proxy.ListenerAddress = "0.0.0.0:15432"
				_, err = (config{}).withSharedConfig(shared)
				require.ErrorContains(t, err, "loopback")
			})
		}
	}
}

func TestRequiredTLSFailsBeforeBackendConnect(t *testing.T) {
	cfg := backendConfig(t, "postgres://postgres@127.0.0.1:1/postgres?connect_timeout=1")
	cfg.requireTLS = true
	_, err := newProxy(t.Context(), cfg)
	require.ErrorContains(t, err, "requires tls_cert_file and tls_key_file")
}

func TestInvalidClientTLSConfiguration(t *testing.T) {
	certFile, keyFile := clientTLSFiles(t)
	_, otherKey := clientTLSFiles(t)
	invalid := filepath.Join(t.TempDir(), "private-test-value.pem")
	require.NoError(t, os.WriteFile(invalid, []byte("private-test-value"), 0600))
	badCertificate := filepath.Join(t.TempDir(), "private-test-value-cert.pem")
	require.NoError(t, os.WriteFile(badCertificate, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: []byte("private-test-value")}), 0600))
	badKey := filepath.Join(t.TempDir(), "private-test-value-key.pem")
	require.NoError(t, os.WriteFile(badKey, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: []byte("private-test-value")}), 0600))
	badLabel := filepath.Join(t.TempDir(), "private-test-value-label.pem")
	require.NoError(t, os.WriteFile(badLabel, pem.EncodeToMemory(&pem.Block{Type: "private-test-value", Bytes: []byte("private-test-value")}), 0600))
	for _, tc := range []struct {
		name, cert, key, want string
		required              bool
	}{
		{name: "required without pair", required: true, want: "proxy.tls_required requires tls_cert_file and tls_key_file"},
		{name: "certificate only", cert: certFile, want: "proxy.tls_cert_file and tls_key_file must be configured together"},
		{name: "key only", key: keyFile, want: "proxy.tls_cert_file and tls_key_file must be configured together"},
		{name: "missing certificate", cert: invalid + ".missing", key: keyFile, want: "proxy.tls_cert_file: file not found"},
		{name: "missing key", cert: certFile, key: invalid + ".missing", want: "proxy.tls_key_file: file not found"},
		{name: "unreadable certificate", cert: t.TempDir(), key: keyFile, want: "proxy.tls_cert_file: could not read file"},
		{name: "unreadable key", cert: certFile, key: t.TempDir(), want: "proxy.tls_key_file: could not read file"},
		{name: "malformed certificate", cert: invalid, key: keyFile, want: "proxy.tls_cert_file: invalid PEM certificate"},
		{name: "malformed key", cert: certFile, key: invalid, want: "proxy.tls_key_file: invalid or unsupported PEM private key"},
		{name: "malformed certificate DER", cert: badCertificate, key: keyFile, want: "proxy.tls_cert_file: invalid PEM certificate"},
		{name: "malformed key DER", cert: certFile, key: badKey, want: "proxy.tls_key_file: invalid or unsupported PEM private key"},
		{name: "unexpected certificate PEM label", cert: badLabel, key: keyFile, want: "proxy.tls_cert_file: invalid PEM certificate"},
		{name: "unexpected key PEM label", cert: certFile, key: badLabel, want: "proxy.tls_key_file: invalid or unsupported PEM private key"},
		{name: "swapped files", cert: keyFile, key: certFile, want: "proxy.tls_cert_file: invalid PEM certificate"},
		{name: "mismatched key", cert: certFile, key: otherKey, want: "proxy.tls_key_file: private key does not match proxy.tls_cert_file"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			shared := ossConfig()
			shared.Proxy.TLSCertFile, shared.Proxy.TLSKeyFile, shared.Proxy.TLSRequired = tc.cert, tc.key, tc.required
			_, err := (config{}).withSharedConfig(shared)
			require.EqualError(t, err, tc.want)
			require.NotContains(t, err.Error(), "private-test-value")
			require.NotContains(t, err.Error(), filepath.Dir(certFile))
			require.NotContains(t, err.Error(), filepath.Dir(keyFile))
		})
	}
}

func TestClientTLSFilePermissionErrors(t *testing.T) {
	certFile, keyFile := clientTLSFiles(t)
	denied := filepath.Join(t.TempDir(), "private-test-value.pem")
	require.NoError(t, os.WriteFile(denied, []byte("private-test-value"), 0000))
	if _, err := os.ReadFile(denied); !errors.Is(err, os.ErrPermission) {
		t.Skip("filesystem does not enforce the test file's read permissions")
	}
	for _, setting := range []string{"proxy.tls_cert_file", "proxy.tls_key_file"} {
		t.Run(setting, func(t *testing.T) {
			shared := ossConfig()
			shared.Proxy.TLSCertFile, shared.Proxy.TLSKeyFile = certFile, keyFile
			if setting == "proxy.tls_cert_file" {
				shared.Proxy.TLSCertFile = denied
			} else {
				shared.Proxy.TLSKeyFile = denied
			}
			_, err := (config{}).withSharedConfig(shared)
			require.EqualError(t, err, setting+": permission denied")
			require.NotContains(t, err.Error(), "private-test-value")
		})
	}
}

func TestClientTLSKeyMismatchFormats(t *testing.T) {
	certFile, _ := clientTLSFiles(t)
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	ecKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	ecDER, err := x509.MarshalECPrivateKey(ecKey)
	require.NoError(t, err)
	for _, block := range []*pem.Block{
		{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(rsaKey)},
		{Type: "EC PRIVATE KEY", Bytes: ecDER},
	} {
		t.Run(block.Type, func(t *testing.T) {
			keyFile := filepath.Join(t.TempDir(), "private-test-value.pem")
			require.NoError(t, os.WriteFile(keyFile, pem.EncodeToMemory(block), 0600))
			shared := ossConfig()
			shared.Proxy.TLSCertFile, shared.Proxy.TLSKeyFile = certFile, keyFile
			_, err := (config{}).withSharedConfig(shared)
			require.EqualError(t, err, "proxy.tls_key_file: private key does not match proxy.tls_cert_file")
		})
	}
}

// Called by the PostgreSQL integration test to exercise command TLS forwarding,
// verified client handshakes, queries, and cancellation through the real server.
func verifyProxyTLS(t *testing.T, backendURL string, observer *pgx.Conn) {
	certFile, keyFile := clientTLSFiles(t)
	for _, mode := range []string{"disabled", "optional", "required"} {
		t.Run(mode, func(t *testing.T) {
			shared := ossConfig()
			shared.LogicalDatabases = map[string]pgconfig.LogicalDatabase{"local_db": {Cluster: "orders"}}
			if mode != "disabled" {
				shared.Proxy.TLSCertFile, shared.Proxy.TLSKeyFile = certFile, keyFile
			}
			shared.Proxy.TLSRequired = mode == "required"
			data, err := yaml.Marshal(shared)
			require.NoError(t, err)
			cfg, err := parseConfig([]string{"-config", writeConfig(t, "yaml", string(data)), "-telemetry", "none"})
			require.NoError(t, err)
			cfg.databases["primary"] = backendConfig(t, backendURL).databases["primary"]
			ctx, stop := context.WithTimeout(t.Context(), 15*time.Second)
			defer stop()
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
					t.Error("TLS proxy did not stop")
				}
			})
			baseURL := "postgres://" + cfg.databases["primary"].ConnConfig.User + "@" + proxy.server.Addr().String() + "/local_db"
			for _, clientMode := range []string{"disable", "verify-full"} {
				t.Run(clientMode, func(t *testing.T) {
					client, err := pgx.Connect(ctx, baseURL+"?sslmode="+clientMode+"&sslrootcert="+certFile)
					if mode == "required" && clientMode == "disable" {
						require.ErrorContains(t, err, "TLS is required")
						return
					}
					if mode == "disabled" && clientMode == "verify-full" {
						require.Error(t, err)
						return
					}
					require.NoError(t, err)
					defer client.Close(context.Background())
					var value int
					require.NoError(t, client.QueryRow(ctx, "select $1::int", 42).Scan(&value))
					require.Equal(t, 42, value)
					if clientMode == "verify-full" {
						for _, transport := range []string{"TLS", "plaintext"} {
							t.Run("cancel over "+transport, func(t *testing.T) {
								verifyTLSQueryCancellation(t, ctx, client, observer, transport)
							})
						}
					}
				})
			}
			if mode != "disabled" {
				clientConfig, err := pgx.ParseConfig(baseURL + "?sslmode=verify-full&sslrootcert=" + certFile)
				require.NoError(t, err)
				clientConfig.TLSConfig.ServerName = "wrong.example"
				_, err = pgx.ConnectConfig(ctx, clientConfig)
				require.ErrorContains(t, err, "certificate")
				clientConfig.TLSConfig.ServerName = "127.0.0.1"
				clientConfig.TLSConfig.RootCAs = x509.NewCertPool()
				_, err = pgx.ConnectConfig(ctx, clientConfig)
				require.ErrorContains(t, err, "certificate")
			}
		})
	}
}

func verifyTLSQueryCancellation(t *testing.T, ctx context.Context, client, observer *pgx.Conn, transport string) {
	t.Helper()
	require.IsType(t, &tls.Conn{}, client.PgConn().Conn())
	// Hold the backend for both the PID lookup and the query being canceled.
	tx, err := client.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(context.Background())
	var backendPID int
	require.NoError(t, tx.QueryRow(ctx, "select pg_backend_pid()").Scan(&backendPID))

	queryCtx, stopQuery := context.WithCancel(ctx)
	queryDone := make(chan error, 1)
	go func() {
		_, err := tx.Exec(queryCtx, "select pg_sleep(30)", pgx.QueryExecModeSimpleProtocol)
		queryDone <- err
	}()
	joined := false
	defer func() {
		stopQuery()
		if !joined {
			select {
			case <-queryDone:
			case <-time.After(5 * time.Second):
				t.Error("cancellation test query did not stop")
			}
		}
	}()
	require.Eventually(t, func() bool {
		var running bool
		err := observer.QueryRow(ctx, "select exists(select 1 from pg_stat_activity where pid = $1 and state = 'active' and query = 'select pg_sleep(30)')", backendPID).Scan(&running)
		return err == nil && running
	}, 5*time.Second, 10*time.Millisecond)

	// Use an explicit connection so the test controls whether cancellation is encrypted.
	cancelConn, err := (&net.Dialer{}).DialContext(ctx, "tcp", client.PgConn().Conn().RemoteAddr().String())
	require.NoError(t, err)
	defer func() { _ = cancelConn.Close() }()
	require.NoError(t, cancelConn.SetDeadline(time.Now().Add(5*time.Second)))
	if transport == "TLS" {
		frontend := pgproto3.NewFrontend(cancelConn, cancelConn)
		frontend.Send(&pgproto3.SSLRequest{})
		require.NoError(t, frontend.Flush())
		var reply [1]byte
		_, err := io.ReadFull(cancelConn, reply[:])
		require.NoError(t, err)
		require.Equal(t, byte('S'), reply[0])
		secured := tls.Client(cancelConn, client.Config().TLSConfig)
		require.NoError(t, secured.HandshakeContext(ctx))
		require.NotEmpty(t, secured.ConnectionState().VerifiedChains)
		cancelConn = secured
	}
	frontend := pgproto3.NewFrontend(cancelConn, cancelConn)
	frontend.Send(&pgproto3.CancelRequest{ProcessID: client.PgConn().PID(), SecretKey: client.PgConn().SecretKey()})
	require.NoError(t, frontend.Flush())
	response, err := io.ReadAll(cancelConn)
	require.NoError(t, err)
	require.Empty(t, response, "cancellation closes the separate connection without a response")

	select {
	case err := <-queryDone:
		joined = true
		var pgErr *pgconn.PgError
		require.ErrorAs(t, err, &pgErr)
		require.Equal(t, pgerrcode.QueryCanceled, pgErr.Code)
	case <-time.After(5 * time.Second):
		t.Fatal("cancel request did not stop the running PostgreSQL query")
	}
	require.NoError(t, tx.Rollback(ctx))
	var value int
	require.NoError(t, client.QueryRow(ctx, "select 1").Scan(&value))
	require.Equal(t, 1, value, "the TLS session remains usable after cancellation")
}
