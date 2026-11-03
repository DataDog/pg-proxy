package scram

import (
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	xdgscram "github.com/xdg-go/scram"

	"github.com/DataDog/pg-proxy/pkg/protocol"
)

type disconnectExchange struct {
	beforeFinal bool
	received    bool
}

func (*disconnectExchange) Send(pgproto3.BackendMessage) error { return nil }
func (e *disconnectExchange) Receive() (pgproto3.FrontendMessage, error) {
	if e.beforeFinal && !e.received {
		e.received = true
		return &pgproto3.SASLInitialResponse{Data: []byte("n,,n=,r=nonce")}, nil
	}
	return nil, io.EOF
}

func TestAuthenticatePreservesDisconnect(t *testing.T) {
	for _, beforeFinal := range []bool{false, true} {
		t.Run(fmt.Sprintf("before_final=%t", beforeFinal), func(t *testing.T) {
			err := Authenticate(&disconnectExchange{beforeFinal: beforeFinal}, "user", "password")
			require.ErrorIs(t, err, io.EOF)
		})
	}
}

// scramClient uses an independent SCRAM implementation with explicit prepared
// password bytes, so neither client proofs nor server-signature checks share the
// server's normalization or cryptographic helpers.
type scramClient struct {
	frontend  *pgproto3.Frontend
	password  string
	clientNon string
}

func (c *scramClient) run() (pgproto3.BackendMessage, error) {
	client, err := xdgscram.SHA256.NewClientUnprepped("", c.password, "")
	if err != nil {
		return nil, err
	}
	client.WithNonceGenerator(func() string { return c.clientNon })
	conversation := client.NewConversation()
	msg, err := c.frontend.Receive()
	if err != nil {
		return nil, err
	}
	if _, ok := msg.(*pgproto3.AuthenticationSASL); !ok {
		return nil, fmt.Errorf("expected AuthenticationSASL, got %T", msg)
	}
	first, err := conversation.Step("")
	if err != nil {
		return nil, err
	}
	c.frontend.Send(&pgproto3.SASLInitialResponse{AuthMechanism: "SCRAM-SHA-256", Data: []byte(first)})
	if err := c.frontend.Flush(); err != nil {
		return nil, err
	}
	msg, err = c.frontend.Receive()
	if err != nil {
		return nil, err
	}
	cont, ok := msg.(*pgproto3.AuthenticationSASLContinue)
	if !ok {
		return nil, fmt.Errorf("expected AuthenticationSASLContinue, got %T", msg)
	}
	response, err := conversation.Step(string(cont.Data))
	if err != nil {
		return nil, err
	}
	c.frontend.Send(&pgproto3.SASLResponse{Data: []byte(response)})
	if err := c.frontend.Flush(); err != nil {
		return nil, err
	}
	msg, err = c.frontend.Receive()
	if err != nil {
		return nil, err
	}
	final, ok := msg.(*pgproto3.AuthenticationSASLFinal)
	if !ok {
		return nil, fmt.Errorf("expected AuthenticationSASLFinal, got %T", msg)
	}
	if _, err := conversation.Step(string(final.Data)); err != nil {
		return nil, err
	}
	if !conversation.Valid() {
		return nil, errors.New("server signature was not verified")
	}
	return final, nil
}

func parseServerFirst(data string) (nonce, salt string, iterations int, err error) {
	for _, section := range strings.Split(data, ",") {
		switch {
		case strings.HasPrefix(section, "r="):
			nonce = strings.TrimPrefix(section, "r=")
		case strings.HasPrefix(section, "s="):
			salt = strings.TrimPrefix(section, "s=")
		case strings.HasPrefix(section, "i="):
			if _, scanErr := fmt.Sscanf(section, "i=%d", &iterations); scanErr != nil {
				return "", "", 0, fmt.Errorf("parsing iteration count from %q: %w", section, scanErr)
			}
		}
	}
	if nonce == "" || salt == "" || iterations == 0 {
		return "", "", 0, fmt.Errorf("malformed server-first-message: %q", data)
	}
	return nonce, salt, iterations, nil
}

// startScramServer runs Authenticate on the server end of an in-memory pipe and
// returns the client-side frontend plus a channel carrying the server's result. It
// runs over a real ConnExchange, so the messages are genuinely encoded and decoded.
func startScramServer(t *testing.T, username, serverPassword string) (*pgproto3.Frontend, <-chan error, func()) {
	t.Helper()
	server, client := net.Pipe()
	server.SetDeadline(time.Now().Add(3 * time.Second))
	client.SetDeadline(time.Now().Add(3 * time.Second))

	errChan := make(chan error, 1)
	go func() {
		defer server.Close()
		ex := protocol.NewConnExchange(server, pgproto3.NewBackend(server, server))
		errChan <- Authenticate(ex, username, serverPassword)
	}()

	cleanup := func() {
		server.Close()
		client.Close()
	}
	return pgproto3.NewFrontend(client, client), errChan, cleanup
}

func TestAuthenticate(t *testing.T) {
	const username = "testuser"
	const password = "correct-horse-battery-staple"

	t.Run("valid credentials complete the handshake", func(t *testing.T) {
		frontend, errChan, cleanup := startScramServer(t, username, password)
		defer cleanup()

		client := &scramClient{frontend: frontend, password: password, clientNon: "Y2xpZW50bm9uY2VhYmNk"}
		final, err := client.run()
		require.NoError(t, err)
		_, ok := final.(*pgproto3.AuthenticationSASLFinal)
		assert.True(t, ok, "expected AuthenticationSASLFinal, got %T", final)

		assert.NoError(t, <-errChan, "server should report successful auth")
	})

	t.Run("wrong client proof fails verification", func(t *testing.T) {
		frontend, errChan, cleanup := startScramServer(t, username, password)
		defer cleanup()

		// Drive the client in the background: the server rejects the bad proof without sending
		// the final SASL message, so the client's last Receive would otherwise block until its
		// deadline. We only care about the server-side verification result.
		client := &scramClient{frontend: frontend, password: "wrong-password", clientNon: "Y2xpZW50bm9uY2VhYmNk"}
		go func() { _, _ = client.run() }()

		err := <-errChan

		// What reaches the client says only that authentication failed. Which step
		// failed, or how close the password was, is not something an
		// unauthenticated caller should be able to learn by asking.
		assert.EqualError(t, err, "authentication failed: invalid credentials")

		// The reason travels with the error, for the caller's logs.
		var authErr *AuthFailedError
		require.True(t, errors.As(err, &authErr), "expected an *AuthFailedError, got %T", err)
		assert.ErrorContains(t, authErr.Reason, "client proof mismatch")
	})

	t.Run("malformed client-first-message is rejected", func(t *testing.T) {
		frontend, errChan, cleanup := startScramServer(t, username, password)
		defer cleanup()

		// Receive the SASL advertisement, then send garbage that is not a valid
		// GS2-header-prefixed client-first-message.
		_, err := frontend.Receive()
		require.NoError(t, err)
		frontend.Send(&pgproto3.SASLInitialResponse{AuthMechanism: "SCRAM-SHA-256", Data: []byte("garbage")})
		require.NoError(t, frontend.Flush())

		err = <-errChan
		assert.ErrorContains(t, err, "invalid SCRAM client-first message format")
	})

	t.Run("unexpected message type is reported by type alone", func(t *testing.T) {
		frontend, errChan, cleanup := startScramServer(t, username, password)
		defer cleanup()

		_, err := frontend.Receive()
		require.NoError(t, err)

		// A Query where the client-first-message belongs. The type belongs in the
		// error; the client's SQL does not, since the error is logged while the
		// client is still unauthenticated.
		frontend.Send(&pgproto3.Query{String: "select 'client controlled'"})
		require.NoError(t, frontend.Flush())

		err = <-errChan
		assert.ErrorContains(t, err, "expected client to send SASLInitialResponse request")
		assert.ErrorContains(t, err, "*pgproto3.Query")
		assert.NotContains(t, err.Error(), "client controlled")
	})

	t.Run("mismatched authentication identity is rejected", func(t *testing.T) {
		frontend, errChan, cleanup := startScramServer(t, username, password)
		defer cleanup()

		_, err := frontend.Receive()
		require.NoError(t, err)

		// A client that names an identity has to name the one being authenticated;
		// PostgreSQL clients leave it empty.
		frontend.Send(&pgproto3.SASLInitialResponse{
			AuthMechanism: "SCRAM-SHA-256",
			Data:          []byte("n,,n=someone-else,r=Y2xpZW50bm9uY2VhYmNk"),
		})
		require.NoError(t, frontend.Flush())

		err = <-errChan
		assert.ErrorContains(t, err, "does not match expected username")
	})
}

func TestAuthenticatePasswordNormalization(t *testing.T) {
	for _, tt := range passwordVectors {
		t.Run(tt.name, func(t *testing.T) {
			frontend, result, cleanup := startScramServer(t, "testuser", tt.password)
			defer cleanup()
			client := &scramClient{frontend: frontend, password: tt.prepared, clientNon: "independent-client-nonce"}
			_, err := client.run()
			require.NoError(t, err)
			require.NoError(t, <-result)
		})
	}

	for _, tt := range []struct{ name, password, rejected string }{
		{"stripped Unicode", "abc密码", "abc"},
		{"empty against Unicode", "密码", ""},
		{"empty against mapped-to-empty", "\u00ad", ""},
		{"stripped prohibited", "a\u00ad\a", "a"},
		{"stripped invalid UTF-8", "a\u00ad\xff", "a"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			frontend, result, cleanup := startScramServer(t, "testuser", tt.password)
			defer cleanup()
			client := &scramClient{frontend: frontend, password: tt.rejected, clientNon: "independent-client-nonce"}
			_, err := client.run()
			require.Error(t, err)
			var authErr *AuthFailedError
			require.ErrorAs(t, <-result, &authErr)
			assert.ErrorContains(t, authErr.Reason, "client proof mismatch")
		})
	}
}
