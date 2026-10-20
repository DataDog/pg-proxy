package scram

import (
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/DataDog/pg-proxy/pkg/protocol"
)

// scramClient drives the client half of the PostgreSQL SCRAM-SHA-256 SASL exchange
// over a pgproto3 frontend. It is a minimal, self-contained client used only to
// behavior-test Authenticate: it speaks the real wire protocol and computes the
// client proof from the password it is given (which may intentionally differ from the
// server's expected password to exercise the failure path).
type scramClient struct {
	frontend  *pgproto3.Frontend
	password  string
	clientNon string
}

// run performs the full client side of the handshake and returns the final message the
// server sent (an *AuthenticationSASLFinal on success), or an error if the exchange
// could not be completed from the client's perspective.
func (c *scramClient) run() (pgproto3.BackendMessage, error) {
	// 1. Server advertises SASL mechanisms.
	msg, err := c.frontend.Receive()
	if err != nil {
		return nil, fmt.Errorf("receiving AuthenticationSASL: %w", err)
	}
	if _, ok := msg.(*pgproto3.AuthenticationSASL); !ok {
		return nil, fmt.Errorf("expected AuthenticationSASL, got %T", msg)
	}

	// 2. Client sends client-first-message. PostgreSQL clients send an empty username
	//    in the SCRAM exchange (it was already provided in the startup message).
	clientFirstBare := "n=,r=" + c.clientNon
	c.frontend.Send(&pgproto3.SASLInitialResponse{
		AuthMechanism: "SCRAM-SHA-256",
		Data:          []byte("n,," + clientFirstBare),
	})
	if err := c.frontend.Flush(); err != nil {
		return nil, fmt.Errorf("sending SASLInitialResponse: %w", err)
	}

	// 3. Server replies with server-first-message (combined nonce, salt, iterations).
	msg, err = c.frontend.Receive()
	if err != nil {
		return nil, fmt.Errorf("receiving AuthenticationSASLContinue: %w", err)
	}
	cont, ok := msg.(*pgproto3.AuthenticationSASLContinue)
	if !ok {
		return nil, fmt.Errorf("expected AuthenticationSASLContinue, got %T", msg)
	}
	serverFirst := string(cont.Data)
	combinedNonce, salt, iterations, err := parseServerFirst(serverFirst)
	if err != nil {
		return nil, err
	}

	// 4. Client computes the proof and sends client-final-message.
	clientFinalWithoutProof := "c=biws,r=" + combinedNonce
	authMessage := clientFirstBare + "," + serverFirst + "," + clientFinalWithoutProof
	proof, err := clientProof(c.password, salt, iterations, authMessage)
	if err != nil {
		return nil, err
	}
	c.frontend.Send(&pgproto3.SASLResponse{
		Data: []byte(clientFinalWithoutProof + ",p=" + proof),
	})
	if err := c.frontend.Flush(); err != nil {
		return nil, fmt.Errorf("sending SASLResponse: %w", err)
	}

	// 5. Server sends server-final-message on success.
	return c.frontend.Receive()
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

// clientProof computes the SCRAM-SHA-256 client proof the way a real Postgres client
// would, from this package's primitives. Sharing them is a known weakness of this
// test — an error common to both sides cancels out — which is why the app's
// listener test drives the same exchange with pgx's independent implementation. What
// this covers is the conversation and the state machine.
func clientProof(password, b64Salt string, iterations int, authMessage string) (string, error) {
	salt, err := base64.StdEncoding.DecodeString(b64Salt)
	if err != nil {
		return "", fmt.Errorf("decoding salt: %w", err)
	}
	saltedPassword := hi([]byte(normalize(password)), salt, iterations)
	clientKey := hmacSHA256(saltedPassword, []byte("Client Key"))
	storedKey := hash(string(clientKey))
	clientSignature := hmacSHA256(storedKey, []byte(authMessage))
	proof := make([]byte, len(clientSignature))
	for i := range clientSignature {
		proof[i] = clientKey[i] ^ clientSignature[i]
	}
	return base64.StdEncoding.EncodeToString(proof), nil
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
