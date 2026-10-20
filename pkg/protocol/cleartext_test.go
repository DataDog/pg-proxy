package protocol

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newPipe returns a ConnExchange over one end of an in-memory pipe, so the tests
// exercise the real adapter rather than a stand-in.
func newPipe(conn net.Conn) ConnExchange {
	return NewConnExchange(conn, pgproto3.NewBackend(conn, conn))
}

// failingExchange reports a send error without needing a connection.
type failingExchange struct{ sendErr error }

func (e failingExchange) Send(pgproto3.BackendMessage) error { return e.sendErr }

func (e failingExchange) Receive() (pgproto3.FrontendMessage, error) {
	return nil, errors.New("Receive should not be reached after Send fails")
}

// TestGetCleartextPassword exercises the cleartext password exchange against a real
// pgproto3 client over an in-memory pipe. It is a behavior test: it asserts on the
// protocol messages observed on the wire and the returned value, not on internals.
func TestGetCleartextPassword(t *testing.T) {
	t.Run("happy path returns the client-provided password", func(t *testing.T) {
		server, client := net.Pipe()
		defer server.Close()
		defer client.Close()
		server.SetDeadline(time.Now().Add(2 * time.Second))
		client.SetDeadline(time.Now().Add(2 * time.Second))

		resultChan := make(chan struct {
			pw  string
			err error
		}, 1)
		go func() {
			pw, err := GetCleartextPassword(newPipe(server))
			resultChan <- struct {
				pw  string
				err error
			}{pw, err}
		}()

		frontend := pgproto3.NewFrontend(client, client)

		// The server should first ask for a cleartext password.
		msg, err := frontend.Receive()
		require.NoError(t, err)
		_, ok := msg.(*pgproto3.AuthenticationCleartextPassword)
		assert.True(t, ok, "expected AuthenticationCleartextPassword, got %#v", msg)

		// The client answers with the password.
		frontend.Send(&pgproto3.PasswordMessage{Password: "hunter2"})
		require.NoError(t, frontend.Flush())

		got := <-resultChan
		assert.NoError(t, got.err)
		assert.Equal(t, "hunter2", got.pw)
	})

	t.Run("unexpected message type is rejected", func(t *testing.T) {
		server, client := net.Pipe()
		defer server.Close()
		defer client.Close()
		server.SetDeadline(time.Now().Add(2 * time.Second))
		client.SetDeadline(time.Now().Add(2 * time.Second))

		errChan := make(chan error, 1)
		go func() {
			_, err := GetCleartextPassword(newPipe(server))
			errChan <- err
		}()

		frontend := pgproto3.NewFrontend(client, client)
		_, err := frontend.Receive()
		require.NoError(t, err)

		// Send a Query instead of a PasswordMessage: the proxy must not accept it as a password.
		frontend.Send(&pgproto3.Query{String: "select 1"})
		require.NoError(t, frontend.Flush())

		err = <-errChan
		assert.ErrorContains(t, err, "expected password message request")
		// The type is named, so the failure is diagnosable, but the contents are
		// not: the client is unauthenticated here and this error is logged, so it
		// must not carry anything the client chose.
		assert.ErrorContains(t, err, "*pgproto3.Query")
		assert.NotContains(t, err.Error(), "select 1")
	})

	t.Run("connection closed before reply surfaces a receive error", func(t *testing.T) {
		server, client := net.Pipe()
		server.SetDeadline(time.Now().Add(2 * time.Second))

		errChan := make(chan error, 1)
		go func() {
			_, err := GetCleartextPassword(newPipe(server))
			errChan <- err
		}()

		frontend := pgproto3.NewFrontend(client, client)
		_, err := frontend.Receive()
		require.NoError(t, err)

		// Client disconnects without sending a password.
		client.Close()

		err = <-errChan
		// The auth-request write may succeed, so the failure surfaces while receiving the password.
		assert.ErrorContains(t, err, "error receiving password message")
		server.Close()
	})

	// Not reachable through a pipe: only a stub can fail the send.
	t.Run("send failure is reported and stops the exchange", func(t *testing.T) {
		_, err := GetCleartextPassword(failingExchange{sendErr: errors.New("broken pipe")})

		assert.ErrorContains(t, err, "error sending message to get password")
		assert.ErrorContains(t, err, "broken pipe")
	})
}
