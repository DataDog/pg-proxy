// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package protocol

import (
	"fmt"
	"net"

	"github.com/jackc/pgx/v5/pgproto3"
)

// Exchange sends messages to a client and reads the client's replies.
//
// Authentication is a conversation rather than a single check, so driving it
// through an interface keeps a mechanism free of sockets and wire encoding and
// lets it be tested by scripting messages instead of opening a connection.
type Exchange interface {
	// Send delivers a message to the client.
	Send(msg pgproto3.BackendMessage) error

	// Receive blocks until the client sends a message.
	Receive() (pgproto3.FrontendMessage, error)
}

// ConnExchange is the Exchange for a live client connection.
//
// Outbound messages are encoded and written straight to the connection, not
// through the backend's buffered Send and Flush. Inbound messages are read
// through the backend, which decodes them — so Send also keeps it told which
// authentication request is outstanding. See authTypeFor.
type ConnExchange struct {
	conn    net.Conn
	backend *pgproto3.Backend
}

// Fails the build here, rather than at a call site, if ConnExchange stops
// satisfying Exchange.
var _ Exchange = ConnExchange{}

// NewConnExchange returns an Exchange over conn, decoding replies with backend.
// Both must refer to the same client connection.
func NewConnExchange(conn net.Conn, backend *pgproto3.Backend) ConnExchange {
	return ConnExchange{conn: conn, backend: backend}
}

func (e ConnExchange) Send(msg pgproto3.BackendMessage) error {
	buf, err := msg.Encode(nil)
	if err != nil {
		return fmt.Errorf("error encoding %T: %w", msg, err)
	}

	// Set before writing, so a failure here sends nothing. Writing first would
	// leave the backend on the wrong auth type, and it would then misdecode the
	// client's reply.
	if authType, ok := authTypeFor(msg); ok {
		if err := e.backend.SetAuthType(authType); err != nil {
			return fmt.Errorf("error setting auth type for %T: %w", msg, err)
		}
	}

	_, err = e.conn.Write(buf)
	return err
}

// authTypeFor returns the auth type the backend needs in order to decode the
// reply to msg, and whether msg is an authentication request at all.
//
// PostgreSQL sends every kind of password reply with the same type byte, 'p', so
// the backend can only tell a SASL reply from a plain password by knowing which
// request it answers. Send writes to the connection itself, so the backend never
// sees the request and has to be told.
func authTypeFor(msg pgproto3.BackendMessage) (uint32, bool) {
	switch msg.(type) {
	case *pgproto3.AuthenticationCleartextPassword:
		return pgproto3.AuthTypeCleartextPassword, true
	case *pgproto3.AuthenticationSASL:
		return pgproto3.AuthTypeSASL, true
	case *pgproto3.AuthenticationSASLContinue:
		return pgproto3.AuthTypeSASLContinue, true
	case *pgproto3.AuthenticationSASLFinal:
		// Nothing is read after this one, but keep the state right anyway.
		return pgproto3.AuthTypeSASLFinal, true
	case *pgproto3.AuthenticationGSS:
		return pgproto3.AuthTypeGSS, true
	case *pgproto3.AuthenticationGSSContinue:
		return pgproto3.AuthTypeGSSCont, true
	default:
		// Also covers AuthTypeSSPI: pgproto3 decodes its reply as a PasswordMessage
		// too, same as the zero value, so it needs no case here.
		return 0, false
	}
}

func (e ConnExchange) Receive() (pgproto3.FrontendMessage, error) {
	return e.backend.Receive()
}
