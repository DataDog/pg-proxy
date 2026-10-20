package scram

import (
	"fmt"

	"github.com/DataDog/pg-proxy/pkg/protocol"
	"github.com/jackc/pgx/v5/pgproto3"
)

// mechanism is the only SASL mechanism this package offers. Channel binding
// (SCRAM-SHA-256-PLUS) is not implemented.
const mechanism = "SCRAM-SHA-256"

// AuthFailedError reports that the client did not prove knowledge of the password.
//
// Its message says nothing about why, on purpose. The error reaches the client,
// and a client that can tell a rejected proof from a mismatched nonce learns
// something about the server it should not. Reason carries the detail, for the
// caller's logs — do not send it to the client.
//
// It means the credential was rejected, and only that: everything else
// Authenticate can fail on is a protocol or internal fault and is returned as an
// ordinary error, so a caller may treat this type as the authentication verdict.
type AuthFailedError struct {
	Reason error
}

func (e *AuthFailedError) Error() string {
	return "authentication failed: invalid credentials"
}

// Authenticate runs the PostgreSQL SCRAM-SHA-256 exchange with the client and
// reports whether it proved knowledge of password.
//
// The messages, in order:
//  1. AuthenticationSASL, advertising SCRAM-SHA-256
//  2. SASLInitialResponse, the client-first-message
//  3. AuthenticationSASLContinue, the server-first-message
//  4. SASLResponse, the client-final-message
//  5. AuthenticationSASLFinal, the server-final-message
//
// password is the one the client has to prove it knows; whether that credential
// exists and may be used here is the caller's decision, made before calling.
// username is only checked against the identity in the client-first-message,
// which PostgreSQL clients leave empty because the startup message already
// carried it.
//
// A failure to prove the password is returned as *AuthFailedError, whose message
// is safe to show the client. Every other error describes a protocol or internal
// fault and is not a statement about the credential.
func Authenticate(ex protocol.Exchange, username, password string) error {
	state := &messageState{}

	// Step 1: Inform the client that we support the SCRAM-SHA-256 SASL mechanism
	if err := ex.Send(&pgproto3.AuthenticationSASL{AuthMechanisms: []string{mechanism}}); err != nil {
		return fmt.Errorf("error sending AuthenticationSASL message to client: %v", err)
	}

	// Step 2: Receive the client-first-message
	encMsg, err := ex.Receive()
	if err != nil {
		return fmt.Errorf("error receiving SASLInitialResponse message from client: %v", err)
	}
	initialResponse, ok := encMsg.(*pgproto3.SASLInitialResponse)
	if !ok {
		// Only the type. The client is unauthenticated at this point and the error
		// reaches the logs, so the message contents must not: they would put
		// attacker-controlled text of arbitrary length there.
		return fmt.Errorf("expected client to send SASLInitialResponse request, but received: %T", encMsg)
	}
	if err := state.setClientFirstMessage(initialResponse); err != nil {
		return fmt.Errorf("error processing client first message: %v", err)
	}

	// Verify the username - PostgreSQL SCRAM clients send empty username in SCRAM exchange
	// since the username was already provided in the startup message
	if err := state.validateUsername(username); err != nil {
		return err
	}

	// Step 3: Send the server-first-message
	if err := state.setServerFirstMessage(); err != nil {
		return fmt.Errorf("error generating server first message: %w", err)
	}
	if err := ex.Send(&pgproto3.AuthenticationSASLContinue{Data: state.ServerFirstMessage.Encode()}); err != nil {
		return fmt.Errorf("error sending AuthenticationSASLContinue message to client: %v", err)
	}

	// Step 4: Receive the client-final-message
	encMsg, err = ex.Receive()
	if err != nil {
		return fmt.Errorf("error receiving SASLResponse message from client: %v", err)
	}
	response, ok := encMsg.(*pgproto3.SASLResponse)
	if !ok {
		return fmt.Errorf("expected client to send SASLResponse request, but received: %T", encMsg)
	}
	state.setClientFinalMessage(response)

	// Verify the SCRAM authentication
	if err := state.verifyAuthentication(password); err != nil {
		return &AuthFailedError{Reason: err}
	}

	// Step 5: Send the server-final-message
	state.setServerFinalMessage(password)
	if err := ex.Send(&pgproto3.AuthenticationSASLFinal{Data: state.ServerFinalMessage.Encode()}); err != nil {
		return fmt.Errorf("error sending AuthenticationSASLFinal message to client: %v", err)
	}

	return nil
}
