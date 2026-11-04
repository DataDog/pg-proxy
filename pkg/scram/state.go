package scram

import (
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgproto3"
)

// clientNonceLen defines the length of cryptographic nonces used in SCRAM authentication.
// This follows PostgreSQL's implementation for compatibility.
const clientNonceLen = 18

// messageState maintains the state of a SCRAM authentication exchange.
//
// This struct tracks all messages exchanged during the SCRAM protocol,
// allowing for proper verification and response generation at each step.
type messageState struct {
	ClientFirstMessage *clientFirstMessage // Client's initial authentication message
	ServerFirstMessage *serverFirstMessage // Server's challenge message
	ClientFinalMessage *clientFinalMessage // Client's proof message
	ServerFinalMessage *serverFinalMessage // Server's verification message
}

// clientFirstMessage represents the initial message sent by the client.
//
// This corresponds to the "client-first-message" in RFC 5802, typically
// sent within a SASLInitialResponse PostgreSQL message.
//
// Format: n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL
// Where:
//   - First part is GS2 header:
//   - "n,," = no channel binding, no authzid
//   - "y,," = channel binding supported but not used, no authzid
//   - "p=tls-server-end-point,," = TLS channel binding, no authzid
//   - Second comma-separated value in header is authzid which starts with `a=`.
//     postgres forbids it so we check for it and error out if sent
//   - see:  https://github.com/postgres/postgres/blob/167ed8082f40ee1f3f4cd18cf02bd6d17df57dab/src/backend/libpq/auth-scram.c#L1069-L1082
//   - n=user specifies the authentication identity (ignored by postgres)
//   - see: https://github.com/postgres/postgres/blob/167ed8082f40ee1f3f4cd18cf02bd6d17df57dab/src/backend/libpq/auth-scram.c#L970-L975
//   - r=nonce is a random sequence of printable ASCII characters
type clientFirstMessage struct {
	bare               string // Exact transcript, including optional extensions.
	ChannelBindingType string // "n", "y", or "p=tls-server-end-point"
	AuthenticationID   string // Authentication identity (username, ignored and usually empty in postgres)
	Nonce              string // Client-generated random nonce
}

// ClientFirstMessageBare returns the bare client-first message without GS2 header.
//
// This is used in AuthMessage computation and should only include the
// essential authentication data: "n=user,r=nonce"
func (c *clientFirstMessage) ClientFirstMessageBare() string {
	if c.bare != "" {
		return c.bare
	}
	// The client-first-message-bare is used in AuthMessage computation
	// It should NOT include the GS2 header, just "n=user,r=nonce"
	// PostgreSQL clients typically send empty username: "n=,r=nonce"
	return "n=" + c.AuthenticationID + ",r=" + c.Nonce
}

// GS2Header returns the formatted GS2 header component of the message.
//
// Format: <channel binding type>,<authzid>, (i.e. "n,,", "y,,", "p=tls-server-end-point,,")
// Note: authzid is forbidden in PostgreSQL SCRAM authentication so we don't include it
func (c *clientFirstMessage) GS2Header() string {
	return c.ChannelBindingType + ",,"
}

// serverFirstMessage represents the server's challenge message.
//
// This corresponds to the "server-first-message" in RFC 5802, sent within
// an AuthenticationSASLContinue PostgreSQL message.
//
// Format: r=fyko+d2lbbFgONRv9qkxdawL,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096
// Where:
//   - r= contains the combined client+server nonce
//   - s= contains the base64-encoded salt
//   - i= contains the iteration count for PBKDF2
type serverFirstMessage struct {
	Nonce          string // Combined client and server nonce
	IterationCount int    // PBKDF2 iteration count (typically 4096)
	Salt           []byte // Salt for password hashing, base64-encoded only on the wire
}

// Encode returns the server-first-message as bytes for transmission.
func (s *serverFirstMessage) Encode() []byte {
	return []byte(s.String())
}

// String returns the formatted server-first-message string.
//
// The salt is held decoded and encoded here, at the one point it goes on the wire,
// so no other code has to decode it back — and nothing can fail to.
//
// Format: r=nonce,s=salt,i=iterations
func (s *serverFirstMessage) String() string {
	return "r=" + s.Nonce + ",s=" + string(encodeB64(s.Salt)) + ",i=" + fmt.Sprintf("%d", s.IterationCount)
}

// clientFinalMessage represents the client's proof message.
//
// This corresponds to the "client-final-message" in RFC 5802, sent within
// a SASLResponse PostgreSQL message.
//
// Format: c=biws,r=fyko+d2lbbFgONRv9qkxdawL,p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts=
// Where:
//   - c= contains base64-encoded channel binding data
//   - r= contains the same nonce from server-first-message
//   - p= contains the base64-encoded client proof
type clientFinalMessage struct {
	withoutProof   string // Exact transcript, including optional extensions.
	Nonce          string // Combined nonce (must match server-first-message)
	ClientProof    string // Base64-encoded cryptographic proof of password knowledge
	ChannelBinding string // Base64-encoded channel binding data
}

// ClientFinalMessageWithoutProof returns the client-final message without the proof.
//
// This is used in AuthMessage computation and includes channel binding and nonce
// but excludes the client proof: "c=biws,r=nonce"
//
// For PostgreSQL compatibility:
//   - "biws" = base64("n,,") for no channel binding
//   - "eSws" = base64("y,,") for channel binding not supported
func (c *clientFinalMessage) ClientFinalMessageWithoutProof() string {
	if c.withoutProof != "" {
		return c.withoutProof
	}
	// For PostgreSQL compatibility, channel binding should be base64 encoded
	// "biws" = base64("n,,") for no channel binding
	// "eSws" = base64("y,,") for channel binding not supported
	return "c=" + c.ChannelBinding + ",r=" + c.Nonce
}

// serverFinalMessage represents the server's verification message.
//
// This corresponds to the "server-final-message" in RFC 5802, sent within
// an AuthenticationSASLFinal PostgreSQL message.
//
// Format: v=rmF9pqV8S7suAoZWja4dJRkFsKQ=
// Where v= contains the base64-encoded server signature that proves
// the server knows the correct password without revealing it.
type serverFinalMessage struct {
	// ServerSignature is computed as HMAC(ServerKey, AuthMessage)
	// where ServerKey = HMAC(SaltedPassword, "Server Key")
	ServerSignature string // Base64-encoded server signature
}

// Encode returns the server-final-message as bytes for transmission.
func (s *serverFinalMessage) Encode() []byte {
	return []byte(s.String())
}

// String returns the formatted server-final-message string.
//
// Format: v=signature
func (s *serverFinalMessage) String() string {
	return "v=" + s.ServerSignature
}

// validateUsername validates that the client-provided username matches expectations.
//
// PostgreSQL SCRAM clients typically send empty username in SCRAM exchange
// since the username was already provided in the startup message.
func (s *messageState) validateUsername(expectedUsername string) error {
	name := strings.ReplaceAll(strings.ReplaceAll(s.ClientFirstMessage.AuthenticationID, "=2C", ","), "=3D", "=")
	if name != "" && name != expectedUsername {
		return fmt.Errorf("authentication identity does not match expected username")
	}
	return nil
}

// verifyAuthentication performs the complete SCRAM authentication verification.
//
// This includes nonce verification and client proof validation.
func (s *messageState) verifyAuthentication(password string) error {
	return s.verifyVerifier(deriveVerifier(password, s.ServerFirstMessage.Salt, s.ServerFirstMessage.IterationCount))
}

// setClientFirstMessage parses and stores the client's first message.
//
// This method extracts the authentication identity and nonce from the
// SASLInitialResponse message, handling the GS2 header format correctly.
//
// The message format is: [GS2 header] + client-first-message-bare
// Example: "n,,n=user,r=clientnonce" where "n,," is the GS2 header
func (s *messageState) setClientFirstMessage(m *pgproto3.SASLInitialResponse) error {
	data := string(m.Data)

	c := &clientFirstMessage{}

	// SCRAM client-first message format: [GS2 header] + client-first-message-bare
	// For SCRAM-SHA-256 without channel binding: "n,,n=user,r=clientnonce"
	// We need to parse the GS2 header and the bare message separately

	// Find the client-first-message-bare part (after the GS2 header)
	parts := strings.SplitN(data, ",", 3)
	if len(parts) < 3 {
		// The count, not the message: the client is unauthenticated here, so the
		// bytes it sent must not travel with the error.
		return fmt.Errorf("invalid SCRAM client-first message format: expected a GS2 header and a bare message, got %d comma-separated fields", len(parts))
	}

	// Parse GS2 header components
	c.ChannelBindingType = parts[0]
	authorizationId := parts[1]
	if authorizationId != "" {
		return fmt.Errorf("authorization identity (authzid) is not supported in PostgreSQL SCRAM authentication")
	}

	// Validate channel binding type
	// The GS2 header should be either "n,," for no channel binding or "y,," for channel binding supported
	// There is also "p=tls-server-end-point,," for TLS channel binding, but we don't support that yet (scram-sha-256-plus)
	// See: https://www.postgresql.org/docs/current/sasl-authentication.html#SASL-SCRAM-SHA-256
	switch {
	case c.ChannelBindingType == "n" || c.ChannelBindingType == "y":
		// These are supported
	case strings.HasPrefix(c.ChannelBindingType, "p=tls-server-end-point"):
		return fmt.Errorf("tls channel binding is not yet supported")
	default:
		// The requested type is client-supplied, so it is described rather than
		// quoted. It is one of three values when it is valid at all, which makes
		// the requirement the whole diagnostic.
		return fmt.Errorf("invalid GS2 header: must be either 'n,,' for no channel binding or 'y,,' for channel binding supported")
	}

	clientFirstMessageBare := parts[2]
	attributes, err := scramAttributes(clientFirstMessageBare)
	if err != nil || len(attributes) < 2 || !strings.HasPrefix(attributes[0], "n=") || !strings.HasPrefix(attributes[1], "r=") {
		return fmt.Errorf("invalid SCRAM client-first attributes")
	}
	c.bare = clientFirstMessageBare

	// Parse the client-first-message-bare: "n=user,r=clientnonce"
	for _, section := range strings.Split(clientFirstMessageBare, ",") {
		switch {
		case strings.HasPrefix(section, "r="):
			c.Nonce = strings.TrimPrefix(section, "r=")
		case strings.HasPrefix(section, "n="):
			c.AuthenticationID = strings.TrimPrefix(section, "n=")
		}
	}
	if !validNonce(c.Nonce) {
		return fmt.Errorf("invalid SCRAM nonce")
	}
	for name := c.AuthenticationID; strings.Contains(name, "="); {
		_, suffix, _ := strings.Cut(name, "=")
		if !strings.HasPrefix(suffix, "2C") && !strings.HasPrefix(suffix, "3D") {
			return fmt.Errorf("invalid SCRAM username encoding")
		}
		name = suffix[2:]
	}

	s.ClientFirstMessage = c
	return nil
}

// setServerFirstMessage generates and stores the server's first message.
//
// This method creates a server nonce, generates a random salt, combines
// the client and server nonces as required by the SCRAM specification,
// and prepares the server-first-message for transmission.
//
// Returns an error if the OS random number generator fails to produce
// cryptographically secure random bytes for the nonce or salt.
func (s *messageState) setServerFirstMessage() error {
	serverNonce, err := genSalt(clientNonceLen)
	if err != nil {
		return fmt.Errorf("error generating server nonce: %w", err)
	}
	salt, err := genSalt(clientNonceLen)
	if err != nil {
		return fmt.Errorf("error generating salt: %w", err)
	}

	// According to SCRAM spec, server appends its nonce to client nonce
	combinedNonce := s.ClientFirstMessage.Nonce + string(encodeB64(serverNonce))

	s.ServerFirstMessage = &serverFirstMessage{
		Nonce:          combinedNonce,
		IterationCount: 4096,
		Salt:           salt,
	}

	return nil
}

// setClientFinalMessage parses and stores the client's final message.
//
// This method extracts the nonce, channel binding, and client proof from
// the SASLResponse message for verification.
func (s *messageState) setClientFinalMessage(m *pgproto3.SASLResponse) error {
	data := string(m.Data)
	attributes, err := scramAttributes(data)
	if err != nil || len(attributes) < 3 || !strings.HasPrefix(attributes[0], "c=") || !strings.HasPrefix(attributes[1], "r=") || !strings.HasPrefix(attributes[len(attributes)-1], "p=") {
		return fmt.Errorf("invalid SCRAM client-final attributes")
	}

	c := &clientFinalMessage{}
	c.withoutProof = data[:strings.LastIndex(data, ",p=")]

	for _, section := range strings.Split(data, ",") {
		switch {
		case strings.HasPrefix(section, "r="):
			c.Nonce = strings.TrimPrefix(section, "r=")
		case strings.HasPrefix(section, "c="):
			c.ChannelBinding = strings.TrimPrefix(section, "c=")
		case strings.HasPrefix(section, "p="):
			c.ClientProof = strings.TrimPrefix(section, "p=")
		}
	}

	s.ClientFinalMessage = c
	return nil
}

func scramAttributes(data string) ([]string, error) {
	parts := strings.Split(data, ",")
	seen := make(map[byte]bool, len(parts))
	for _, part := range parts {
		if len(part) < 2 || part[1] != '=' || !(part[0] >= 'a' && part[0] <= 'z' || part[0] >= 'A' && part[0] <= 'Z') || part[0] == 'm' || seen[part[0]] {
			return nil, fmt.Errorf("invalid or unsupported SCRAM attribute")
		}
		seen[part[0]] = true
	}
	return parts, nil
}

func validNonce(nonce string) bool {
	if nonce == "" {
		return false
	}
	for i := range nonce {
		if nonce[i] < '!' || nonce[i] > '~' || nonce[i] == ',' {
			return false
		}
	}
	return true
}

// setServerFinalMessage generates and stores the server's final message.
//
// This method computes the server signature using the provided password
// and stores it in the server-final-message for transmission to the client.
func (s *messageState) setServerFinalMessage(password string) {
	s.ServerFinalMessage = &serverFinalMessage{
		ServerSignature: s.computeServerSignature(password),
	}
}

// computeClientProof computes the expected client proof for verification.
//
// This implements the SCRAM-SHA-256 client proof computation as defined in RFC
// 5802. The client proof is ClientKey XOR ClientSignature, where:
//   - ClientKey = HMAC(SaltedPassword, "Client Key")
//   - ClientSignature = HMAC(StoredKey, AuthMessage)
//   - StoredKey = H(ClientKey)
//
// The computed proof is compared against the client's submitted proof to
// verify the client knows the correct password.
//
// Returns the base64-encoded client proof that should match the client's submission.
func (s *messageState) computeClientProof(password string) string {
	clientKey := hmacSHA256(s.saltedPassword(password), []byte("Client Key"))
	storedKey := hash(string(clientKey))
	clientSignature := hmacSHA256(storedKey, []byte(s.authMessage()))

	clientProof := make([]byte, len(clientSignature))
	for i := 0; i < len(clientSignature); i++ {
		clientProof[i] = clientKey[i] ^ clientSignature[i]
	}

	return base64.StdEncoding.EncodeToString(clientProof)
}

// computeServerSignature computes the server signature for client verification.
//
// This implements the SCRAM-SHA-256 server signature computation as defined in
// RFC 5802. The signature proves to the client that the server knows the correct
// password without revealing it:
//
//	ServerSignature = HMAC(ServerKey, AuthMessage), ServerKey = HMAC(SaltedPassword, "Server Key")
//
// Returns the base64-encoded server signature for transmission to the client.
func (s *messageState) computeServerSignature(password string) string {
	serverKey := hmacSHA256(s.saltedPassword(password), []byte("Server Key"))
	serverSignature := hmacSHA256(serverKey, []byte(s.authMessage()))

	return base64.StdEncoding.EncodeToString(serverSignature)
}

// saltedPassword derives the key both proofs are built from, RFC 5802's
// SaltedPassword.
func (s *messageState) saltedPassword(password string) []byte {
	return hi([]byte(normalize(password)), s.ServerFirstMessage.Salt, s.ServerFirstMessage.IterationCount)
}

// authMessage returns RFC 5802's AuthMessage, the transcript both sides sign:
//
//	client-first-message-bare + "," + server-first-message + "," + client-final-message-without-proof
func (s *messageState) authMessage() string {
	return s.ClientFirstMessage.ClientFirstMessageBare() + "," +
		s.ServerFirstMessage.String() + "," +
		s.ClientFinalMessage.ClientFinalMessageWithoutProof()
}
