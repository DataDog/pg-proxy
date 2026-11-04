package scram

import (
	"crypto/hmac"
	"encoding/base64"
	"errors"
	"strconv"
	"strings"
)

const (
	DefaultIterations = 4096
	MaxIterations     = 1_000_000
	maxSaltSize       = 1024
	maxVerifierSize   = 4096
)

// Verifier holds PostgreSQL SCRAM-SHA-256 stored credentials. Its fields are
// private so a parsed verifier can be shared safely by concurrent exchanges.
type Verifier struct {
	iterations           int
	salt                 []byte
	storedKey, serverKey []byte
}

func (Verifier) String() string   { return "[REDACTED]" }
func (Verifier) GoString() string { return "[REDACTED]" }

// Validate rejects a nil or zero verifier before a listener is opened.
func (v *Verifier) Validate() error {
	if v == nil || v.iterations < 1 || v.iterations > MaxIterations || len(v.salt) == 0 || len(v.salt) > maxSaltSize || len(v.storedKey) != 32 || len(v.serverKey) != 32 {
		return errors.New("invalid SCRAM-SHA-256 verifier")
	}
	return nil
}

// ParseVerifier accepts PostgreSQL's stored-verifier format. Errors never include
// credential material. Salt and iteration bounds apply before any key derivation.
func ParseVerifier(value string) (*Verifier, error) {
	invalid := errors.New("invalid SCRAM-SHA-256 verifier")
	if len(value) > maxVerifierSize {
		return nil, invalid
	}
	parts := strings.Split(value, "$")
	if len(parts) != 3 || parts[0] != mechanism {
		return nil, invalid
	}
	iterationText, saltText, ok := strings.Cut(parts[1], ":")
	if !ok {
		return nil, invalid
	}
	iterations, err := strconv.Atoi(iterationText)
	if err != nil || iterations < 1 || iterations > MaxIterations {
		return nil, invalid
	}
	salt, err := base64.StdEncoding.Strict().DecodeString(saltText)
	if err != nil || len(salt) == 0 || len(salt) > maxSaltSize {
		return nil, invalid
	}
	storedText, serverText, ok := strings.Cut(parts[2], ":")
	if !ok {
		return nil, invalid
	}
	stored, err := base64.StdEncoding.Strict().DecodeString(storedText)
	if err != nil || len(stored) != 32 {
		return nil, invalid
	}
	server, err := base64.StdEncoding.Strict().DecodeString(serverText)
	if err != nil || len(server) != 32 {
		return nil, invalid
	}
	return &Verifier{iterations: iterations, salt: salt, storedKey: stored, serverKey: server}, nil
}

// NewVerifier derives stored credentials using PostgreSQL password preparation,
// a fresh random salt, and DefaultIterations. The plaintext need not be retained.
func NewVerifier(password string) (*Verifier, error) {
	salt, err := genSalt(clientNonceLen)
	if err != nil {
		return nil, errors.New("could not generate SCRAM salt")
	}
	return deriveVerifier(password, salt, DefaultIterations), nil
}

func deriveVerifier(password string, salt []byte, iterations int) *Verifier {
	salted := hi([]byte(normalize(password)), salt, iterations)
	clientKey := hmacSHA256(salted, []byte("Client Key"))
	return &Verifier{iterations: iterations, salt: salt,
		storedKey: hash(string(clientKey)), serverKey: hmacSHA256(salted, []byte("Server Key"))}
}

// Encode returns sensitive credential material for explicit storage. Ordinary
// formatting redacts the verifier; do not log the result of Encode.
func (v *Verifier) Encode() string {
	return mechanism + "$" + strconv.Itoa(v.iterations) + ":" + base64.StdEncoding.EncodeToString(v.salt) + "$" +
		base64.StdEncoding.EncodeToString(v.storedKey) + ":" + base64.StdEncoding.EncodeToString(v.serverKey)
}

func (s *messageState) verifyVerifier(v *Verifier) error {
	if s.ClientFinalMessage.Nonce != s.ServerFirstMessage.Nonce {
		return errors.New("client nonce does not match server nonce")
	}
	bound, err := base64.StdEncoding.Strict().DecodeString(s.ClientFinalMessage.ChannelBinding)
	if err != nil || string(bound) != s.ClientFirstMessage.GS2Header() {
		return errors.New("channel binding mismatch")
	}
	proof, err := base64.StdEncoding.Strict().DecodeString(s.ClientFinalMessage.ClientProof)
	if err != nil || len(proof) != 32 {
		return errors.New("invalid client proof")
	}
	signature := hmacSHA256(v.storedKey, []byte(s.authMessage()))
	for i := range proof {
		proof[i] ^= signature[i]
	}
	if !hmac.Equal(hash(string(proof)), v.storedKey) {
		return errors.New("client proof mismatch")
	}
	return nil
}

func (s *messageState) setVerifierChallenge(v *Verifier) error {
	nonce, err := genSalt(clientNonceLen)
	if err != nil {
		return errors.New("could not generate SCRAM nonce")
	}
	s.ServerFirstMessage = &serverFirstMessage{Nonce: s.ClientFirstMessage.Nonce + string(encodeB64(nonce)), Salt: v.salt, IterationCount: v.iterations}
	return nil
}
