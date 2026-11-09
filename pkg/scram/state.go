// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package scram

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgproto3"
)

const clientNonceLen = 18

// messageState holds the three messages covered by the SCRAM proof.
// Client message bytes are retained exactly, including optional extensions.
type messageState struct {
	ClientFirstMessage *clientFirstMessage
	ServerFirstMessage *serverFirstMessage
	ClientFinalMessage *clientFinalMessage
}

type clientFirstMessage struct {
	bare               string // Client-first message without the GS2 header.
	ChannelBindingType string // "n" or "y"; channel binding is not supported.
	Nonce              string
}

func (c *clientFirstMessage) GS2Header() string {
	return c.ChannelBindingType + ",,"
}

type serverFirstMessage struct {
	Nonce          string // Client nonce followed by a fresh server nonce.
	IterationCount int
	Salt           []byte
}

func (s *serverFirstMessage) Encode() []byte { return []byte(s.String()) }

func (s *serverFirstMessage) String() string {
	return fmt.Sprintf("r=%s,s=%s,i=%d", s.Nonce, encodeB64(s.Salt), s.IterationCount)
}

type clientFinalMessage struct {
	withoutProof   string // Exact client-final message excluding its last p= field.
	Nonce          string
	ClientProof    string
	ChannelBinding string
}

// setClientFirstMessage parses "n,,n=user,r=nonce" (or "y,,...").
// PostgreSQL uses the startup username to select credentials; the SCRAM username
// is checked for valid encoding but does not select an account.
func (s *messageState) setClientFirstMessage(m *pgproto3.SASLInitialResponse) error {
	parts := strings.SplitN(string(m.Data), ",", 3)
	if len(parts) != 3 {
		return fmt.Errorf("invalid SCRAM client-first message format: expected a GS2 header and a bare message, got %d comma-separated fields", len(parts))
	}
	if parts[1] != "" {
		return fmt.Errorf("authorization identity (authzid) is not supported in PostgreSQL SCRAM authentication")
	}
	switch parts[0] {
	case "n", "y":
	default:
		if strings.HasPrefix(parts[0], "p=tls-server-end-point") {
			return fmt.Errorf("tls channel binding is not yet supported")
		}
		return fmt.Errorf("invalid GS2 header: must be either 'n,,' for no channel binding or 'y,,' for channel binding supported")
	}

	attributes, err := scramAttributes(parts[2])
	if err != nil || len(attributes) < 2 || !strings.HasPrefix(attributes[0], "n=") || !strings.HasPrefix(attributes[1], "r=") {
		return fmt.Errorf("invalid SCRAM client-first attributes")
	}
	if !validNonce(attributes[1][2:]) {
		return fmt.Errorf("invalid SCRAM nonce")
	}
	if !validUsernameEncoding(attributes[0][2:]) {
		return fmt.Errorf("invalid SCRAM username encoding")
	}
	s.ClientFirstMessage = &clientFirstMessage{
		bare: parts[2], ChannelBindingType: parts[0], Nonce: attributes[1][2:],
	}
	return nil
}

// setClientFinalMessage parses "c=binding,r=nonce,...,p=proof". Keeping the
// original bytes prevents optional fields from being lost from the signed data.
func (s *messageState) setClientFinalMessage(m *pgproto3.SASLResponse) error {
	data := string(m.Data)
	attributes, err := scramAttributes(data)
	if err != nil || len(attributes) < 3 || !strings.HasPrefix(attributes[0], "c=") || !strings.HasPrefix(attributes[1], "r=") || !strings.HasPrefix(attributes[len(attributes)-1], "p=") {
		return fmt.Errorf("invalid SCRAM client-final attributes")
	}
	s.ClientFinalMessage = &clientFinalMessage{
		withoutProof:   data[:strings.LastIndex(data, ",p=")],
		ChannelBinding: attributes[0][2:], Nonce: attributes[1][2:],
		ClientProof: attributes[len(attributes)-1][2:],
	}
	return nil
}

// scramAttributes rejects duplicates and mandatory extensions we do not support.
func scramAttributes(data string) ([]string, error) {
	parts := strings.Split(data, ",")
	seen := make(map[byte]bool, len(parts))
	for _, part := range parts {
		if len(part) < 2 || part[1] != '=' {
			return nil, fmt.Errorf("invalid or unsupported SCRAM attribute")
		}
		key := part[0]
		letter := key >= 'a' && key <= 'z' || key >= 'A' && key <= 'Z'
		if !letter || key == 'm' || seen[key] {
			return nil, fmt.Errorf("invalid or unsupported SCRAM attribute")
		}
		seen[key] = true
	}
	return parts, nil
}

func validUsernameEncoding(name string) bool {
	for strings.Contains(name, "=") {
		_, suffix, _ := strings.Cut(name, "=")
		if !strings.HasPrefix(suffix, "2C") && !strings.HasPrefix(suffix, "3D") {
			return false
		}
		name = suffix[2:]
	}
	return true
}

func validNonce(nonce string) bool {
	if nonce == "" {
		return false
	}
	for i := 0; i < len(nonce); i++ {
		if nonce[i] < '!' || nonce[i] > '~' || nonce[i] == ',' {
			return false
		}
	}
	return true
}

// authMessage is the exact transcript both peers sign (RFC 5802).
func (s *messageState) authMessage() string {
	return s.ClientFirstMessage.bare + "," + s.ServerFirstMessage.String() + "," + s.ClientFinalMessage.withoutProof
}
