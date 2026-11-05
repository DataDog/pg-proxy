package scram

import (
	"encoding/base64"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/DataDog/pg-proxy/pkg/protocol"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
	xdgscram "github.com/xdg-go/scram"
)

func independentVerifier(t *testing.T, password string) string {
	t.Helper()
	client, err := xdgscram.SHA256.NewClientUnprepped("", password, "")
	require.NoError(t, err)
	stored := client.GetStoredCredentials(xdgscram.KeyFactors{Salt: "independent-salt", Iters: 8192})
	return fmt.Sprintf("SCRAM-SHA-256$%d:%s$%s:%s", stored.Iters, base64.StdEncoding.EncodeToString([]byte(stored.Salt)), base64.StdEncoding.EncodeToString(stored.StoredKey), base64.StdEncoding.EncodeToString(stored.ServerKey))
}

func TestStoredVerifierExchange(t *testing.T) {
	encoded := independentVerifier(t, "stored-password")
	verifier, err := ParseVerifier(encoded)
	require.NoError(t, err)
	require.Equal(t, encoded, verifier.Encode())
	for _, password := range []string{"stored-password", "wrong-password"} {
		t.Run(password, func(t *testing.T) {
			server, client := net.Pipe()
			t.Cleanup(func() { server.Close(); client.Close() })
			require.NoError(t, server.SetDeadline(time.Now().Add(3*time.Second)))
			require.NoError(t, client.SetDeadline(time.Now().Add(3*time.Second)))
			done := make(chan error, 1)
			go func() {
				defer server.Close()
				done <- AuthenticateVerifier(protocol.NewConnExchange(server, pgproto3.NewBackend(server, server)), verifier)
			}()
			_, clientErr := (&scramClient{frontend: pgproto3.NewFrontend(client, client), password: password, clientNon: "independent-client-nonce"}).run()
			serverErr := <-done
			if password == "stored-password" {
				require.NoError(t, clientErr)
				require.NoError(t, serverErr)
			} else {
				require.Error(t, clientErr)
				var failed *AuthFailedError
				require.ErrorAs(t, serverErr, &failed)
			}
		})
	}
}

func TestVerifierValidationAndRedaction(t *testing.T) {
	verifier, err := NewVerifier("private-password")
	require.NoError(t, err)
	_, err = ParseVerifier(verifier.Encode())
	require.NoError(t, err)
	for _, output := range []string{fmt.Sprint(verifier), fmt.Sprintf("%+v %#v", verifier, verifier), fmt.Sprintf("%+v %#v", *verifier, *verifier)} {
		require.NotContains(t, output, verifier.Encode())
		require.Contains(t, output, "[REDACTED]")
	}
	valid := independentVerifier(t, "private-password")
	oversizedSalt := "SCRAM-SHA-256$8192:" + base64.StdEncoding.EncodeToString(make([]byte, maxSaltSize+1)) + "$" + strings.Split(valid, "$")[2]
	for _, invalid := range []string{
		"private-password", strings.Repeat("x", maxVerifierSize+1),
		strings.Replace(valid, "8192:", "0:", 1), strings.Replace(valid, "8192:", "1000001:", 1),
		strings.Replace(valid, "8192:", "-1:", 1), strings.Replace(valid, "8192:", "99999999999999999999999:", 1),
		strings.Replace(valid, "SCRAM-SHA-256", "SCRAM-SHA-1", 1),
		"SCRAM-SHA-256$4096:$$", "SCRAM-SHA-256$4096:c2FsdA==$YQ==:YQ==",
		oversizedSalt,
	} {
		_, err := ParseVerifier(invalid)
		require.EqualError(t, err, "invalid SCRAM-SHA-256 verifier")
	}
	var missing *Verifier
	require.Error(t, missing.Validate())
	require.Error(t, (&Verifier{}).Validate())
	require.Error(t, AuthenticateVerifier(nil, nil))
}

func TestMockVerifier(t *testing.T) {
	key := [32]byte{1}
	for _, saltSize := range []int{1, 16, 18, maxSaltSize} {
		t.Run(fmt.Sprint(saltSize), func(t *testing.T) {
			encoded := independentVerifier(t, "password")
			parts := strings.Split(encoded, "$")
			parts[1] = "8192:" + base64.StdEncoding.EncodeToString(make([]byte, saltSize))
			template, err := ParseVerifier(strings.Join(parts, "$"))
			require.NoError(t, err)
			before := template.Encode()
			mock, err := NewMockVerifier("unknown", key, template)
			require.NoError(t, err)
			require.NoError(t, mock.Validate())
			require.Equal(t, template.iterations, mock.iterations)
			require.Len(t, mock.salt, saltSize)
			repeated, err := NewMockVerifier("unknown", key, template)
			require.NoError(t, err)
			require.Equal(t, mock.Encode(), repeated.Encode())
			other, err := NewMockVerifier("other", key, template)
			require.NoError(t, err)
			require.NotEqual(t, mock.storedKey, other.storedKey)
			require.NotEqual(t, mock.serverKey, other.serverKey)
			other, err = NewMockVerifier("unknown", [32]byte{2}, template)
			require.NoError(t, err)
			require.NotEqual(t, mock.Encode(), other.Encode())
			mock.salt[0] ^= 0xff
			mock.storedKey[0] ^= 0xff
			mock.serverKey[0] ^= 0xff
			require.Equal(t, before, template.Encode(), "fake credentials must not share template storage")
		})
	}
	_, err := NewMockVerifier("unknown", key, nil)
	require.Error(t, err)
	_, err = NewMockVerifier("unknown", key, &Verifier{})
	require.Error(t, err)
}

// Published RFC 7677 section 3 values. No client-proof generation uses server code.
const (
	rfcFirst     = "n,,n=user,r=rOprNGfwEbeRWgbNEkqO"
	rfcNonce     = "rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0"
	rfcProof     = "dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
	rfcSignature = "6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="
	rfcFinal     = "c=biws,r=" + rfcNonce + ",p=" + rfcProof
)

func rfc7677State(t *testing.T, final string) (*messageState, *Verifier) {
	t.Helper()
	salt, err := base64.StdEncoding.DecodeString("W22ZaJ0SNY7soEsUEjb6gQ==")
	require.NoError(t, err)
	state := &messageState{}
	require.NoError(t, state.setClientFirstMessage(&pgproto3.SASLInitialResponse{Data: []byte(rfcFirst)}))
	state.ServerFirstMessage = &serverFirstMessage{Nonce: rfcNonce, Salt: salt, IterationCount: 4096}
	require.NoError(t, state.setClientFinalMessage(&pgproto3.SASLResponse{Data: []byte(final)}))
	return state, deriveVerifier("pencil", salt, 4096)
}

func TestVerifierRejectsTamperedProofAndBinding(t *testing.T) {
	for _, tc := range []struct{ name, final, reason string }{
		{"wrong binding", strings.Replace(rfcFinal, "c=biws", "c=eSws", 1), "channel binding mismatch"},
		{"wrong nonce", strings.Replace(rfcFinal, rfcNonce, "other-nonce", 1), "nonce does not match"},
		{"short proof", strings.Replace(rfcFinal, rfcProof, base64.StdEncoding.EncodeToString(make([]byte, 31)), 1), "invalid client proof"},
		{"long proof", strings.Replace(rfcFinal, rfcProof, base64.StdEncoding.EncodeToString(make([]byte, 33)), 1), "invalid client proof"},
		{"bad base64", strings.Replace(rfcFinal, rfcProof, "malformed", 1), "invalid client proof"},
		{"wrong proof", strings.Replace(rfcFinal, rfcProof, base64.StdEncoding.EncodeToString(make([]byte, 32)), 1), "client proof mismatch"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state, verifier := rfc7677State(t, tc.final)
			require.ErrorContains(t, state.verifyVerifier(verifier), tc.reason)
		})
	}
}

func TestRFC7677Vector(t *testing.T) {
	// https://www.rfc-editor.org/rfc/rfc7677.html#section-3
	state, verifier := rfc7677State(t, rfcFinal)
	require.NoError(t, state.verifyVerifier(verifier))
	require.Equal(t, rfcSignature, state.serverSignature(verifier))
}

func TestVerifierSignsOptionalExtensions(t *testing.T) {
	state, verifier := rfc7677State(t, rfcFinal)
	require.NoError(t, state.setClientFirstMessage(&pgproto3.SASLInitialResponse{Data: []byte(rfcFirst + ",x=changed-transcript")}))
	require.ErrorContains(t, state.verifyVerifier(verifier), "proof mismatch")
	state, verifier = rfc7677State(t, strings.Replace(rfcFinal, ",p=", ",x=changed-transcript,p=", 1))
	require.ErrorContains(t, state.verifyVerifier(verifier), "proof mismatch")
}

func TestVerifierChallengeUsesStoredParameters(t *testing.T) {
	verifier, err := ParseVerifier(independentVerifier(t, "password"))
	require.NoError(t, err)
	state := &messageState{}
	require.NoError(t, state.setClientFirstMessage(&pgproto3.SASLInitialResponse{Data: []byte("n,,n=,r=client-nonce")}))
	require.NoError(t, state.setVerifierChallenge(verifier))
	firstNonce := state.ServerFirstMessage.Nonce
	require.True(t, strings.HasPrefix(firstNonce, "client-nonce"))
	require.Greater(t, len(firstNonce), len("client-nonce"))
	require.Equal(t, verifier.salt, state.ServerFirstMessage.Salt)
	require.Equal(t, 8192, state.ServerFirstMessage.IterationCount)
	require.NoError(t, state.setVerifierChallenge(verifier))
	require.NotEqual(t, firstNonce, state.ServerFirstMessage.Nonce)
}
