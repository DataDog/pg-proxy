package scram

import (
	"encoding/base64"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
)

// Test vectors from RFC 5802 examples adapted for SCRAM-SHA-256
const (
	testPassword      = "pencil"
	testSalt          = "W22ZaJ0SNY7soEsUEjb6gQ==" // base64, as it appears on the wire
	testIterations    = 4096
	testClientNonce   = "fyko+d2lbbFgONRv9qkxdawL"
	testServerNonce   = "3rfcNHYJY1ZVvWVs7j"
	testCombinedNonce = testClientNonce + testServerNonce
)

// testSaltBytes is testSalt as the state holds it: decoded, since base64 is only a
// wire encoding.
var testSaltBytes = func() []byte {
	salt, err := base64.StdEncoding.DecodeString(testSalt)
	if err != nil {
		panic("test salt is not valid base64: " + err.Error())
	}
	return salt
}()

func TestMessageState_setClientFirstMessage(t *testing.T) {
	tests := []struct {
		name             string
		input            string
		expectedAuth     string
		expectedNonce    string
		expectedChanBind string
		expectError      bool
		expectedError    string
	}{
		{
			name:             "SCRAM-SHA-256 standard with no channel binding",
			input:            "n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL",
			expectedAuth:     "user",
			expectedNonce:    "fyko+d2lbbFgONRv9qkxdawL",
			expectedChanBind: "n",
			expectError:      false,
		},
		{
			name:             "SCRAM-SHA-256 empty username (PostgreSQL style)",
			input:            "n,,n=,r=clientnonce123",
			expectedAuth:     "",
			expectedNonce:    "clientnonce123",
			expectedChanBind: "n",
			expectError:      false,
		},
		{
			name:             "SCRAM-SHA-256 with complex nonce",
			input:            "n,,n=testuser,r=AbCdEf123456!@#$%^",
			expectedAuth:     "testuser",
			expectedNonce:    "AbCdEf123456!@#$%^",
			expectedChanBind: "n",
			expectError:      false,
		},
		{
			name:             "SCRAM-SHA-256 with channel binding supported but not used",
			input:            "y,,n=user,r=nonce",
			expectedAuth:     "user",
			expectedNonce:    "nonce",
			expectedChanBind: "y",
			expectError:      false,
		},
		{
			name:          "Reject any authzid value",
			input:         "n,somedata,n=user,r=nonce",
			expectError:   true,
			expectedError: "authorization identity (authzid) is not supported in PostgreSQL SCRAM authentication",
		},
		{
			name:          "Reject authzid with a= prefix",
			input:         "n,a=admin,n=user,r=nonce",
			expectError:   true,
			expectedError: "authorization identity (authzid) is not supported in PostgreSQL SCRAM authentication",
		},
		{
			name:          "SCRAM-SHA-256-PLUS (TLS channel binding) not supported",
			input:         "p=tls-server-end-point,,n=user,r=nonce",
			expectError:   true,
			expectedError: "tls channel binding is not yet supported",
		},
		{
			name:          "Invalid channel binding type",
			input:         "x,,n=user,r=nonce",
			expectError:   true,
			expectedError: "invalid GS2 header: must be either 'n,,' for no channel binding or 'y,,' for channel binding supported",
		},
		{
			name:          "Invalid message format",
			input:         "invalid",
			expectError:   true,
			expectedError: "invalid SCRAM client-first message format",
		},
		{
			name:          "Missing message parts",
			input:         "n,",
			expectError:   true,
			expectedError: "invalid SCRAM client-first message format",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sms := &messageState{}
			msg := &pgproto3.SASLInitialResponse{
				Data: []byte(tt.input),
			}

			err := sms.setClientFirstMessage(msg)

			if tt.expectError {
				assert.Error(t, err)
				if tt.expectedError != "" {
					assert.Contains(t, err.Error(), tt.expectedError)
				}
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expectedAuth, sms.ClientFirstMessage.AuthenticationID)
			assert.Equal(t, tt.expectedNonce, sms.ClientFirstMessage.Nonce)
			assert.Equal(t, tt.expectedChanBind, sms.ClientFirstMessage.ChannelBindingType)
		})
	}
}

func TestMessageState_setClientFinalMessage(t *testing.T) {
	tests := []struct {
		name             string
		input            string
		expectedNonce    string
		expectedProof    string
		expectedChanBind string
	}{
		{
			name:             "Standard client final message",
			input:            "c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=",
			expectedNonce:    "fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j",
			expectedProof:    "dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=",
			expectedChanBind: "biws",
		},
		{
			name:             "Alternative channel binding",
			input:            "c=eSws,r=testnonce,p=someproof123",
			expectedNonce:    "testnonce",
			expectedProof:    "someproof123",
			expectedChanBind: "eSws",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sms := &messageState{}
			msg := &pgproto3.SASLResponse{
				Data: []byte(tt.input),
			}

			sms.setClientFinalMessage(msg)

			assert.Equal(t, tt.expectedNonce, sms.ClientFinalMessage.Nonce)
			assert.Equal(t, tt.expectedProof, sms.ClientFinalMessage.ClientProof)
			assert.Equal(t, tt.expectedChanBind, sms.ClientFinalMessage.ChannelBinding)
		})
	}
}

func TestMessageState_setServerFirstMessage(t *testing.T) {
	sms := &messageState{
		ClientFirstMessage: &clientFirstMessage{
			Nonce: testClientNonce,
		},
	}

	err := sms.setServerFirstMessage()
	assert.NoError(t, err)

	// Verify that server first message is properly formatted
	assert.NotNil(t, sms.ServerFirstMessage)
	assert.True(t, strings.HasPrefix(sms.ServerFirstMessage.Nonce, testClientNonce))
	assert.Equal(t, 4096, sms.ServerFirstMessage.IterationCount)
	assert.Len(t, sms.ServerFirstMessage.Salt, clientNonceLen)

	// Verify the message format, and that the salt is base64-encoded for the wire
	msg := sms.ServerFirstMessage.String()
	assert.Contains(t, msg, "r="+sms.ServerFirstMessage.Nonce)
	assert.Contains(t, msg, "s="+base64.StdEncoding.EncodeToString(sms.ServerFirstMessage.Salt))
	assert.Contains(t, msg, "i=4096")

	// The wire form has to decode back to the salt the proofs are computed with,
	// or the client and the server sign different transcripts.
	_, b64Salt, _, err := parseServerFirst(msg)
	assert.NoError(t, err)
	decoded, err := base64.StdEncoding.DecodeString(b64Salt)
	assert.NoError(t, err)
	assert.Equal(t, sms.ServerFirstMessage.Salt, decoded)
}

func TestClientFirstMessage_ClientFirstMessageBare(t *testing.T) {
	tests := []struct {
		name     string
		authId   string
		nonce    string
		expected string
	}{
		{
			name:     "With username",
			authId:   "user",
			nonce:    "fyko+d2lbbFgONRv9qkxdawL",
			expected: "n=user,r=fyko+d2lbbFgONRv9qkxdawL",
		},
		{
			name:     "Empty username (PostgreSQL style)",
			authId:   "",
			nonce:    "clientnonce123",
			expected: "n=,r=clientnonce123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfm := &clientFirstMessage{
				AuthenticationID: tt.authId,
				Nonce:            tt.nonce,
			}

			result := cfm.ClientFirstMessageBare()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestClientFirstMessage_GS2Header(t *testing.T) {
	tests := []struct {
		name     string
		message  clientFirstMessage
		expected string
	}{
		{
			name: "SCRAM-SHA-256 no channel binding",
			message: clientFirstMessage{
				ChannelBindingType: "n",
			},
			expected: "n,,",
		},
		{
			name: "SCRAM-SHA-256 channel binding supported but not used",
			message: clientFirstMessage{
				ChannelBindingType: "y",
			},
			expected: "y,,",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.message.GS2Header())
		})
	}
}

func TestClientFinalMessage_ClientFinalMessageWithoutProof(t *testing.T) {
	cfm := &clientFinalMessage{
		Nonce:          testCombinedNonce,
		ChannelBinding: "biws",
	}

	result := cfm.ClientFinalMessageWithoutProof()
	expected := "c=biws,r=" + testCombinedNonce
	assert.Equal(t, expected, result)
}

func TestServerFirstMessage_String(t *testing.T) {
	sfm := &serverFirstMessage{
		Nonce:          testCombinedNonce,
		Salt:           testSaltBytes,
		IterationCount: testIterations,
	}

	result := sfm.String()
	expected := "r=" + testCombinedNonce + ",s=" + testSalt + ",i=4096"
	assert.Equal(t, expected, result)
}

func TestServerFinalMessage_String(t *testing.T) {
	signature := "rmF9pqV8S7suAoZWja4dJRkFsKQ="
	sfm := &serverFinalMessage{
		ServerSignature: signature,
	}

	result := sfm.String()
	expected := "v=" + signature
	assert.Equal(t, expected, result)
}

// completeState returns a state with every message needed to compute a proof or a
// signature, using the fixed test vectors so the results are deterministic.
func completeState() *messageState {
	return &messageState{
		ClientFirstMessage: &clientFirstMessage{
			AuthenticationID: "",
			Nonce:            testClientNonce,
		},
		ServerFirstMessage: &serverFirstMessage{
			Nonce:          testCombinedNonce,
			Salt:           testSaltBytes,
			IterationCount: testIterations,
		},
		ClientFinalMessage: &clientFinalMessage{
			Nonce:          testCombinedNonce,
			ChannelBinding: "biws",
		},
	}
}

func TestMessageState_computeClientProof(t *testing.T) {
	sms := completeState()

	proof := sms.computeClientProof(testPassword)

	// Verify it's valid base64
	_, err := base64.StdEncoding.DecodeString(proof)
	assert.NoError(t, err)

	// Verify it's deterministic
	assert.Equal(t, proof, sms.computeClientProof(testPassword))

	// Verify different password produces different proof
	assert.NotEqual(t, proof, sms.computeClientProof("different_password"))
}

func TestMessageState_computeServerSignature(t *testing.T) {
	sms := completeState()

	signature := sms.computeServerSignature(testPassword)

	// Verify it's valid base64
	_, err := base64.StdEncoding.DecodeString(signature)
	assert.NoError(t, err)

	// Verify it's deterministic
	assert.Equal(t, signature, sms.computeServerSignature(testPassword))

	// Verify different password produces different signature
	assert.NotEqual(t, signature, sms.computeServerSignature("different_password"))

	// The proof and the signature are computed from the same transcript and must
	// not be interchangeable: the client checks the signature to authenticate the
	// server, so a server that returned the proof would prove nothing.
	assert.NotEqual(t, sms.computeClientProof(testPassword), signature)
}

// Integration test for the complete SCRAM flow
func TestScramStateIntegration(t *testing.T) {
	// This test verifies that the SCRAM computation functions work correctly together
	// by simulating a complete authentication exchange

	// Step 1: Set up initial state
	sms := &messageState{}

	// Step 2: Simulate client first message
	clientFirstMsg := &pgproto3.SASLInitialResponse{
		Data: []byte("n,,n=,r=" + testClientNonce),
	}
	err := sms.setClientFirstMessage(clientFirstMsg)
	assert.NoError(t, err)

	// Step 3: Generate server first message
	err = sms.setServerFirstMessage()
	assert.NoError(t, err)

	// Step 4: Simulate client final message with computed proof
	// For testing, we'll compute what the client proof should be
	tempSms := &messageState{
		ClientFirstMessage: sms.ClientFirstMessage,
		ServerFirstMessage: sms.ServerFirstMessage,
		ClientFinalMessage: &clientFinalMessage{
			Nonce:          sms.ServerFirstMessage.Nonce,
			ChannelBinding: "biws",
		},
	}

	expectedProof := tempSms.computeClientProof(testPassword)

	clientFinalMsg := &pgproto3.SASLResponse{
		Data: []byte("c=biws,r=" + sms.ServerFirstMessage.Nonce + ",p=" + expectedProof),
	}
	sms.setClientFinalMessage(clientFinalMsg)

	// Step 5: Verify proof matches
	computedProof := sms.computeClientProof(testPassword)
	assert.Equal(t, expectedProof, computedProof)
	assert.Equal(t, sms.ClientFinalMessage.ClientProof, computedProof)
	assert.NoError(t, sms.verifyAuthentication(testPassword))

	// Step 6: Generate and verify server final message
	sms.setServerFinalMessage(testPassword)
	assert.NotEmpty(t, sms.ServerFinalMessage.ServerSignature)

	// Verify server signature is valid base64
	_, err = base64.StdEncoding.DecodeString(sms.ServerFinalMessage.ServerSignature)
	assert.NoError(t, err)
}

// Test error conditions
func TestScramErrorConditions(t *testing.T) {
	t.Run("Malformed client first message", func(t *testing.T) {
		sms := &messageState{}
		msg := &pgproto3.SASLInitialResponse{
			Data: []byte("malformed"),
		}

		err := sms.setClientFirstMessage(msg)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid SCRAM client-first message format")
	})

	// The rejecting caller logs these errors while the client is still
	// unauthenticated, so a client could otherwise write text of its choosing, at
	// a length of its choosing, into the proxy's logs.
	t.Run("Rejection does not echo the bytes the client sent", func(t *testing.T) {
		const marker = "client-chosen-payload"

		cases := map[string][]byte{
			"malformed message": []byte(marker),
			"bad GS2 header":    []byte("x" + marker + ",,n=user,r=nonce"),
			"authzid":           []byte("n," + marker + ",n=user,r=nonce"),
		}
		for name, data := range cases {
			t.Run(name, func(t *testing.T) {
				err := (&messageState{}).setClientFirstMessage(&pgproto3.SASLInitialResponse{Data: data})
				assert.Error(t, err)
				assert.NotContains(t, err.Error(), marker)
			})
		}
	})

	t.Run("Nonce mismatch", func(t *testing.T) {
		sms := completeState()
		sms.ClientFinalMessage.Nonce = testCombinedNonce + "tampered"

		err := sms.verifyAuthentication(testPassword)
		assert.ErrorContains(t, err, "client nonce does not match server nonce")
	})

	t.Run("Proof mismatch", func(t *testing.T) {
		sms := completeState()
		sms.ClientFinalMessage.ClientProof = sms.computeClientProof("some-other-password")

		assert.ErrorContains(t, sms.verifyAuthentication(testPassword), "client proof mismatch")
	})
}

func BenchmarkComputeClientProof(b *testing.B) {
	sms := completeState()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sms.computeClientProof(testPassword)
	}
}
