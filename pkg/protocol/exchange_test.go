// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package protocol

import (
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConnExchangeDecodesReplyForRequest pins the one thing a scripted Exchange
// cannot check: that the reply to an authentication request decodes as the message
// the protocol says it is. PostgreSQL gives all of them the type byte 'p', so
// without Send tracking the auth type a SASL reply arrives as a PasswordMessage
// and the mechanism rejects its own client.
func TestConnExchangeDecodesReplyForRequest(t *testing.T) {
	cases := []struct {
		name    string
		request pgproto3.BackendMessage
		reply   pgproto3.FrontendMessage
		want    pgproto3.FrontendMessage
	}{
		{
			name:    "cleartext request decodes a password message",
			request: &pgproto3.AuthenticationCleartextPassword{},
			reply:   &pgproto3.PasswordMessage{Password: "hunter2"},
			want:    &pgproto3.PasswordMessage{},
		},
		{
			name:    "SASL request decodes a SASL initial response",
			request: &pgproto3.AuthenticationSASL{AuthMechanisms: []string{"SCRAM-SHA-256"}},
			reply:   &pgproto3.SASLInitialResponse{AuthMechanism: "SCRAM-SHA-256", Data: []byte("n,,n=,r=nonce")},
			want:    &pgproto3.SASLInitialResponse{},
		},
		{
			name:    "SASL continue decodes a SASL response",
			request: &pgproto3.AuthenticationSASLContinue{Data: []byte("r=nonce,s=c2FsdA==,i=4096")},
			reply:   &pgproto3.SASLResponse{Data: []byte("c=biws,r=nonce,p=proof")},
			want:    &pgproto3.SASLResponse{},
		},
		{
			name:    "GSS request decodes a GSS response",
			request: &pgproto3.AuthenticationGSS{},
			reply:   &pgproto3.GSSResponse{Data: []byte("token")},
			want:    &pgproto3.GSSResponse{},
		},
		{
			name:    "GSS continue decodes a GSS response",
			request: &pgproto3.AuthenticationGSSContinue{Data: []byte("challenge")},
			reply:   &pgproto3.GSSResponse{Data: []byte("token")},
			want:    &pgproto3.GSSResponse{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server, client := net.Pipe()
			defer server.Close()
			defer client.Close()
			server.SetDeadline(time.Now().Add(2 * time.Second))
			client.SetDeadline(time.Now().Add(2 * time.Second))

			ex := newPipe(server)
			frontend := pgproto3.NewFrontend(client, client)

			// The write has to complete before the client reads, and the pipe is
			// unbuffered, so the client half runs concurrently.
			go func() {
				if _, err := frontend.Receive(); err != nil {
					return
				}
				frontend.Send(tc.reply)
				_ = frontend.Flush()
			}()

			require.NoError(t, ex.Send(tc.request))

			got, err := ex.Receive()
			require.NoError(t, err)
			assert.IsType(t, tc.want, got)
		})
	}
}
