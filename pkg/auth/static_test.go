// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package auth

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/DataDog/pg-proxy/pkg/listener"
	"github.com/DataDog/pg-proxy/pkg/scram"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
	xdgscram "github.com/xdg-go/scram"
)

func TestStaticRequiresUniqueCompleteUsers(t *testing.T) {
	verifier, err := scram.NewVerifier("password")
	require.NoError(t, err)
	valid := StaticUser{Username: "alice", Verifier: verifier, Databases: []string{"orders"}}
	for _, users := range [][]StaticUser{nil, {valid, valid}, {{Username: "alice", Databases: []string{"orders"}}}, {{Verifier: verifier, Databases: []string{"orders"}}}, {{Username: "alice", Verifier: verifier}}, {{Username: "alice", Verifier: verifier, Databases: []string{""}}}} {
		_, err := NewStatic(users)
		require.Error(t, err)
	}
	_, err = NewStatic([]StaticUser{valid})
	require.NoError(t, err)
}

type staticChallengeExchange struct {
	step      int
	challenge string
}

func (e *staticChallengeExchange) Send(message pgproto3.BackendMessage) error {
	if challenge, ok := message.(*pgproto3.AuthenticationSASLContinue); ok {
		e.challenge = string(challenge.Data)
	}
	return nil
}

func (e *staticChallengeExchange) Receive() (pgproto3.FrontendMessage, error) {
	e.step++
	if e.step == 1 {
		return &pgproto3.SASLInitialResponse{AuthMechanism: "SCRAM-SHA-256", Data: []byte("n,,n=,r=client-nonce")}, nil
	}
	nonce := strings.TrimPrefix(strings.Split(e.challenge, ",")[0], "r=")
	return &pgproto3.SASLResponse{Data: []byte("c=biws,r=" + nonce + ",p=" + base64.StdEncoding.EncodeToString(make([]byte, 32)))}, nil
}

type staticChallenge struct {
	salt       string
	iterations int
}

func failedStaticChallenge(t *testing.T, provider *Static, username string) staticChallenge {
	t.Helper()
	exchange := &staticChallengeExchange{}
	err := provider.Authenticate(t.Context(), username, "orders", exchange)
	require.ErrorIs(t, err, listener.ErrInvalidCredentials)
	parts := strings.Split(exchange.challenge, ",")
	require.Len(t, parts, 3)
	iterations, err := strconv.Atoi(strings.TrimPrefix(parts[2], "i="))
	require.NoError(t, err)
	return staticChallenge{salt: strings.TrimPrefix(parts[1], "s="), iterations: iterations}
}

func TestStaticUnknownUserSalts(t *testing.T) {
	verifier, err := scram.NewVerifier("password")
	require.NoError(t, err)
	users := []StaticUser{{Username: "alice", Verifier: verifier, Databases: []string{"orders"}}}
	provider, err := NewStatic(users)
	require.NoError(t, err)
	known := failedStaticChallenge(t, provider, "alice")
	salts := map[string]bool{known.salt: true}
	for i := 0; i < 16; i++ {
		username := fmt.Sprintf("unknown-%d", i)
		challenge := failedStaticChallenge(t, provider, username)
		require.Equal(t, challenge, failedStaticChallenge(t, provider, username), "the same username must retain its salt across attempts")
		require.False(t, salts[challenge.salt], "different usernames must not share a recognizable dummy salt")
		salts[challenge.salt] = true
	}
	other, err := NewStatic(users)
	require.NoError(t, err)
	require.Equal(t, known, failedStaticChallenge(t, other, "alice"), "configured credentials must remain unchanged")
	require.NotEqual(t, failedStaticChallenge(t, provider, "unknown-0"), failedStaticChallenge(t, other, "unknown-0"), "independent providers must not share their fake salts")
}

func TestStaticUnknownUsersMatchConfiguredParameters(t *testing.T) {
	plain, err := scram.NewVerifier("alice-password")
	require.NoError(t, err)
	client, err := xdgscram.SHA256.NewClientUnprepped("", "bob-password", "")
	require.NoError(t, err)
	stored := client.GetStoredCredentials(xdgscram.KeyFactors{Salt: "postgresql-salt!", Iters: 8192})
	imported, err := scram.ParseVerifier(fmt.Sprintf("SCRAM-SHA-256$%d:%s$%s:%s", stored.Iters,
		base64.StdEncoding.EncodeToString([]byte(stored.Salt)), base64.StdEncoding.EncodeToString(stored.StoredKey), base64.StdEncoding.EncodeToString(stored.ServerKey)))
	require.NoError(t, err)
	provider, err := NewStatic([]StaticUser{
		{Username: "alice", Verifier: plain, Databases: []string{"orders"}},
		{Username: "bob", Verifier: imported, Databases: []string{"orders"}},
	})
	require.NoError(t, err)
	provider.mockKey = [32]byte{1} // Keep profile selection deterministic in this test.
	type parameters struct{ saltSize, iterations int }
	profile := func(challenge staticChallenge) parameters {
		salt, err := base64.StdEncoding.DecodeString(challenge.salt)
		require.NoError(t, err)
		return parameters{len(salt), challenge.iterations}
	}
	configured := map[parameters]bool{
		profile(failedStaticChallenge(t, provider, "alice")): true,
		profile(failedStaticChallenge(t, provider, "bob")):   true,
	}
	require.Len(t, configured, 2)
	seen := make(map[parameters]bool)
	for i := 0; i < 32; i++ {
		username := fmt.Sprintf("unknown-%d", i)
		challenge := failedStaticChallenge(t, provider, username)
		require.Equal(t, challenge, failedStaticChallenge(t, provider, username))
		parameters := profile(challenge)
		require.True(t, configured[parameters], "unknown users must not have distinct salt lengths or iteration counts")
		seen[parameters] = true
	}
	require.Equal(t, configured, seen, "both configured parameter sets must be used for fake challenges")
}

func TestStaticConcurrentUnknownUsers(t *testing.T) {
	verifier, err := scram.NewVerifier("password")
	require.NoError(t, err)
	provider, err := NewStatic([]StaticUser{{Username: "alice", Verifier: verifier, Databases: []string{"orders"}}})
	require.NoError(t, err)
	for i := 0; i < 16; i++ {
		t.Run(fmt.Sprintf("unknown-%d", i), func(t *testing.T) {
			t.Parallel()
			challenge := failedStaticChallenge(t, provider, t.Name())
			require.Equal(t, challenge, failedStaticChallenge(t, provider, t.Name()))
		})
	}
}

// A separate client implementation exercises real proofs and server signatures.
type staticClientExchange struct {
	conversation *xdgscram.ClientConversation
	response     string
	initial      bool
	onFinal      func()
}

func (e *staticClientExchange) Send(message pgproto3.BackendMessage) (err error) {
	switch msg := message.(type) {
	case *pgproto3.AuthenticationSASL:
		e.response, err = e.conversation.Step("")
	case *pgproto3.AuthenticationSASLContinue:
		e.response, err = e.conversation.Step(string(msg.Data))
	case *pgproto3.AuthenticationSASLFinal:
		_, err = e.conversation.Step(string(msg.Data))
		if e.onFinal != nil {
			e.onFinal()
		}
	}
	return err
}

func (e *staticClientExchange) Receive() (pgproto3.FrontendMessage, error) {
	if !e.initial {
		e.initial = true
		return &pgproto3.SASLInitialResponse{AuthMechanism: "SCRAM-SHA-256", Data: []byte(e.response)}, nil
	}
	return &pgproto3.SASLResponse{Data: []byte(e.response)}, nil
}

func TestStaticProofAndDatabaseGrant(t *testing.T) {
	verifier, err := scram.NewVerifier("password")
	require.NoError(t, err)
	users := []StaticUser{{Username: "alice", Verifier: verifier, Databases: []string{"orders"}}}
	provider, err := NewStatic(users)
	require.NoError(t, err)
	// Mutating caller-owned configuration cannot change an existing provider.
	users[0].Databases[0] = "reports"
	users[0].Username = "bob"
	for _, tc := range []struct {
		name, username, password, database string
		cancelBefore, cancelAfter, allowed bool
	}{
		{name: "valid", username: "alice", password: "password", database: "orders", allowed: true},
		{name: "wrong password", username: "alice", password: "wrong", database: "orders"},
		{name: "unknown user", username: "bob", password: "password", database: "orders"},
		{name: "denied database", username: "alice", password: "password", database: "reports"},
		{name: "empty database", username: "alice", password: "password"},
		{name: "canceled before proof", username: "alice", password: "password", database: "orders", cancelBefore: true},
		{name: "canceled after proof", username: "alice", password: "password", database: "orders", cancelAfter: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, err := xdgscram.SHA256.NewClientUnprepped("", tc.password, "")
			require.NoError(t, err)
			ex := &staticClientExchange{conversation: client.NewConversation()}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tc.cancelBefore {
				cancel()
			}
			if tc.cancelAfter {
				ex.onFinal = cancel
			}
			err = provider.Authenticate(ctx, tc.username, tc.database, ex)
			if tc.allowed {
				require.NoError(t, err)
				require.True(t, ex.conversation.Valid())
			} else if tc.cancelBefore || tc.cancelAfter {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.ErrorIs(t, err, listener.ErrInvalidCredentials)
			}
		})
	}
}
