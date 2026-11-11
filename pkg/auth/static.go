// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

// Package auth authenticates static proxy users with SCRAM and explicit database grants.
package auth

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"

	"github.com/DataDog/pg-proxy/pkg/listener"
	"github.com/DataDog/pg-proxy/pkg/protocol"
	"github.com/DataDog/pg-proxy/pkg/scram"
)

// StaticUser is a proxy login, independent of all backend PostgreSQL accounts.
type StaticUser struct {
	Username  string
	Verifier  *scram.Verifier
	Databases []string
}

// Static holds immutable credentials and database grants for concurrent logins.
type Static struct {
	users         map[string]staticUser
	mockKey       [32]byte
	mockTemplates []*scram.Verifier
}

type staticUser struct {
	verifier  *scram.Verifier
	databases map[string]struct{}
}

func NewStatic(users []StaticUser) (*Static, error) {
	if len(users) == 0 {
		return nil, errors.New("authentication requires explicit database access")
	}
	credentials := make(map[string]staticUser, len(users))
	templates := make([]*scram.Verifier, 0, len(users))
	for _, user := range users {
		if user.Username == "" || user.Verifier.Validate() != nil {
			return nil, errors.New("each proxy user requires a username and valid SCRAM credentials")
		}
		if _, exists := credentials[user.Username]; exists {
			return nil, errors.New("proxy usernames must be unique")
		}
		if len(user.Databases) == 0 {
			return nil, errors.New("each proxy user requires allowed databases")
		}
		allowed := make(map[string]struct{}, len(user.Databases))
		for _, database := range user.Databases {
			if database == "" {
				return nil, errors.New("allowed database names must not be empty")
			}
			allowed[database] = struct{}{}
		}
		verifier := *user.Verifier
		credentials[user.Username] = staticUser{verifier: &verifier, databases: allowed}
		templates = append(templates, &verifier)
	}
	var mockKey [32]byte
	if _, err := rand.Read(mockKey[:]); err != nil {
		return nil, errors.New("could not generate mock SCRAM secret")
	}
	return &Static{users: credentials, mockKey: mockKey, mockTemplates: templates}, nil
}

// Authenticate verifies the startup username's SCRAM proof and database grant.
// It returns no identity on failure. Callers own TLS policy and read deadlines.
func (s *Static) Authenticate(ctx context.Context, username, logicalDatabase string, ex protocol.Exchange) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	user, exists := s.users[username]
	verifier := user.verifier
	if !exists {
		if len(s.mockTemplates) == 0 {
			return listener.ErrInvalidCredentials
		}
		// Give each unknown username stable fake credentials without retaining
		// attacker-controlled names. Match a configured salt length and iteration
		// count so imported credentials do not leave a distinct dummy profile.
		mac := hmac.New(sha256.New, s.mockKey[:])
		_, _ = mac.Write([]byte("SCRAM parameters\x00" + username))
		index := binary.BigEndian.Uint64(mac.Sum(nil)) % uint64(len(s.mockTemplates))
		var err error
		verifier, err = scram.NewMockVerifier(username, s.mockKey, s.mockTemplates[index])
		if err != nil {
			return err
		}
	}
	err := scram.AuthenticateVerifier(ex, verifier)
	if err != nil {
		var failed *scram.AuthFailedError
		if errors.As(err, &failed) {
			return listener.ErrInvalidCredentials
		}
		return err
	}
	if !exists {
		return listener.ErrInvalidCredentials
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, allowed := user.databases[logicalDatabase]; !allowed {
		return listener.ErrInvalidCredentials
	}
	return nil
}
