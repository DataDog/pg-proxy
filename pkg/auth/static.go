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
	users         map[string]*scram.Verifier
	access        *DatabaseAccess
	mockKey       [32]byte
	mockTemplates []*scram.Verifier
}

func NewStatic(users []StaticUser) (*Static, error) {
	credentials := make(map[string]*scram.Verifier, len(users))
	grants := make(map[string][]string, len(users))
	templates := make([]*scram.Verifier, 0, len(users))
	for _, user := range users {
		if user.Username == "" || user.Verifier.Validate() != nil {
			return nil, errors.New("each proxy user requires a username and valid SCRAM credentials")
		}
		if _, exists := credentials[user.Username]; exists {
			return nil, errors.New("proxy usernames must be unique")
		}
		verifier := *user.Verifier
		credentials[user.Username], grants[user.Username] = &verifier, user.Databases
		templates = append(templates, &verifier)
	}
	access, err := NewDatabaseAccess(grants)
	if err != nil {
		return nil, err
	}
	var mockKey [32]byte
	if _, err := rand.Read(mockKey[:]); err != nil {
		return nil, errors.New("could not generate mock SCRAM secret")
	}
	return &Static{users: credentials, access: access, mockKey: mockKey, mockTemplates: templates}, nil
}

func (s *Static) Verify(ctx context.Context, request Request, ex protocol.Exchange) (Identity[struct{}], error) {
	if err := ctx.Err(); err != nil {
		return Identity[struct{}]{}, err
	}
	verifier, exists := s.users[request.User]
	if !exists {
		if len(s.mockTemplates) == 0 {
			return Identity[struct{}]{}, listener.ErrInvalidCredentials
		}
		// Give each unknown username stable fake credentials without retaining
		// attacker-controlled names. Match a configured salt length and iteration
		// count so imported credentials do not leave a distinct dummy profile.
		mac := hmac.New(sha256.New, s.mockKey[:])
		_, _ = mac.Write([]byte("SCRAM parameters\x00" + request.User))
		index := binary.BigEndian.Uint64(mac.Sum(nil)) % uint64(len(s.mockTemplates))
		var err error
		verifier, err = scram.NewMockVerifier(request.User, s.mockKey, s.mockTemplates[index])
		if err != nil {
			return Identity[struct{}]{}, err
		}
	}
	err := scram.AuthenticateVerifier(ex, verifier)
	if err != nil {
		var failed *scram.AuthFailedError
		if errors.As(err, &failed) {
			return Identity[struct{}]{}, listener.ErrInvalidCredentials
		}
		return Identity[struct{}]{}, err
	}
	if !exists {
		return Identity[struct{}]{}, listener.ErrInvalidCredentials
	}
	return Identity[struct{}]{Service: request.User}, nil
}

func (s *Static) Authorize(ctx context.Context, identity Identity[struct{}], request Request) error {
	return s.access.Authorize(ctx, identity, request)
}
