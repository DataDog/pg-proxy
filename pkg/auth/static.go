package auth

import (
	"context"
	"crypto/rand"
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
	users  map[string]*scram.Verifier
	access *DatabaseAccess
	dummy  *scram.Verifier
}

func NewStatic(users []StaticUser) (*Static, error) {
	credentials := make(map[string]*scram.Verifier, len(users))
	grants := make(map[string][]string, len(users))
	for _, user := range users {
		if user.Username == "" || user.Verifier.Validate() != nil {
			return nil, errors.New("each proxy user requires a username and valid SCRAM credentials")
		}
		if _, exists := credentials[user.Username]; exists {
			return nil, errors.New("proxy usernames must be unique")
		}
		verifier := *user.Verifier
		credentials[user.Username], grants[user.Username] = &verifier, user.Databases
	}
	access, err := NewDatabaseAccess(grants)
	if err != nil {
		return nil, err
	}
	// Unknown users still perform the exchange, with an unpredictable credential.
	dummy, err := scram.NewVerifier(rand.Text())
	if err != nil {
		return nil, err
	}
	return &Static{users: credentials, access: access, dummy: dummy}, nil
}

func (s *Static) Verify(ctx context.Context, request Request, ex protocol.Exchange) (Identity, error) {
	if err := ctx.Err(); err != nil {
		return Identity{}, err
	}
	verifier, exists := s.users[request.User]
	if !exists {
		verifier = s.dummy
	}
	err := scram.AuthenticateVerifier(ex, request.User, verifier)
	if err != nil {
		var failed *scram.AuthFailedError
		if errors.As(err, &failed) {
			return Identity{}, listener.ErrInvalidCredentials
		}
		return Identity{}, err
	}
	if !exists {
		return Identity{}, listener.ErrInvalidCredentials
	}
	return Identity{Service: request.User}, nil
}

func (s *Static) Authorize(ctx context.Context, identity Identity, request Request) error {
	return s.access.Authorize(ctx, identity, request)
}
