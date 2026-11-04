// Package auth composes client verification and access checks for listener hooks.
// Credential sources and backend PostgreSQL accounts remain caller-owned.
package auth

import (
	"context"
	"errors"
	"fmt"

	pgproxy "github.com/DataDog/pg-proxy"
	"github.com/DataDog/pg-proxy/pkg/listener"
	"github.com/DataDog/pg-proxy/pkg/protocol"
)

// Identity contains only verified identity data. Service is the nonempty session
// accounting key; it must distinguish principals even when they share a backend.
type Identity[P any] struct {
	Service string
	// Principal carries verified provider data to authorization. The hook never
	// logs it. Providers must not put passwords, tokens, or SCRAM proofs here.
	Principal P
}

// Request names a proxy login and the logical database resolved by the server.
// User does not select a backend PostgreSQL role.
type Request struct {
	User, LogicalDatabase string
}

type Verifier[P any] func(context.Context, Request, protocol.Exchange) (Identity[P], error)
type Authorizer[P any] func(context.Context, Identity[P], Request) error

// Hook verifies a client, authorizes the complete request, then returns its
// accounting identity. Failures are returned to the listener, which alone owns
// terminal errors and successful startup messages. Passthrough is unsupported.
func Hook[T, P any](verify Verifier[P], authorize Authorizer[P]) (func(context.Context, listener.StartupRequest, listener.StartupResult[T], protocol.Exchange) (listener.AuthenticationResult, error), error) {
	if verify == nil || authorize == nil {
		return nil, errors.New("authentication requires a verifier and authorizer")
	}
	return func(ctx context.Context, startup listener.StartupRequest, resolved listener.StartupResult[T], ex protocol.Exchange) (listener.AuthenticationResult, error) {
		if startup.Options.PassthroughUser {
			return listener.AuthenticationResult{}, listener.ErrUnsupportedAuthOption
		}
		if err := ctx.Err(); err != nil {
			return listener.AuthenticationResult{}, err
		}
		request := Request{User: startup.User, LogicalDatabase: resolved.LogicalDatabase}
		if request.User == "" || request.LogicalDatabase == "" {
			return listener.AuthenticationResult{}, listener.ErrAuthenticationFailed
		}
		identity, err := verify(ctx, request, ex)
		if err != nil {
			return listener.AuthenticationResult{}, err
		}
		if identity.Service == "" {
			return listener.AuthenticationResult{}, listener.ErrAuthenticationFailed
		}
		if err := authorize(ctx, identity, request); err != nil {
			return listener.AuthenticationResult{}, err
		}
		if err := ctx.Err(); err != nil {
			return listener.AuthenticationResult{}, err
		}
		pgproxy.LoggerFromContext(ctx).Info("client authenticated",
			"authenticated_user", fmt.Sprintf("%.256s", request.User),
			"authenticated_service", fmt.Sprintf("%.256s", identity.Service),
			"logical_database", fmt.Sprintf("%.256s", request.LogicalDatabase))
		return listener.AuthenticationResult{Service: identity.Service}, nil
	}, nil
}

// DatabaseAccess is an immutable, exact-match allowlist for proxy usernames.
type DatabaseAccess struct {
	users map[string]map[string]struct{}
}

// NewDatabaseAccess copies explicit user/database grants. An empty grant is an
// error; absence from the copied map always denies access. The application must
// also check that database names exist in its routing configuration.
func NewDatabaseAccess(grants map[string][]string) (*DatabaseAccess, error) {
	if len(grants) == 0 {
		return nil, errors.New("authentication requires explicit database access")
	}
	access := &DatabaseAccess{users: make(map[string]map[string]struct{}, len(grants))}
	for user, databases := range grants {
		if user == "" || len(databases) == 0 {
			return nil, errors.New("each proxy user requires a username and allowed databases")
		}
		allowed := make(map[string]struct{}, len(databases))
		for _, database := range databases {
			if database == "" {
				return nil, errors.New("allowed database names must not be empty")
			}
			allowed[database] = struct{}{}
		}
		access.users[user] = allowed
	}
	return access, nil
}

func (a *DatabaseAccess) Authorize(_ context.Context, _ Identity[struct{}], request Request) error {
	if a != nil {
		if _, ok := a.users[request.User][request.LogicalDatabase]; ok {
			return nil
		}
	}
	return listener.ErrInvalidCredentials
}
