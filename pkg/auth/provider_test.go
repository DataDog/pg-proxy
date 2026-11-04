package auth_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/DataDog/pg-proxy/pkg/auth"
	"github.com/DataDog/pg-proxy/pkg/listener"
	"github.com/DataDog/pg-proxy/pkg/protocol"
	"github.com/stretchr/testify/require"
)

// A provider can return its own verified claims without adding its identity
// library or access policy to pg-proxy. This is the boundary dd-source needs.
func TestProviderClaimsReachAuthorization(t *testing.T) {
	type principal struct {
		Email  string
		Groups []string
	}
	for _, tc := range []struct {
		name, database string
		groups         []string
		verification   error
		passthrough    bool
		allowed        bool
	}{
		{name: "allowed", database: "orders", groups: []string{"orders-readers"}, allowed: true},
		{name: "wrong group", database: "orders", groups: []string{"reports-readers"}},
		{name: "wrong database", database: "reports", groups: []string{"orders-readers"}},
		{name: "failed verification", database: "orders", verification: errors.New("provider unavailable")},
		{name: "passthrough", database: "orders", groups: []string{"orders-readers"}, passthrough: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			verified, authorized := false, false
			hook, err := auth.Hook[struct{}](
				func(_ context.Context, request auth.Request, _ protocol.Exchange) (auth.Identity[principal], error) {
					verified = true
					require.Equal(t, "proxy-login", request.User)
					return auth.Identity[principal]{Service: "checkout", Principal: principal{Email: "checkout@example.test", Groups: tc.groups}}, tc.verification
				},
				func(_ context.Context, identity auth.Identity[principal], request auth.Request) error {
					authorized = true
					if identity.Principal.Email != "checkout@example.test" || !slices.Contains(identity.Principal.Groups, "orders-readers") || request.LogicalDatabase != "orders" {
						return listener.ErrInvalidCredentials
					}
					return nil
				})
			require.NoError(t, err)
			result, err := hook(t.Context(), listener.StartupRequest{User: "proxy-login", Database: "unresolved", Options: listener.SessionOptions{PassthroughUser: tc.passthrough}}, listener.StartupResult[struct{}]{LogicalDatabase: tc.database}, nil)
			require.Equal(t, !tc.passthrough, verified)
			require.Equal(t, !tc.passthrough && tc.verification == nil, authorized)
			if tc.allowed {
				// Accounting uses the verified principal, not the requested login.
				require.NoError(t, err)
				require.Equal(t, "checkout", result.Service)
			} else {
				require.Error(t, err)
				require.Empty(t, result.Service)
			}
		})
	}
}

func TestCanceledAuthenticationCannotReturnIdentity(t *testing.T) {
	for _, cancelBefore := range []bool{true, false} {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		if cancelBefore {
			cancel()
		}
		verified := false
		hook, err := auth.Hook[struct{}](func(context.Context, auth.Request, protocol.Exchange) (auth.Identity[struct{}], error) {
			verified = true
			return auth.Identity[struct{}]{Service: "alice"}, nil
		}, func(context.Context, auth.Identity[struct{}], auth.Request) error {
			cancel()
			return nil
		})
		require.NoError(t, err)
		result, err := hook(ctx, listener.StartupRequest{User: "alice"}, listener.StartupResult[struct{}]{LogicalDatabase: "orders"}, nil)
		require.ErrorIs(t, err, context.Canceled)
		require.Empty(t, result.Service)
		require.Equal(t, !cancelBefore, verified)
	}
}
