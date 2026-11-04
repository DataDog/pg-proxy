package auth

import (
	"context"
	"errors"
	"testing"

	"github.com/DataDog/pg-proxy/pkg/listener"
	"github.com/DataDog/pg-proxy/pkg/protocol"
	"github.com/stretchr/testify/require"
)

func TestHookRequiresVerificationAndAuthorization(t *testing.T) {
	verify := func(context.Context, Request, protocol.Exchange) (Identity, error) {
		return Identity{Service: "alice"}, nil
	}
	authorize := func(context.Context, Identity, Request) error { return nil }
	_, err := Hook[struct{}](nil, authorize)
	require.Error(t, err)
	_, err = Hook[struct{}](verify, nil)
	require.Error(t, err)
	for _, tc := range []struct {
		name                      string
		service                   string
		verifyErr, authorizeErr   error
		passthrough               bool
		wantVerify, wantAuthorize bool
	}{
		{name: "verified", service: "alice", wantVerify: true, wantAuthorize: true},
		{name: "verification failure", verifyErr: errors.New("private verification detail"), wantVerify: true},
		{name: "empty identity", wantVerify: true},
		{name: "access failure", service: "alice", authorizeErr: listener.ErrInvalidCredentials, wantVerify: true, wantAuthorize: true},
		{name: "passthrough", passthrough: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			verified, authorized := false, false
			hook, err := Hook[struct{}](func(_ context.Context, request Request, _ protocol.Exchange) (Identity, error) {
				verified = true
				require.Equal(t, Request{User: "login", LogicalDatabase: "resolved"}, request)
				return Identity{Service: tc.service}, tc.verifyErr
			}, func(_ context.Context, identity Identity, request Request) error {
				authorized = true
				require.Equal(t, tc.service, identity.Service)
				require.Equal(t, "resolved", request.LogicalDatabase)
				return tc.authorizeErr
			})
			require.NoError(t, err)
			result, err := hook(t.Context(), listener.StartupRequest{User: "login", Database: "unresolved", Options: listener.SessionOptions{PassthroughUser: tc.passthrough}}, listener.StartupResult[struct{}]{LogicalDatabase: "resolved"}, nil)
			if tc.name == "verified" {
				require.NoError(t, err)
				require.Equal(t, "alice", result.Service)
			} else {
				require.Error(t, err)
				require.Empty(t, result.Service)
			}
			require.Equal(t, tc.wantVerify, verified)
			require.Equal(t, tc.wantAuthorize, authorized)
		})
	}
}

func TestDatabaseAccessCopiesExactUserGrants(t *testing.T) {
	grants := map[string][]string{"alice": {"orders"}, "bob": {"orders", "reports"}}
	access, err := NewDatabaseAccess(grants)
	require.NoError(t, err)
	grants["alice"][0] = "reports"
	delete(grants, "bob")
	for _, request := range []Request{{"alice", "orders"}, {"bob", "orders"}, {"bob", "reports"}} {
		require.NoError(t, access.Authorize(t.Context(), Identity{}, request))
	}
	for _, request := range []Request{{"alice", "reports"}, {"Alice", "orders"}, {"bob", "Orders"}, {"unknown", "orders"}, {"alice", "*"}, {"", ""}} {
		require.ErrorIs(t, access.Authorize(t.Context(), Identity{}, request), listener.ErrInvalidCredentials)
	}
	var missing *DatabaseAccess
	require.Error(t, missing.Authorize(t.Context(), Identity{}, Request{"alice", "orders"}))
	for _, grants := range []map[string][]string{nil, {"": {"orders"}}, {"alice": nil}, {"alice": {""}}} {
		_, err := NewDatabaseAccess(grants)
		require.Error(t, err)
	}
}
