package auth

import (
	"testing"

	"github.com/DataDog/pg-proxy/pkg/scram"
	"github.com/stretchr/testify/require"
)

func TestStaticRequiresUniqueCompleteUsers(t *testing.T) {
	verifier, err := scram.NewVerifier("password")
	require.NoError(t, err)
	valid := StaticUser{Username: "alice", Verifier: verifier, Databases: []string{"orders"}}
	for _, users := range [][]StaticUser{nil, {valid, valid}, {{Username: "alice", Databases: []string{"orders"}}}, {{Verifier: verifier, Databases: []string{"orders"}}}, {{Username: "alice", Verifier: verifier}}} {
		_, err := NewStatic(users)
		require.Error(t, err)
	}
	_, err = NewStatic([]StaticUser{valid})
	require.NoError(t, err)
}
