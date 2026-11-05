package scram

import (
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

func TestClientFirstMessage(t *testing.T) {
	for _, tc := range []struct {
		input, nonce, header string
	}{
		{"n,,n=user,r=clientnonce", "clientnonce", "n,,"},
		{"n,,n=,r=clientnonce", "clientnonce", "n,,"},
		{"y,,n=user,r=AbCd123!@#$%^", "AbCd123!@#$%^", "y,,"},
		{"n,,n=ali=2Cce=3D,r=clientnonce,x=extension", "clientnonce", "n,,"},
	} {
		t.Run(tc.input, func(t *testing.T) {
			state := &messageState{}
			require.NoError(t, state.setClientFirstMessage(&pgproto3.SASLInitialResponse{Data: []byte(tc.input)}))
			require.Equal(t, tc.nonce, state.ClientFirstMessage.Nonce)
			require.Equal(t, tc.header, state.ClientFirstMessage.GS2Header())
			require.Equal(t, tc.input[len(tc.header):], state.ClientFirstMessage.bare)
		})
	}
}

func TestClientFirstMessageRejectsMalformedFields(t *testing.T) {
	for _, input := range []string{
		"malformed", "n,", "x,,n=user,r=nonce", "p=tls-server-end-point,,n=user,r=nonce",
		"n,a=admin,n=user,r=nonce", "n,somedata,n=user,r=nonce",
		"n,,n=,r=", "n,,n=,r=one,r=two", "n,,m=required,n=,r=one",
		"n,,n=bad=00,r=one", "n,,n=bad=,r=one", "n,,n=,r=bad\x00nonce",
		"n,,r=nonce,n=user", "n,,n=user,r=nonce,", "n,,n=user,r=nonce,x=a,x=b",
	} {
		t.Run(input, func(t *testing.T) {
			require.Error(t, (&messageState{}).setClientFirstMessage(&pgproto3.SASLInitialResponse{Data: []byte(input)}))
		})
	}
}

func TestClientFinalMessage(t *testing.T) {
	for _, binding := range []string{"biws", "eSws"} {
		state := &messageState{}
		withoutProof := "c=" + binding + ",r=client-server,x=extension"
		require.NoError(t, state.setClientFinalMessage(&pgproto3.SASLResponse{Data: []byte(withoutProof + ",p=proof")}))
		require.Equal(t, "client-server", state.ClientFinalMessage.Nonce)
		require.Equal(t, "proof", state.ClientFinalMessage.ClientProof)
		require.Equal(t, binding, state.ClientFinalMessage.ChannelBinding)
		require.Equal(t, withoutProof, state.ClientFinalMessage.withoutProof)
	}
}

func TestClientFinalMessageRejectsMalformedFields(t *testing.T) {
	for _, input := range []string{
		"c=biws,r=one,p=a,p=b", "c=biws,r=one,m=required,p=a", "c=biws,p=a",
		"r=one,c=biws,p=a", "c=biws,r=one,p=a,x=after-proof", "c=biws,r=one,x=a,x=b,p=proof",
	} {
		t.Run(input, func(t *testing.T) {
			require.Error(t, (&messageState{}).setClientFinalMessage(&pgproto3.SASLResponse{Data: []byte(input)}))
		})
	}
}

func TestSCRAMParseErrorsDoNotEchoClientData(t *testing.T) {
	const marker = "client-chosen-payload"
	for _, input := range []string{marker, "x" + marker + ",,n=user,r=nonce", "n," + marker + ",n=user,r=nonce"} {
		err := (&messageState{}).setClientFirstMessage(&pgproto3.SASLInitialResponse{Data: []byte(input)})
		require.Error(t, err)
		require.NotContains(t, err.Error(), marker)
	}
	err := (&messageState{}).setClientFinalMessage(&pgproto3.SASLResponse{Data: []byte(marker)})
	require.Error(t, err)
	require.NotContains(t, err.Error(), marker)
}
