// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package session

import (
	"bytes"
	"context"
	"io"
	"testing"

	pgproxy "github.com/DataDog/pg-proxy"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

func TestClientMessageBodyLengthMatchesEncoding(t *testing.T) {
	for name, msg := range map[string]pgproto3.FrontendMessage{
		"empty bind": &pgproto3.Bind{},
		"bind": &pgproto3.Bind{
			DestinationPortal: "portal", PreparedStatement: "statement",
			ParameterFormatCodes: []int16{0, 1, 0, 0}, ResultFormatCodes: []int16{1, 0},
			Parameters: [][]byte{nil, {}, {0, 1, 255}, []byte("héllo")},
		},
		"empty query":         &pgproto3.Query{},
		"query":               &pgproto3.Query{String: "SELECT 'héllo'"},
		"empty parse":         &pgproto3.Parse{},
		"parse":               &pgproto3.Parse{Name: "stmt", Query: "SELECT $1, $2", ParameterOIDs: []uint32{25, 23}},
		"empty copy":          &pgproto3.CopyData{},
		"copy data":           &pgproto3.CopyData{Data: []byte{0, 1, 255}},
		"copy fail":           &pgproto3.CopyFail{Message: "failed"},
		"close":               &pgproto3.Close{ObjectType: 'S', Name: "stmt"},
		"describe":            &pgproto3.Describe{ObjectType: 'P', Name: "portal"},
		"execute":             &pgproto3.Execute{Portal: "portal", MaxRows: 10},
		"empty function call": &pgproto3.FunctionCall{},
		"function call": &pgproto3.FunctionCall{
			Function: 42, ArgFormatCodes: []uint16{0, 1, 0},
			Arguments: [][]byte{nil, {}, {0, 255}}, ResultFormatCode: 1,
		},
		"copy done": &pgproto3.CopyDone{},
		"flush":     &pgproto3.Flush{},
		"sync":      &pgproto3.Sync{},
		"terminate": &pgproto3.Terminate{},
	} {
		t.Run(name, func(t *testing.T) {
			encoded, err := msg.Encode(nil) // Encoding is only used as a test oracle.
			require.NoError(t, err)
			decoded, err := pgproto3.NewBackend(bytes.NewReader(encoded), io.Discard).Receive()
			require.NoError(t, err)
			size, ok := clientMessageBodyLength(decoded)
			require.True(t, ok)
			require.Equal(t, len(encoded)-5, size)
		})
	}
}

func TestClientMessageBodyLengthNoPayloadAllocation(t *testing.T) {
	msg := &pgproto3.Bind{Parameters: [][]byte{make([]byte, 1024*1024), nil, {}}}
	var size int
	allocs := testing.AllocsPerRun(100, func() { size, _ = clientMessageBodyLength(msg) })
	require.Equal(t, 1024*1024+8+3*4, size)
	require.Zero(t, allocs)
}

func bodyLengthTestSession(wire []byte) (*Session, *recordingSessionMetrics) {
	metrics := &recordingSessionMetrics{}
	return New(Config{
		Context: context.Background(), Metrics: metrics, MetricTags: []string{"database:test"},
		Client: pgproto3.NewBackend(bytes.NewReader(wire), io.Discard),
	}), metrics
}

func bodyLengthCalls(metrics *recordingSessionMetrics) []metricCall {
	var calls []metricCall
	for _, call := range metrics.distributions {
		if call.name == pgproxy.MetricName(MetricClientMessageBodyLength) {
			calls = append(calls, call)
		}
	}
	return calls
}

func TestClientMessageBodyLengthSessionReads(t *testing.T) {
	t.Run("idle terminate is counted before interception", func(t *testing.T) {
		wire, err := (&pgproto3.Terminate{}).Encode(nil)
		require.NoError(t, err)
		s, metrics := bodyLengthTestSession(wire)
		require.ErrorIs(t, s.receiveAndHandleMessage(), io.EOF)
		require.Equal(t, []metricCall{{
			name: pgproxy.MetricName(MetricClientMessageBodyLength), value: 0, tags: []string{"database:test", "message:terminate"},
		}}, bodyLengthCalls(metrics))
	})
	t.Run("in flight", func(t *testing.T) {
		bind := &pgproto3.Bind{Parameters: [][]byte{[]byte("hello"), nil}}
		wire, err := bind.Encode(nil)
		require.NoError(t, err)
		s, metrics := bodyLengthTestSession(wire)
		msg, err := s.receiveMessage()
		require.NoError(t, err)
		encoded, err := msg.Encode(nil)
		require.NoError(t, err)
		require.Equal(t, wire, encoded)
		require.Equal(t, []metricCall{{
			name: pgproxy.MetricName(MetricClientMessageBodyLength), value: float64(len(wire) - 5), tags: []string{"database:test", "message:bind"},
		}}, bodyLengthCalls(metrics))
	})
	t.Run("discard until sync counts each message once", func(t *testing.T) {
		wire, err := (&pgproto3.Bind{Parameters: [][]byte{[]byte("hello")}}).Encode(nil)
		require.NoError(t, err)
		bindSize := len(wire) - 5
		wire, err = (&pgproto3.Sync{}).Encode(wire)
		require.NoError(t, err)
		s, metrics := bodyLengthTestSession(wire)
		s.setDiscardUntilSync(true)
		require.NoError(t, s.drainUntilSync())
		require.Equal(t, []metricCall{
			{name: pgproxy.MetricName(MetricClientMessageBodyLength), value: float64(bindSize), tags: []string{"database:test", "message:bind"}},
			{name: pgproxy.MetricName(MetricClientMessageBodyLength), value: 0, tags: []string{"database:test", "message:sync"}},
		}, bodyLengthCalls(metrics))
	})
}

func TestClientMessageBodyLengthSkipsFailedReads(t *testing.T) {
	for name, wire := range map[string][]byte{
		"EOF":          nil,
		"partial body": {'B', 0, 0, 0, 12},
		"invalid body": {'B', 0, 0, 0, 4},
	} {
		t.Run(name, func(t *testing.T) {
			s, metrics := bodyLengthTestSession(wire)
			_, err := s.receiveMessage()
			require.Error(t, err)
			require.Empty(t, bodyLengthCalls(metrics))
			s, metrics = bodyLengthTestSession(wire)
			require.Error(t, s.receiveAndHandleMessage())
			require.Empty(t, bodyLengthCalls(metrics))
		})
	}
}

func TestClientMessageBodyLengthSkipsNonSessionMessages(t *testing.T) {
	s, metrics := bodyLengthTestSession(nil)
	for _, msg := range []pgproto3.FrontendMessage{nil, &pgproto3.StartupMessage{}, &pgproto3.PasswordMessage{Password: "secret"}} {
		_, ok := clientMessageBodyLength(msg)
		require.False(t, ok)
		s.recordClientMessageBodyLength(msg)
	}
	require.Empty(t, bodyLengthCalls(metrics))
}
