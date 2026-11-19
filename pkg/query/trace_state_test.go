// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package query

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"

	pgproxy "github.com/DataDog/pg-proxy"
)

func TestTraceMessageTypePreservesFmtType(t *testing.T) {
	testCases := []interface{}{
		&pgproto3.AuthenticationCleartextPassword{},
		&pgproto3.AuthenticationGSS{},
		&pgproto3.AuthenticationGSSContinue{},
		&pgproto3.AuthenticationMD5Password{},
		&pgproto3.AuthenticationOk{},
		&pgproto3.AuthenticationSASL{},
		&pgproto3.AuthenticationSASLContinue{},
		&pgproto3.AuthenticationSASLFinal{},
		&pgproto3.BackendKeyData{},
		&pgproto3.Bind{},
		&pgproto3.BindComplete{},
		&pgproto3.CancelRequest{},
		&pgproto3.Close{},
		&pgproto3.CloseComplete{},
		&pgproto3.CommandComplete{},
		&pgproto3.CopyBothResponse{},
		&pgproto3.CopyData{},
		&pgproto3.CopyDone{},
		&pgproto3.CopyFail{},
		&pgproto3.CopyInResponse{},
		&pgproto3.CopyOutResponse{},
		&pgproto3.DataRow{},
		&pgproto3.Describe{},
		&pgproto3.EmptyQueryResponse{},
		&pgproto3.ErrorResponse{},
		&pgproto3.Execute{},
		&pgproto3.Flush{},
		&pgproto3.FunctionCall{},
		&pgproto3.FunctionCallResponse{},
		&pgproto3.GSSEncRequest{},
		&pgproto3.NoData{},
		&pgproto3.NoticeResponse{},
		&pgproto3.NotificationResponse{},
		&pgproto3.ParameterDescription{},
		&pgproto3.ParameterStatus{},
		&pgproto3.Parse{},
		&pgproto3.ParseComplete{},
		&pgproto3.PortalSuspended{},
		&pgproto3.Query{},
		&pgproto3.ReadyForQuery{},
		&pgproto3.RowDescription{},
		&pgproto3.SSLRequest{},
		&pgproto3.StartupMessage{},
		&pgproto3.Sync{},
		&pgproto3.Terminate{},
	}

	for _, testCase := range testCases {
		got := traceMessageType(testCase)
		want := fmt.Sprintf("%T", testCase)
		if got != want {
			t.Fatalf("traceMessageType(%T) = %q, want %q", testCase, got, want)
		}
	}
}

func TestTraceStateAddMessageSkipsDataMessages(t *testing.T) {
	traceState := &TraceState{}

	traceState.AddMessage(&pgproto3.Query{})
	traceState.AddMessage(&pgproto3.DataRow{})
	traceState.AddMessage(&pgproto3.CopyData{})
	traceState.AddMessage(&pgproto3.ReadyForQuery{})

	got := traceState.getMessages()
	want := []string{"*pgproto3.Query", "*pgproto3.ReadyForQuery"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("getMessages() = %v, want %v", got, want)
	}
}

type samplingSpan struct {
	pgproxy.Span
	sampled bool
}

func (s samplingSpan) Sampled() bool { return s.sampled }

func TestTraceStateSampled(t *testing.T) {
	testCases := []struct {
		name string
		root pgproxy.Span
		want bool
	}{
		{"no root", nil, false},
		{"root without sampling decision", struct{ pgproxy.Span }{}, true},
		{"sampled root", samplingSpan{sampled: true}, true},
		{"unsampled root", samplingSpan{sampled: false}, false},
	}

	for _, testCase := range testCases {
		traceState := &TraceState{rootTrace: testCase.root}
		if got := traceState.Sampled(); got != testCase.want {
			t.Fatalf("%s: Sampled() = %v, want %v", testCase.name, got, testCase.want)
		}
	}
}

func TestTraceStateIgnoresBindParametersWithoutRoot(t *testing.T) {
	traceState := &TraceState{}

	traceState.RecordBindParameters(&pgproto3.Bind{Parameters: [][]byte{[]byte("1")}}, nil, nil)
	traceState.FinishRootTrace() // must not render onto the missing root span

	if traceState.bindParams != nil && traceState.bindParams.Recorded() {
		t.Fatal("RecordBindParameters kept a Bind without a root span")
	}
}
