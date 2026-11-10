// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

//go:build integration

package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

type pingProtocolStep struct {
	send []pgproto3.FrontendMessage
	want []string
}

// Run identical raw wire messages against PostgreSQL and the OSS binary.
// Using the frontend directly avoids driver statement caching and makes
// unsolicited ReadyForQuery responses and missing EmptyQueryResponse visible.
func testPingProtocolAgainstPostgres(t *testing.T, directURL, proxyURL string) {
	simple := func(sql string, want ...string) pingProtocolStep {
		return pingProtocolStep{send: []pgproto3.FrontendMessage{&pgproto3.Query{String: sql}}, want: want}
	}
	begin := simple("BEGIN", "CommandComplete(BEGIN)", "ReadyForQuery(T)")
	rollback := simple("ROLLBACK", "CommandComplete(ROLLBACK)", "ReadyForQuery(I)")
	idlePing := simple("-- ping", "EmptyQueryResponse", "ReadyForQuery(I)")
	for _, tc := range []struct {
		name  string
		steps []pingProtocolStep
	}{
		{name: "ping_idle", steps: []pingProtocolStep{idlePing, idlePing}},
		{name: "ping_in_transaction", steps: []pingProtocolStep{
			begin, simple("-- ping", "EmptyQueryResponse", "ReadyForQuery(T)"), rollback, idlePing,
		}},
		{name: "ping_in_failed_transaction", steps: []pingProtocolStep{
			begin, simple("SELECT 1/0", "ErrorResponse(22012)", "ReadyForQuery(E)"),
			simple("-- ping", "EmptyQueryResponse", "ReadyForQuery(E)"), rollback, idlePing,
		}},
		{name: "extended_ping", steps: []pingProtocolStep{
			{
				send: []pgproto3.FrontendMessage{&pgproto3.Parse{Query: "-- ping"}, &pgproto3.Flush{}},
				want: []string{"ParseComplete"},
			},
			{
				send: []pgproto3.FrontendMessage{&pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{}},
				want: []string{"BindComplete", "EmptyQueryResponse", "ReadyForQuery(I)"},
			},
			idlePing,
		}},
		{name: "named_extended_ping", steps: []pingProtocolStep{
			{
				send: []pgproto3.FrontendMessage{&pgproto3.Parse{Name: "ping", Query: "-- ping"}, &pgproto3.Flush{}},
				want: []string{"ParseComplete"},
			},
			{
				send: []pgproto3.FrontendMessage{&pgproto3.Bind{PreparedStatement: "ping"}, &pgproto3.Execute{}, &pgproto3.Sync{}},
				want: []string{"BindComplete", "EmptyQueryResponse", "ReadyForQuery(I)"},
			},
		}},
		{name: "ping_after_pending_parse", steps: []pingProtocolStep{
			{
				send: []pgproto3.FrontendMessage{&pgproto3.Parse{Query: "SELECT 1"}, &pgproto3.Query{String: "-- ping"}},
				want: []string{"ParseComplete", "EmptyQueryResponse", "ReadyForQuery(I)"},
			},
			idlePing,
		}},
		{name: "ping_after_pending_error", steps: []pingProtocolStep{
			{
				send: []pgproto3.FrontendMessage{
					&pgproto3.Parse{Query: "SELECT ("}, &pgproto3.Query{String: "-- ping"}, &pgproto3.Sync{},
				},
				want: []string{"ErrorResponse(42601)", "ReadyForQuery(I)"},
			},
			idlePing,
		}},
		{name: "ping_during_error_recovery", steps: []pingProtocolStep{
			begin,
			{
				send: []pgproto3.FrontendMessage{&pgproto3.Parse{Query: "SELECT ("}, &pgproto3.Flush{}},
				want: []string{"ErrorResponse(42601)"},
			},
			{
				send: []pgproto3.FrontendMessage{
					&pgproto3.Query{String: "-- ping"}, &pgproto3.Parse{Query: "-- ping"}, &pgproto3.Sync{},
				},
				want: []string{"ReadyForQuery(E)"},
			},
			simple("-- ping", "EmptyQueryResponse", "ReadyForQuery(E)"), rollback, idlePing,
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// A final row-producing query catches replies left over from the ping.
			steps := append(append([]pingProtocolStep(nil), tc.steps...), simple("SELECT 42",
				"RowDescription", "DataRow", "CommandComplete(SELECT 1)", "ReadyForQuery(I)"))
			direct := runPingProtocolSteps(t, "postgres", directURL, steps)
			proxied := runPingProtocolSteps(t, "proxy", proxyURL, steps)
			require.Equal(t, direct, proxied, "proxy must preserve PostgreSQL's response sequence and transaction status")
		})
	}
}

func runPingProtocolSteps(t *testing.T, endpoint, url string, steps []pingProtocolStep) [][]string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	conn, err := pgconn.Connect(ctx, url)
	require.NoError(t, err)
	defer conn.Close(context.Background())
	// Raw frontend reads do not use ctx: bound the socket too, so a missing
	// response fails with a timeout instead of hanging the regression test.
	require.NoError(t, conn.Conn().SetDeadline(time.Now().Add(10*time.Second)))
	frontend := conn.Frontend()
	var trace [][]string
	for i, step := range steps {
		for _, msg := range step.send {
			frontend.Send(msg)
		}
		require.NoError(t, frontend.Flush())
		var received []string
		for _, want := range step.want {
			msg, err := frontend.Receive()
			require.NoError(t, err, "%s step %d waiting for %s", endpoint, i, want)
			got := pingProtocolResponse(msg)
			received = append(received, got)
			require.Equal(t, want, got, "%s step %d", endpoint, i)
		}
		t.Logf("%s step %d: %v", endpoint, i, received)
		trace = append(trace, received)
	}
	return trace
}

func pingProtocolResponse(msg pgproto3.BackendMessage) string {
	switch m := msg.(type) {
	case *pgproto3.ReadyForQuery:
		return fmt.Sprintf("ReadyForQuery(%c)", m.TxStatus)
	case *pgproto3.CommandComplete:
		return fmt.Sprintf("CommandComplete(%s)", m.CommandTag)
	case *pgproto3.ErrorResponse:
		return fmt.Sprintf("ErrorResponse(%s)", m.Code)
	case *pgproto3.EmptyQueryResponse:
		return "EmptyQueryResponse"
	case *pgproto3.ParseComplete:
		return "ParseComplete"
	case *pgproto3.BindComplete:
		return "BindComplete"
	case *pgproto3.RowDescription:
		return "RowDescription"
	case *pgproto3.DataRow:
		return "DataRow"
	default:
		return fmt.Sprintf("unexpected %T", msg)
	}
}
