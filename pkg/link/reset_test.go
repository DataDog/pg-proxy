// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package link

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pgproxy "github.com/DataDog/pg-proxy"
	"github.com/DataDog/pg-proxy/pkg/config"
)

func mustResetPolicy(t *testing.T, mode config.ResetMode, auditSettings ...string) ResetPolicy {
	t.Helper()
	policy, err := NewResetPolicy(mode, auditSettings)
	require.NoError(t, err)
	return policy
}

func TestResetDecision(t *testing.T) {
	tests := []struct {
		mode       config.ResetMode
		marks      SessionMarks
		wantSend   bool
		wantReason string
	}{
		{config.ResetDefault, 0, true, "always"},
		{config.ResetAlways, MarkClientSQL, true, "always"},
		{config.ResetOff, MarkClientSQL, false, "off"},
		{config.ResetDynamic, 0, false, "clean"},
		{config.ResetDynamic, MarkReadsSequenceState, false, "clean"},
		{config.ResetDynamic, MarkClientSQL, true, "client_sql"},
		{config.ResetDynamic, MarkPreparedStmt, true, "prepared_stmt"},
		{config.ResetDynamic, MarkParameterStatus, true, "parameter_status"},
		{config.ResetDynamic, MarkPreparedStmt | MarkClientSQL, true, "client_sql"},
		{config.ResetAudit, 0, true, "audit"},
		{config.ResetAudit, MarkParameterStatus, true, "parameter_status"},
	}
	for _, test := range tests {
		send, reason := mustResetPolicy(t, test.mode).resetDecision(test.marks)
		assert.Equal(t, test.wantSend, send, "mode %q marks %b", test.mode, test.marks)
		assert.Equal(t, test.wantReason, reason, "mode %q marks %b", test.mode, test.marks)
	}
	send, reason := ResetPolicy{}.resetDecision(MarkClientSQL)
	assert.True(t, send, "the zero policy resets on every release")
	assert.Equal(t, "always", reason)
}

func TestResetSQLByMode(t *testing.T) {
	const identity = "RESET SESSION AUTHORIZATION; RESET ROLE; "
	const extra = "; CLOSE ALL; DISCARD SEQUENCES"
	for mode, want := range map[config.ResetMode]string{
		config.ResetDefault: "RESET ALL",
		config.ResetAlways:  "RESET ALL",
		config.ResetAudit:   "RESET ALL" + extra,
		config.ResetDynamic: "RESET ALL" + extra,
	} {
		policy := mustResetPolicy(t, mode)
		assert.Equal(t, want, policy.resetSQL(true), "mode %q with a RoleSession", mode)
		assert.Equal(t, identity+want, policy.resetSQL(false), "mode %q without a RoleSession", mode)
	}
}

func TestNewResetPolicyBuildsAuditQueryOnce(t *testing.T) {
	policy := mustResetPolicy(t, config.ResetAudit, "app.tenant_id", "app.tenant_id")
	assert.Equal(t, config.ResetAudit, policy.Mode())
	assert.True(t, strings.HasPrefix(policy.auditSQL, "SELECT pg_catalog.current_setting('search_path', true), "))
	assert.Contains(t, policy.auditSQL, "pg_catalog.current_setting('transaction_timeout', true)")
	assert.Contains(t, policy.auditSQL, ", current_user, ")
	assert.Contains(t, policy.auditSQL, "FROM pg_catalog.pg_cursors WHERE is_holdable")
	assert.True(t, strings.HasSuffix(policy.auditSQL, ", pg_catalog.current_setting('app.tenant_id', true)"))
	assert.Equal(t, 1, strings.Count(policy.auditSQL, "app.tenant_id"), "duplicate names are checked once")
	assert.Len(t, policy.auditColumns, len(auditedSettings)+3)

	assert.Empty(t, mustResetPolicy(t, config.ResetDynamic).auditSQL, "only audit runs the check")

	_, err := NewResetPolicy("sometimes", nil)
	assert.ErrorContains(t, err, `reset_mode: unknown mode "sometimes"`)
	_, err = NewResetPolicy(config.ResetAudit, []string{"app.x'); SELECT 1; --"})
	assert.ErrorContains(t, err, "reset_audit_settings[0]")
}

func TestChangedColumns(t *testing.T) {
	policy := ResetPolicy{auditColumns: []string{"search_path", "transaction_timeout", "held_cursor"}}
	before := [][]byte{[]byte("app"), nil, []byte("1")}
	after := [][]byte{[]byte(`"$user", public`), nil, []byte("0")}
	assert.Equal(t, []string{"search_path", "held_cursor"}, policy.changedColumns(before, after))
	assert.Empty(t, policy.changedColumns(after, after))
	assert.Equal(t, []string{"transaction_timeout"}, policy.changedColumns(
		[][]byte{nil, []byte(""), nil}, [][]byte{nil, nil, nil}), "NULL and an empty string differ")
}

// serveReset answers the first Query with one result per statement, using rows for
// SELECT statements in order, then reads the Terminate sent at pool close.
func serveReset(received chan<- pgproto3.FrontendMessage, rows ...[][]byte) func(*pgproto3.Backend) {
	return func(backend *pgproto3.Backend) {
		msg, err := backend.Receive()
		if err != nil {
			return
		}
		received <- msg
		if q, ok := msg.(*pgproto3.Query); ok {
			for _, statement := range strings.Split(q.String, "; ") {
				if strings.HasPrefix(statement, "SELECT ") && len(rows) > 0 {
					row := rows[0]
					rows = rows[1:]
					fields := make([]pgproto3.FieldDescription, len(row))
					for i := range fields {
						fields[i] = pgproto3.FieldDescription{Name: []byte("c"), DataTypeOID: 25}
					}
					backend.Send(&pgproto3.RowDescription{Fields: fields})
					backend.Send(&pgproto3.DataRow{Values: row})
					backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
					continue
				}
				backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("RESET")})
			}
			backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			_ = backend.Flush()
		}
		_, _ = backend.Receive()
	}
}

func TestReleaseByMode(t *testing.T) {
	tests := []struct {
		mode       config.ResetMode
		marks      SessionMarks
		wantQuery  string // empty: no reset sent
		wantReason string
	}{
		{config.ResetAlways, 0, "RESET SESSION AUTHORIZATION; RESET ROLE; RESET ALL", "always"},
		{config.ResetDynamic, 0, "", "clean"},
		{config.ResetDynamic, MarkClientSQL, "RESET SESSION AUTHORIZATION; RESET ROLE; RESET ALL; CLOSE ALL; DISCARD SEQUENCES", "client_sql"},
		{config.ResetOff, MarkClientSQL, "", "off"},
	}
	for _, test := range tests {
		t.Run(string(test.mode)+"/"+test.wantReason, func(t *testing.T) {
			metrics := newRecordingMetrics()
			t.Cleanup(pgproxy.Bootstrap(pgproxy.Dependencies{Metrics: metrics}))
			received := make(chan pgproto3.FrontendMessage, 1)
			conn := newSessionLinkTestPoolConnServing(t, serveReset(received))
			tags := []string{"database_cluster:test-cluster"}
			link, err := New(conn, Options{MetricTags: tags, Reset: mustResetPolicy(t, test.mode)})
			require.NoError(t, err)
			link.MarkSession(test.marks)

			require.NoError(t, link.Release())

			select {
			case msg := <-received:
				require.NotEmpty(t, test.wantQuery, "sent %#v, want no reset", msg)
				require.Equal(t, &pgproto3.Query{String: test.wantQuery}, msg)
			default:
				require.Empty(t, test.wantQuery, "Release sent no reset query")
			}
			outcome := "outcome:reset"
			if test.wantQuery == "" {
				outcome = "outcome:skipped"
			}
			counts, emitted := metrics.getCount("datadog.pg_proxy.sessions.connections.reset",
				append(tags, "reset_mode:"+string(test.mode), outcome, "reason:"+test.wantReason))
			assert.True(t, emitted)
			assert.Equal(t, []int64{1}, counts)
		})
	}
}

func TestReleaseAuditCountsWouldLeak(t *testing.T) {
	metrics := newRecordingMetrics()
	t.Cleanup(pgproxy.Bootstrap(pgproxy.Dependencies{Metrics: metrics}))
	policy := mustResetPolicy(t, config.ResetAudit, "app.tenant_id")

	// Before the reset a client's search_path and held cursor are still there, and
	// a function set app.tenant_id; the reset restores all three.
	before := make([][]byte, len(policy.auditColumns))
	after := make([][]byte, len(policy.auditColumns))
	for i, column := range policy.auditColumns {
		before[i], after[i] = []byte("baseline"), []byte("baseline")
		switch column {
		case "search_path":
			before[i] = []byte("leaked")
		case "held_cursor":
			before[i], after[i] = []byte("1"), []byte("0")
		case "app.tenant_id":
			before[i], after[i] = []byte("t1"), nil
		}
	}
	received := make(chan pgproto3.FrontendMessage, 1)
	conn := newSessionLinkTestPoolConnServing(t, serveReset(received, before, after))
	tags := []string{"database_cluster:test-cluster"}
	link, err := New(conn, Options{MetricTags: tags, Reset: policy})
	require.NoError(t, err)

	require.NoError(t, link.Release())

	msg := <-received
	reset := "RESET SESSION AUTHORIZATION; RESET ROLE; RESET ALL; CLOSE ALL; DISCARD SEQUENCES"
	require.Equal(t, &pgproto3.Query{String: policy.auditSQL + "; " + reset + "; " + policy.auditSQL}, msg,
		"the check and the reset share one round trip")
	modeTags := append(append([]string(nil), tags...), "reset_mode:audit")
	for _, setting := range []string{"search_path", "held_cursor", "app.tenant_id"} {
		counts, emitted := metrics.getCount("datadog.pg_proxy.sessions.connections.reset.would_leak", append(modeTags, "setting:"+setting))
		assert.True(t, emitted, setting)
		assert.Equal(t, []int64{1}, counts, setting)
	}
	_, emitted := metrics.getCount("datadog.pg_proxy.sessions.connections.reset.would_leak", append(modeTags, "setting:statement_timeout"))
	assert.False(t, emitted, "unchanged settings are not counted")
	counts, _ := metrics.getCount("datadog.pg_proxy.sessions.connections.reset", append(modeTags, "outcome:reset", "reason:audit"))
	assert.Equal(t, []int64{1}, counts)
}

func TestReleaseAuditCheckFailureClosesConnection(t *testing.T) {
	t.Cleanup(pgproxy.Bootstrap(pgproxy.Dependencies{Metrics: newRecordingMetrics()}))
	conn := newSessionLinkTestPoolConnServing(t, func(backend *pgproto3.Backend) {
		if _, err := backend.Receive(); err != nil {
			return
		}
		backend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "42501", Message: "permission denied"})
		backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		_ = backend.Flush()
		_, _ = backend.Receive()
	})
	pgxConn := conn.Conn()
	link, err := New(conn, Options{Reset: mustResetPolicy(t, config.ResetAudit)})
	require.NoError(t, err)

	require.Error(t, link.Release())
	assert.True(t, pgxConn.IsClosed(), "the failed check rolled back the reset, so the connection must not return to the pool")
}

func TestReceiveMarksParameterStatusAndCountsCrossCheckoutReads(t *testing.T) {
	metrics := newRecordingMetrics()
	t.Cleanup(pgproxy.Bootstrap(pgproxy.Dependencies{Metrics: metrics}))
	link, fakePg := newTestSessionLink()
	crossCheckoutRead := func() []int64 {
		counts, _ := metrics.getCount("datadog.pg_proxy.sessions.sequence_state.cross_checkout_read", []string{"reset_mode:always"})
		return counts
	}
	receive := func(msg pgproto3.BackendMessage) {
		fakePg.Send(msg)
		require.NoError(t, fakePg.Flush())
		_, err := link.Receive()
		require.NoError(t, err)
	}

	receive(&pgproto3.ParameterStatus{Name: "TimeZone", Value: "UTC"})
	assert.Equal(t, MarkParameterStatus, link.marks)

	sequenceError := &pgproto3.ErrorResponse{Severity: "ERROR", Code: "55000", Message: "lastval is not yet defined in this session"}
	receive(sequenceError)
	assert.Empty(t, crossCheckoutRead(), "only a checkout whose SQL may read sequence state counts")

	link.MarkSession(MarkReadsSequenceState)
	receive(sequenceError)
	assert.Equal(t, []int64{1}, crossCheckoutRead())
}
