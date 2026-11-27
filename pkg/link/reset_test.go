// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package link

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pgproxy "github.com/DataDog/pg-proxy"
	"github.com/DataDog/pg-proxy/internal/fakepg"
	"github.com/DataDog/pg-proxy/pkg/config"
	"github.com/DataDog/pg-proxy/pkg/query"
)

func mustResetPolicy(t *testing.T, mode config.ResetMode, auditSettings ...string) ResetPolicy {
	t.Helper()
	policy, err := NewResetPolicy(config.DatabaseSettings{ResetMode: mode, ResetAuditSettings: auditSettings})
	require.NoError(t, err)
	return policy
}

// auditEveryRelease returns an audit policy that checks every release
// ResetDynamic would skip, and full-checks them at fullCheckRate, 0 or 1.
func auditEveryRelease(t *testing.T, fullCheckRate float64, auditSettings ...string) ResetPolicy {
	t.Helper()
	policy := mustResetPolicy(t, config.ResetAudit, auditSettings...)
	policy.auditRate, policy.fullCheckRate = 1, fullCheckRate
	return policy
}

func TestResetDecision(t *testing.T) {
	tests := []struct {
		mode       config.ResetMode
		marks      SessionMarks
		wantAction resetAction
		wantReason string
	}{
		{config.ResetDefault, 0, resetSend, "always"},
		{config.ResetAlways, MarkClientSQL, resetSend, "always"},
		{config.ResetOff, MarkClientSQL, resetSkip, "off"},
		{config.ResetDynamic, 0, resetSkip, "clean"},
		{config.ResetDynamic, MarkClientSQL, resetSend, "client_sql"},
		{config.ResetDynamic, MarkPreparedStmt, resetSend, "prepared_stmt"},
		{config.ResetDynamic, MarkParameterStatus, resetSend, "parameter_status"},
		{config.ResetDynamic, MarkPreparedStmt | MarkClientSQL, resetSend, "client_sql"},
		{config.ResetAudit, MarkParameterStatus, resetSend, "parameter_status"},
	}
	for _, test := range tests {
		action, reason := mustResetPolicy(t, test.mode).resetDecision(test.marks, true)
		assert.Equal(t, test.wantAction, action, "mode %q marks %b", test.mode, test.marks)
		assert.Equal(t, test.wantReason, reason, "mode %q marks %b", test.mode, test.marks)
	}
	action, reason := ResetPolicy{}.resetDecision(MarkClientSQL, true)
	assert.Equal(t, resetSend, action, "the zero policy resets on every release")
	assert.Equal(t, "always", reason)

	// On a server that may have dropped the role, a clean checkout resets too.
	for _, test := range []struct {
		mode       config.ResetMode
		marks      SessionMarks
		wantAction resetAction
		wantReason string
	}{
		{config.ResetDynamic, 0, resetSend, "server_version"},
		{config.ResetAudit, 0, resetSend, "server_version"},
		{config.ResetDynamic, MarkClientSQL, resetSend, "client_sql"},
		{config.ResetOff, 0, resetSkip, "off"},
		{config.ResetAlways, 0, resetSend, "always"},
	} {
		action, reason := mustResetPolicy(t, test.mode).resetDecision(test.marks, false)
		assert.Equal(t, test.wantAction, action, "mode %q marks %b", test.mode, test.marks)
		assert.Equal(t, test.wantReason, reason, "mode %q marks %b", test.mode, test.marks)
	}
}

func TestResetDropsRole(t *testing.T) {
	for version, want := range map[string]bool{
		"17.0":                               true,
		"17.1":                               false,
		"17.11 (Ubuntu 17.11-1.pgdg24.04+2)": false,
		"17beta1":                            true,
		"16.4":                               true,
		"16.5":                               false,
		"15.8":                               true,
		"15.9":                               false,
		"14.0":                               true,
		"14.13":                              true,
		"14.14":                              false,
		"13.16":                              false, // reports session_authorization on every RESET
		"18.0":                               false,
		"19devel":                            false,
		"":                                   true,
		"unknown":                            true,
	} {
		assert.Equal(t, want, resetDropsRole(version), "server_version %q", version)
	}
}

func TestAuditSampling(t *testing.T) {
	policy := mustResetPolicy(t, config.ResetAudit)
	assert.Equal(t, defaultAuditRate, policy.auditRate)
	assert.Equal(t, defaultFullCheckRate, policy.fullCheckRate)
	custom, err := NewResetPolicy(config.DatabaseSettings{ResetMode: config.ResetAudit, ResetAuditRate: 0.25})
	require.NoError(t, err)
	assert.Equal(t, 0.25, custom.auditRate)

	for _, test := range []struct {
		auditRate, fullCheckRate float64
		want                     resetAction
	}{
		{0, 0, resetSend},
		{0, 1, resetSend}, // only an audited release runs the full check
		{1, 0, resetAudited},
		{1, 1, resetFullAudited},
	} {
		policy.auditRate, policy.fullCheckRate = test.auditRate, test.fullCheckRate
		action, reason := policy.resetDecision(0, true)
		assert.Equal(t, test.want, action, "audit rate %v, full-check rate %v", test.auditRate, test.fullCheckRate)
		assert.Equal(t, "audit", reason, "every release dynamic would skip counts as audit, checked or not")
	}

	// The full-check rate is a share of all the releases dynamic would skip, not
	// of the audited ones. Both bounds are more than five standard deviations wide.
	policy.auditRate, policy.fullCheckRate = defaultAuditRate, defaultFullCheckRate
	const draws = 100_000
	counts := map[resetAction]int{}
	for range draws {
		counts[policy.sampleAudit()]++
	}
	assert.InDelta(t, draws*(defaultAuditRate-defaultFullCheckRate), counts[resetAudited], 500)
	assert.InDelta(t, draws*defaultFullCheckRate, counts[resetFullAudited], 200)
}

func TestNewResetPolicyBuildsAuditQueryOnce(t *testing.T) {
	policy := mustResetPolicy(t, config.ResetAudit, "app.tenant_id", "app.tenant_id")
	assert.Equal(t, config.ResetAudit, policy.Mode())
	assert.True(t, strings.HasPrefix(policy.auditSQL, "SELECT pg_catalog.current_setting('search_path', true), "))
	assert.Contains(t, policy.auditSQL, "pg_catalog.current_setting('transaction_timeout', true)")
	assert.Contains(t, policy.auditSQL, "pg_catalog.current_setting('synchronous_commit', true)")
	assert.Contains(t, policy.auditSQL, ", current_user, ")
	assert.True(t, strings.HasSuffix(policy.auditSQL, ", pg_catalog.current_setting('app.tenant_id', true)"))
	assert.Equal(t, 1, strings.Count(policy.auditSQL, "app.tenant_id"), "duplicate names are checked once")
	assert.Len(t, policy.auditColumns, len(auditedSettings)+2)
	assert.Len(t, auditedSettings, 29)

	dynamic := mustResetPolicy(t, config.ResetDynamic)
	assert.Empty(t, dynamic.auditSQL, "only audit runs the check")
	assert.Zero(t, dynamic.auditRate)

	_, err := NewResetPolicy(config.DatabaseSettings{ResetMode: "sometimes"})
	assert.ErrorContains(t, err, `reset_mode: unknown mode "sometimes"`)
	_, err = NewResetPolicy(config.DatabaseSettings{ResetMode: config.ResetAudit, ResetAuditSettings: []string{"app.x'); SELECT 1; --"}})
	assert.ErrorContains(t, err, "reset_audit_settings[0]")
	_, err = NewResetPolicy(config.DatabaseSettings{ResetMode: config.ResetAudit, ResetAuditRate: 1.5})
	assert.ErrorContains(t, err, "reset_audit_rate: 1.5 is not between 0 and 1")
}

func TestChangedColumns(t *testing.T) {
	policy := ResetPolicy{&resetPolicy{auditColumns: []string{"search_path", "transaction_timeout", "synchronous_commit"}}}
	before := [][]byte{[]byte("app"), nil, []byte("off")}
	after := [][]byte{[]byte(`"$user", public`), nil, []byte("on")}
	assert.Equal(t, []string{"search_path", "synchronous_commit"}, policy.changedColumns(before, after))
	assert.Empty(t, policy.changedColumns(after, after))
	assert.Equal(t, []string{"transaction_timeout"}, policy.changedColumns(
		[][]byte{nil, []byte(""), nil}, [][]byte{nil, nil, nil}), "NULL and an empty string differ")
}

// serveReset answers the first message, a Query, with one result per statement,
// using rows for SELECT statements in order, and sends the message to received.
func serveReset(t *testing.T, received chan<- pgproto3.FrontendMessage, rows ...[][]byte) func(msg []byte) []byte {
	answered := false
	return func(msg []byte) []byte {
		if answered {
			return nil // the Terminate sent at pool close
		}
		answered = true
		decoded, err := fakepg.Decode(msg)
		require.NoError(t, err)
		received <- decoded
		q, ok := decoded.(*pgproto3.Query)
		if !ok {
			return nil
		}
		var reply []pgproto3.BackendMessage
		for _, statement := range strings.Split(q.String, "; ") {
			if strings.HasPrefix(statement, "SELECT ") && len(rows) > 0 {
				row := rows[0]
				rows = rows[1:]
				fields := make([]pgproto3.FieldDescription, len(row))
				for i := range fields {
					fields[i] = pgproto3.FieldDescription{Name: []byte("c"), DataTypeOID: 25}
				}
				reply = append(reply,
					&pgproto3.RowDescription{Fields: fields},
					&pgproto3.DataRow{Values: row},
					&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
				continue
			}
			reply = append(reply, &pgproto3.CommandComplete{CommandTag: []byte("RESET")})
		}
		return fakepg.Encode(t, append(reply, &pgproto3.ReadyForQuery{TxStatus: 'I'})...)
	}
}

func TestReleaseByMode(t *testing.T) {
	tests := []struct {
		mode          config.ResetMode
		marks         SessionMarks
		serverVersion string // empty: testServerVersion
		wantQuery     string // empty: no reset sent
		wantReason    string // empty: no reset metrics
	}{
		{config.ResetAlways, MarkClientSQL, "", testReset, ""},
		{config.ResetDynamic, 0, "", "", "clean"},
		{config.ResetDynamic, MarkClientSQL, "", testReset, "client_sql"},
		{config.ResetDynamic, 0, "16.4", testReset, "server_version"},
		{config.ResetOff, MarkClientSQL, "", "", "off"},
	}
	for _, test := range tests {
		t.Run(string(test.mode)+"/"+test.wantReason, func(t *testing.T) {
			metrics := newRecordingMetrics()
			t.Cleanup(pgproxy.Bootstrap(pgproxy.Dependencies{Metrics: metrics}))
			received := make(chan pgproto3.FrontendMessage, 1)
			serverVersion := test.serverVersion
			if serverVersion == "" {
				serverVersion = testServerVersion
			}
			conn := newSessionLinkTestPoolConnServingVersion(t, serverVersion, serveReset(t, received))
			state := setUpConnMetaData(conn)
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
			assert.Equal(t, test.wantQuery == "", state.resetSkipped, "a skipped reset marks the connection")
			if test.wantReason == "" {
				assert.Empty(t, metrics.counts, "always reports nothing, as before reset_mode")
				assert.Empty(t, metrics.distributions)
				return
			}
			outcome := "outcome:reset"
			if test.wantQuery == "" {
				outcome = "outcome:skipped"
			}
			counts, emitted := metrics.getCount("datadog.pg_proxy.sessions.connections.reset",
				append(tags, "reset_mode:"+string(test.mode), outcome, "reason:"+test.wantReason))
			assert.True(t, emitted)
			assert.Equal(t, []int64{1}, counts)
			wantLatencies := 0
			if test.wantQuery != "" {
				wantLatencies = 1
			}
			assert.Equal(t, wantLatencies, metrics.distributions["datadog."+pgproxy.MetricResetLatency])
		})
	}
}

// TestReleaseWithRoleSessionOnServerThatDropsRole: a RoleSession restores the
// role on every release, so ResetDynamic still skips a clean checkout's reset on
// a server that may drop it.
func TestReleaseWithRoleSessionOnServerThatDropsRole(t *testing.T) {
	received := make(chan pgproto3.FrontendMessage, 1)
	conn := newSessionLinkTestPoolConnServingVersion(t, "16.4", serveReset(t, received))
	link, err := New(conn, Options{Role: noopRoleSession{}, Reset: mustResetPolicy(t, config.ResetDynamic)})
	require.NoError(t, err)

	require.NoError(t, link.Release())
	select {
	case msg := <-received:
		t.Fatalf("sent %#v, want no reset", msg)
	default:
	}
}

// TestReleaseClearsSkippedReset checks the mark the next link reads: New takes it
// over, and a release that runs the reset discards the unnamed statement, so it
// leaves the mark cleared in every mode.
func TestReleaseClearsSkippedReset(t *testing.T) {
	for _, mode := range []config.ResetMode{config.ResetDynamic, config.ResetAlways} {
		t.Run(string(mode), func(t *testing.T) {
			t.Cleanup(pgproxy.Bootstrap(pgproxy.Dependencies{Metrics: newRecordingMetrics()}))
			received := make(chan pgproto3.FrontendMessage, 1)
			conn := newSessionLinkTestPoolConnServing(t, serveReset(t, received))
			state := setUpConnMetaData(conn)
			state.resetSkipped = true
			link, err := New(conn, Options{Reset: mustResetPolicy(t, mode)})
			require.NoError(t, err)
			assert.True(t, link.(*SessionLink).foreignUnnamed, "New reads the mark the last release left")
			link.MarkSession(MarkClientSQL)

			require.NoError(t, link.Release())

			require.Equal(t, &pgproto3.Query{String: testReset}, <-received)
			assert.False(t, state.resetSkipped)
		})
	}
}

// TestFirstSendClosesForeignUnnamedStmt: on a connection whose last release
// skipped the reset, the first request goes after a Close of the unnamed
// statement, unless it is a simple Query, which drops the statement itself. A
// Flush expects no reply and leaves the Close to the request after it.
func TestFirstSendClosesForeignUnnamedStmt(t *testing.T) {
	for _, test := range []struct {
		first       pgproto3.FrontendMessage
		expectation query.ReplyExpectation
		wantClose   bool
	}{
		{&pgproto3.Bind{}, query.BindReply, true},
		{&pgproto3.Parse{Name: "named", Query: "SELECT 1"}, query.ParseReply, true},
		{&pgproto3.Sync{}, query.SyncReply, true},
		{&pgproto3.Query{String: "SELECT 1"}, query.QueryReply, false},
	} {
		link, backend := newTestSessionLink()
		link.foreignUnnamed = true
		link.Send(&pgproto3.Flush{}, query.NoReply)
		link.Send(test.first, test.expectation)
		link.Send(&pgproto3.Bind{}, query.BindReply)
		require.NoError(t, link.Flush())

		want := []query.ReplyExpectation{test.expectation, query.BindReply}
		wantSent := []pgproto3.FrontendMessage{&pgproto3.Flush{}, test.first}
		if test.wantClose {
			want = append([]query.ReplyExpectation{query.ProxyCloseReply}, want...)
			wantSent = []pgproto3.FrontendMessage{&pgproto3.Flush{}, &pgproto3.Close{ObjectType: 'S'}, test.first}
		}
		assert.Equal(t, want, link.replyExpectations, "%T first", test.first)
		for _, msg := range wantSent {
			got, err := backend.Receive()
			require.NoError(t, err)
			assert.IsType(t, msg, got, "%T first", test.first)
		}
	}

	link, _ := newTestSessionLink()
	link.Send(&pgproto3.Bind{}, query.BindReply)
	assert.Equal(t, []query.ReplyExpectation{query.BindReply}, link.replyExpectations, "a connection whose last release reset")
}

func TestUsesSessionMarks(t *testing.T) {
	for mode, want := range map[config.ResetMode]bool{
		config.ResetDefault: false,
		config.ResetAlways:  false,
		config.ResetAudit:   true,
		config.ResetDynamic: true,
		config.ResetOff:     false,
	} {
		link := &SessionLink{reset: mustResetPolicy(t, mode)}
		assert.Equal(t, want, link.UsesSessionMarks(), "mode %q", mode)
	}
}

// auditRows returns two audit rows for policy, both "baseline" except for the
// columns in changed, which the first row holds instead.
func auditRows(policy ResetPolicy, changed map[string][]byte) (before, baseline [][]byte) {
	before = make([][]byte, len(policy.auditColumns))
	baseline = make([][]byte, len(policy.auditColumns))
	for i, column := range policy.auditColumns {
		before[i], baseline[i] = []byte("baseline"), []byte("baseline")
		if value, ok := changed[column]; ok {
			before[i] = value
		}
	}
	return before, baseline
}

const testReset = "RESET SESSION AUTHORIZATION; RESET ROLE; RESET ALL"

func wouldLeak(metrics *recordingMetrics, setting, check string) []int64 {
	counts, _ := metrics.getCount("datadog.pg_proxy.sessions.connections.reset.would_leak",
		[]string{"database_cluster:test-cluster", "reset_mode:audit", "setting:" + setting, "check:" + check})
	return counts
}

func TestReleaseAuditUnsampledSendsPlainReset(t *testing.T) {
	metrics := newRecordingMetrics()
	t.Cleanup(pgproxy.Bootstrap(pgproxy.Dependencies{Metrics: metrics}))
	policy := mustResetPolicy(t, config.ResetAudit)
	policy.auditRate = 0
	received := make(chan pgproto3.FrontendMessage, 1)
	conn := newSessionLinkTestPoolConnServing(t, serveReset(t, received))
	link, err := New(conn, Options{MetricTags: []string{"database_cluster:test-cluster"}, Reset: policy})
	require.NoError(t, err)

	require.NoError(t, link.Release())

	require.Equal(t, &pgproto3.Query{String: testReset}, <-received, "a release outside the sample gets the plain reset")
	counts, _ := metrics.getCount("datadog.pg_proxy.sessions.connections.reset",
		[]string{"database_cluster:test-cluster", "reset_mode:audit", "outcome:reset", "reason:audit", "check:none"})
	assert.Equal(t, []int64{1}, counts)
}

func TestReleaseAuditReadsBaselineOnFirstUse(t *testing.T) {
	metrics := newRecordingMetrics()
	t.Cleanup(pgproxy.Bootstrap(pgproxy.Dependencies{Metrics: metrics}))
	policy := auditEveryRelease(t, 0, "app.tenant_id")

	// A function changed search_path and app.tenant_id; the reset restores both.
	before, after := auditRows(policy, map[string][]byte{"search_path": []byte("leaked"), "app.tenant_id": []byte("t1")})
	received := make(chan pgproto3.FrontendMessage, 1)
	conn := newSessionLinkTestPoolConnServing(t, serveReset(t, received, before, after))
	state := setUpConnMetaData(conn)
	link, err := New(conn, Options{MetricTags: []string{"database_cluster:test-cluster"}, Reset: policy})
	require.NoError(t, err)

	require.NoError(t, link.Release())

	require.Equal(t, &pgproto3.Query{String: policy.auditSQL + "; " + testReset + "; " + policy.auditSQL}, <-received,
		"a connection without a baseline reads the settings after the reset too, in the same round trip")
	for _, setting := range []string{"search_path", "app.tenant_id"} {
		assert.Equal(t, []int64{1}, wouldLeak(metrics, setting, "list"), setting)
	}
	assert.Empty(t, wouldLeak(metrics, "statement_timeout", "list"), "unchanged settings are not counted")
	assert.Equal(t, auditBaseline{sql: policy.auditSQL, values: after}, state.auditBaseline,
		"the row read after the reset becomes the connection's baseline")
	counts, _ := metrics.getCount("datadog.pg_proxy.sessions.connections.reset",
		[]string{"database_cluster:test-cluster", "reset_mode:audit", "outcome:reset", "reason:audit", "check:list"})
	assert.Equal(t, []int64{1}, counts)
}

func TestReleaseAuditComparesWithBaseline(t *testing.T) {
	for _, test := range []struct {
		name         string
		changed      map[string][]byte
		keepBaseline bool
	}{
		{"clean", nil, true},
		{"leaked", map[string][]byte{"synchronous_commit": []byte("off")}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			metrics := newRecordingMetrics()
			t.Cleanup(pgproxy.Bootstrap(pgproxy.Dependencies{Metrics: metrics}))
			policy := auditEveryRelease(t, 0)
			before, baseline := auditRows(policy, test.changed)
			received := make(chan pgproto3.FrontendMessage, 1)
			conn := newSessionLinkTestPoolConnServing(t, serveReset(t, received, before))
			state := setUpConnMetaData(conn)
			state.auditBaseline = auditBaseline{sql: policy.auditSQL, values: baseline}
			link, err := New(conn, Options{MetricTags: []string{"database_cluster:test-cluster"}, Reset: policy})
			require.NoError(t, err)

			require.NoError(t, link.Release())

			require.Equal(t, &pgproto3.Query{String: policy.auditSQL + "; " + testReset}, <-received,
				"with a baseline the settings are read only before the reset")
			for setting := range test.changed {
				assert.Equal(t, []int64{1}, wouldLeak(metrics, setting, "list"), setting)
			}
			kept := state.auditBaseline.sql == policy.auditSQL
			assert.Equal(t, test.keepBaseline, kept, "a change drops the baseline, so the next check reads it again")
		})
	}
}

func TestReleaseAuditSampledFullCheck(t *testing.T) {
	metrics := newRecordingMetrics()
	var logs bytes.Buffer
	t.Cleanup(pgproxy.Bootstrap(pgproxy.Dependencies{Metrics: metrics, LogHandler: slog.NewTextHandler(&logs, nil)}))
	policy := auditEveryRelease(t, 1)
	before, baseline := auditRows(policy, map[string][]byte{"synchronous_commit": []byte("off")})
	full := [][]byte{[]byte("synchronous_commit,work_mem")}
	received := make(chan pgproto3.FrontendMessage, 1)
	conn := newSessionLinkTestPoolConnServing(t, serveReset(t, received, full, before))
	setUpConnMetaData(conn).auditBaseline = auditBaseline{sql: policy.auditSQL, values: baseline}
	link, err := New(conn, Options{MetricTags: []string{"database_cluster:test-cluster"}, Reset: policy})
	require.NoError(t, err)

	require.NoError(t, link.Release())

	require.Equal(t, &pgproto3.Query{String: fullCheckSQL + "; " + policy.auditSQL + "; " + testReset}, <-received,
		"the full check runs first, in the same round trip, while the checkout's settings are still in place")
	assert.Equal(t, []int64{1}, wouldLeak(metrics, "synchronous_commit", "list"))
	assert.Equal(t, []int64{1}, wouldLeak(metrics, "synchronous_commit", "full"))
	assert.Equal(t, []int64{1}, wouldLeak(metrics, "work_mem", "full"))
	assert.Empty(t, wouldLeak(metrics, "work_mem", "list"), "work_mem is not on the list")
	counts, _ := metrics.getCount("datadog.pg_proxy.sessions.connections.reset",
		[]string{"database_cluster:test-cluster", "reset_mode:audit", "outcome:reset", "reason:audit", "check:full"})
	assert.Equal(t, []int64{1}, counts)
	assert.Contains(t, logs.String(), "settings=[synchronous_commit] unlisted_settings=[work_mem]")
	assert.NotContains(t, logs.String(), "off", "values are never logged")
}

func TestReleaseAuditFullCheckFindingNothing(t *testing.T) {
	metrics := newRecordingMetrics()
	t.Cleanup(pgproxy.Bootstrap(pgproxy.Dependencies{Metrics: metrics}))
	policy := auditEveryRelease(t, 1)
	before, baseline := auditRows(policy, nil)
	received := make(chan pgproto3.FrontendMessage, 1)
	conn := newSessionLinkTestPoolConnServing(t, serveReset(t, received, [][]byte{nil}, before))
	setUpConnMetaData(conn).auditBaseline = auditBaseline{sql: policy.auditSQL, values: baseline}
	link, err := New(conn, Options{MetricTags: []string{"database_cluster:test-cluster"}, Reset: policy})
	require.NoError(t, err)

	require.NoError(t, link.Release())

	<-received
	counts, _ := metrics.getCount("datadog.pg_proxy.sessions.connections.reset",
		[]string{"database_cluster:test-cluster", "reset_mode:audit", "outcome:reset", "reason:audit", "check:full"})
	assert.Equal(t, []int64{1}, counts)
	_, emitted := metrics.counts["datadog.pg_proxy.sessions.connections.reset.would_leak"]
	assert.False(t, emitted, "a NULL full check names no setting")
}

func TestReleaseAuditCheckFailureClosesConnection(t *testing.T) {
	t.Cleanup(pgproxy.Bootstrap(pgproxy.Dependencies{Metrics: newRecordingMetrics()}))
	conn := newSessionLinkTestPoolConnServing(t, func(msg []byte) []byte {
		if msg[0] != 'Q' {
			return nil // the Terminate sent when Release closes the connection
		}
		return fakepg.Encode[pgproto3.BackendMessage](t,
			&pgproto3.ErrorResponse{Severity: "ERROR", Code: "42501", Message: "permission denied"},
			&pgproto3.ReadyForQuery{TxStatus: 'I'})
	})
	pgxConn := conn.Conn()
	link, err := New(conn, Options{Reset: auditEveryRelease(t, 0)})
	require.NoError(t, err)

	require.Error(t, link.Release())
	assert.True(t, pgxConn.IsClosed(), "the failed check rolled back the reset, so the connection must not return to the pool")
}

func TestReceiveMarksParameterStatus(t *testing.T) {
	link, fakePg := newTestSessionLink()
	receive := func(msg pgproto3.BackendMessage) {
		fakePg.Send(msg)
		require.NoError(t, fakePg.Flush())
		_, err := link.Receive()
		require.NoError(t, err)
	}

	receive(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
	assert.Zero(t, link.marks)
	receive(&pgproto3.ParameterStatus{Name: "TimeZone", Value: "UTC"})
	assert.Equal(t, MarkParameterStatus, link.marks)
}
