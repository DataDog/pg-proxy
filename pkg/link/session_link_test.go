// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package link

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pgproxy "github.com/DataDog/pg-proxy"
	"github.com/DataDog/pg-proxy/internal/fakepg"
	"github.com/DataDog/pg-proxy/pkg/prepared"
	"github.com/DataDog/pg-proxy/pkg/query"
)

// recordingMetrics is a minimal pgproxy.MetricsClient that records Count, Gauge,
// and Distribution calls.
type recordingMetrics struct {
	pgproxy.NoopMetrics
	mu            sync.Mutex
	counts        map[string]map[string][]int64 // metric -> tagsKey -> values
	gauges        map[string]float64            // metric + tagsKey -> latest value
	distributions map[string]int                // metric -> number of values
}

func newRecordingMetrics() *recordingMetrics {
	return &recordingMetrics{
		counts:        make(map[string]map[string][]int64),
		gauges:        make(map[string]float64),
		distributions: make(map[string]int),
	}
}

func (m *recordingMetrics) Distribution(name string, _ float64, _ []string, _ float64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.distributions[name]++
	return nil
}

func tagsKey(tags []string) string {
	return fmtTags(tags)
}

func fmtTags(tags []string) string {
	out := ""
	for i, t := range tags {
		if i > 0 {
			out += ","
		}
		out += t
	}
	return out
}

func (m *recordingMetrics) Count(name string, value int64, tags []string, rate float64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.counts[name] == nil {
		m.counts[name] = make(map[string][]int64)
	}
	key := tagsKey(tags)
	m.counts[name][key] = append(m.counts[name][key], value)
	return nil
}

func (m *recordingMetrics) Gauge(name string, value float64, tags []string, rate float64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gauges[name+"|"+tagsKey(tags)] = value
	return nil
}

// lastGauge returns the most recent value reported for name and tags.
func (m *recordingMetrics) lastGauge(name string, tags []string) (value float64, ok bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	value, ok = m.gauges[name+"|"+tagsKey(tags)]
	return value, ok
}

func (m *recordingMetrics) getCount(name string, tags []string) (values []int64, ok bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	byTags, ok := m.counts[name]
	if !ok {
		return nil, false
	}
	values, ok = byTags[tagsKey(tags)]
	return values, ok
}

func TestReleaseNonIdleCountsDestroyedConnection(t *testing.T) {
	metrics := newRecordingMetrics()
	restore := pgproxy.Bootstrap(pgproxy.Dependencies{Metrics: metrics})
	t.Cleanup(restore)

	tests := []struct {
		name      string
		state     string
		configure func(*SessionLink)
	}{
		{
			name:  "transaction open",
			state: sessionLinkStateTxOpen,
			configure: func(link *SessionLink) {
				link.txStatus = query.InTransaction
				link.discardingUntilSync = true
				link.replyExpectations = []query.ReplyExpectation{query.QueryReply}
			},
		},
		{
			name:  "discarding",
			state: sessionLinkStateDiscarding,
			configure: func(link *SessionLink) {
				link.discardingUntilSync = true
				link.replyExpectations = []query.ReplyExpectation{query.QueryReply}
			},
		},
		{
			name:  "expectations pending",
			state: sessionLinkStateExpectationsPending,
			configure: func(link *SessionLink) {
				link.replyExpectations = []query.ReplyExpectation{query.QueryReply}
			},
		},
		{
			name:  "no ready for query",
			state: sessionLinkStateNoReadyForQuery,
			configure: func(link *SessionLink) {
				link.readyForQuery = false
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sessionLinkTags := make([]string, 2, 4)
			copy(sessionLinkTags, []string{"session:test-session", "database_cluster:test-cluster"})
			link := &SessionLink{
				conn:                       newSessionLinkTestPoolConn(t),
				txStatus:                   query.Idle,
				readyForQuery:              true,
				namedPreparedStmtThreshold: 0,
				sessionLinkTags:            sessionLinkTags,
			}
			assert.Equal(t, sessionLinkStateIdle, link.getComputedState())
			test.configure(link)
			assert.Equal(t, test.state, link.getComputedState())

			require.NoError(t, link.Release())

			tags := []string{
				"session:test-session",
				"database_cluster:test-cluster",
				"state:" + test.state,
			}
			counts, emitted := metrics.getCount("datadog.pg_proxy.sessions.connections.destroyed.release_non_idle", tags)
			assert.True(t, emitted)
			assert.Equal(t, []int64{1}, counts)
			_, emitted = metrics.getCount("datadog.pg_proxy.sessions.connections.destroyed", tags)
			assert.False(t, emitted, "release-state telemetry must not double-count the destroyed-connections metric")
			assert.Equal(t, []string{"session:test-session", "database_cluster:test-cluster", "", ""}, sessionLinkTags[:cap(sessionLinkTags)])
		})
	}
}

func TestNonIdleReleaseLogMessageIncludesTagsWithoutToken(t *testing.T) {
	link := &SessionLink{
		sessionLinkTags: []string{"user:test-user", "database_cluster:test-cluster"},
		token:           "secret-token",
	}

	message := link.nonIdleReleaseLogMessage(sessionLinkStateNoReadyForQuery, 42)

	assert.Contains(t, message, `state: no_ready_for_query, backend PID: 42, expectations: ""`)
	assert.Contains(t, message, `tags: ["user:test-user" "database_cluster:test-cluster"]`)
	assert.NotContains(t, message, link.token)
}

// blockingRoleSession hangs on Reset so a Release that waits forever proves SQL was issued.
type blockingRoleSession struct{}

func (blockingRoleSession) Set(context.Context, *pgxpool.Conn, []string) (string, error) {
	return "test-token", nil
}

func (blockingRoleSession) Reset(context.Context, *pgxpool.Conn, string, []string) error {
	select {} // hang — Release must not reach here after destroying a non-idle link
}

type emptyTokenRoleSession struct {
	resetCalled bool
	resetToken  string
}

func (role *emptyTokenRoleSession) Set(context.Context, *pgxpool.Conn, []string) (string, error) {
	return "", nil
}

func (role *emptyTokenRoleSession) Reset(_ context.Context, _ *pgxpool.Conn, token string, _ []string) error {
	role.resetCalled = true
	role.resetToken = token
	return context.Canceled
}

func TestReleaseResetsRoleAfterSetReturnsEmptyToken(t *testing.T) {
	role := &emptyTokenRoleSession{}
	link, err := New(newSessionLinkTestPoolConn(t), Options{Role: role})
	require.NoError(t, err)

	err = link.Release()

	require.ErrorIs(t, err, context.Canceled)
	assert.True(t, role.resetCalled)
	assert.Empty(t, role.resetToken)
}

// TestReleaseNonIdleDestroysBeforeRoleReset pins the ordering inside Release: a link
// that is not at a reusable command boundary must be destroyed, and counted, before any
// SQL is issued on it.
func TestReleaseNonIdleDestroysBeforeRoleReset(t *testing.T) {
	metrics := newRecordingMetrics()
	restore := pgproxy.Bootstrap(pgproxy.Dependencies{Metrics: metrics})
	t.Cleanup(restore)

	link := &SessionLink{
		conn:                       newSessionLinkTestPoolConn(t),
		txStatus:                   query.Idle,
		readyForQuery:              false, // mid-COPY or an extended batch that ended in Flush
		namedPreparedStmtThreshold: 0,
		sessionLinkTags:            []string{"database_cluster:test-cluster"},
		role:                       blockingRoleSession{},
		token:                      "test-token",
	}
	require.Equal(t, sessionLinkStateNoReadyForQuery, link.getComputedState())

	// The test backend answers nothing after the startup handshake, so a query Release
	// issued would fail and Release would return an error, and blockingRoleSession's
	// Reset never returns. A Release that returns promptly and cleanly is therefore
	// the assertion that it issued nothing.
	released := make(chan error, 1)
	go func() { released <- link.Release() }()

	select {
	case err := <-released:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Release issued SQL on a link it had already decided to destroy")
	}

	tags := []string{"database_cluster:test-cluster", "state:" + sessionLinkStateNoReadyForQuery}
	counts, emitted := metrics.getCount("datadog.pg_proxy.sessions.connections.destroyed.release_non_idle", tags)
	assert.True(t, emitted, "a destroyed passthrough-role backend must still be counted")
	assert.Equal(t, []int64{1}, counts)
	_, emitted = metrics.getCount("datadog.pg_proxy.sessions.connections.resetrole", []string{"database_cluster:test-cluster", "outcome:error"})
	assert.False(t, emitted, "no role reset should be attempted on a backend being destroyed")
}

// noopRoleSession manages the role without issuing SQL.
type noopRoleSession struct{}

func (noopRoleSession) Set(context.Context, *pgxpool.Conn, []string) (string, error) {
	return "test-token", nil
}

func (noopRoleSession) Reset(context.Context, *pgxpool.Conn, string, []string) error {
	return nil
}

// TestReleaseResetSQL pins the statement Release runs on an idle link. Without a
// RoleSession, a client's SET ROLE or SET SESSION AUTHORIZATION must not reach the
// next client, and RESET ALL does not cover either. With one, the RoleSession owns
// the role, so only GUCs are reset.
//
// It also pins the cost: releasing a link with no excess prepared statements to
// deallocate sends one reset query and nothing else, so a client statement costs
// at most one reset round trip. The session package's
// TestProtocolCycleRoundTripBudgets counts one release per protocol cycle.
func TestReleaseResetSQL(t *testing.T) {
	tests := []struct {
		name string
		role RoleSession
		want string
	}{
		{name: "without role management", want: "RESET SESSION AUTHORIZATION; RESET ROLE; RESET ALL"},
		{name: "with role management", role: noopRoleSession{}, want: "RESET ALL"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// The fake records each message as Release writes it, so a second message is
			// recorded too, and answers it, so it cannot hang Release.
			var sent []pgproto3.FrontendMessage
			conn := newSessionLinkTestPoolConnServing(t, func(msg []byte) []byte {
				decoded, err := fakepg.Decode(msg)
				require.NoError(t, err)
				sent = append(sent, decoded)
				return fakepg.Answer(msg)
			})
			link, err := New(conn, Options{Role: test.role})
			require.NoError(t, err)

			releaseErr := link.Release()
			require.Equal(t, []pgproto3.FrontendMessage{&pgproto3.Query{String: test.want}}, sent,
				"Release must reset the connection with exactly one simple query")
			require.NoError(t, releaseErr)
		})
	}
}

// TestReleaseResetFailureClosesConnection verifies that a failed reset counts
// guc_reset_failure and closes the connection instead of returning it to the pool.
func TestReleaseResetFailureClosesConnection(t *testing.T) {
	metrics := newRecordingMetrics()
	restore := pgproxy.Bootstrap(pgproxy.Dependencies{Metrics: metrics})
	t.Cleanup(restore)

	resetFailed := fakepg.Encode[pgproto3.BackendMessage](t,
		&pgproto3.ErrorResponse{Severity: "ERROR", Code: "XX000", Message: "reset failed"},
		&pgproto3.ReadyForQuery{TxStatus: 'I'})
	conn := newSessionLinkTestPoolConnServing(t, func(msg []byte) []byte {
		if msg[0] == 'Q' {
			return resetFailed
		}
		return nil // the Terminate sent when Release closes the connection
	})
	pgxConn := conn.Conn()
	tags := []string{"database_cluster:test-cluster"}
	link, err := New(conn, Options{MetricTags: tags})
	require.NoError(t, err)

	require.Error(t, link.Release())

	assert.True(t, pgxConn.IsClosed(), "a connection whose reset failed must not return to the pool")
	counts, emitted := metrics.getCount("datadog.pg_proxy.sessions.connections.guc_reset_failure", tags)
	assert.True(t, emitted)
	assert.Equal(t, []int64{1}, counts)
}

// newSessionLinkTestPoolConn returns a pool conn whose fake Postgres answers
// nothing after the startup handshake, so any query on it fails at once.
func newSessionLinkTestPoolConn(t *testing.T) *pgxpool.Conn {
	t.Helper()
	return newSessionLinkTestPoolConnServing(t, nil)
}

// newSessionLinkTestPoolConnServing returns a pool conn whose fake Postgres reports
// testServerVersion, passes each message to respond, and queues the reply it returns.
func newSessionLinkTestPoolConnServing(t *testing.T, respond func(msg []byte) []byte) *pgxpool.Conn {
	t.Helper()
	return newSessionLinkTestPoolConnServingVersion(t, testServerVersion, respond)
}

// testServerVersion is the server_version the fake Postgres reports by default:
// a release that keeps the connection's role (see resetDropsRole).
const testServerVersion = "17.6"

// newSessionLinkTestPoolConnServingVersion is newSessionLinkTestPoolConnServing with
// a fake Postgres that reports serverVersion.
func newSessionLinkTestPoolConnServingVersion(t *testing.T, serverVersion string, respond func(msg []byte) []byte) *pgxpool.Conn {
	t.Helper()
	server := fakepg.NewServer(respond)
	server.ServerVersion = serverVersion
	conn, err := fakepg.NewPool(t, server).Acquire(context.Background())
	require.NoError(t, err)
	t.Cleanup(conn.Release) // a no-op once the test releases it; keeps a failure from hanging the pool's cleanup
	return conn
}

func TestSendSimple(t *testing.T) {
	assert := assert.New(t)
	link, fakePg := newTestSessionLink()

	assert.Equal(query.NoReply, link.NextExpectation())
	assert.True(link.Idle())

	link.Send(&pgproto3.Query{String: "SELECT 1"}, query.QueryReply)
	link.Send(&pgproto3.Query{String: "SELECT 2"}, query.QueryReply)
	assert.False(link.Idle())
	assert.NoError(link.Flush())
	assert.False(link.Idle())

	msg, err := fakePg.Receive()
	assert.EqualValues(&pgproto3.Query{String: "SELECT 1"}, msg)
	assert.NoError(err)

	assert.Equal(query.QueryReply, link.NextExpectation())
	assert.False(link.Idle())

	link.ResolveNextExpectation()
	assert.Equal(query.QueryReply, link.NextExpectation())
	assert.False(link.Idle())

	link.ResolveNextExpectation()
	assert.Equal(query.NoReply, link.NextExpectation())
	assert.False(link.Idle())
	link.SetTxStatus(query.Idle)
	assert.False(link.Idle(), "setting transaction status alone must not imply ReadyForQuery")
	link.HandleReadyForQuery(query.Idle)
	assert.True(link.Idle())
}

func TestSendPipeline(t *testing.T) {
	assert := assert.New(t)
	link, fakePg := newTestSessionLink()

	assert.Equal(query.NoReply, link.NextExpectation())
	assert.True(link.Idle())

	pipeline := []struct {
		msg      pgproto3.FrontendMessage
		replyExp query.ReplyExpectation
	}{
		{&pgproto3.Query{String: "SELECT 1"}, query.QueryReply},
		{&pgproto3.Query{String: "SELECT 2"}, query.QueryReply},
		{&pgproto3.Parse{Query: "SELECT 3"}, query.ParseReply},
		{&pgproto3.Bind{ResultFormatCodes: []int16{}}, query.BindReply},
		{&pgproto3.Execute{}, query.ExecuteReply},
		{&pgproto3.Sync{}, query.SyncReply},
	}

	for _, send := range pipeline {
		link.Send(send.msg, send.replyExp)
	}
	assert.NoError(link.Flush())

	assert.Equal(
		"QueryReply->QueryReply->ParseReply->BindReply->ExecuteReply->SyncReply",
		link.DebugExpectationString())

	for _, want := range pipeline {
		msg, err := fakePg.Receive()
		assert.EqualValues(want.msg, msg)
		assert.NoError(err)

		assert.False(link.Idle())
		assert.Equal(want.replyExp, link.NextExpectation())
		link.ResolveNextExpectation()
	}

	assert.False(link.Idle())
	link.HandleReadyForQuery(query.Idle)
	assert.True(link.Idle())
	assert.Equal(query.NoReply, link.NextExpectation())
}

// In this test, we encounter an extended query error, and drop
// all expectations until the Sync (as per Postgres protocol)
func TestDiscardUntilSyncEnqueued(t *testing.T) {
	assert := assert.New(t)
	link, _ := newTestSessionLink()

	assert.Equal(query.NoReply, link.NextExpectation())
	assert.True(link.Idle())

	link.Send(&pgproto3.Parse{Query: "syntax error"}, query.ParseReply)
	link.Send(&pgproto3.Bind{}, query.BindReply)
	link.Send(&pgproto3.Describe{}, query.DescribeReply)
	link.Send(&pgproto3.Execute{}, query.ExecuteReply)
	link.Send(&pgproto3.Sync{}, query.SyncReply)
	link.Send(&pgproto3.Parse{Query: "SELECT 1"}, query.ParseReply)

	assert.Equal(
		"ParseReply->BindReply->DescribeReply->ExecuteReply->SyncReply->ParseReply",
		link.DebugExpectationString())
	assert.Equal(query.ParseReply, link.NextExpectation())

	link.DiscardUntilSync()

	assert.Equal("SyncReply->ParseReply", link.DebugExpectationString())
	assert.Equal(query.SyncReply, link.NextExpectation())
}

// In this test, we encounter an extended query error, and
// need to drop all expectations until a Sync. However, we
// don't have any Sync enqueued so we need to maintain state
// until we see that Sync.
func TestDiscardUntilSyncInFuture(t *testing.T) {
	assert := assert.New(t)
	link, _ := newTestSessionLink()

	assert.Equal(query.NoReply, link.NextExpectation())
	assert.True(link.Idle())

	link.Send(&pgproto3.Parse{Query: "syntax error"}, query.ParseReply)
	link.Send(&pgproto3.Bind{}, query.BindReply)
	link.Send(&pgproto3.Describe{}, query.DescribeReply)

	assert.Equal(
		"ParseReply->BindReply->DescribeReply",
		link.DebugExpectationString())
	assert.Equal(query.ParseReply, link.NextExpectation())
	assert.False(link.Idle())

	link.DiscardUntilSync()

	assert.Equal("[DiscardingUntilSync]", link.DebugExpectationString())
	assert.Equal(query.NoReply, link.NextExpectation())
	assert.False(link.Idle())

	// This will be discarded
	link.Send(&pgproto3.Execute{}, query.ExecuteReply)
	assert.Equal("[DiscardingUntilSync]", link.DebugExpectationString())
	assert.Equal(query.NoReply, link.NextExpectation())

	// As will a simple query
	link.Send(&pgproto3.Query{}, query.QueryReply)
	assert.Equal("[DiscardingUntilSync]", link.DebugExpectationString())
	assert.Equal(query.NoReply, link.NextExpectation())

	// But a Sync will return the connection to health and receive a reply
	link.Send(&pgproto3.Sync{}, query.SyncReply)
	assert.Equal("SyncReply", link.DebugExpectationString())
	assert.Equal(query.SyncReply, link.NextExpectation())
	assert.False(link.Idle())

	// And after the Sync reply, the connection is now idle and healthy
	link.ResolveNextExpectation()
	assert.False(link.Idle())
	link.HandleReadyForQuery(query.Idle)
	assert.True(link.Idle())
}

func TestReceive(t *testing.T) {
	assert := assert.New(t)
	link, fakePg := newTestSessionLink()

	msg := &pgproto3.NoticeResponse{Message: "testing"}
	fakePg.Send(msg)
	fakePg.Flush()
	resp, err := link.Receive()
	assert.EqualValues(msg, resp)
	assert.NoError(err)
}

func newTestSessionLink() (*SessionLink, *pgproto3.Backend) {
	fe2be := newBlockingBuffer()
	be2fe := newBlockingBuffer()
	fe := pgproto3.NewFrontend(be2fe, fe2be)
	be := pgproto3.NewBackend(fe2be, be2fe)
	return NewFrontendLink(fe), be
}

// blockingBuffer is a minimal io.ReadWriter queue for Frontend/Backend pairs in tests.
type blockingBuffer struct {
	ch chan []byte
}

func newBlockingBuffer() *blockingBuffer {
	return &blockingBuffer{ch: make(chan []byte, 1)}
}

func (bb *blockingBuffer) Read(p []byte) (int, error) {
	res := <-bb.ch
	n := copy(p, res)
	return n, nil
}

func (bb *blockingBuffer) Write(p []byte) (int, error) {
	cp := make([]byte, len(p))
	copy(cp, p)
	bb.ch <- cp
	return len(p), nil
}

// TestCancelAndForceCloseDoNothingOnceReleaseStarts pins the Link contract that
// lets sessions call Cancel and ForceClose without a lock: once Release starts,
// neither may touch the connection, which is back in the pool afterwards.
func TestCancelAndForceCloseDoNothingOnceReleaseStarts(t *testing.T) {
	resetReceived := make(chan struct{})
	replyToReset := make(chan struct{})
	held := false
	conn := newSessionLinkTestPoolConnServing(t, func(msg []byte) []byte {
		// The first message is the reset. Holding its reply inside Release's write
		// keeps Release running.
		if !held {
			held = true
			close(resetReceived)
			<-replyToReset
		}
		return fakepg.Answer(msg)
	})
	pooled := conn.Conn()
	link, err := New(conn, Options{})
	require.NoError(t, err)

	var releaseErr error
	released := make(chan struct{})
	go func() {
		defer close(released)
		releaseErr = link.Release()
	}()
	reply := sync.OnceFunc(func() { close(replyToReset) })
	// After a failed check, let Release finish before the pool closes, which waits
	// for the conn Release holds.
	t.Cleanup(func() { reply(); <-released })
	<-resetReceived

	// ForceClose and Cancel overlap Release, so they must wait for Release and then
	// do nothing.
	forceClosed := make(chan bool, 1)
	go func() { forceClosed <- link.ForceClose() }()
	cancelled := make(chan error, 1)
	go func() { cancelled <- link.Cancel() }()
	select {
	case <-forceClosed:
		t.Fatal("ForceClose did not wait for Release")
	case <-cancelled:
		t.Fatal("Cancel did not wait for Release")
	case <-time.After(50 * time.Millisecond):
	}
	reply()
	<-released
	require.NoError(t, releaseErr)
	assert.False(t, <-forceClosed)
	select {
	case err := <-cancelled:
		assert.NoError(t, err, "a cancel request fails here: the fake refuses a second dial")
	case <-time.After(time.Second):
		t.Fatal("Cancel did not return after Release")
	}

	assert.False(t, link.ForceClose())
	assert.NoError(t, link.Cancel())
	assert.False(t, pooled.IsClosed(), "the pooled connection must stay open")
}

// Statement presence belongs to the connection, so it must outlive the link that
// recorded it, and a released link must stop answering for a connection it no longer holds.
func TestConnStateOutlivesLink(t *testing.T) {
	pool := newFakePgPool(t).pool
	stmt := prepared.CreatePreparedStatement("s1", "proxy_s1", "SELECT id FROM users WHERE id = $1", []uint32{23})

	first := newFakePgLink(t, pool, DefaultPreparedStmtsPerConnection)
	require.NoError(t, first.RecordNewNamedPreparedStmt(stmt))
	require.True(t, first.HasExistingNamedPreparedStmt(stmt))
	require.NoError(t, first.Release())
	require.False(t, first.HasExistingNamedPreparedStmt(stmt))
	require.ErrorIs(t, first.RecordExistingNamedPreparedStmtUsed(stmt), errNoConnState)

	second := newFakePgLink(t, pool, DefaultPreparedStmtsPerConnection)
	require.True(t, second.HasExistingNamedPreparedStmt(stmt), "a new link on the same conn must see statements created earlier")
	require.NoError(t, second.RecordExistingNamedPreparedStmtUsed(stmt))
	require.NoError(t, second.Release())

	unlinked := NewFrontendLink(nil)
	require.False(t, unlinked.HasExistingNamedPreparedStmt(stmt))
	require.ErrorIs(t, unlinked.RecordNewNamedPreparedStmt(stmt), errNoConnState)
}

// Counting a use of a statement the connection lacks changes nothing: an entry with
// no statement behind it could never be evicted, and an error would end the session.
func TestRecordUseOfAbsentStmtIsIgnored(t *testing.T) {
	l := newFakePgLink(t, newFakePgPool(t).pool, DefaultPreparedStmtsPerConnection)
	stmt := testStmts("absent", 1)[0]

	require.NoError(t, l.RecordExistingNamedPreparedStmtUsed(stmt))
	require.False(t, l.HasExistingNamedPreparedStmt(stmt))
	require.Empty(t, l.(*SessionLink).connState.stmts)
}

// Release deallocates what exceeds the cap, one Close per statement, and the
// connection's gauges report the creations, the deallocations and the top usage.
func TestReleaseDeallocatesExcessAndReportsStats(t *testing.T) {
	metrics := newRecordingMetrics()
	t.Cleanup(pgproxy.Bootstrap(pgproxy.Dependencies{Metrics: metrics}))
	fake := newFakePgPool(t)
	const limit = 2
	stmts := testStmts("cap", limit+1)

	l := newFakePgLink(t, fake.pool, limit)
	for _, st := range stmts {
		require.NoError(t, l.RecordNewNamedPreparedStmt(st))
	}
	require.NoError(t, l.RecordExistingNamedPreparedStmtUsed(stmts[0]))
	require.NoError(t, l.RecordExistingNamedPreparedStmtUsed(stmts[0]))
	require.NoError(t, l.Release())
	require.Equal(t, 1, fake.closes, "one statement over the cap means one Deallocate")

	// The next checkout reports the connection's stats and sees only the survivors.
	next := newFakePgLink(t, fake.pool, limit)
	held := 0
	for _, st := range stmts {
		if next.HasExistingNamedPreparedStmt(st) {
			held++
		}
	}
	require.Equal(t, limit, held)

	tags := []string{"database:app_db"}
	for name, want := range map[string]float64{
		pgproxy.MetricName(connPreparedStmtCreatedCount):     limit + 1,
		pgproxy.MetricName(connPreparedStmtDeallocatedCount): 1,
		pgproxy.MetricName(connPreparedStmtMaxUsage):         2,
	} {
		got, ok := metrics.lastGauge(name, tags)
		require.True(t, ok, "%s was not reported", name)
		require.Equal(t, want, got, name)
	}
}

// failingRoleSession fails to set the role.
type failingRoleSession struct{ noopRoleSession }

func (failingRoleSession) Set(context.Context, *pgxpool.Conn, []string) (string, error) {
	return "", errors.New("permission denied to set role")
}

// A link that fails to set the role is never built. The conn stays the caller's:
// New must neither use it again nor hand it back to the pool, since the caller may
// still close it, and backend.Runtime.Acquire does.
func TestNewLeavesConnToCallerWhenRoleSetFails(t *testing.T) {
	pool := newFakePgPool(t).pool
	conn, err := pool.Acquire(context.Background())
	require.NoError(t, err)
	t.Cleanup(conn.Release)

	l, err := New(conn, Options{Role: failingRoleSession{}})
	require.Error(t, err)
	require.Nil(t, l)
	require.Equal(t, int32(1), pool.Stat().AcquiredConns(), "the caller still holds the conn")
	require.False(t, conn.Conn().IsClosed(), "the caller decides what to do with the conn")
}
