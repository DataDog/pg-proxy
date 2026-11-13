// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package link

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pgproxy "github.com/DataDog/pg-proxy"
	"github.com/DataDog/pg-proxy/pkg/query"
)

// recordingMetrics is a minimal pgproxy.MetricsClient that records Count calls.
type recordingMetrics struct {
	pgproxy.NoopMetrics
	mu     sync.Mutex
	counts map[string]map[string][]int64 // metric -> tagsKey -> values
}

func newRecordingMetrics() *recordingMetrics {
	return &recordingMetrics{counts: make(map[string]map[string][]int64)}
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

	// The test backend answers the startup handshake and then reads exactly one more
	// message (the Terminate from closing the connection). Any query Release issues
	// would therefore never be answered, so a Release that returns promptly is itself
	// the assertion that no query was issued.
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

func newSessionLinkTestPoolConn(t *testing.T, configure ...func(*pgxpool.Config)) *pgxpool.Conn {
	t.Helper()

	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	})

	handshakeResult := make(chan error, 1)
	go func() {
		backend := pgproto3.NewBackend(serverConn, serverConn)
		if _, err := backend.ReceiveStartupMessage(); err != nil {
			handshakeResult <- err
			return
		}

		backend.Send(&pgproto3.AuthenticationOk{})
		backend.Send(&pgproto3.BackendKeyData{ProcessID: 1, SecretKey: []byte{0, 0, 0, 2}})
		backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		if err := backend.Flush(); err != nil {
			handshakeResult <- err
			return
		}
		handshakeResult <- nil

		// Read the Terminate sent when Release closes the non-idle connection.
		_, _ = backend.Receive()
	}()

	config, err := pgxpool.ParseConfig("postgres://test@127.0.0.1/test?sslmode=disable")
	require.NoError(t, err)
	config.ConnConfig.DialFunc = func(context.Context, string, string) (net.Conn, error) {
		return endpointTestConn{Conn: clientConn, remote: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 5432}}, nil
	}
	for _, apply := range configure {
		apply(config)
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	conn, err := pool.Acquire(context.Background())
	require.NoError(t, err)
	require.NoError(t, <-handshakeResult)
	setUpConnMetaData(conn)
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

func TestDatabaseInfoUsesAcquiredConnection(t *testing.T) {
	conn := newSessionLinkTestPoolConn(t)
	t.Cleanup(conn.Release)
	backend := &SessionLink{conn: conn}
	require.Equal(t, pgproxy.DatabaseInfo{Name: "test", Host: "127.0.0.1", Port: 5432}, backend.DatabaseInfo())
	require.Equal(t, pgproxy.DatabaseInfo{}, (&SessionLink{}).DatabaseInfo())
}

type endpointTestConn struct {
	net.Conn
	remote net.Addr
}

func (c endpointTestConn) RemoteAddr() net.Addr { return c.remote }

func TestDatabaseInfoUsesSuccessfulFallbackEndpoint(t *testing.T) {
	conn := newSessionLinkTestPoolConn(t, func(config *pgxpool.Config) {
		config.ConnConfig.Host = "127.0.0.1"
		config.ConnConfig.Port = 1
		config.ConnConfig.Fallbacks = []*pgconn.FallbackConfig{{Host: "127.0.0.2", Port: 5433}}
		dial := config.ConnConfig.DialFunc
		config.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
			if address == "127.0.0.1:1" {
				return nil, errors.New("primary unavailable")
			}
			c, err := dial(ctx, network, address)
			if err != nil {
				return nil, err
			}
			return endpointTestConn{Conn: c, remote: &net.TCPAddr{IP: net.ParseIP("127.0.0.2"), Port: 5433}}, nil
		}
	})
	t.Cleanup(conn.Release)
	require.Equal(t, "127.0.0.1", conn.Conn().Config().Host)
	require.EqualValues(t, 1, conn.Conn().Config().Port)
	require.Equal(t, pgproxy.DatabaseInfo{Name: "test", Host: "127.0.0.2", Port: 5433}, (&SessionLink{conn: conn}).DatabaseInfo())
}
