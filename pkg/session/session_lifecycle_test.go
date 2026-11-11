// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package session

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pgproxy "github.com/DataDog/pg-proxy"
	"github.com/DataDog/pg-proxy/pkg/actions"
	"github.com/DataDog/pg-proxy/pkg/link"
	"github.com/DataDog/pg-proxy/pkg/prepared"
	"github.com/DataDog/pg-proxy/pkg/query"
)

var _ link.Link = (*testLink)(nil)

type testLink struct {
	calls                  testLinkCalls
	send                   func(pgproto3.FrontendMessage, query.ReplyExpectation)
	flush                  func() error
	receive                func() (pgproto3.BackendMessage, error)
	nextExpectation        func() query.ReplyExpectation
	resolveNextExpectation func()
	discardUntilSync       func()
	idle                   func() bool
	cancel                 func() error
	release                func() error
	forceClose             func() bool
	debugExpectation       func() string
	txStatus               func() query.TxStatus
	setTxStatus            func(query.TxStatus)
	handleReadyForQuery    func(query.TxStatus)
	recordNew              func(prepared.PreparedStatement) error
	recordExisting         func(prepared.PreparedStatement) error
	hasExisting            func(prepared.PreparedStatement) bool
}

type testLinkCalls struct {
	send, flush, receive, nextExpectation, resolveNextExpectation int
	discardUntilSync, idle, cancel, release, forceClose           int
	debugExpectation, txStatus, setTxStatus, handleReadyForQuery  int
	recordNew, recordExisting, hasExisting                        int
}

func (l *testLink) assertCalls(t *testing.T, expected testLinkCalls) {
	t.Helper()
	assert.Equal(t, expected, l.calls)
}

func (l *testLink) Send(msg pgproto3.FrontendMessage, expectation query.ReplyExpectation) {
	l.calls.send++
	if l.send != nil {
		l.send(msg, expectation)
	}
}
func (l *testLink) Flush() error {
	l.calls.flush++
	if l.flush != nil {
		return l.flush()
	}
	return nil
}
func (l *testLink) Receive() (pgproto3.BackendMessage, error) {
	l.calls.receive++
	if l.receive != nil {
		return l.receive()
	}
	return nil, nil
}
func (l *testLink) NextExpectation() query.ReplyExpectation {
	l.calls.nextExpectation++
	if l.nextExpectation != nil {
		return l.nextExpectation()
	}
	return query.NoReply
}
func (l *testLink) ResolveNextExpectation() {
	l.calls.resolveNextExpectation++
	if l.resolveNextExpectation != nil {
		l.resolveNextExpectation()
	}
}
func (l *testLink) DiscardingUntilSync() bool { return false }

func (l *testLink) DiscardUntilSync() {
	l.calls.discardUntilSync++
	if l.discardUntilSync != nil {
		l.discardUntilSync()
	}
}
func (l *testLink) Idle() bool {
	l.calls.idle++
	if l.idle != nil {
		return l.idle()
	}
	return true
}
func (l *testLink) Cancel() error {
	l.calls.cancel++
	if l.cancel != nil {
		return l.cancel()
	}
	return nil
}
func (l *testLink) Release() error {
	l.calls.release++
	if l.release != nil {
		return l.release()
	}
	return nil
}
func (l *testLink) ForceClose() bool {
	l.calls.forceClose++
	if l.forceClose != nil {
		return l.forceClose()
	}
	return false
}
func (l *testLink) DebugExpectationString() string {
	l.calls.debugExpectation++
	if l.debugExpectation != nil {
		return l.debugExpectation()
	}
	return ""
}
func (l *testLink) TxStatus() query.TxStatus {
	l.calls.txStatus++
	if l.txStatus != nil {
		return l.txStatus()
	}
	return query.Idle
}
func (l *testLink) SetTxStatus(status query.TxStatus) {
	l.calls.setTxStatus++
	if l.setTxStatus != nil {
		l.setTxStatus(status)
	}
}
func (l *testLink) HandleReadyForQuery(status query.TxStatus) {
	l.calls.handleReadyForQuery++
	if l.handleReadyForQuery != nil {
		l.handleReadyForQuery(status)
	}
}
func (l *testLink) RecordNewNamedPreparedStmt(statement prepared.PreparedStatement) error {
	l.calls.recordNew++
	if l.recordNew != nil {
		return l.recordNew(statement)
	}
	return nil
}
func (l *testLink) RecordExistingNamedPreparedStmtUsed(statement prepared.PreparedStatement) error {
	l.calls.recordExisting++
	if l.recordExisting != nil {
		return l.recordExisting(statement)
	}
	return nil
}
func (l *testLink) HasExistingNamedPreparedStmt(statement prepared.PreparedStatement) bool {
	l.calls.hasExisting++
	return l.hasExisting != nil && l.hasExisting(statement)
}

type channelBuffer struct {
	ch chan []byte
}

func newChannelBuffer() *channelBuffer {
	return &channelBuffer{ch: make(chan []byte, 1)}
}

func (b *channelBuffer) Read(p []byte) (int, error) {
	value := <-b.ch
	if len(value) > len(p) {
		return 0, errors.New("channelBuffer does not support payloads larger than the read buffer")
	}
	return copy(p, value), nil
}

func (b *channelBuffer) Write(p []byte) (int, error) {
	value := append([]byte(nil), p...)
	b.ch <- value
	return len(p), nil
}

type channelConn struct {
	*channelBuffer
}

func (channelConn) Close() error                     { return nil }
func (channelConn) LocalAddr() net.Addr              { return nil }
func (channelConn) RemoteAddr() net.Addr             { return nil }
func (channelConn) SetDeadline(time.Time) error      { return nil }
func (channelConn) SetReadDeadline(time.Time) error  { return nil }
func (channelConn) SetWriteDeadline(time.Time) error { return nil }

func newFrontendBackendPair() (*pgproto3.Frontend, *pgproto3.Backend, net.Conn) {
	frontendToBackend := newChannelBuffer()
	backendToFrontend := newChannelBuffer()
	return pgproto3.NewFrontend(backendToFrontend, frontendToBackend),
		pgproto3.NewBackend(frontendToBackend, backendToFrontend),
		channelConn{backendToFrontend}
}

type passthroughLoadShedder struct {
	active      atomic.Bool
	runCount    atomic.Int64
	activeCount atomic.Int64
}

func (s *passthroughLoadShedder) Run(_ context.Context, fn func() error, _ Priority) (error, error) {
	if s.active.Load() {
		return nil, errors.New("shedding load")
	}
	s.runCount.Add(1)
	s.activeCount.Add(1)
	defer func() {
		if count := s.activeCount.Add(-1); count < 0 {
			panic("passthroughLoadShedder: RIF slot released more than acquired")
		}
	}()
	return fn(), nil
}

func (s *passthroughLoadShedder) getRunCount() int64 {
	return s.runCount.Load()
}

func (s *passthroughLoadShedder) getActiveCount() int64 {
	return s.activeCount.Load()
}

type fixedBackend struct {
	backendLink link.Link
	acquired    atomic.Bool
	acquireErr  error
	loadShedder *passthroughLoadShedder
}

func (b *fixedBackend) Acquire(context.Context, LinkRequest) (link.Link, error) {
	if b.acquireErr != nil {
		return nil, b.acquireErr
	}
	if !b.acquired.CompareAndSwap(false, true) {
		return nil, errors.New("only expect a single link")
	}
	return b.backendLink, nil
}

func (b *fixedBackend) LoadShedder() LoadShedder {
	if b.loadShedder == nil {
		b.loadShedder = &passthroughLoadShedder{}
	}
	return b.loadShedder
}

type fakeLivenessProbe struct {
	disconnected atomic.Bool
}

func (p *fakeLivenessProbe) IsDisconnected() bool { return p.disconnected.Load() }
func (p *fakeLivenessProbe) triggerDisconnect()   { p.disconnected.Store(true) }

func newRunningTestSession(t *testing.T) (*Session, *pgproto3.Frontend, *pgproto3.Backend, func()) {
	t.Helper()
	s, fakeClient, fakePG, _, closer := newRunningTestSessionWithBackend(t)
	return s, fakeClient, fakePG, closer
}

func newRunningTestSessionWithBackend(t *testing.T, configure ...func(*Config)) (*Session, *pgproto3.Frontend, *pgproto3.Backend, *fixedBackend, func()) {
	t.Helper()
	fakeClient, client, clientConn := newFrontendBackendPair()
	pg, fakePG, _ := newFrontendBackendPair()
	backend := &fixedBackend{
		backendLink: link.NewFrontendLink(pg),
		loadShedder: &passthroughLoadShedder{},
	}
	cfg := Config{
		Context:               context.Background(),
		User:                  "testuser",
		Database:              "testdb",
		Service:               "testservice",
		PID:                   1234,
		Client:                client,
		ClientConn:            clientConn,
		Backend:               backend,
		PostgresMsgBufferSize: 8192,
		PreparedStatements:    prepared.CreateProxyPreparedStmtsState(nil),
		MetricTags:            []string{"user:testuser", "database:testdatabase", "client_service_account:testclient"},
	}
	for _, apply := range configure {
		apply(&cfg)
	}
	s := New(cfg)

	done := make(chan error, 1)
	go func() { done <- s.Run() }()

	closer := sync.OnceFunc(func() {
		fakeClient.Send(&pgproto3.Terminate{})
		require.NoError(t, fakeClient.Flush())
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("session did not exit")
		}
		// Ideally we would be able to check this before closing the session, but doing so
		// produces racing conditions in the tests, since the trace is closed after sending the response to the client.
		assert.Nil(t, s.TraceState().RootTrace())
		assert.Nil(t, s.TraceState().Trace())
	})
	return s, fakeClient, fakePG, backend, closer
}

func newLifecycleRecordingMetrics(t *testing.T) *recordingMetrics {
	t.Helper()
	metrics := &recordingMetrics{}
	restore := pgproxy.Bootstrap(pgproxy.Dependencies{Metrics: metrics})
	t.Cleanup(restore)
	return metrics
}

func (m *recordingMetrics) countValues(name string, tags []string) []int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	var values []int64
	for _, call := range m.counts {
		if call.name == name && assert.ObjectsAreEqual(tags, call.tags) {
			values = append(values, int64(call.value))
		}
	}
	return values
}

func TestSessionIdentifier(t *testing.T) {
	s := New(Config{Context: context.Background(), PID: 1, Client: &testClient{}})
	assert.Equal(t, "1", s.Identifier())
}

func TestClientDisconnectedError(t *testing.T) {
	cause := errors.New("use of closed network connection")
	err := NewClientDisconnectedError(cause)

	assert.Contains(t, err.Error(), "client disconnected")
	assert.ErrorIs(t, err, cause)

	var disconnectedError *ClientDisconnectedError
	assert.True(t, errors.As(err, &disconnectedError))
}

func TestClientEOFMarksClientDisconnectedAndCountsDestroyedBackend(t *testing.T) {
	metrics := newLifecycleRecordingMetrics(t)
	backendLink := &testLink{
		idle:    func() bool { return false },
		release: func() error { return nil },
	}
	s := New(Config{
		Context:     context.Background(),
		Client:      pgproto3.NewBackend(strings.NewReader(""), io.Discard),
		InitialLink: backendLink,
		MetricTags:  []string{"user:testuser", "database:testdatabase", "client_service_account:testclient"},
	})

	err := s.ReceiveAndHandleMessage()
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
	assert.True(t, s.ClientDisconnected())

	tags := append(s.MetricTags(), "reason:client_disconnect", "path:release")
	assert.Equal(t, []int64{1}, metrics.countValues("datadog.pg_proxy.sessions.connections.destroyed", tags))
	backendLink.assertCalls(t, testLinkCalls{idle: 1, release: 1})
}

func TestTerminateMarksClientDisconnectedAndCountsDestroyedBackend(t *testing.T) {
	metrics := newLifecycleRecordingMetrics(t)
	backendLink := &testLink{
		idle:    func() bool { return false },
		release: func() error { return nil },
	}
	fakeClient, client, _ := newFrontendBackendPair()
	s := New(Config{
		Context:     context.Background(),
		Client:      client,
		InitialLink: backendLink,
		MetricTags:  []string{"user:testuser", "database:testdatabase", "client_service_account:testclient"},
	})

	fakeClient.Send(&pgproto3.Terminate{})
	require.NoError(t, fakeClient.Flush())

	err := s.ReceiveAndHandleMessage()
	assert.ErrorIs(t, err, io.EOF)
	assert.True(t, s.ClientDisconnected())

	tags := append(s.MetricTags(), "reason:client_disconnect", "path:release")
	assert.Equal(t, []int64{1}, metrics.countValues("datadog.pg_proxy.sessions.connections.destroyed", tags))
	backendLink.assertCalls(t, testLinkCalls{idle: 1, release: 1})
}

func TestCloseDoesNotCountIdleBackendOnClientDisconnect(t *testing.T) {
	metrics := newLifecycleRecordingMetrics(t)
	disconnected := &atomic.Bool{}
	disconnected.Store(true)
	backendLink := &testLink{idle: func() bool { return true }}
	s := New(Config{
		Context:            context.Background(),
		Client:             &testClient{},
		InitialLink:        backendLink,
		ClientDisconnected: disconnected,
		MetricTags:         []string{"user:testuser", "database:testdatabase", "client_service_account:testclient"},
	})
	s.Close(io.EOF)

	tags := append(s.MetricTags(), "reason:client_disconnect", "path:release")
	assert.Empty(t, metrics.countValues("datadog.pg_proxy.sessions.connections.destroyed", tags))
	backendLink.assertCalls(t, testLinkCalls{idle: 1, release: 1})
}

func TestCloseDoesNotCountBackendEOFAsClientDisconnect(t *testing.T) {
	metrics := newLifecycleRecordingMetrics(t)
	backendLink := &testLink{}
	s := New(Config{
		Context:     context.Background(),
		Client:      &testClient{},
		InitialLink: backendLink,
		MetricTags:  []string{"user:testuser", "database:testdatabase", "client_service_account:testclient"},
	})
	s.Close(fmt.Errorf("backend receive failed: %w", io.EOF))

	tags := append(s.MetricTags(), "reason:client_disconnect", "path:release")
	assert.Empty(t, metrics.countValues("datadog.pg_proxy.sessions.connections.destroyed", tags))
	backendLink.assertCalls(t, testLinkCalls{release: 1})
}

func TestClientWriteFailureCountsDestroyedBackendBeforeMonitor(t *testing.T) {
	t.Run("reused_buffer", func(t *testing.T) {
		metrics := newLifecycleRecordingMetrics(t)
		backendLink := &testLink{
			nextExpectation: func() query.ReplyExpectation { return query.QueryReply },
			receive: func() (pgproto3.BackendMessage, error) {
				return &pgproto3.ReadyForQuery{TxStatus: 'I'}, nil
			},
			idle:    func() bool { return false },
			release: func() error { return nil },
		}

		proxyClientConn, clientPeer := net.Pipe()
		require.NoError(t, clientPeer.Close())
		t.Cleanup(func() { _ = proxyClientConn.Close() })
		s := New(Config{
			Context:                  context.Background(),
			Client:                   pgproto3.NewBackend(proxyClientConn, proxyClientConn),
			ClientConn:               proxyClientConn,
			InitialClientReplyBuffer: []byte{},
			InitialLink:              backendLink,
			MetricTags:               []string{"user:testuser", "database:testdatabase", "client_service_account:testclient"},
			PostgresMsgBufferSize:    1,
		})

		_, err := s.FlushPostgres()
		assert.ErrorContains(t, err, "error flushing proxied msg(s) to client")
		assert.True(t, s.ClientDisconnected())

		s.Close(err)

		tags := append(s.MetricTags(), "reason:client_disconnect", "path:release")
		assert.Equal(t, []int64{1}, metrics.countValues("datadog.pg_proxy.sessions.connections.destroyed", tags))
		backendLink.assertCalls(t, testLinkCalls{flush: 1, receive: 1, nextExpectation: 1, idle: 1, release: 1})
	})
}

func TestAfterFlushActionWriteFailureCountsDestroyedBackendBeforeMonitor(t *testing.T) {
	metrics := newLifecycleRecordingMetrics(t)
	backendLink := &testLink{
		send: func(msg pgproto3.FrontendMessage, expectation query.ReplyExpectation) {
			assert.Equal(t, &pgproto3.Flush{}, msg)
			assert.Equal(t, query.NoReply, expectation)
		},
		nextExpectation: func() query.ReplyExpectation { return query.NoReply },
		idle:            func() bool { return false },
	}
	proxyClientConn, clientPeer := net.Pipe()
	require.NoError(t, clientPeer.Close())
	t.Cleanup(func() { _ = proxyClientConn.Close() })
	s := New(Config{
		Context:     context.Background(),
		Client:      pgproto3.NewBackend(proxyClientConn, proxyClientConn),
		InitialLink: backendLink,
		MetricTags:  []string{"user:testuser", "database:testdatabase", "client_service_account:testclient"},
	})

	err := s.HandleAction(actions.ClientMessageAction{
		ProxyMsgToPg:  &pgproto3.Flush{},
		FlushPostgres: true,
		AfterFlushAction: &actions.ClientMessageAction{
			ReplyMsgToClient: &pgproto3.CloseComplete{},
		},
	})
	assert.ErrorContains(t, err, "error flushing proxied msg(s) to client")
	assert.True(t, s.ClientDisconnected())

	s.Close(err)

	tags := append(s.MetricTags(), "reason:client_disconnect", "path:release")
	assert.Equal(t, []int64{1}, metrics.countValues("datadog.pg_proxy.sessions.connections.destroyed", tags))
	backendLink.assertCalls(t, testLinkCalls{send: 1, flush: 1, nextExpectation: 1, idle: 1, release: 1})
}

func TestSyntheticParseCompleteWriteFailureCountsDestroyedBackendBeforeMonitor(t *testing.T) {
	metrics := newLifecycleRecordingMetrics(t)
	preparedStatement := prepared.CreatePreparedStatement("client_name", "proxy_name", "SELECT 1", nil)
	backendLink := &testLink{
		hasExisting: func(statement prepared.PreparedStatement) bool {
			assert.Equal(t, preparedStatement, statement)
			return statement == preparedStatement
		},
		send: func(msg pgproto3.FrontendMessage, expectation query.ReplyExpectation) {
			assert.Equal(t, &pgproto3.Flush{}, msg)
			assert.Equal(t, query.NoReply, expectation)
		},
		nextExpectation: func() query.ReplyExpectation { return query.NoReply },
		idle:            func() bool { return false },
	}
	proxyClientConn, clientPeer := net.Pipe()
	require.NoError(t, clientPeer.Close())
	t.Cleanup(func() { _ = proxyClientConn.Close() })
	s := New(Config{
		Context:            context.Background(),
		Client:             pgproto3.NewBackend(proxyClientConn, proxyClientConn),
		InitialLink:        backendLink,
		PreparedStatements: prepared.CreateProxyPreparedStmtsState(nil),
		MetricTags:         []string{"user:testuser", "database:testdatabase", "client_service_account:testclient"},
	})

	_, err := s.RecordNewPreparedStatement(actions.ClientMessageAction{RecordNewPreparedStmt: preparedStatement})
	assert.ErrorContains(t, err, "error flushing proxied msg(s) to client")
	assert.True(t, s.ClientDisconnected())

	s.Close(err)

	tags := append(s.MetricTags(), "reason:client_disconnect", "path:release")
	assert.Equal(t, []int64{1}, metrics.countValues("datadog.pg_proxy.sessions.connections.destroyed", tags))
	backendLink.assertCalls(t, testLinkCalls{send: 1, flush: 1, nextExpectation: 1, idle: 1, release: 1, hasExisting: 1})
}

func TestCloseDoesNotDoubleCountBackendDestroyedByClientMonitor(t *testing.T) {
	metrics := newLifecycleRecordingMetrics(t)
	disconnected := &atomic.Bool{}
	disconnected.Store(true)
	backendLink := &testLink{
		forceClose: func() bool { return true },
		idle:       func() bool { return false },
	}
	s := New(Config{
		Context:            context.Background(),
		Client:             &testClient{},
		InitialLink:        backendLink,
		ClientDisconnected: disconnected,
		MetricTags:         []string{"user:testuser", "database:testdatabase", "client_service_account:testclient"},
	})

	// Model the monitor force-closing the connection before close() releases
	// the same non-idle link.
	s.ForceCloseBackendOnClientDisconnect(backendLink)
	s.Close(io.EOF)

	tags := append(s.MetricTags(), "reason:client_disconnect", "path:force_close")
	assert.Equal(t, []int64{1}, metrics.countValues("datadog.pg_proxy.sessions.connections.destroyed", tags))

	releaseTags := append(s.MetricTags(), "reason:client_disconnect", "path:release")
	assert.Empty(t, metrics.countValues("datadog.pg_proxy.sessions.connections.destroyed", releaseTags))
	backendLink.assertCalls(t, testLinkCalls{idle: 1, release: 1, forceClose: 1})
}

func TestFailedForceCloseDoesNotCountBackendDestroyed(t *testing.T) {
	metrics := newLifecycleRecordingMetrics(t)
	s := New(Config{
		Context:    context.Background(),
		Client:     &testClient{},
		MetricTags: []string{"user:testuser", "database:testdatabase", "client_service_account:testclient"},
	})
	backendLink := &testLink{forceClose: func() bool { return false }}
	s.ForceCloseBackendOnClientDisconnect(backendLink)

	tags := append(s.MetricTags(), "reason:client_disconnect", "path:force_close")
	assert.Empty(t, metrics.countValues("datadog.pg_proxy.sessions.connections.destroyed", tags))
	backendLink.assertCalls(t, testLinkCalls{forceClose: 1})
}

// TestFatal57P03_SessionReceivesSwitchoverHint verifies the end-to-end FATAL path:
// the fake PG backend sends a FATAL 57P03, and the client side of the session
// receives a FATAL ErrorResponse with the switchover hint appended exactly once.
func TestFatal57P03_SessionReceivesSwitchoverHint(t *testing.T) {
	t.Run("reused_buffer", func(t *testing.T) {
		_, fakeClient, fakePG, closer := newRunningTestSession(t)
		defer closer()

		// Client sends a simple query to acquire a session link.
		fakeClient.Send(&pgproto3.Query{String: "SELECT 1"})
		require.NoError(t, fakeClient.Flush())

		// PG side receives the query.
		_, err := fakePG.Receive()
		require.NoError(t, err)

		// PG backend responds with a FATAL 57P03 (switchover in progress).
		fakePG.Send(&pgproto3.ErrorResponse{
			Severity: "FATAL",
			Code:     "57P03",
			Message:  "the database system is shutting down",
		})
		require.NoError(t, fakePG.Flush())

		// The client should receive an ErrorResponse with:
		// 1. FATAL severity
		// 2. Original message preserved
		// 3. Switchover hint appended exactly once
		msg, err := fakeClient.Receive()
		require.NoError(t, err)
		errResp, ok := msg.(*pgproto3.ErrorResponse)
		if !assert.True(t, ok, "expected *pgproto3.ErrorResponse, got %T", msg) {
			return
		}
		assert.Equal(t, "FATAL", errResp.Severity)
		assert.Contains(t, errResp.Message, "the database system is shutting down", "original message must be preserved")
		assert.Contains(t, errResp.Message, query.LeaderSwitchoverHint)
		assert.Equal(t, 1, strings.Count(errResp.Message, query.LeaderSwitchoverHint), "hint must be appended exactly once")
	})
}

// TestClientMonitorConcurrentLinkAccess verifies that the monitor goroutine
// and the session goroutine can concurrently access s.link without a data
// race. The monitor reads s.link (under linkMu) to call ForceClose, while the
// session goroutine writes s.link = nil (under linkMu) when releasing an
// idle link. With race = "on" this test would fail if the mutex were missing.
func TestClientMonitorConcurrentLinkAccess(t *testing.T) {
	t.Run("reused_buffer", func(t *testing.T) {
		probe := &fakeLivenessProbe{}
		s, fakeClient, fakePG, closer := newRunningTestSession(t)
		s.SetLivenessProbe(probe)
		defer closer()

		// Send a query — session acquires a link and forwards to PG.
		fakeClient.Send(&pgproto3.Query{String: "SELECT 1"})
		require.NoError(t, fakeClient.Flush())

		_, err := fakePG.Receive()
		require.NoError(t, err)

		// Trigger client disconnect and give the monitor time to poll (it ticks
		// every 100ms). The session goroutine is blocked on link.Receive()
		// waiting for PG, so the monitor will detect the disconnect, read s.link
		// (under linkMu), and call ForceClose(). Since the test SessionLink has
		// conn=nil, ForceClose is a no-op — we still need PG's response to unblock
		// Receive().
		probe.triggerDisconnect()
		time.Sleep(200 * time.Millisecond)

		// PG responds with ReadyForQuery{I}. The session goroutine unblocks,
		// acquires linkMu, and writes s.link = nil. If the mutex were missing,
		// this would race with the monitor's read above.
		fakePG.Send(&pgproto3.EmptyQueryResponse{})
		fakePG.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		require.NoError(t, fakePG.Flush())

		// Drain client responses.
		_, err = fakeClient.Receive()
		require.NoError(t, err)
		_, err = fakeClient.Receive()
		require.NoError(t, err)

		// Verify the monitor detected the disconnect.
		assert.True(t, s.ClientDisconnected())
	})
}
