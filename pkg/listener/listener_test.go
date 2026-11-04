package listener

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/DataDog/pg-proxy/pkg/limiter"
	pgsession "github.com/DataDog/pg-proxy/pkg/session"
)

type testLoadShedder struct{ active atomic.Bool }

func (s *testLoadShedder) Run(_ context.Context, fn func()) error {
	if s.active.Load() {
		return errors.New("shed")
	}
	fn()
	return nil
}

type testLifecycle struct {
	mu       sync.Mutex
	sessions map[uint32]pgsession.SessionHandle
}

func (l *testLifecycle) register(session Session) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.sessions[session.(*pgsession.Session).PID()] = session
}

func (l *testLifecycle) delete(session Session) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.sessions, session.(*pgsession.Session).PID())
}

func (l *testLifecycle) lookup(pid uint32) (pgsession.SessionHandle, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	session, ok := l.sessions[pid]
	return session, ok
}

func newTestListener(ctx context.Context) (*Listener[struct{}], *limiter.Limiter, *testLoadShedder, error) {
	admission := limiter.New(limiter.Params{
		GlobalLimit:       100,
		DefaultServiceMax: 100,
	})
	shedder := &testLoadShedder{}
	lifecycle := &testLifecycle{sessions: make(map[uint32]pgsession.SessionHandle)}
	engine, err := New(ctx, Config[struct{}]{
		ListenAddress: "127.0.0.1:0",
		Admission:     admission,
		Hooks: Hooks[struct{}]{
			ResolveStartup: func(_ context.Context, request StartupRequest) (StartupResult[struct{}], error) {
				if request.Database != "testdb" {
					return StartupResult[struct{}]{}, fmt.Errorf("no configuration found for logical database %s", request.Database)
				}
				return StartupResult[struct{}]{
					LogicalDatabase: request.Database,
					ParameterStatuses: map[string]string{
						"ps_key1": "ps_val1",
						"ps_key2": "ps_val2",
					},
				}, nil
			},
			NewSession: func(cfg SessionConfig[struct{}]) (Session, error) {
				return pgsession.New(pgsession.Config{
					Context:         cfg.Context,
					User:            cfg.Request.User,
					Database:        cfg.Resolved.LogicalDatabase,
					LogicalDatabase: cfg.Resolved.LogicalDatabase,
					PID:             cfg.PID,
					SecretKey:       cfg.SecretKey,
					Service:         cfg.Service,
					Client:          cfg.Client,
					ClientConn:      cfg.ClientConn,
					MetricTags:      cfg.MetricTags,
				}), nil
			},
			LoadShedder: shedder,
			Lifecycle: Lifecycle{
				Register: lifecycle.register,
				Delete:   lifecycle.delete,
				Lookup:   lifecycle.lookup,
			},
		},
	})
	return engine, admission, shedder, err
}

func TestListenerHandleStartup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	engine, _, _, err := newTestListener(ctx)
	require.NoError(t, err)
	defer engine.Close()

	server, client := net.Pipe()
	server.SetDeadline(time.Now().Add(5 * time.Second))
	client.SetDeadline(time.Now().Add(5 * time.Second))
	type startupOutcome struct {
		session Session
		err     error
	}
	type readOutcome struct {
		bytes []byte
		err   error
	}
	sessionChan := make(chan startupOutcome, 1)
	bytesChan := make(chan readOutcome, 1)
	go func() {
		defer client.Close()
		session, startupErr := engine.handleStartup(t.Context(), t.Context(), client, &pgproto3.StartupMessage{Parameters: map[string]string{
			"user": "testuser", "database": "testdb",
		}})
		sessionChan <- startupOutcome{session: session, err: startupErr}
	}()
	go func() {
		bytes, readErr := io.ReadAll(server)
		bytesChan <- readOutcome{bytes: bytes, err: readErr}
	}()

	startup := <-sessionChan
	require.NoError(t, startup.err)
	read := <-bytesChan
	require.NoError(t, read.err)
	session := startup.session
	actual := read.bytes
	expected, err := (&pgproto3.AuthenticationOk{}).Encode(nil)
	require.NoError(t, err)
	for _, status := range []pgproto3.ParameterStatus{
		{Name: "ps_key1", Value: "ps_val1"},
		{Name: "ps_key2", Value: "ps_val2"},
	} {
		expected, err = status.Encode(expected)
		require.NoError(t, err)
	}
	expected, err = (&pgproto3.BackendKeyData{
		ProcessID: session.(*pgsession.Session).PID(),
		SecretKey: session.SecretKey(),
	}).Encode(expected)
	require.NoError(t, err)
	expected, err = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(expected)
	require.NoError(t, err)
	assert.Equal(t, expected, actual)
}

func TestHandleStartupPassesSessionOptions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	engine, _, _, err := newTestListener(ctx)
	require.NoError(t, err)
	defer engine.Close()

	var got SessionConfig[struct{}]
	originalNewSession := engine.cfg.Hooks.NewSession
	engine.cfg.Hooks.NewSession = func(cfg SessionConfig[struct{}]) (Session, error) {
		got = cfg
		return originalNewSession(cfg)
	}

	server, client := net.Pipe()
	server.SetDeadline(time.Now().Add(5 * time.Second))
	client.SetDeadline(time.Now().Add(5 * time.Second))
	done := make(chan error, 1)
	go func() {
		defer client.Close()
		_, startupErr := engine.handleStartup(t.Context(), t.Context(), client, &pgproto3.StartupMessage{Parameters: map[string]string{
			"user":     "testuser",
			"database": "testdb",
			"options": url.QueryEscape(
				"-c pg_proxy.dynamic_routing=true -c pg_proxy.auto_cached_prepared_statements=true",
			),
		}})
		done <- startupErr
	}()
	go func() { _, _ = io.ReadAll(server) }()
	require.NoError(t, <-done)
	assert.Equal(t, SessionOptions{
		DynamicRoutingEnabled:        true,
		AutoCachedPreparedStatements: true,
	}, got.Request.Options)
}

func TestHandleCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	engine, _, _, err := newTestListener(ctx)
	require.NoError(t, err)
	defer engine.Close()

	assert.ErrorContains(t, engine.handleCancel(&pgproto3.CancelRequest{ProcessID: 999}), "no session found for pid: 999")
	validPID := uint32(2)
	validSecret := secretKeyFromUint32(19347913)
	session := pgsession.New(pgsession.Config{PID: validPID, SecretKey: validSecret})
	engine.cfg.Hooks.Lifecycle.Register(session)
	assert.ErrorContains(t, engine.handleCancel(&pgproto3.CancelRequest{
		ProcessID: validPID, SecretKey: secretKeyFromUint32(12345678),
	}), "incorrect cancel request secret key for pid: 2")
	assert.NoError(t, engine.handleCancel(&pgproto3.CancelRequest{ProcessID: validPID, SecretKey: validSecret}))
}

func TestAttemptTLSUpgrade(t *testing.T) {
	validStartup := &pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters:      map[string]string{"foo": "bar"},
	}
	tests := []struct {
		name                string
		tlsConfig           *tls.Config
		messages            []pgproto3.FrontendMessage
		responses           [][]byte
		expectedMessage     pgproto3.FrontendMessage
		expectedErrContains string
		upgraded            bool
	}{
		{
			name: "happy path", tlsConfig: &tls.Config{},
			messages:  []pgproto3.FrontendMessage{&pgproto3.SSLRequest{}, validStartup},
			responses: [][]byte{[]byte("S")}, expectedMessage: validStartup, upgraded: true,
		},
		{
			name: "CancelRequest immediately after SSLRequest", tlsConfig: &tls.Config{},
			messages:  []pgproto3.FrontendMessage{&pgproto3.SSLRequest{}, &pgproto3.CancelRequest{SecretKey: secretKeyFromUint32(0)}},
			responses: [][]byte{[]byte("S")}, expectedMessage: &pgproto3.CancelRequest{SecretKey: secretKeyFromUint32(0)}, upgraded: true,
		},
		{
			name: "missing TLS config", messages: []pgproto3.FrontendMessage{&pgproto3.SSLRequest{}, validStartup},
			responses: [][]byte{[]byte("N")}, expectedMessage: validStartup,
		},
		{
			name: "client did not request SSL", messages: []pgproto3.FrontendMessage{validStartup},
			expectedMessage: validStartup,
		},
		{
			name: "unsupported first message (no SSL request)", messages: []pgproto3.FrontendMessage{&pgproto3.Query{}},
			expectedErrContains: "error receiving startup message:",
		},
		{
			name: "unsupported first message (after SSL request)", tlsConfig: &tls.Config{},
			messages:  []pgproto3.FrontendMessage{&pgproto3.SSLRequest{}, &pgproto3.Query{}},
			responses: [][]byte{[]byte("S")}, expectedErrContains: "error receiving post-ssl startup message:", upgraded: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			engine, _, _, err := newTestListener(ctx)
			require.NoError(t, err)
			defer engine.Close()
			engine.cfg.TLSConfig = test.tlsConfig
			wasUpgraded := false
			engine.cfg.TLSConnUpgrader = func(conn net.Conn, _ *tls.Config) net.Conn {
				wasUpgraded = true
				return conn
			}
			server, client := net.Pipe()
			server.SetDeadline(time.Now().Add(time.Second))
			client.SetDeadline(time.Now().Add(time.Second))
			messageChan := make(chan pgproto3.FrontendMessage, 1)
			errChan := make(chan error, 1)
			go func() {
				defer client.Close()
				_, message, upgradeErr := engine.attemptTLSUpgrade(client)
				messageChan <- message
				errChan <- upgradeErr
			}()
			for index, message := range test.messages {
				buffer, encodeErr := message.Encode(nil)
				require.NoError(t, encodeErr)
				_, writeErr := server.Write(buffer)
				require.NoError(t, writeErr)
				if index < len(test.responses) {
					actual := make([]byte, len(test.responses[index]))
					_, readErr := io.ReadFull(server, actual)
					require.NoError(t, readErr)
					assert.Equal(t, test.responses[index], actual)
				}
			}
			if test.expectedErrContains != "" {
				assert.ErrorContains(t, <-errChan, test.expectedErrContains)
			} else {
				assert.Equal(t, test.expectedMessage, <-messageChan)
				assert.NoError(t, <-errChan)
			}
			assert.Equal(t, test.upgraded, wasUpgraded)
			server.Close()
		})
	}
}

func TestLoadShedding(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	engine, _, shedder, err := newTestListener(ctx)
	require.NoError(t, err)
	defer engine.Close()
	go func() { _ = engine.Run() }()

	connect := func(t *testing.T) (net.Conn, *pgproto3.Frontend) {
		t.Helper()
		conn, err := net.Dial("tcp", engine.Addr().String())
		require.NoError(t, err)
		conn.SetDeadline(time.Now().Add(2 * time.Second))
		return conn, pgproto3.NewFrontend(conn, conn)
	}
	startup := &pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters:      map[string]string{"user": "testuser", "database": "testdb"},
	}

	client, frontend := connect(t)
	defer client.Close()
	frontend.Send(startup)
	require.NoError(t, frontend.Flush())
	message, err := frontend.Receive()
	require.NoError(t, err)
	_, ok := message.(*pgproto3.AuthenticationOk)
	require.True(t, ok)

	shedder.active.Store(true)
	client, frontend = connect(t)
	defer client.Close()
	frontend.Send(startup)
	require.NoError(t, frontend.Flush())
	message, err = frontend.Receive()
	require.NoError(t, err)
	response, ok := message.(*pgproto3.ErrorResponse)
	require.True(t, ok)
	assert.Equal(t, "FATAL", response.Severity)
	assert.Equal(t, pgerrcode.ConnectionFailure, response.Code)
}

func TestLoadSheddingConnectionRejectionSSL(t *testing.T) {
	server, client := net.Pipe()
	server.SetDeadline(time.Now().Add(time.Second))
	client.SetDeadline(time.Now().Add(time.Second))
	replyChan := make(chan []byte, 1)
	go func() {
		defer client.Close()
		buffer, _ := (&pgproto3.SSLRequest{}).Encode(nil)
		_, _ = client.Write(buffer)
		reply, _ := io.ReadAll(client)
		replyChan <- reply
	}()
	handleLoadSheddingRejection(server)
	assert.Equal(t, []byte("N"), <-replyChan)
}

func TestLoadSheddingConnectionRejectionDoesNotBlockWithoutStartupBytes(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		handleLoadSheddingRejection(server)
	}()
	select {
	case <-done:
	case <-time.After(loadSheddingStartupPeekTimeout + 500*time.Millisecond):
		t.Fatal("load-shedding rejection blocked waiting for startup bytes")
	}
}

func TestConnectionDrainingWatcherUnblocksIdleSession(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	engine, _, _, err := newTestListener(ctx)
	require.NoError(t, err)
	defer engine.Close()
	server, client := net.Pipe()
	handleDone := make(chan struct{})
	go func() {
		defer close(handleDone)
		engine.handleConn(server, func() {})
	}()
	ready := make(chan struct{}, 1)
	errorResponse := make(chan *pgproto3.ErrorResponse, 1)
	go func() {
		defer client.Close()
		frontend := pgproto3.NewFrontend(client, client)
		frontend.Send(&pgproto3.StartupMessage{
			ProtocolVersion: pgproto3.ProtocolVersionNumber,
			Parameters:      map[string]string{"user": "testuser", "database": "testdb"},
		})
		_ = frontend.Flush()
		for {
			message, receiveErr := frontend.Receive()
			if receiveErr != nil {
				return
			}
			switch message := message.(type) {
			case *pgproto3.ReadyForQuery:
				ready <- struct{}{}
			case *pgproto3.ErrorResponse:
				errorResponse <- message
			}
		}
	}()
	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("session did not reach idle")
	}
	cancel()
	select {
	case response := <-errorResponse:
		assert.Equal(t, pgerrcode.AdminShutdown, response.Code)
	case <-time.After(2 * time.Second):
		t.Fatal("client did not receive AdminShutdown")
	}
	select {
	case <-handleDone:
	case <-time.After(2 * time.Second):
		t.Fatal("handleConn did not return after cancellation")
	}
}

func TestStartDrainWatcher(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	engine, _, _, err := newTestListener(ctx)
	require.NoError(t, err)
	defer engine.Close()
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	sessionDone := make(chan struct{})
	defer close(sessionDone)
	engine.startDrainWatcher(sessionDone, server)
	readErr := make(chan error, 1)
	go func() {
		_, err := server.Read(make([]byte, 1))
		readErr <- err
	}()
	cancel()
	select {
	case err := <-readErr:
		var netErr net.Error
		assert.True(t, errors.As(err, &netErr) && netErr.Timeout())
	case <-time.After(2 * time.Second):
		t.Fatal("drain watcher did not unblock the read")
	}
}

// Run must observe cancellation while blocked in Accept, not only when the next
// client connects, and wait for active sessions before returning.
func TestRunReturnsOnCancellationWhileIdle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	engine, _, _, err := newTestListener(ctx)
	require.NoError(t, err)
	defer engine.Close()
	waited := make(chan struct{})
	engine.cfg.Hooks.Lifecycle.WaitAll = func() { close(waited) }
	done := make(chan error, 1)
	go func() { done <- engine.Run() }()

	// Complete one startup so Run is back in Accept with no connection pending.
	conn, err := net.Dial("tcp", engine.Addr().String())
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetDeadline(time.Now().Add(2*time.Second)))
	frontend := pgproto3.NewFrontend(conn, conn)
	frontend.Send(&pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters:      map[string]string{"user": "testuser", "database": "testdb"},
	})
	require.NoError(t, frontend.Flush())
	for {
		message, err := frontend.Receive()
		require.NoError(t, err)
		if _, ok := message.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
	select {
	case <-waited:
	default:
		t.Fatal("Run returned without waiting for sessions")
	}
	// Let the session handler finish before another test installs global metrics.
	for {
		if _, err := frontend.Receive(); err != nil {
			require.ErrorIs(t, err, io.ErrUnexpectedEOF)
			break
		}
	}
}

// TestShouldReportError verifies that shouldReportError suppresses the errors that
// already notified the client (or are expected disconnects) and reports everything
// else. The client-notified case is the session-limit rejection path added alongside
// the pre-PID cap enforcement.
func TestShouldReportError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"client-notified rejection is suppressed", ClientNotified(errors.New("cap reached")), false},
		{"wrapped client-notified rejection is suppressed", fmt.Errorf("wrap: %w", ClientNotified(errors.New("cap reached"))), false},
		{"client disconnected is suppressed", &pgsession.ClientDisconnectedError{}, false},
		{"EOF is suppressed", io.EOF, false},
		{"unexpected EOF is suppressed", io.ErrUnexpectedEOF, false},
		{"generic error is reported", errors.New("boom"), true},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, shouldReportError(test.err))
			wrapped := startupErrorAt("authenticate", test.err)
			assert.ErrorIs(t, wrapped, test.err)
			assert.Equal(t, test.want, shouldReportError(wrapped))
			assert.NotContains(t, wrapped.Error(), test.err.Error())
		})
	}
}

// TestHandleSessionLimitExceeded verifies that a session-limit rejection is
// delivered to the client as a FATAL SQLSTATE 53300 (TooManyConnections), with a
// message that distinguishes a global proxy-capacity rejection from a per-service
// quota rejection, and falls back to the capacity message for any non-limiter.Error.
func TestHandleSessionLimitExceeded(t *testing.T) {
	cases := []struct {
		name    string
		err     error
		wantMsg string
	}{
		{"global limit", &limiter.Error{Type: limiter.GlobalLimitExceeded, Err: errors.New("global")}, "pg-proxy is at capacity. Please retry."},
		{"service limit", &limiter.Error{Type: limiter.ServiceLimitExceeded, Err: errors.New("service")}, "Service connection limit reached. Please retry."},
		{"non session-limit error falls back to capacity message", errors.New("boom"), "pg-proxy is at capacity. Please retry."},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			server, client := net.Pipe()
			server.SetDeadline(time.Now().Add(3 * time.Second))
			client.SetDeadline(time.Now().Add(3 * time.Second))
			writeErr := make(chan error, 1)
			go func() {
				defer client.Close()
				writeErr <- handleSessionLimitExceeded(client, test.err)
			}()
			message, err := pgproto3.NewFrontend(server, server).Receive()
			require.NoError(t, err)
			response, ok := message.(*pgproto3.ErrorResponse)
			require.True(t, ok)
			assert.Equal(t, "FATAL", response.Severity)
			assert.Equal(t, pgerrcode.TooManyConnections, response.Code)
			assert.Equal(t, test.wantMsg, response.Message)
			assert.NoError(t, <-writeErr)
		})
	}
}

// TestHandleStartupRejectsOverServiceLimit exercises the pre-PID cap enforcement:
// a connection that exceeds its per-service session limit is sent a FATAL 53300,
// never receives a backend PID, and leaves no leaked slot behind.
func TestHandleStartupRejectsOverServiceLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	engine, admission, _, err := newTestListener(ctx)
	require.NoError(t, err)
	defer engine.Close()
	admission.RegisterService("limited", 1)
	require.NoError(t, admission.RegisterSession("limited"))

	server, client := net.Pipe()
	server.SetDeadline(time.Now().Add(3 * time.Second))
	client.SetDeadline(time.Now().Add(3 * time.Second))
	errChan := make(chan error, 1)
	go func() {
		defer client.Close()
		_, startupErr := engine.handleStartup(t.Context(), t.Context(), client, &pgproto3.StartupMessage{Parameters: map[string]string{
			"user": "limited", "database": "testdb",
		}})
		errChan <- startupErr
	}()
	message, err := pgproto3.NewFrontend(server, server).Receive()
	require.NoError(t, err)
	response, ok := message.(*pgproto3.ErrorResponse)
	require.True(t, ok)
	assert.Equal(t, pgerrcode.TooManyConnections, response.Code)
	assert.Equal(t, "Service connection limit reached. Please retry.", response.Message)
	var notified *clientNotifiedError
	require.True(t, errors.As(<-errChan, &notified))
	assert.Equal(t, int32(1), admission.GetGlobalCount())
}

// TestHandleStartupRollsBackSlotOnPostRegisterFailure verifies the rollback defer:
// when handleStartup fails AFTER reserving a session slot (here, because the client
// is gone and the ReadyForQuery write fails), the reserved slot is released rather
// than leaked.
func TestHandleStartupRollsBackSlotOnPostRegisterFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	engine, admission, _, err := newTestListener(ctx)
	require.NoError(t, err)
	defer engine.Close()
	server, client := net.Pipe()
	server.SetDeadline(time.Now().Add(3 * time.Second))
	require.NoError(t, client.Close())
	_, err = engine.handleStartup(t.Context(), t.Context(), server, &pgproto3.StartupMessage{Parameters: map[string]string{
		"user": "newservice", "database": "testdb",
	}})
	require.Error(t, err)
	var notified *clientNotifiedError
	assert.False(t, errors.As(err, &notified))
	assert.Equal(t, int32(0), admission.GetGlobalCount())
}
