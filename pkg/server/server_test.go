package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/DataDog/pg-proxy/pkg/session"
)

type testAdmission struct{}

func (testAdmission) RegisterSession(string) error   { return nil }
func (testAdmission) DeregisterSession(string) error { return nil }
func (testAdmission) GetErrorType(error) string      { return "" }

type testSessions struct {
	mu         sync.Mutex
	registered chan string
	deleted    chan string
	sessions   map[string]session.SessionHandle
	waited     bool
	cancelled  chan struct{}
}

func (s *testSessions) Register(id string, active session.SessionHandle) {
	s.mu.Lock()
	if s.cancelled != nil {
		active = testCancelSession{SessionHandle: active, cancelled: s.cancelled}
	}
	s.sessions[id] = active
	s.mu.Unlock()
	if s.registered != nil {
		s.registered <- id
	}
}
func (s *testSessions) Delete(id string) {
	s.mu.Lock()
	delete(s.sessions, id)
	s.mu.Unlock()
	if s.deleted != nil {
		s.deleted <- id
	}
}
func (s *testSessions) Get(id string) (session.SessionHandle, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	active, ok := s.sessions[id]
	return active, ok
}
func (s *testSessions) Wait() { s.waited = true }

type testCancelSession struct {
	session.SessionHandle
	cancelled chan struct{}
}

func (s testCancelSession) Cancel() error {
	err := s.SessionHandle.Cancel()
	s.cancelled <- struct{}{}
	return err
}

type testSession struct {
	id        string
	secret    []byte
	run       func() error
	cancelled chan struct{}
}

func (s testSession) Identifier() string      { return s.id }
func (testSession) LogicalDatabaseID() string { return "" }
func (s testSession) Run() error {
	if s.run != nil {
		return s.run()
	}
	return nil
}
func (testSession) RemapBackend(session.Backend)              {}
func (testSession) RemapReadStates([]session.ReadStateConfig) {}
func (s testSession) Cancel() error {
	if s.cancelled != nil {
		s.cancelled <- struct{}{}
	}
	return nil
}
func (s testSession) SecretKey() []byte         { return s.secret }
func (testSession) User() string                { return "" }
func (testSession) Database() string            { return "" }
func (testSession) Service() string             { return "" }
func (testSession) MetricTags() []string        { return nil }
func (testSession) DynamicRoutingEnabled() bool { return false }

func TestNewServerRequiresSessionRegistry(t *testing.T) {
	_, err := NewServer(context.Background(), Config{})
	assert.EqualError(t, err, "server session registry is required")
}

func TestNewServerRequiresRoutingInventory(t *testing.T) {
	_, err := NewServer(t.Context(), Config{Sessions: &testSessions{}})
	assert.EqualError(t, err, "server routing inventory is required")
}

func TestNewServerBindsListener(t *testing.T) {
	server, err := NewServer(t.Context(), Config{
		Sessions:      &testSessions{},
		Runtime:       RuntimeConfig{Inventory: newInventoryForTest()},
		ListenAddress: "127.0.0.1:0",
		Admission:     testAdmission{},
	})
	require.NoError(t, err)
	assert.NotNil(t, server.Addr())
	require.NoError(t, server.Close())
}

func TestSessionLifecycleDelegatesToRegistry(t *testing.T) {
	sessions := &testSessions{sessions: make(map[string]session.SessionHandle)}
	lifecycle := sessionLifecycle(sessions)
	active := testSession{id: "42"}

	lifecycle.Register(active)
	actual, found := sessions.Get("42")
	require.True(t, found)
	assert.Equal(t, active, actual)

	actual, found = lifecycle.Lookup(42)
	require.True(t, found)
	assert.Equal(t, active, actual)

	lifecycle.Delete(active)
	_, found = sessions.Get("42")
	assert.False(t, found)

	lifecycle.WaitAll()
	assert.True(t, sessions.waited)
}

// Exercise the constructor's lifecycle wiring through real startup and cancel packets.
func TestServerRegistersCancelsAndRemovesSessions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	registry := &testSessions{sessions: make(map[string]session.SessionHandle), registered: make(chan string, 1), deleted: make(chan string, 1)}
	registry.cancelled = make(chan struct{}, 1)
	server, err := NewServer(ctx, Config{
		Sessions:      registry,
		Runtime:       RuntimeConfig{Inventory: newInventoryForTest()},
		ListenAddress: "127.0.0.1:0",
		Admission:     testAdmission{},
	})
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- server.Run() }()
	defer func() { cancel(); _ = server.Close(); <-done }()
	client, err := pgconn.Connect(ctx, fmt.Sprintf("postgres://testuser@%s/testdb?sslmode=disable", server.Addr()))
	require.NoError(t, err)
	defer client.Close(context.Background())
	var id string
	select {
	case id = <-registry.registered:
	case <-ctx.Done():
		t.Fatal("session was not registered")
	}
	assert.Equal(t, fmt.Sprint(client.PID()), id)
	require.NoError(t, client.CancelRequest(ctx))
	select {
	case <-registry.cancelled:
	case <-ctx.Done():
		t.Fatal("cancel request did not find the session")
	}
	require.NoError(t, client.Close(ctx))
	select {
	case deleted := <-registry.deleted:
		assert.Equal(t, id, deleted)
	case <-ctx.Done():
		t.Fatal("finished session was not removed")
	}
	_, found := registry.Get(id)
	assert.False(t, found)
}

func TestServerWaitsForRegistryOnContextShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	registry := &testSessions{}
	server, err := NewServer(ctx, Config{
		Sessions:      registry,
		Runtime:       RuntimeConfig{Inventory: newInventoryForTest()},
		ListenAddress: "127.0.0.1:0",
		Admission:     testAdmission{},
	})
	require.NoError(t, err)
	defer server.Close()
	cancel()
	require.NoError(t, server.Run())
	assert.True(t, registry.waited)
}

// Construct and run the real session using only portable server components.
func TestServerWithRuntime(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	registry := &testSessions{sessions: make(map[string]session.SessionHandle), registered: make(chan string, 1), deleted: make(chan string, 1)}
	server, err := NewServer(ctx, Config{
		Sessions: registry,
		Runtime: RuntimeConfig{
			Inventory:         newInventoryForTest(),
			ParameterStatuses: func(parameters map[string]string) { parameters["application_name"] = "server-test" },
		},
		ListenAddress: "127.0.0.1:0",
		Admission:     testAdmission{},
	})
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- server.Run() }()
	defer func() { cancel(); _ = server.Close(); <-done }()
	client, err := pgconn.Connect(ctx, fmt.Sprintf("postgres://testuser@%s/testdb?sslmode=disable", server.Addr()))
	require.NoError(t, err)
	defer client.Close(context.Background())
	assert.Equal(t, "16", client.ParameterStatus("server_version"))
	assert.Equal(t, "server-test", client.ParameterStatus("application_name"))
	var id string
	select {
	case id = <-registry.registered:
	case <-ctx.Done():
		t.Fatal("session was not registered")
	}
	active, ok := registry.Get(id)
	require.True(t, ok)
	require.IsType(t, &session.Session{}, active)
	assert.Equal(t, "testdb", active.LogicalDatabaseID())
	assert.Equal(t, "testuser", active.(*session.Session).User())
	require.NoError(t, client.Close(ctx))
	select {
	case deleted := <-registry.deleted:
		assert.Equal(t, id, deleted)
	case <-ctx.Done():
		t.Fatal("finished session was not removed")
	}
}

func TestServerForwardsTLSConfig(t *testing.T) {
	for _, tlsConfig := range []*tls.Config{nil, {}} {
		server, err := NewServer(t.Context(), Config{
			Sessions: &testSessions{}, Runtime: RuntimeConfig{Inventory: newInventoryForTest()},
			ListenAddress: "127.0.0.1:0", Admission: testAdmission{},
			RequireTLS: true, TLSConfig: tlsConfig,
		})
		if tlsConfig == nil {
			require.EqualError(t, err, "listener requires TLS but TLS configuration is missing")
		} else {
			require.NoError(t, err)
			require.NoError(t, server.Close())
		}
	}
}
