// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package session

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/DataDog/pg-proxy/pkg/link"
	"github.com/DataDog/pg-proxy/pkg/prepared"
)

type namedBackend struct {
	name         string
	acquire      func(context.Context, LinkRequest) (link.Link, error)
	loadShedder  LoadShedder
	acquisitions atomic.Int32
}

func (b *namedBackend) Acquire(ctx context.Context, request LinkRequest) (link.Link, error) {
	b.acquisitions.Add(1)
	if b.acquire == nil {
		return nil, errors.New("no link configured for backend " + b.name)
	}
	return b.acquire(ctx, request)
}

func (b *namedBackend) LoadShedder() LoadShedder {
	if b.loadShedder == nil {
		b.loadShedder = &passthroughLoadShedder{}
	}
	return b.loadShedder
}

func frontendBackend(name string, frontend *pgproto3.Frontend) *namedBackend {
	return &namedBackend{
		name: name,
		acquire: func(context.Context, LinkRequest) (link.Link, error) {
			return link.NewFrontendLink(frontend), nil
		},
		loadShedder: &passthroughLoadShedder{},
	}
}

func runTestSession(t *testing.T, session *Session, fakeClient *pgproto3.Frontend) (done <-chan error, closer func()) {
	t.Helper()
	doneCh := make(chan error, 1)
	go func() { doneCh <- session.Run() }()
	return doneCh, sync.OnceFunc(func() {
		fakeClient.Send(&pgproto3.Terminate{})
		require.NoError(t, fakeClient.Flush())
		select {
		case <-doneCh:
		case <-time.After(5 * time.Second):
			t.Fatal("session did not exit")
		}
	})
}

func TestIdleSessionRemappingIsTransparentToClient(t *testing.T) {
	t.Run("reused_buffer", func(t *testing.T) {
		assert := assert.New(t)
		fakeClient, clientConn, clientNetConn := newFrontendBackendPair()
		pgConn, fakePg, _ := newFrontendBackendPair()
		oldBackend := frontendBackend("old", pgConn)
		newBackend := frontendBackend("new", pgConn)
		session := New(Config{
			Context: context.Background(), User: "testuser", Database: "testdb",
			Client: clientConn, ClientConn: clientNetConn, Backend: oldBackend,
			PostgresMsgBufferSize: 8192,
		})
		_, closer := runTestSession(t, session, fakeClient)
		defer closer()

		// Simulate logical database re-mapping by a configuration refresh
		session.RemapBackend(newBackend)

		// RemapState doesn't immediately change original state from NewTestSession
		assert.NotEqual(session.PendingBackend(), session.Backend())
		// But the remap has been scheduled
		assert.True(session.PendingBackend() == newBackend)

		// Client finally sends a message
		fakeClient.Send(&pgproto3.Query{String: "SELECT 1"})
		assert.NoError(fakeClient.Flush())
		_, err := fakePg.Receive()
		assert.NoError(err)

		// Session's state has been remapped
		assert.Nil(session.PendingBackend())
		assert.True(session.Backend() == newBackend)

		fakePg.Send(&pgproto3.EmptyQueryResponse{})
		fakePg.Send(buildReadyForQueryIdleMessage())
		assert.NoError(fakePg.Flush())

		// Process both messages from the PG server
		emptyResponse, err := fakeClient.Receive()
		assert.NoError(err)
		assert.Equal(&pgproto3.EmptyQueryResponse{}, emptyResponse)
		readyForQueryMessage, err := fakeClient.Receive()
		assert.NoError(err)
		assert.Equal(buildReadyForQueryIdleMessage(), readyForQueryMessage)
	})
}

func TestSessionRemappingDoesNotTakeEffectInBetweenMessages(t *testing.T) {
	assert := assert.New(t)
	fakeClient, clientConn, clientNetConn := newFrontendBackendPair()
	pgConn1, fakePg1, _ := newFrontendBackendPair()
	pgConn2, fakePg2, _ := newFrontendBackendPair()

	var session *Session
	backend2 := frontendBackend("testcluster2", pgConn2)
	resetState := sync.OnceFunc(func() {
		session.RemapBackend(backend2)
	})
	backend1 := &namedBackend{
		name: "testcluster1",
		acquire: func(context.Context, LinkRequest) (link.Link, error) {
			resetState()
			// Decide where to route the traffic based on the current session.state,
			// right after running the remapping.
			assert.Equal("testcluster1", session.Backend().(*namedBackend).name)
			return link.NewFrontendLink(pgConn1), nil
		},
		loadShedder: &passthroughLoadShedder{},
	}
	session = New(Config{
		Context: context.Background(), User: "testuser", Database: "testdb",
		Client: clientConn, ClientConn: clientNetConn, Backend: backend1,
		PostgresMsgBufferSize: 8192,
	})
	done, closer := runTestSession(t, session, fakeClient)
	defer closer()
	_ = done

	// Client sends a message
	fakeClient.Send(&pgproto3.Query{String: "SELECT 1"})
	assert.NoError(fakeClient.Flush())
	// First message should have been sent to fakePg1 *despite* having called session.ResetState
	_, err := fakePg1.Receive()
	assert.NoError(err)
	// Session's state should still be the original value
	assert.Equal(backend1, session.Backend())
	// Session's state has been scheduled for remapping
	assert.Equal(backend2, session.PendingBackend())

	// PG responds normally
	fakePg1.Send(&pgproto3.EmptyQueryResponse{})
	fakePg1.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	assert.NoError(fakePg1.Flush())

	// Now we send another message from the client and expect the remapping to take place
	fakeClient.Send(&pgproto3.Query{String: "SELECT 1"})
	assert.NoError(fakeClient.Flush())
	// Second message should have been sent to fakePg2
	_, err = fakePg2.Receive()
	assert.NoError(err)
	// Session's state should have been swapped by now
	assert.Equal(backend2, session.Backend())
	// Session's state remapping has been processed
	assert.Nil(session.PendingBackend())

	// PG responds normally
	fakePg2.Send(&pgproto3.EmptyQueryResponse{})
	fakePg2.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	assert.NoError(fakePg2.Flush())

	// Process both messages from the PG server
	emptyResponse, err := fakeClient.Receive()
	assert.NoError(err)
	assert.Equal(&pgproto3.EmptyQueryResponse{}, emptyResponse)
	readyForQueryMessage, err := fakeClient.Receive()
	assert.NoError(err)
	assert.Equal(buildReadyForQueryIdleMessage(), readyForQueryMessage)
}

func TestSessionRemappingReturnsErrorDuringTransaction(t *testing.T) {
	t.Run("reused_buffer", func(t *testing.T) {
		assert := assert.New(t)
		session, fakeClient, fakePg, closer := newRunningTestSession(t)
		defer closer()

		// Client starts a transaction
		fakeClient.Send(&pgproto3.Query{String: "BEGIN"})
		assert.NoError(fakeClient.Flush())
		_, err := fakePg.Receive()
		assert.NoError(err)

		// Server reply
		fakePg.Send(&pgproto3.EmptyQueryResponse{})
		fakePg.Send(&pgproto3.ReadyForQuery{TxStatus: 'T'})
		assert.NoError(fakePg.Flush())

		// Client receives
		emptyResponse, err := fakeClient.Receive()
		assert.NoError(err)
		assert.Equal(&pgproto3.EmptyQueryResponse{}, emptyResponse)
		readyForQueryMessage, err := fakeClient.Receive()
		assert.NoError(err)
		assert.Equal(&pgproto3.ReadyForQuery{TxStatus: 'T'}, readyForQueryMessage)

		// Simulate logical database re-mapping by a configuration refresh
		newBackend := &fixedBackend{backendLink: &testLink{}, loadShedder: &passthroughLoadShedder{}}
		session.RemapBackend(newBackend)

		fakeClient.Send(&pgproto3.Query{String: "UPDATE SET foo = 'bar'"})
		assert.NoError(fakeClient.Flush())

		// The proxy should outright reject the query, the server doesn't send anything
		transactionRollbackErrorResponse, err := fakeClient.Receive()
		assert.NoError(err)
		assert.Equal(buildTransactionRollbackErrorResponse(), transactionRollbackErrorResponse)
		// Expect a ReadyForQuery message as well.
		// This informs the client that the session/link/connection is still alive.
		readyForQueryIdleMessage, err := fakeClient.Receive()
		assert.NoError(err)
		assert.Equal(buildReadyForQueryIdleMessage(), readyForQueryIdleMessage)
		// The state should have been remapped
		assert.NotNil(session.Backend())
		assert.True(session.Backend() == newBackend)
	})
}

type ReadStatesTestSuite struct {
	suite.Suite

	session    *Session
	fakeClient *pgproto3.Frontend
	fakePg1    *pgproto3.Backend
	fakePg2    *pgproto3.Backend
	fakePg3    *pgproto3.Backend
	readState2 *ReadState
	readState3 *ReadState
	backend1   *namedBackend
	backend2   *namedBackend
	backend3   *namedBackend

	done   <-chan error
	closer func()
}

func TestReadsStatesTestSuite(t *testing.T) {
	suite.Run(t, new(ReadStatesTestSuite))
}

func (suite *ReadStatesTestSuite) SetupTest() {
	fakeClient, clientConn, clientNetConn := newFrontendBackendPair()
	pgConn1, fakePg1, _ := newFrontendBackendPair()
	pgConn2, fakePg2, _ := newFrontendBackendPair()
	pgConn3, fakePg3, _ := newFrontendBackendPair()
	backend1 := frontendBackend("testcluster1", pgConn1)
	backend2 := frontendBackend("testcluster2", pgConn2)
	backend3 := frontendBackend("testcluster3", pgConn3)
	readState2 := &ReadState{backend: backend2, weight: 0}
	readState3 := &ReadState{backend: backend3, weight: 0}

	session := New(Config{
		Context: context.Background(), User: "testuser", Database: "testdb",
		PID: 1234, SecretKey: []byte{0, 0, 22, 46},
		Client: clientConn, ClientConn: clientNetConn, Backend: backend1,
		ReadStates: []ReadStateConfig{
			{Backend: backend2, Weight: 0},
			{Backend: backend3, Weight: 0},
		},
		RandomInt:             func(int) int { return 80 },
		DynamicRoutingEnabled: true,
		PostgresMsgBufferSize: 8192,
		PreparedStatements:    prepared.CreateProxyPreparedStmtsState([]string{}),
	})

	suite.fakeClient = fakeClient
	suite.fakePg1 = fakePg1
	suite.fakePg2 = fakePg2
	suite.fakePg3 = fakePg3
	suite.readState2 = readState2
	suite.readState3 = readState3
	suite.backend1 = backend1
	suite.backend2 = backend2
	suite.backend3 = backend3
	suite.session = session
	suite.done, suite.closer = runTestSession(suite.T(), session, fakeClient)
}

func (suite *ReadStatesTestSuite) updateWeights(weight2, weight3 int) {
	suite.readState2.weight = uint8(weight2)
	suite.readState3.weight = uint8(weight3)
	suite.session.readStates = []*ReadState{suite.readState2, suite.readState3}
}

func (suite *ReadStatesTestSuite) sendClientMsg(query string) {
	suite.fakeClient.Send(&pgproto3.Query{String: query})
	assert.NoError(suite.T(), suite.fakeClient.Flush())
}

func (suite *ReadStatesTestSuite) assertPGReceivesMsg(pg *pgproto3.Backend) {
	_, err := pg.Receive()
	assert.NoError(suite.T(), err)
}

func (suite *ReadStatesTestSuite) sendPGResponse(pg *pgproto3.Backend, txStatus byte) {
	pg.Send(&pgproto3.EmptyQueryResponse{})
	pg.Send(&pgproto3.ReadyForQuery{TxStatus: txStatus})
	assert.NoError(suite.T(), pg.Flush())
}

func (suite *ReadStatesTestSuite) assertClientReceivesMsgs(txStatus byte) {
	emptyResponse, err := suite.fakeClient.Receive()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), &pgproto3.EmptyQueryResponse{}, emptyResponse)
	readyForQueryMessage, err := suite.fakeClient.Receive()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), &pgproto3.ReadyForQuery{TxStatus: txStatus}, readyForQueryMessage)
}

func (suite *ReadStatesTestSuite) TearDownTest() {
	suite.closer()
}

func (suite *ReadStatesTestSuite) TestUseSessionStateWhenNoReadsStates() {
	suite.session.readStates = []*ReadState{}

	suite.sendClientMsg("SELECT * FROM dogs")
	// First message should have been sent to fakePg1 since the readStates map is empty
	suite.assertPGReceivesMsg(suite.fakePg1)
	suite.sendPGResponse(suite.fakePg1, 'I')
	suite.assertClientReceivesMsgs('I')
}

func (suite *ReadStatesTestSuite) TestUseSessionStateWhenReadsStatesWeightZero() {
	// All reads states' weights are set to zero in test set-up
	suite.sendClientMsg("SELECT * FROM dogs")
	// Message should still have been sent to fakePg1 since the total traffic weight is 0
	suite.assertPGReceivesMsg(suite.fakePg1)
	// PG responds normally
	suite.sendPGResponse(suite.fakePg1, 'I')
	suite.assertClientReceivesMsgs('I')
}

func (suite *ReadStatesTestSuite) TestRouteReadToSessionReadStates() {
	// Config.RandomInt fixes the return value of the rng.

	// Add equal weight to both states
	suite.updateWeights(100, 100)

	suite.sendClientMsg("SELECT * FROM dogs")
	// Read is selected to be sent to fakePg2
	suite.assertPGReceivesMsg(suite.fakePg2)
	suite.sendPGResponse(suite.fakePg2, 'I')
	suite.assertClientReceivesMsgs('I')

	// Add un-equal weight to each state
	suite.updateWeights(50, 100)

	suite.sendClientMsg("SELECT * FROM dogs")
	// Read is selected to be sent to fakePg3
	suite.assertPGReceivesMsg(suite.fakePg3)
	suite.sendPGResponse(suite.fakePg3, 'I')
	suite.assertClientReceivesMsgs('I')
}

func (suite *ReadStatesTestSuite) TestWeightUpdateDoesntAffectExistingSessionLink() {
	// Client starts a transaction
	suite.sendClientMsg("BEGIN")
	suite.assertPGReceivesMsg(suite.fakePg1)
	suite.sendPGResponse(suite.fakePg1, 'T')
	suite.assertClientReceivesMsgs('T')

	// Simulate update to the weights of the reads states
	suite.updateWeights(50, 70)

	suite.sendClientMsg("SELECT * FROM dogs")
	// Message should still have been sent to fakePg1
	suite.assertPGReceivesMsg(suite.fakePg1)

	suite.sendPGResponse(suite.fakePg1, 'T')
	// check the session link is not nil
	assert.NotNil(suite.T(), suite.session.CurrentLink())
	suite.assertClientReceivesMsgs('T')

	suite.sendClientMsg("COMMIT")
	suite.assertPGReceivesMsg(suite.fakePg1)
	suite.sendPGResponse(suite.fakePg1, 'I')
	suite.assertClientReceivesMsgs('I')
}

func (suite *ReadStatesTestSuite) TestAlwaysUseSessionStateForWrites() {
	// All read queries should be sent to fakePg2 since there's no weight for fakePg3
	suite.updateWeights(100, 0)
	suite.sendClientMsg("SELECT * FROM dogs")
	suite.assertPGReceivesMsg(suite.fakePg2)
	suite.sendPGResponse(suite.fakePg2, 'I')
	suite.assertClientReceivesMsgs('I')

	// While reads should always go to fakePg2, writes should always go to fakePg1
	suite.sendClientMsg("INSERT INTO dogs (so, much, data, '!!')")
	suite.assertPGReceivesMsg(suite.fakePg1)
	suite.sendPGResponse(suite.fakePg1, 'I')
	suite.assertClientReceivesMsgs('I')
}

func TestReadStatesHelperFunctions(t *testing.T) {
	cases := map[string]struct {
		readStates          []*ReadState
		expectedHasStates   bool
		expectedTotalWeight int
	}{
		"nil read states":               {readStates: nil, expectedHasStates: false, expectedTotalWeight: 0},
		"empty read states":             {readStates: []*ReadState{}, expectedHasStates: false, expectedTotalWeight: 0},
		"single read state":             {readStates: []*ReadState{{weight: 42}}, expectedHasStates: true, expectedTotalWeight: 42},
		"multiple read states":          {readStates: []*ReadState{{weight: 30}, {weight: 70}}, expectedHasStates: true, expectedTotalWeight: 100},
		"read states with zero weights": {readStates: []*ReadState{{weight: 0}, {weight: 0}}, expectedHasStates: true, expectedTotalWeight: 0},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s := &Session{readStates: tc.readStates}
			assert.Equal(t, tc.expectedHasStates, s.hasReadStates())
			assert.Equal(t, tc.expectedTotalWeight, totalReadWeight(tc.readStates))
		})
	}
}

// TestReadClustersRemappingEnablesDynamicRouting verifies that when a session
// with no read clusters receives updated read cluster routing from the warden,
// subsequent read queries are routed to the new read cluster rather than the primary.
func TestReadClustersRemappingEnablesDynamicRouting(t *testing.T) {
	fakeClient, clientConn, clientNetConn := newFrontendBackendPair()
	primaryPgConn, _, _ := newFrontendBackendPair()
	readPgConn, readFakePg, _ := newFrontendBackendPair()
	primaryBackend := frontendBackend("primary-cluster", primaryPgConn)
	readBackend := frontendBackend("read-cluster", readPgConn)
	s := New(Config{
		Context: context.Background(), User: "testuser", Database: "testdb",
		Client: clientConn, ClientConn: clientNetConn, Backend: primaryBackend,
		ReadStates: []ReadStateConfig{}, DynamicRoutingEnabled: false,
		PostgresMsgBufferSize: 8192,
	})
	_, closer := runTestSession(t, s, fakeClient)
	defer closer()

	// Warden notifies session of new read cluster routing
	s.RemapReadStates([]ReadStateConfig{{Backend: readBackend, Weight: 100}})

	// Read query should be routed to the read cluster, not the primary
	fakeClient.Send(&pgproto3.Query{String: "SELECT * FROM dogs"})
	assert.NoError(t, fakeClient.Flush())
	_, err := readFakePg.Receive()
	assert.NoError(t, err)
	readFakePg.Send(&pgproto3.EmptyQueryResponse{})
	readFakePg.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	assert.NoError(t, readFakePg.Flush())
	_, err = fakeClient.Receive()
	assert.NoError(t, err)
	_, err = fakeClient.Receive()
	assert.NoError(t, err)

	assert.True(t, s.dynamicRoutingEnabled, "dynamic routing should be enabled after applying non-empty read states")
	assert.Len(t, s.readStates, 1)
	assert.Nil(t, s.pending.Load(), "pending update should be cleared after applying")
}

// TestReadClustersRemappingToEmptyRoutesReadsToDefault verifies that when a session's
// read clusters are cleared, subsequent reads route to the primary state.
func TestReadClustersRemappingToEmptyRoutesReadsToDefault(t *testing.T) {
	fakeClient, clientConn, clientNetConn := newFrontendBackendPair()
	primaryPgConn, primaryFakePg, _ := newFrontendBackendPair()
	readPgConn, _, _ := newFrontendBackendPair()
	primaryBackend := frontendBackend("primary-cluster", primaryPgConn)
	readBackend := frontendBackend("read-cluster", readPgConn)
	s := New(Config{
		Context: context.Background(), User: "testuser", Database: "testdb",
		Client: clientConn, ClientConn: clientNetConn, Backend: primaryBackend,
		ReadStates:            []ReadStateConfig{{Backend: readBackend, Weight: 100}},
		DynamicRoutingEnabled: true, PostgresMsgBufferSize: 8192,
	})
	_, closer := runTestSession(t, s, fakeClient)
	defer closer()

	// Warden clears read clusters
	s.RemapReadStates([]ReadStateConfig{})

	// Read query should go to primary, not the former read cluster
	fakeClient.Send(&pgproto3.Query{String: "SELECT * FROM dogs"})
	assert.NoError(t, fakeClient.Flush())
	_, err := primaryFakePg.Receive()
	assert.NoError(t, err)
	primaryFakePg.Send(&pgproto3.EmptyQueryResponse{})
	primaryFakePg.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	assert.NoError(t, primaryFakePg.Flush())
	_, err = fakeClient.Receive()
	assert.NoError(t, err)
	_, err = fakeClient.Receive()
	assert.NoError(t, err)

	assert.Empty(t, s.readStates, "read states should be cleared after applying empty remapping")
	assert.Nil(t, s.pending.Load(), "pending update should be cleared after applying")
}

// TestStateRemapDuringHeldLink verifies that handleStateRemapping guards link
// teardown under s.linkMu so the client monitor goroutine cannot race with the
// session goroutine. This test requires -race to catch violations.
func TestStateRemapDuringHeldLink(t *testing.T) {
	forEachDisconnectMonitor(t, testStateRemapDuringHeldLink)
}

func testStateRemapDuringHeldLink(t *testing.T, timer bool) {
	assert := assert.New(t)
	session, fakeClient, fakePg, backend, closer := newRunningTestSessionWithBackend(t, withDisconnectTimer(timer))
	session.SetLivenessProbe(&fakeLivenessProbe{})
	defer closer()
	oldShedder := backend.loadShedder

	// Acquire a link via BEGIN.
	fakeClient.Send(&pgproto3.Query{String: "BEGIN"})
	assert.NoError(fakeClient.Flush())
	_, err := fakePg.Receive()
	assert.NoError(err)
	fakePg.Send(&pgproto3.EmptyQueryResponse{})
	fakePg.Send(&pgproto3.ReadyForQuery{TxStatus: 'T'})
	assert.NoError(fakePg.Flush())
	_, err = fakeClient.Receive()
	assert.NoError(err)
	_, err = fakeClient.Receive()
	assert.NoError(err)
	assert.NotNil(session.CurrentLink())
	// Shedder ran once for the BEGIN (one handleTransaction call).
	assert.Equal(int64(1), oldShedder.getRunCount())

	// Remap to a new state while the link is held.
	newShedder := &passthroughLoadShedder{}
	newBackend := &fixedBackend{backendLink: &testLink{}, loadShedder: newShedder}
	session.RemapBackend(newBackend)

	// Send SELECT 1 — this is received inside the handleTransaction inner
	// loop, which calls handleStateRemapping.
	fakeClient.Send(&pgproto3.Query{String: "SELECT 1"})
	assert.NoError(fakeClient.Flush())

	// Client gets TransactionRollbackErrorResponse + ReadyForQuery.
	txErr, err := fakeClient.Receive()
	assert.NoError(err)
	assert.Equal(buildTransactionRollbackErrorResponse(), txErr)
	rfq, err := fakeClient.Receive()
	assert.NoError(err)
	assert.Equal(buildReadyForQueryIdleMessage(), rfq)

	// State was remapped; link was released.
	assert.Equal(newBackend, session.Backend())
	assert.Nil(session.CurrentLink())
	// The old shedder's fn was called exactly once (for BEGIN); the remap
	// happened inside the inner loop and did NOT call Run again.
	assert.Equal(int64(1), oldShedder.getRunCount())
}

// queryInHeldTransaction sends query inside an open transaction and checks that the
// held link answers it, rather than a remap aborting the transaction.
func queryInHeldTransaction(t *testing.T, fakeClient *pgproto3.Frontend, fakePg *pgproto3.Backend, query string) {
	t.Helper()
	fakeClient.Send(&pgproto3.Query{String: query})
	require.NoError(t, fakeClient.Flush())
	forwarded := make(chan pgproto3.FrontendMessage, 1)
	go func() {
		msg, _ := fakePg.Receive()
		forwarded <- msg
	}()
	select {
	case msg := <-forwarded:
		require.Equal(t, &pgproto3.Query{String: query}, msg)
	case <-time.After(5 * time.Second):
		t.Fatal("the held link did not receive the query")
	}
	fakePg.Send(&pgproto3.EmptyQueryResponse{})
	fakePg.Send(&pgproto3.ReadyForQuery{TxStatus: 'T'})
	require.NoError(t, fakePg.Flush())
	for _, want := range []pgproto3.BackendMessage{&pgproto3.EmptyQueryResponse{}, &pgproto3.ReadyForQuery{TxStatus: 'T'}} {
		reply, err := fakeClient.Receive()
		require.NoError(t, err)
		require.Equal(t, want, reply)
	}
}

// A read-states remap only affects later link acquisitions, so it applies at the
// next boundary without aborting the transaction that holds a link.
func TestReadStatesRemapKeepsHeldTransaction(t *testing.T) {
	session, fakeClient, fakePg, backend, closer := newRunningTestSessionWithBackend(t)
	defer closer()
	beginHeldTransaction(t, session, fakeClient, fakePg)
	heldLink := session.CurrentLink()

	readBackend := &fixedBackend{backendLink: &testLink{}, loadShedder: &passthroughLoadShedder{}}
	session.RemapReadStates([]ReadStateConfig{{Backend: readBackend, Weight: 1}})
	queryInHeldTransaction(t, fakeClient, fakePg, "SELECT 1")

	assert.Same(t, heldLink, session.CurrentLink())
	assert.Same(t, backend, session.Backend())
	assert.Len(t, session.readStates, 1)
}

// RemapBackend(nil) cancels a queued backend: the next boundary keeps the current
// backend and the transaction that holds a link.
func TestRemapBackendNilCancelsQueuedBackend(t *testing.T) {
	session, fakeClient, fakePg, backend, closer := newRunningTestSessionWithBackend(t)
	defer closer()
	beginHeldTransaction(t, session, fakeClient, fakePg)
	heldLink := session.CurrentLink()

	session.RemapBackend(&fixedBackend{backendLink: &testLink{}, loadShedder: &passthroughLoadShedder{}})
	session.RemapBackend(nil)
	assert.Nil(t, session.PendingBackend())
	queryInHeldTransaction(t, fakeClient, fakePg, "SELECT 1")

	assert.Same(t, backend, session.Backend())
	assert.Same(t, heldLink, session.CurrentLink())
}

// Read-states and backend remaps queued before one boundary apply together: read
// states first, then the backend, which aborts the held transaction once.
func TestQueuedRemapsApplyTogether(t *testing.T) {
	var reports []string
	report := func(kind string, applied bool) {
		state := "queued"
		if applied {
			state = "applied"
		}
		reports = append(reports, kind+" "+state)
	}
	session, fakeClient, fakePg, _, closer := newRunningTestSessionWithBackend(t, func(cfg *Config) {
		cfg.Hooks = Hooks{
			BackendChanged:    func(_, pending Backend) { report("backend", pending == nil) },
			ReadStatesChanged: func(_, pending []ReadStateConfig) { report("read states", pending == nil) },
		}
	})
	defer closer()
	beginHeldTransaction(t, session, fakeClient, fakePg)

	readBackend := &fixedBackend{backendLink: &testLink{}, loadShedder: &passthroughLoadShedder{}}
	nextBackend := &fixedBackend{backendLink: &testLink{}, loadShedder: &passthroughLoadShedder{}}
	session.RemapReadStates([]ReadStateConfig{{Backend: readBackend, Weight: 1}})
	session.RemapBackend(nextBackend)
	fakeClient.Send(&pgproto3.Query{String: "SELECT 1"})
	require.NoError(t, fakeClient.Flush())
	for _, want := range []pgproto3.BackendMessage{buildTransactionRollbackErrorResponse(), buildReadyForQueryIdleMessage()} {
		reply, err := fakeClient.Receive()
		require.NoError(t, err)
		require.Equal(t, want, reply)
	}

	assert.Equal(t, []string{"read states queued", "backend queued", "read states applied", "backend applied"}, reports)
	assert.Same(t, nextBackend, session.Backend())
	assert.Len(t, session.readStates, 1)
	assert.Nil(t, session.CurrentLink())
}

// TestClientDisconnectDuringHeldLink verifies that when the client disconnects
// while a link is held, the client monitor sets clientDisconnected and the session
// continues to handle messages correctly. The session remains responsive and
// eventually releases the link.
func TestClientDisconnectDuringHeldLink(t *testing.T) {
	forEachDisconnectMonitor(t, testClientDisconnectDuringHeldLink)
}

func testClientDisconnectDuringHeldLink(t *testing.T, timer bool) {
	assert := assert.New(t)
	probe := &fakeLivenessProbe{}
	session, fakeClient, fakePg, closer := newRunningTestSession(t, withDisconnectTimer(timer))
	session.SetLivenessProbe(probe)
	defer closer()

	// Acquire a link via BEGIN.
	fakeClient.Send(&pgproto3.Query{String: "BEGIN"})
	assert.NoError(fakeClient.Flush())
	_, err := fakePg.Receive()
	assert.NoError(err)
	fakePg.Send(&pgproto3.EmptyQueryResponse{})
	fakePg.Send(&pgproto3.ReadyForQuery{TxStatus: 'T'})
	assert.NoError(fakePg.Flush())
	_, err = fakeClient.Receive()
	assert.NoError(err)
	_, err = fakeClient.Receive()
	assert.NoError(err)
	assert.NotNil(session.CurrentLink())

	// Trigger client disconnect — monitor sets clientDisconnected and tries ForceClose
	// (no-op in tests since the link's conn is nil).
	probe.triggerDisconnect()
	time.Sleep(200 * time.Millisecond)
	assert.True(session.ClientDisconnected())

	// Send COMMIT to unblock the inner loop's receiveOneMessage.
	fakeClient.Send(&pgproto3.Query{String: "COMMIT"})
	assert.NoError(fakeClient.Flush())
	_, err = fakePg.Receive()
	assert.NoError(err)

	// PG releases the link.
	fakePg.Send(&pgproto3.EmptyQueryResponse{})
	fakePg.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	assert.NoError(fakePg.Flush())
	_, err = fakeClient.Receive()
	assert.NoError(err)
	_, err = fakeClient.Receive()
	assert.NoError(err)

	assert.Nil(session.CurrentLink())
	assert.True(session.ClientDisconnected())
}
