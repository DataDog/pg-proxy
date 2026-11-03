package session

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/DataDog/pg-proxy/pkg/query"
)

// This file contains simple e2e unit tests using fake channels between the client->session->pg.
// TODO(ORGSTORE-303): We should use integration tests using a real client and postgres for functional testing.

func TestSimple(t *testing.T) {
	t.Run("reused_buffer", func(t *testing.T) {
		assert := assert.New(t)
		session, fakeClient, fakePg, closer := newRunningTestSession(t)
		defer closer()
		traceState := session.TraceState()

		fakeClient.Send(&pgproto3.Query{String: "SELECT 1"})
		assert.NoError(fakeClient.Flush())

		bemsg, err := fakePg.Receive()
		assert.NotNil(traceState.RootTrace())
		assert.EqualValues(&pgproto3.Query{String: "SELECT 1"}, bemsg)
		assert.NoError(err)
		assert.NotNil(session.CurrentLink())

		fakePg.Send(&pgproto3.EmptyQueryResponse{})
		fakePg.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		assert.NoError(fakePg.Flush())

		femsg, err := fakeClient.Receive()
		assert.EqualValues(&pgproto3.EmptyQueryResponse{}, femsg)
		assert.NoError(err)
		femsg, err = fakeClient.Receive()
		assert.EqualValues(&pgproto3.ReadyForQuery{TxStatus: 'I'}, femsg)
		assert.NoError(err)
		assert.Nil(session.CurrentLink())
	})
}

// TestClientForwarding verifies a row-returning reply is forwarded to the client byte-for-byte.
func TestClientForwarding(t *testing.T) {
	t.Run("reused_buffer", func(t *testing.T) {
		assert := assert.New(t)
		session, fakeClient, fakePg, closer := newRunningTestSession(t)
		defer closer()

		fakeClient.Send(&pgproto3.Query{String: "SELECT 1"})
		assert.NoError(fakeClient.Flush())
		bemsg, err := fakePg.Receive()
		assert.NoError(err)
		assert.EqualValues(&pgproto3.Query{String: "SELECT 1"}, bemsg)

		// PG returns a complete simple-query reply.
		fakePg.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{Name: []byte("?column?")}}})
		fakePg.Send(&pgproto3.DataRow{Values: [][]byte{[]byte("1")}})
		fakePg.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
		fakePg.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		assert.NoError(fakePg.Flush())

		// The client must receive the full reply in order.
		msg, err := fakeClient.Receive()
		assert.NoError(err)
		assert.IsType(&pgproto3.RowDescription{}, msg)
		msg, err = fakeClient.Receive()
		assert.NoError(err)
		if dr, ok := msg.(*pgproto3.DataRow); assert.Truef(ok, "expected *pgproto3.DataRow, got %T", msg) {
			assert.Len(dr.Values, 1)
			assert.Equal("1", string(dr.Values[0]))
		}
		msg, err = fakeClient.Receive()
		assert.NoError(err)
		assert.IsType(&pgproto3.CommandComplete{}, msg)
		msg, err = fakeClient.Receive()
		assert.NoError(err)
		assert.IsType(&pgproto3.ReadyForQuery{}, msg)
		assert.Nil(session.CurrentLink())
	})
}

// TestClientForwarding_DeliversBufferedMessagesBeforeError verifies that messages already
// received from PG (e.g. RowDescription + DataRow) still reach the client when a FATAL
// ErrorResponse aborts the reply before the next flush. The reused-buffer path stages them in a
// call-local buffer that close() never sees, so it must flush that buffer on the error return.
func TestClientForwarding_DeliversBufferedMessagesBeforeError(t *testing.T) {
	t.Run("reused_buffer", func(t *testing.T) {
		assert := assert.New(t)
		_, fakeClient, fakePg, closer := newRunningTestSession(t)
		defer closer()

		fakeClient.Send(&pgproto3.Query{String: "SELECT 1"})
		assert.NoError(fakeClient.Flush())
		_, err := fakePg.Receive()
		assert.NoError(err)

		// PG streams a row, then dies with a FATAL before completing the reply. DecidePgMessageAction
		// turns the FATAL into a Go error that aborts flushPostgres before the row is flushed.
		fakePg.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{Name: []byte("?column?")}}})
		fakePg.Send(&pgproto3.DataRow{Values: [][]byte{[]byte("1")}})
		fakePg.Send(&pgproto3.ErrorResponse{Severity: "FATAL", Code: "57P01", Message: "terminating connection due to administrator command"})
		assert.NoError(fakePg.Flush())

		// The already-received row must reach the client before the terminating error. Bail if it
		// is missing rather than block forever on a reply the truncating path never sends.
		msg, err := fakeClient.Receive()
		assert.NoError(err)
		if !assert.IsType(&pgproto3.RowDescription{}, msg, "partial reply was truncated before the error") {
			return
		}
		msg, err = fakeClient.Receive()
		assert.NoError(err)
		if dr, ok := msg.(*pgproto3.DataRow); assert.Truef(ok, "expected *pgproto3.DataRow, got %T", msg) {
			assert.Equal("1", string(dr.Values[0]))
		}
		msg, err = fakeClient.Receive()
		assert.NoError(err)
		if errResp, ok := msg.(*pgproto3.ErrorResponse); assert.Truef(ok, "expected *pgproto3.ErrorResponse, got %T", msg) {
			assert.Equal("FATAL", errResp.Severity)
		}
	})
}

func TestSessionLoadShedding(t *testing.T) {
	assert := assert.New(t)
	session, fakeClient, _, backend, closer := newRunningTestSessionWithBackend(t)
	defer closer()
	backend.loadShedder.active.Store(true)

	fakeClient.Send(&pgproto3.Query{String: "SELECT 1"})
	assert.NoError(fakeClient.Flush())
	femsg, err := fakeClient.Receive()
	assert.NoError(err)
	assert.IsType(&pgproto3.ErrorResponse{}, femsg, "expected ErrorResponse but got %#v", femsg)
	errorResponse := femsg.(*pgproto3.ErrorResponse)
	assert.Equal("ERROR", errorResponse.Severity)
	assert.Equal(pgerrcode.InsufficientResources, errorResponse.Code)
	assert.Contains(errorResponse.Message, "This pg-proxy instance is experiencing high load and has rejected to serve the query as a protection. Please retry.")

	// Simple queries are followed immediately by ReadyForQuery so the client knows the
	// session is ready for a new command.
	rfq, err := fakeClient.Receive()
	assert.NoError(err)
	assert.Equal(&pgproto3.ReadyForQuery{TxStatus: 'I'}, rfq)
	assert.Nil(session.CurrentLink())
}

// TestPingBypassesLoadShedding verifies that pgx ping queries ("-- ping") are
// answered with ReadyForQuery even when the load shedder is active. Pings must
// never be rejected: pgxpool destroys and recreates the connection on error,
// amplifying load during shedding.
func TestPingBypassesLoadShedding(t *testing.T) {
	assert := assert.New(t)
	session, fakeClient, _, backend, closer := newRunningTestSessionWithBackend(t)
	defer closer()
	backend.loadShedder.active.Store(true)
	fakeClient.Send(&pgproto3.Query{String: "-- ping"})
	assert.NoError(fakeClient.Flush())
	msg, err := fakeClient.Receive()
	assert.NoError(err)
	assert.Equal(&pgproto3.ReadyForQuery{TxStatus: 'I'}, msg, "ping must receive ReadyForQuery, not an error")
	// No backend connection should have been acquired.
	assert.Nil(session.CurrentLink())
}

// TestTerminateBypassesLoadShedding verifies that a Terminate message closes the
// session cleanly even when the load shedder is active.
func TestTerminateBypassesLoadShedding(t *testing.T) {
	assert := assert.New(t)
	_, fakeClient, _, backend, closer := newRunningTestSessionWithBackend(t)
	backend.loadShedder.active.Store(true)
	// Send Terminate explicitly. If the load shedder incorrectly intercepted it,
	// the session would send back an ErrorResponse and keep running.
	fakeClient.Send(&pgproto3.Terminate{})
	assert.NoError(fakeClient.Flush())
	// closer() will attempt to send a second Terminate, which goes unread into
	// the buffer (the session has already exited), and then wait for the goroutine
	// to exit. Wrap with a timeout so the test fails fast if the session is stuck.
	done := make(chan struct{})
	go func() { defer close(done); closer() }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("session did not exit after Terminate: load shedder may have rejected the Terminate message")
	}
}

// TestHandleOneMessageFinishesTraceOnError verifies that when handleClientMessage
// returns an error, handleOneMessage calls s.close() and FinishRootTrace before
// returning.
func TestHandleOneMessageFinishesTraceOnError(t *testing.T) {
	assert := assert.New(t)
	fakeClient, client, clientConn := newFrontendBackendPair()
	backend := &fixedBackend{acquireErr: errors.New("injected link error"), loadShedder: &passthroughLoadShedder{}}
	session := New(Config{Context: context.Background(), Client: client, ClientConn: clientConn, Backend: backend})
	done := make(chan error, 1)
	go func() { done <- session.Run() }()

	// Force link acquisition to fail so handleClientMessage returns an error.
	// StartRootTrace has already been called by then, so FinishRootTrace must
	// be called by handleOneMessage's error path to avoid leaking the span.
	fakeClient.Send(&pgproto3.Query{String: "SELECT 1"})
	assert.NoError(fakeClient.Flush())
	// s.close() sends a FATAL ErrorResponse; receiving it confirms close was called.
	msg, err := fakeClient.Receive()
	assert.NoError(err)
	errResp, ok := msg.(*pgproto3.ErrorResponse)
	assert.True(ok, "expected ErrorResponse from s.close(), got %T", msg)
	assert.Equal("FATAL", errResp.Severity)
	// Waiting for the session goroutine establishes happens-before with any writes it made.
	<-done
	assert.Nil(session.TraceState().RootTrace(), "FinishRootTrace must be called by handleOneMessage's error path")
}

func TestSessionLoadSheddingExtendedQuery(t *testing.T) {
	assert := assert.New(t)
	session, fakeClient, _, backend, closer := newRunningTestSessionWithBackend(t)
	defer closer()
	backend.loadShedder.active.Store(true)
	// Send a full extended-query pipeline in one shot (as real drivers do).
	fakeClient.Send(&pgproto3.Parse{Query: "SELECT 1"})
	fakeClient.Send(&pgproto3.Bind{})
	fakeClient.Send(&pgproto3.Execute{})
	fakeClient.Send(&pgproto3.Sync{})
	assert.NoError(fakeClient.Flush())
	// Parse is rejected: client receives ErrorResponse but NOT ReadyForQuery yet —
	// that only arrives after Sync, matching standard Postgres extended-query semantics.
	femsg, err := fakeClient.Receive()
	assert.NoError(err)
	errorResponse, ok := femsg.(*pgproto3.ErrorResponse)
	assert.True(ok, "expected ErrorResponse, got %#v", femsg)
	assert.Equal(pgerrcode.InsufficientResources, errorResponse.Code)
	// Bind and Execute are silently discarded; Sync triggers ReadyForQuery.
	rfq, err := fakeClient.Receive()
	assert.NoError(err)
	assert.Equal(&pgproto3.ReadyForQuery{TxStatus: 'I'}, rfq)
	assert.Nil(session.CurrentLink())
	assert.False(session.DiscardUntilSync())
}

// TestSessionLoadSheddingExtendedQueryOutOfOrder verifies that an out-of-order extended
// query message (e.g. Execute sent without a preceding Parse) is handled the same way as
// any other extended query message when load shedding is active: the proxy enters
// discard-until-sync mode and only sends ReadyForQuery after the client sends Sync.
func TestSessionLoadSheddingExtendedQueryOutOfOrder(t *testing.T) {
	assert := assert.New(t)
	session, fakeClient, _, backend, closer := newRunningTestSessionWithBackend(t)
	defer closer()
	backend.loadShedder.active.Store(true)
	// Send Execute out of order (no preceding Parse/Bind) followed by Sync.
	fakeClient.Send(&pgproto3.Execute{})
	fakeClient.Send(&pgproto3.Sync{})
	assert.NoError(fakeClient.Flush())
	// Execute is rejected: ErrorResponse but no ReadyForQuery until Sync.
	femsg, err := fakeClient.Receive()
	assert.NoError(err)
	errorResponse, ok := femsg.(*pgproto3.ErrorResponse)
	assert.True(ok, "expected ErrorResponse, got %#v", femsg)
	assert.Equal(pgerrcode.InsufficientResources, errorResponse.Code)
	// Sync triggers ReadyForQuery.
	rfq, err := fakeClient.Receive()
	assert.NoError(err)
	assert.Equal(&pgproto3.ReadyForQuery{TxStatus: 'I'}, rfq)
	assert.Nil(session.CurrentLink())
	assert.False(session.DiscardUntilSync())
}

// TestDiscardUntilSyncClearedWithinReceiveAndHandleMessage verifies that
// discardUntilSync is always false on entry to receiveAndHandleMessage — the drain
// is inline (within drainUntilSync) and does not leak into subsequent calls.
//
// If the drain were deferred across calls, the second Parse below would be silently
// dropped (discarded as-if mid-pipeline) rather than rejected with an ErrorResponse.
func TestDiscardUntilSyncClearedWithinReceiveAndHandleMessage(t *testing.T) {
	assert := assert.New(t)
	session, fakeClient, _, backend, closer := newRunningTestSessionWithBackend(t)
	defer closer()
	backend.loadShedder.active.Store(true)
	sendPipeline := func() {
		fakeClient.Send(&pgproto3.Parse{Query: "SELECT 1"})
		fakeClient.Send(&pgproto3.Bind{})
		fakeClient.Send(&pgproto3.Execute{})
		fakeClient.Send(&pgproto3.Sync{})
		assert.NoError(fakeClient.Flush())
	}
	receivePipelineRejection := func() {
		femsg, err := fakeClient.Receive()
		assert.NoError(err)
		errResp, ok := femsg.(*pgproto3.ErrorResponse)
		assert.True(ok, "expected ErrorResponse, got %T", femsg)
		assert.Equal(pgerrcode.InsufficientResources, errResp.Code)
		rfq, err := fakeClient.Receive()
		assert.NoError(err)
		assert.Equal(&pgproto3.ReadyForQuery{TxStatus: 'I'}, rfq)
	}
	// First rejection.
	sendPipeline()
	receivePipelineRejection()
	// Second rejection: if discardUntilSync leaked, this Parse would be silently
	// dropped and receivePipelineRejection would block waiting for an ErrorResponse
	// that never arrives.
	sendPipeline()
	receivePipelineRejection()
	assert.False(session.DiscardUntilSync())
}

// TestSessionLoadSheddingDoesNotRejectActiveLink verifies that a session with an
// active link (in-flight transaction) is never rejected by load shedding, preserving
// transactional correctness.
func TestSessionLoadSheddingDoesNotRejectActiveLink(t *testing.T) {
	assert := assert.New(t)
	session, fakeClient, fakePg, backend, closer := newRunningTestSessionWithBackend(t)
	defer closer()
	// Start a transaction so the session acquires a link.
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
	// Activate load shedding while the link is still open.
	backend.loadShedder.active.Store(true)
	// The query must be forwarded to Postgres, not rejected.
	fakeClient.Send(&pgproto3.Query{String: "SELECT 1"})
	assert.NoError(fakeClient.Flush())
	msg, err := fakePg.Receive()
	assert.NoError(err)
	assert.IsType(&pgproto3.Query{}, msg)
	// Reply with TxStatus 'I' to release the link before the closer sends Terminate.
	fakePg.Send(&pgproto3.EmptyQueryResponse{})
	fakePg.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	assert.NoError(fakePg.Flush())
	_, err = fakeClient.Receive()
	assert.NoError(err)
	_, err = fakeClient.Receive()
	assert.NoError(err)
	assert.Nil(session.CurrentLink())
	// The shedder ran exactly once: for the BEGIN, which started handleTransaction.
	// SELECT 1 was handled inside the inner loop and did NOT trigger an additional Run call.
	assert.Equal(int64(1), backend.loadShedder.runCount.Load())
}

func TestExtendedQueryAndNotice(t *testing.T) {
	t.Run("reused_buffer", func(t *testing.T) {
		assert := assert.New(t)
		session, fakeClient, fakePg, closer := newRunningTestSession(t)
		traceState := session.TraceState()
		defer closer()
		fakeClient.Send(&pgproto3.Parse{Query: "SELECT 1"})
		fakeClient.Send(&pgproto3.Bind{})
		fakeClient.Send(&pgproto3.Execute{})
		fakeClient.Send(&pgproto3.Sync{})
		assert.NoError(fakeClient.Flush())
		for i := 0; i < 4; i++ {
			_, err := fakePg.Receive()
			assert.NoError(err)
		}
		assert.NotNil(session.CurrentLink())
		assert.NotNil(traceState.RootTrace())
		pgResponses := []pgproto3.BackendMessage{
			&pgproto3.ParseComplete{}, &pgproto3.NoticeResponse{Message: "testing"},
			&pgproto3.BindComplete{}, &pgproto3.EmptyQueryResponse{},
			&pgproto3.NoticeResponse{Message: "testing2"},
			&pgproto3.ParameterStatus{Name: "testing", Value: "can be sent at any time"},
			&pgproto3.ReadyForQuery{TxStatus: 'I'},
		}
		for _, resp := range pgResponses {
			fakePg.Send(resp)
		}
		assert.NoError(fakePg.Flush())
		for _, want := range pgResponses {
			msg, err := fakeClient.Receive()
			assert.IsType(want, msg)
			assert.NoError(err)
		}
		assert.Nil(session.CurrentLink()) // session now unlinked
	})
}

func TestMultiStatementTxn(t *testing.T) {
	t.Run("reused_buffer", func(t *testing.T) {
		assert := assert.New(t)
		session, fakeClient, fakePg, closer := newRunningTestSession(t)
		traceState := session.TraceState()
		defer closer()
		// In this example, the fakePg will reply with TxStatus=T so the link will remain
		fakeClient.Send(&pgproto3.Query{String: "BEGIN"})
		assert.NoError(fakeClient.Flush())
		_, err := fakePg.Receive()
		assert.NoError(err)
		assert.NotNil(session.CurrentLink())
		assert.NotNil(traceState.RootTrace())
		fakePg.Send(&pgproto3.EmptyQueryResponse{})
		fakePg.Send(&pgproto3.ReadyForQuery{TxStatus: 'T'})
		assert.NoError(fakePg.Flush())
		_, err = fakeClient.Receive()
		assert.NoError(err)
		_, err = fakeClient.Receive()
		assert.NoError(err)
		assert.NotNil(session.CurrentLink()) // still linked due to TxStatus
		// Now the link will be removed
		fakeClient.Send(&pgproto3.Query{String: "ABORT"})
		assert.NoError(fakeClient.Flush())
		_, err = fakePg.Receive()
		assert.NoError(err)
		assert.NotNil(session.CurrentLink())
		assert.NotNil(traceState.RootTrace())
		fakePg.Send(&pgproto3.EmptyQueryResponse{})
		fakePg.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		assert.NoError(fakePg.Flush())
		_, err = fakeClient.Receive()
		assert.NoError(err)
		_, err = fakeClient.Receive()
		assert.NoError(err)
		assert.Nil(session.CurrentLink()) // link is removed
	})
}

func TestHoldUntilSync(t *testing.T) {
	t.Run("reused_buffer", func(t *testing.T) {
		assert := assert.New(t)
		session, fakeClient, fakePg, closer := newRunningTestSession(t)
		traceState := session.TraceState()
		defer closer()
		// This test uses extended query mode, but sends a Flush between messages.
		// The link should not be released until the Sync.
		fakeClient.Send(&pgproto3.Parse{Query: "SELECT 1"})
		fakeClient.Send(&pgproto3.Bind{})
		fakeClient.Send(&pgproto3.Execute{})
		fakeClient.Send(&pgproto3.Flush{})
		assert.NoError(fakeClient.Flush())
		for i := 0; i < 4; i++ {
			_, err := fakePg.Receive()
			assert.NoError(err)
		}
		assert.NotNil(session.CurrentLink())
		assert.NotNil(traceState.RootTrace())
		fakePg.Send(&pgproto3.ParseComplete{})
		fakePg.Send(&pgproto3.BindComplete{})
		fakePg.Send(&pgproto3.EmptyQueryResponse{})
		assert.NoError(fakePg.Flush())
		for i := 0; i < 3; i++ {
			_, err := fakeClient.Receive()
			assert.NoError(err)
		}
		assert.NotNil(session.CurrentLink()) // still linked until a sync
		// Now send the sync to unlink
		fakeClient.Send(&pgproto3.Sync{})
		assert.NoError(fakeClient.Flush())
		_, err := fakePg.Receive()
		assert.NoError(err)
		assert.NotNil(session.CurrentLink())
		assert.NotNil(traceState.RootTrace()) // Since any message now starts a new trace
		fakePg.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		assert.NoError(fakePg.Flush())
		_, err = fakeClient.Receive()
		assert.NoError(err)
		assert.Nil(session.CurrentLink()) // link is removed
	})
}

func TestDiscardUntilSync(t *testing.T) {
	t.Run("reused_buffer", func(t *testing.T) {
		assert := assert.New(t)
		session, fakeClient, fakePg, closer := newRunningTestSession(t)
		traceState := session.TraceState()
		defer closer()
		fakeClient.Send(&pgproto3.Parse{Query: "sql syntax error"})
		fakeClient.Send(&pgproto3.Bind{})
		fakeClient.Send(&pgproto3.Execute{})
		fakeClient.Send(&pgproto3.Sync{})
		assert.NoError(fakeClient.Flush())
		for i := 0; i < 4; i++ {
			_, err := fakePg.Receive()
			assert.NoError(err)
		}
		assert.NotNil(session.CurrentLink())
		assert.NotNil(traceState.RootTrace())
		fakePg.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Message: "syntax error"})
		fakePg.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		assert.NoError(fakePg.Flush())
		for range 2 {
			_, err := fakeClient.Receive()
			assert.NoError(err)
		}
		assert.Nil(session.CurrentLink()) // session unlinked, even though we never got a Bind/Execute reply
	})
}

func TestSyncToTxStatus(t *testing.T) {
	t.Run("reused_buffer", func(t *testing.T) {
		assert := assert.New(t)
		session, fakeClient, fakePg, closer := newRunningTestSession(t)
		defer closer()
		traceState := session.TraceState()
		syncMsg := func(txByte byte) error {
			fakeClient.Send(&pgproto3.Sync{})
			assert.NoError(fakeClient.Flush())
			_, err := fakePg.Receive()
			assert.NoError(err)
			assert.NotNil(traceState.RootTrace()) // Since any message now starts a new trace
			fakePg.Send(&pgproto3.ReadyForQuery{TxStatus: txByte})
			assert.NoError(fakePg.Flush())
			_, err = fakeClient.Receive()
			return err
		}
		// T will keep a session linked InTransaction
		err := syncMsg('T')
		assert.NoError(err)
		assert.NotNil(session.CurrentLink())
		assert.False(session.CurrentLink().Idle())
		assert.Equal(query.InTransaction, session.CurrentLink().TxStatus())
		// E will continue to keep the session linked InErroredTransaction
		err = syncMsg('E')
		assert.NoError(err)
		assert.NotNil(session.CurrentLink())
		assert.False(session.CurrentLink().Idle())
		assert.Equal(query.InErroredTransaction, session.CurrentLink().TxStatus())
		// I will unlink the session as it is now idle (also tested above)
		err = syncMsg('I')
		assert.NoError(err)
		assert.Nil(session.CurrentLink())
	})
}

func TestSessionFailsToLink(t *testing.T) {
	assert := assert.New(t)
	fakeClient, client, clientConn := newFrontendBackendPair()
	session := New(Config{
		Context: context.Background(), User: "testuser", Database: "testdb", PID: 1234,
		Client: client, ClientConn: clientConn,
		Backend: &fixedBackend{acquireErr: errors.New("testing"), loadShedder: &passthroughLoadShedder{}},
	})
	done := make(chan bool, 1)
	go func() {
		err := session.Run()
		assert.ErrorContains(err, "error linking session to a postgres conn: testing")
		done <- true
	}()
	fakeClient.Send(&pgproto3.Query{String: "SELECT 1"})
	assert.NoError(fakeClient.Flush())
	reply, err := fakeClient.Receive()
	assert.NoError(err)
	assert.Equal(reply, &pgproto3.ErrorResponse{
		Severity: "FATAL", Code: pgerrcode.ConnectionFailure,
		Message: "closing pg-proxy session with err: error linking session to a postgres conn: testing",
	})
	<-done
}

func TestCientReceivesErrorOnCloseWithError(t *testing.T) {
	assert := assert.New(t)
	session, fakeClient, _, closer := newRunningTestSession(t)
	defer closer()
	session.Close(errors.New("this is my test!"))
	reply, err := fakeClient.Receive()
	assert.NoError(err)
	assert.Equal(reply, &pgproto3.ErrorResponse{
		Severity: "FATAL", Code: pgerrcode.ConnectionFailure,
		Message: "closing pg-proxy session with err: this is my test!",
	})
}

func beginHeldTransaction(t *testing.T, session *Session, fakeClient *pgproto3.Frontend, fakePg *pgproto3.Backend) {
	t.Helper()
	fakeClient.Send(&pgproto3.Query{String: "BEGIN"})
	require.NoError(t, fakeClient.Flush())
	_, err := fakePg.Receive()
	require.NoError(t, err)
	fakePg.Send(&pgproto3.EmptyQueryResponse{})
	fakePg.Send(&pgproto3.ReadyForQuery{TxStatus: 'T'})
	require.NoError(t, fakePg.Flush())
	_, err = fakeClient.Receive()
	require.NoError(t, err)
	_, err = fakeClient.Receive()
	require.NoError(t, err)
	require.NotNil(t, session.CurrentLink())
}

// TestRIFHeldAcrossMultiMessageTransaction verifies that the load shedder's Run
// is called exactly once for a full BEGIN→COMMIT transaction: both messages are
// handled inside the same handleTransaction call.
func TestRIFHeldAcrossMultiMessageTransaction(t *testing.T) {
	assert := assert.New(t)
	session, fakeClient, fakePg, backend, closer := newRunningTestSessionWithBackend(t)
	defer closer()
	beginHeldTransaction(t, session, fakeClient, fakePg)
	assert.Equal(int64(1), backend.loadShedder.runCount.Load())
	// COMMIT — received inside the inner loop of the same handleTransaction call.
	fakeClient.Send(&pgproto3.Query{String: "COMMIT"})
	assert.NoError(fakeClient.Flush())
	_, err := fakePg.Receive()
	assert.NoError(err)
	fakePg.Send(&pgproto3.EmptyQueryResponse{})
	fakePg.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	assert.NoError(fakePg.Flush())
	_, err = fakeClient.Receive()
	assert.NoError(err)
	_, err = fakeClient.Receive()
	assert.NoError(err)
	// Still 1: COMMIT was handled inside the same loadShedder.Run call as BEGIN.
	assert.Equal(int64(1), backend.loadShedder.runCount.Load())
	assert.Nil(session.CurrentLink())
}

// TestDiscardUntilSyncInsideInnerLoop verifies that when a Parse inside a held
// transaction causes a PG error, the proxy enters discard-until-sync mode and
// the Sync correctly releases the link.
func TestDiscardUntilSyncInsideInnerLoop(t *testing.T) {
	assert := assert.New(t)
	session, fakeClient, fakePg, closer := newRunningTestSession(t)
	defer closer()
	// Acquire a link via BEGIN.
	beginHeldTransaction(t, session, fakeClient, fakePg)
	// Send Parse (which will error) + Sync inside the inner loop.
	fakeClient.Send(&pgproto3.Parse{Query: "sql syntax error"})
	fakeClient.Send(&pgproto3.Sync{})
	assert.NoError(fakeClient.Flush())
	// PG receives Parse + Sync (flushed together when Sync triggers flushPostgres).
	for i := 0; i < 2; i++ {
		_, err := fakePg.Receive()
		assert.NoError(err)
	}
	// PG responds with an error + ReadyForQuery{I} (releasing the link).
	fakePg.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Message: "syntax error"})
	fakePg.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	assert.NoError(fakePg.Flush())
	// Client receives ErrorResponse + ReadyForQuery.
	for i := 0; i < 2; i++ {
		_, err := fakeClient.Receive()
		assert.NoError(err)
	}
	// Link was released by flushPostgres after ReadyForQuery{I}.
	assert.Nil(session.CurrentLink())
}

// TestTerminateDuringHeldLink verifies that a Terminate message received inside
// the handleTransaction inner loop causes the session to close the link and
// exit cleanly (wrapping io.EOF).
func TestTerminateDuringHeldLink(t *testing.T) {
	assert := assert.New(t)
	session, fakeClient, fakePg, closer := newRunningTestSession(t)
	beginHeldTransaction(t, session, fakeClient, fakePg)
	// Send Terminate while inside the inner loop.
	// interceptBypassingMessage detects it, calls s.close (which releases the link),
	// and returns an error wrapping io.EOF.
	fakeClient.Send(&pgproto3.Terminate{})
	assert.NoError(fakeClient.Flush())
	// closer() waits for the session goroutine to finish (via the done channel).
	// This also sends a second Terminate (harmless — nobody reads it after exit).
	closer()
	assert.Nil(session.CurrentLink())
}

// TestCheapQueryDoesNotAcquireRIF verifies that pgx health-check pings bypass
// the load shedder entirely (runCount stays 0), while other "cheap" queries
// such as ";" go through the shedder (runCount increments).
func TestCheapQueryDoesNotAcquireRIF(t *testing.T) {
	assert := assert.New(t)
	_, fakeClient, fakePg, backend, closer := newRunningTestSessionWithBackend(t)
	defer closer()
	// pgx ping is intercepted and bypasses the shedder.
	fakeClient.Send(&pgproto3.Query{String: "-- ping"})
	assert.NoError(fakeClient.Flush())
	rfq, err := fakeClient.Receive()
	assert.NoError(err)
	assert.Equal(&pgproto3.ReadyForQuery{TxStatus: 'I'}, rfq)
	assert.Equal(int64(0), backend.loadShedder.runCount.Load())
	// ";" is not a ping — it goes through the shedder and reaches PG.
	fakeClient.Send(&pgproto3.Query{String: ";"})
	assert.NoError(fakeClient.Flush())
	_, err = fakePg.Receive()
	assert.NoError(err)
	// runCount incremented by the shedder BEFORE calling fn().
	assert.Equal(int64(1), backend.loadShedder.runCount.Load())
	fakePg.Send(&pgproto3.EmptyQueryResponse{})
	fakePg.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	assert.NoError(fakePg.Flush())
	_, err = fakeClient.Receive()
	assert.NoError(err)
	_, err = fakeClient.Receive()
	assert.NoError(err)
}

// TestPgxPingDuringHeldLinkKeepsRIFUntilRelease verifies that a pgx ping received
// while a backend connection is held (link != nil) is answered with ReadyForQuery{I}
// but does NOT release the checkout/RIF. The shedder's Run count stays at 1
// throughout, and a subsequent in-transaction message is still forwarded to PG
// through the same link rather than being treated as a new checkout.
func TestPgxPingDuringHeldLinkKeepsRIFUntilRelease(t *testing.T) {
	assert := assert.New(t)
	session, fakeClient, fakePg, backend, closer := newRunningTestSessionWithBackend(t)
	defer closer()
	beginHeldTransaction(t, session, fakeClient, fakePg)
	assert.Equal(int64(1), backend.loadShedder.runCount.Load())
	// pgx ping while the link is held: must be answered but NOT release the RIF.
	fakeClient.Send(&pgproto3.Query{String: "-- ping"})
	assert.NoError(fakeClient.Flush())
	rfq, err := fakeClient.Receive()
	assert.NoError(err)
	assert.Equal(&pgproto3.ReadyForQuery{TxStatus: 'I'}, rfq, "ping must receive ReadyForQuery{I}")
	// RIF is still held: the shedder was not called again and the link is non-nil.
	assert.Equal(int64(1), backend.loadShedder.runCount.Load(), "shedder must not be called again after ping")
	assert.NotNil(session.CurrentLink(), "link must still be held after ping")
	// COMMIT: forwarded through the existing link (same checkout, runCount stays 1).
	fakeClient.Send(&pgproto3.Query{String: "COMMIT"})
	assert.NoError(fakeClient.Flush())
	msg, err := fakePg.Receive()
	assert.NoError(err)
	assert.IsType(&pgproto3.Query{}, msg, "COMMIT must reach PG, not be rejected")
	fakePg.Send(&pgproto3.EmptyQueryResponse{})
	fakePg.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	assert.NoError(fakePg.Flush())
	_, err = fakeClient.Receive()
	assert.NoError(err)
	_, err = fakeClient.Receive()
	assert.NoError(err)
	assert.Equal(int64(1), backend.loadShedder.runCount.Load(), "COMMIT must be in the same checkout as BEGIN")
	assert.Nil(session.CurrentLink())
}

// TestLoadSheddingExtendedDrainIgnoresPgxPingUntilSync verifies that a pgx ping
// received during an extended-query drain (after a load-shedding rejection, before
// Sync) is discarded along with the rest of the pipeline. It must NOT produce its
// own ReadyForQuery response, and it must NOT truncate the drain early. The client
// must receive exactly one ErrorResponse followed by exactly one ReadyForQuery{I},
// and discardUntilSync must be false afterwards.
func TestLoadSheddingExtendedDrainIgnoresPgxPingUntilSync(t *testing.T) {
	assert := assert.New(t)
	session, fakeClient, _, backend, closer := newRunningTestSessionWithBackend(t)
	defer closer()
	backend.loadShedder.active.Store(true)
	fakeClient.Send(&pgproto3.Parse{Query: "SELECT 1"})
	// pgx ping arrives mid-drain (before Sync): must be silently discarded.
	fakeClient.Send(&pgproto3.Query{String: "-- ping"})
	// Sync terminates the drain and triggers exactly one ReadyForQuery.
	fakeClient.Send(&pgproto3.Sync{})
	assert.NoError(fakeClient.Flush())
	femsg, err := fakeClient.Receive()
	assert.NoError(err)
	errResp, ok := femsg.(*pgproto3.ErrorResponse)
	assert.True(ok, "expected ErrorResponse, got %T", femsg)
	assert.Equal(pgerrcode.InsufficientResources, errResp.Code)
	rfq, err := fakeClient.Receive()
	assert.NoError(err)
	assert.Equal(&pgproto3.ReadyForQuery{TxStatus: 'I'}, rfq, "Sync must produce exactly one ReadyForQuery")
	assert.False(session.DiscardUntilSync(), "discardUntilSync must be cleared after Sync")
	assert.Nil(session.CurrentLink())
}

// TestLoadSheddingExtendedDrainTerminateClosesSession verifies that a Terminate
// received during an extended-query load-shedding drain closes the session cleanly.
// The drain loop must honor Terminate (via interceptBypassingMessage) instead of
// discarding it and blocking forever waiting for a Sync that never arrives.
func TestLoadSheddingExtendedDrainTerminateClosesSession(t *testing.T) {
	assert := assert.New(t)
	_, fakeClient, _, backend, closer := newRunningTestSessionWithBackend(t)
	backend.loadShedder.active.Store(true)
	fakeClient.Send(&pgproto3.Parse{Query: "SELECT 1"})
	fakeClient.Send(&pgproto3.Terminate{})
	assert.NoError(fakeClient.Flush())
	// The load-shedding ErrorResponse for Parse is delivered before the drain reads Terminate.
	femsg, err := fakeClient.Receive()
	assert.NoError(err)
	errResp, ok := femsg.(*pgproto3.ErrorResponse)
	assert.True(ok, "expected ErrorResponse, got %T", femsg)
	assert.Equal(pgerrcode.InsufficientResources, errResp.Code)
	done := make(chan struct{})
	go func() { defer close(done); closer() }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("session did not exit after Terminate during drain: drain may have discarded Terminate")
	}
}

// TestPerMessageMetricsInInnerLoop verifies that the load shedder's fn is called
// once per outer receiveAndHandleMessage cycle (not once per message), so a
// BEGIN+COMMIT transaction has runCount==1. Activating the shedder after the
// transaction and sending a new query rejects it without incrementing runCount.
func TestPerMessageMetricsInInnerLoop(t *testing.T) {
	assert := assert.New(t)
	session, fakeClient, fakePg, backend, closer := newRunningTestSessionWithBackend(t)
	defer closer()
	beginHeldTransaction(t, session, fakeClient, fakePg)
	assert.Equal(int64(1), backend.loadShedder.runCount.Load())
	// COMMIT — handled inside the same handleTransaction call.
	fakeClient.Send(&pgproto3.Query{String: "COMMIT"})
	assert.NoError(fakeClient.Flush())
	_, err := fakePg.Receive()
	assert.NoError(err)
	fakePg.Send(&pgproto3.EmptyQueryResponse{})
	fakePg.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	assert.NoError(fakePg.Flush())
	_, err = fakeClient.Receive()
	assert.NoError(err)
	_, err = fakeClient.Receive()
	assert.NoError(err)
	// fn was called only once for the entire BEGIN→COMMIT transaction.
	assert.Equal(int64(1), backend.loadShedder.runCount.Load())
	assert.Nil(session.CurrentLink())
	// Activate the shedder and send a new query — it must be rejected.
	backend.loadShedder.active.Store(true)
	fakeClient.Send(&pgproto3.Query{String: "SELECT 1"})
	assert.NoError(fakeClient.Flush())
	femsg, err := fakeClient.Receive()
	assert.NoError(err)
	errorResp, ok := femsg.(*pgproto3.ErrorResponse)
	assert.True(ok, "expected ErrorResponse, got %T", femsg)
	assert.Equal(pgerrcode.InsufficientResources, errorResp.Code)
	rfq, err := fakeClient.Receive()
	assert.NoError(err)
	assert.Equal(&pgproto3.ReadyForQuery{TxStatus: 'I'}, rfq)
	// runCount still 1: the shedder rejected the SELECT without calling fn.
	assert.Equal(int64(1), backend.loadShedder.runCount.Load())
}

// TestErrorAfterCommandComplete verifies nonfatal recovery and fatal disconnects
// after Execute has completed and the proxy is awaiting Sync. The timeout case
// injects the wire sequence; it does not reproduce a real statement-timeout race.
func TestErrorAfterCommandComplete(t *testing.T) {
	for _, tc := range []struct {
		name     string
		code     string
		message  string
		severity string
	}{
		{"statement_timeout", pgerrcode.QueryCanceled, "canceling statement due to statement timeout", "ERROR"},
		{"deferred_constraint", pgerrcode.UniqueViolation, "duplicate key value violates unique constraint", "ERROR"},
		{"fatal", pgerrcode.AdminShutdown, "terminating connection due to administrator command", "FATAL"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, proxyClient, clientConn := newFrontendBackendPair()
			proxyPostgres, postgres, _ := newFrontendBackendPair()
			session := New(Config{
				Context: context.Background(), User: "testuser", Database: "testdb",
				Client: proxyClient, ClientConn: clientConn,
				Backend:               frontendBackend("postgres", proxyPostgres),
				PostgresMsgBufferSize: 8192,
			})
			done, closeSession := runTestSession(t, session, client)
			exited := false
			defer func() {
				if !exited {
					closeSession()
				}
			}()

			request := []pgproto3.FrontendMessage{
				&pgproto3.Parse{Query: "INSERT INTO accounts VALUES (1)"},
				&pgproto3.Bind{ResultFormatCodes: []int16{}},
				&pgproto3.Execute{},
				&pgproto3.Sync{},
			}
			for _, msg := range request {
				client.Send(msg)
			}
			require.NoError(t, client.Flush())
			for _, want := range request {
				got, err := postgres.Receive()
				require.NoError(t, err)
				require.Equal(t, want, got)
			}

			// Inject the exact sequence, without depending on a timeout race.
			replies := []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{},
				&pgproto3.BindComplete{},
				&pgproto3.CommandComplete{CommandTag: []byte("INSERT 0 1")},
				&pgproto3.ErrorResponse{Severity: tc.severity, Code: tc.code, Message: tc.message},
			}
			if tc.severity != "FATAL" {
				replies = append(replies, &pgproto3.ReadyForQuery{TxStatus: 'I'})
			}
			for _, msg := range replies {
				postgres.Send(msg)
			}
			require.NoError(t, postgres.Flush())
			for _, want := range replies {
				got, err := client.Receive()
				require.NoError(t, err)
				if pgErr, ok := want.(*pgproto3.ErrorResponse); ok && tc.severity == "FATAL" {
					require.IsType(t, &pgproto3.ErrorResponse{}, got)
					require.Equal(t, pgErr.Severity, got.(*pgproto3.ErrorResponse).Severity)
					require.Equal(t, pgErr.Code, got.(*pgproto3.ErrorResponse).Code)
				} else {
					require.Equal(t, want, got)
				}
			}
			if tc.severity == "FATAL" {
				select {
				case err := <-done:
					exited = true
					require.ErrorContains(t, err, tc.message)
				case <-time.After(5 * time.Second):
					t.Fatal("fatal error did not end the session")
				}
				return
			}

			// A second extended query on the same client must not be discarded or
			// disconnected, and its reply expectations must still line up.
			request[0] = &pgproto3.Parse{Query: "SELECT 1"}
			for _, msg := range request {
				client.Send(msg)
			}
			require.NoError(t, client.Flush())
			for _, want := range request {
				got, err := postgres.Receive()
				require.NoError(t, err)
				require.Equal(t, want, got)
			}
			replies = []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{},
				&pgproto3.BindComplete{},
				&pgproto3.DataRow{Values: [][]byte{[]byte("1")}},
				&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")},
				&pgproto3.ReadyForQuery{TxStatus: 'I'},
			}
			for _, msg := range replies {
				postgres.Send(msg)
			}
			require.NoError(t, postgres.Flush())
			for _, want := range replies {
				got, err := client.Receive()
				require.NoError(t, err)
				require.Equal(t, want, got)
			}
		})
	}
}
