// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package session

import (
	"bytes"
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/DataDog/pg-proxy/pkg/prepared"
)

// deadlineTimeoutError is a net.Error timeout, mirroring os.ErrDeadlineExceeded as
// surfaced by a net.Conn whose read deadline has passed.
type deadlineTimeoutError struct{}

func (deadlineTimeoutError) Error() string   { return "i/o timeout" }
func (deadlineTimeoutError) Timeout() bool   { return true }
func (deadlineTimeoutError) Temporary() bool { return true }

type dummyAddr struct{}

func (dummyAddr) Network() string { return "test" }
func (dummyAddr) String() string  { return "test" }

// scriptableClientConn is a deterministic in-memory net.Conn for driving the
// session's client side. Read returns fed bytes, or a net-timeout error once the
// read deadline is in the past (as a real net.Conn does while draining). It lets a
// test reproduce a drain read deadline firing while the session is blocked
// mid-transaction, without net.Pipe timing races.
type scriptableClientConn struct {
	mu       sync.Mutex
	cond     *sync.Cond
	readBuf  []byte
	writeBuf bytes.Buffer
	deadline time.Time
	closed   bool
	timedOut chan struct{} // signalled each time a Read returns a timeout
}

func newScriptableClientConn() *scriptableClientConn {
	c := &scriptableClientConn{timedOut: make(chan struct{}, 8)}
	c.cond = sync.NewCond(&c.mu)
	return c
}

// feed queues client->proxy bytes for the session's Backend to read.
func (c *scriptableClientConn) feed(b []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.readBuf = append(c.readBuf, b...)
	c.cond.Broadcast()
}

func (c *scriptableClientConn) Read(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		if c.closed {
			return 0, io.EOF
		}
		// Available data is returned before honouring the deadline, matching a
		// real conn that has bytes buffered when the deadline expires.
		if len(c.readBuf) > 0 {
			n := copy(p, c.readBuf)
			c.readBuf = c.readBuf[n:]
			return n, nil
		}
		if !c.deadline.IsZero() && !time.Now().Before(c.deadline) {
			select {
			case c.timedOut <- struct{}{}:
			default:
			}
			return 0, deadlineTimeoutError{}
		}
		c.cond.Wait()
	}
}

func (c *scriptableClientConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, io.ErrClosedPipe
	}
	return c.writeBuf.Write(p)
}

func (c *scriptableClientConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	c.cond.Broadcast()
	return nil
}

func (c *scriptableClientConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deadline = t
	c.cond.Broadcast()
	return nil
}

func (c *scriptableClientConn) SetDeadline(t time.Time) error    { return c.SetReadDeadline(t) }
func (c *scriptableClientConn) SetWriteDeadline(time.Time) error { return nil }
func (c *scriptableClientConn) LocalAddr() net.Addr              { return dummyAddr{} }
func (c *scriptableClientConn) RemoteAddr() net.Addr             { return dummyAddr{} }

func newPortableSession(ctx context.Context, client ClientTransport, clientConn net.Conn, backend Backend, configure func(*Config)) *Session {
	cfg := Config{
		Context:               ctx,
		User:                  "testuser",
		Database:              "testdb",
		Service:               "testservice",
		PID:                   1234,
		SecretKey:             []byte{0, 0, 22, 46},
		Client:                client,
		ClientConn:            clientConn,
		Backend:               backend,
		PostgresMsgBufferSize: 8192,
		PreparedStatements:    prepared.CreateProxyPreparedStmtsState([]string{}),
		MetricTags:            []string{"user:testuser", "database:testdatabase", "client_service_account:testclient"},
	}
	if configure != nil {
		configure(&cfg)
	}
	return New(cfg)
}

func TestDrainDoesNotStartNewTransactionAfterCurrentOneEnds(t *testing.T) {
	assert := assert.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fakeClient, clientConn, clientNetConn := newFrontendBackendPair()
	pgConn, fakePg, _ := newFrontendBackendPair()
	backend := frontendBackend("testcluster", pgConn)
	session := newPortableSession(ctx, clientConn, clientNetConn, backend, nil)

	done := make(chan error, 1)
	go func() { done <- session.Run() }()

	// Open a transaction: the client sends BEGIN and Postgres reports it is now in a
	// transaction ('T'), so the session stays linked and non-idle.
	fakeClient.Send(&pgproto3.Query{String: "BEGIN"})
	assert.NoError(fakeClient.Flush())
	_, err := fakePg.Receive()
	assert.NoError(err)
	fakePg.Send(&pgproto3.CommandComplete{})
	fakePg.Send(&pgproto3.ReadyForQuery{TxStatus: 'T'})
	assert.NoError(fakePg.Flush())
	for i := 0; i < 2; i++ {
		_, err = fakeClient.Receive()
		assert.NoError(err)
	}
	assert.NotNil(session.CurrentLink())
	assert.False(session.Idle())

	// Draining begins (pod shutdown) while the transaction is still open.
	cancel()

	// The client pipelines two messages in a single flush: COMMIT (which ends the
	// in-flight transaction) immediately followed by BEGIN (which would open a new
	// one). The proxy must serve the COMMIT and then refuse to start the new tx.
	fakeClient.Send(&pgproto3.Query{String: "COMMIT"})
	fakeClient.Send(&pgproto3.Query{String: "BEGIN"})
	assert.NoError(fakeClient.Flush())

	// Only the COMMIT should reach Postgres. The session forwards it and we report
	// the transaction has ended ('I'), which releases the link and makes the session
	// idle.
	msg, err := fakePg.Receive()
	require.NoError(t, err)
	require.Equal(t, &pgproto3.Query{String: "COMMIT"}, msg)
	fakePg.Send(&pgproto3.CommandComplete{})
	fakePg.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	assert.NoError(fakePg.Flush())

	// The client sees the COMMIT replies, then an AdminShutdown ErrorResponse as the
	// session drains — never a ReadyForQuery acknowledging a second transaction.
	commandComplete, err := fakeClient.Receive()
	assert.NoError(err)
	assert.IsType(&pgproto3.CommandComplete{}, commandComplete)
	readyForQuery, err := fakeClient.Receive()
	assert.NoError(err)
	assert.Equal(&pgproto3.ReadyForQuery{TxStatus: 'I'}, readyForQuery)
	drainMsg, err := fakeClient.Receive()
	assert.NoError(err)
	errResp, ok := drainMsg.(*pgproto3.ErrorResponse)
	assert.True(ok, "expected ErrorResponse but got %#v", drainMsg)
	assert.Equal(pgerrcode.AdminShutdown, errResp.Code)
	assert.True(strings.HasPrefix(errResp.Message, clientDrainPrefix), errResp.Message)

	// The session terminated cleanly via drain().
	select {
	case runErr := <-done:
		assert.NoError(runErr)
	case <-time.After(2 * time.Second):
		t.Fatal("session did not drain and exit within 2 seconds")
	}

	// The pipelined BEGIN was never serviced: no second link was acquired, so no new
	// transaction was started, and the session is left unlinked.
	assert.Equal(int32(1), backend.acquisitions.Load(), "a second transaction was started during draining")
	assertLinkReleased(t, session)
}

func TestDrainWaitsForAutocommitQuery(t *testing.T) {
	const insert = "INSERT INTO t VALUES (1)"
	for _, tc := range []struct {
		name      string
		requests  []pgproto3.FrontendMessage
		responses []pgproto3.BackendMessage
	}{
		{
			name:     "simple",
			requests: []pgproto3.FrontendMessage{&pgproto3.Query{String: insert}},
			responses: []pgproto3.BackendMessage{
				&pgproto3.CommandComplete{CommandTag: []byte("INSERT 0 1")},
				&pgproto3.ReadyForQuery{TxStatus: 'I'},
			},
		},
		{
			name: "extended",
			requests: []pgproto3.FrontendMessage{
				&pgproto3.Parse{Query: insert},
				&pgproto3.Bind{},
				&pgproto3.Execute{},
				&pgproto3.Sync{},
			},
			responses: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{},
				&pgproto3.BindComplete{},
				&pgproto3.CommandComplete{CommandTag: []byte("INSERT 0 1")},
				&pgproto3.ReadyForQuery{TxStatus: 'I'},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			clientConn, proxyClientConn := net.Pipe()
			proxyPgConn, postgresConn := net.Pipe()
			connections := []net.Conn{clientConn, proxyClientConn, proxyPgConn, postgresConn}
			var done chan struct{}
			t.Cleanup(func() {
				cancel()
				for _, conn := range connections {
					conn.Close()
				}
				if done != nil {
					select {
					case <-done:
					case <-time.After(5 * time.Second):
						t.Error("session did not exit after closing the test connections")
					}
				}
			})
			for _, conn := range connections {
				require.NoError(t, conn.SetDeadline(time.Now().Add(5*time.Second)))
			}

			client := pgproto3.NewFrontend(clientConn, clientConn)
			postgres := pgproto3.NewBackend(postgresConn, postgresConn)
			backend := frontendBackend("testcluster", pgproto3.NewFrontend(proxyPgConn, proxyPgConn))
			session := newPortableSession(ctx, pgproto3.NewBackend(proxyClientConn, proxyClientConn), proxyClientConn, backend, nil)
			var runErr error
			done = make(chan struct{})
			go func() {
				runErr = session.Run()
				close(done)
			}()

			for _, request := range tc.requests {
				client.Send(request)
			}
			require.NoError(t, client.Flush())
			for _, request := range tc.requests {
				msg, err := postgres.Receive()
				require.NoError(t, err)
				require.IsType(t, request, msg)
			}

			// Drain after the full request reaches Postgres, but before it replies.
			require.False(t, session.Idle())
			cancel()
			for _, response := range tc.responses {
				postgres.Send(response)
			}
			require.NoError(t, postgres.Flush())

			// The query must finish before the client receives the retry-safe drain error.
			for _, response := range tc.responses {
				msg, err := client.Receive()
				require.NoError(t, err)
				require.Equal(t, response, msg)
			}
			msg, err := client.Receive()
			require.NoError(t, err)
			drainError, ok := msg.(*pgproto3.ErrorResponse)
			require.True(t, ok, "expected a drain error, got %T", msg)
			require.Equal(t, "FATAL", drainError.Severity)
			require.Equal(t, pgerrcode.AdminShutdown, drainError.Code)
			require.True(t, strings.HasPrefix(drainError.Message, clientDrainPrefix), drainError.Message)

			select {
			case <-done:
				require.NoError(t, runErr)
			case <-time.After(5 * time.Second):
				t.Fatal("session did not drain and exit")
			}
			require.Nil(t, session.CurrentLink())
			require.Equal(t, int32(1), backend.acquisitions.Load())
		})
	}
}

// TestDrainReadTimeoutDoesNotAbortInFlightTransaction verifies that a client
// read deadline armed during graceful drain (to unblock a stuck Receive) must not
// abort an in-flight transaction being served by the connection-checkout inner
// loop. The transaction should be served to completion (COMMIT forwarded to PG)
// and the session should then drain cleanly, exactly as the outer receive loop
// already does for the same timeout.
func TestDrainReadTimeoutDoesNotAbortInFlightTransaction(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clientConn := newScriptableClientConn()
	pgConn, fakePg, _ := newFrontendBackendPair()
	backend := frontendBackend("testcluster", pgConn)
	session := newPortableSession(ctx, pgproto3.NewBackend(clientConn, clientConn), clientConn, backend, nil)

	done := make(chan error, 1)
	go func() { done <- session.Run() }()

	encode := func(m pgproto3.FrontendMessage) []byte {
		b, err := m.Encode(nil)
		require.NoError(err)
		return b
	}

	// 1) Open a transaction: client BEGIN is forwarded to PG, which reports it is
	// now in a transaction ('T'), so the link stays held and the session is not
	// idle. fakePg.Receive() returning proves the outer client read of BEGIN has
	// already completed — so the only remaining client read is the inner loop's.
	clientConn.feed(encode(&pgproto3.Query{String: "BEGIN"}))
	_, err := fakePg.Receive()
	require.NoError(err)
	fakePg.Send(&pgproto3.CommandComplete{})
	fakePg.Send(&pgproto3.ReadyForQuery{TxStatus: 'T'})
	require.NoError(fakePg.Flush())

	// 2) Draining begins and the drain watcher arms a (past) read deadline that
	// fires while the session is blocked mid-transaction inside handleTransaction.
	cancel()
	require.NoError(clientConn.SetReadDeadline(time.Now().Add(-time.Second)))

	// The inner loop must observe the timeout without aborting the transaction.
	select {
	case <-clientConn.timedOut:
	case <-time.After(2 * time.Second):
		t.Fatal("session never observed the drain read timeout")
	}

	// 3) The client sends COMMIT. A correct (non-aborting) inner loop re-armed the
	// deadline and keeps serving, so COMMIT reaches PG. If the timeout had aborted
	// the transaction, COMMIT would never be forwarded and this Receive would hang.
	clientConn.feed(encode(&pgproto3.Query{String: "COMMIT"}))
	msg, err := fakePg.Receive()
	require.NoError(err)
	q, ok := msg.(*pgproto3.Query)
	require.True(ok, "expected a Query, got %#v", msg)
	assert.Equal("COMMIT", q.String)
	fakePg.Send(&pgproto3.CommandComplete{})
	fakePg.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	require.NoError(fakePg.Flush())

	// 4) With the transaction complete and draining active, the session drains
	// cleanly rather than returning an error.
	select {
	case runErr := <-done:
		assert.NoError(runErr)
	case <-time.After(3 * time.Second):
		t.Fatal("session did not drain and exit")
	}
	assert.Equal(int32(1), backend.acquisitions.Load(), "no second transaction should have been started")
	assertLinkReleased(t, session)
}

// This file verifies that a FLASh "requests-in-flight" (RIF) load-shedder slot
// (passthroughLoadShedder.activeCount) is held for exactly the duration of a whole
// backend connection checkout — from the message that starts loadShedder.Run's
// fn (handleTransaction) until that fn returns — and released exactly once,
// across every teardown path handleTransaction can take.
//
// Determinism: activeCount is only asserted to be 1 right after a happens-before
// rendezvous that proves handleTransaction is mid-flight (fakePg.Receive()
// returning a message the test fed to the client side — that can only happen
// once the session goroutine has forwarded it from inside fn()). activeCount is
// only asserted to be 0 after the session goroutine has observably finished the
// checkout, established via newRunningTestSession's closer() (which blocks on the
// session's `done` channel, i.e. on session.Run() having returned) or, in the
// disconnect scenario, via session.Run()'s own return channel. No assertion is
// ever gated on a bare time.Sleep.

// TestRIFSlotHeldExactlyForCheckoutDuration_NormalRelease verifies scenario (a):
// BEGIN acquires the RIF slot, COMMIT (ReadyForQuery{'I'}) releases the link and
// ends the checkout, and the slot is released exactly once.
func TestRIFSlotHeldExactlyForCheckoutDuration_NormalRelease(t *testing.T) {
	assert := assert.New(t)
	session, fakeClient, fakePg, backend, closer := newRunningTestSessionWithBackend(t)
	defer closer()
	shedder := backend.loadShedder

	fakeClient.Send(&pgproto3.Query{String: "BEGIN"})
	assert.NoError(fakeClient.Flush())
	_, err := fakePg.Receive()
	assert.NoError(err)
	// fakePg.Receive() returning proves the proxy forwarded BEGIN from inside
	// loadShedder.Run's fn(), so the RIF slot is currently held.
	assert.Equal(int64(1), shedder.getActiveCount())

	fakePg.Send(&pgproto3.CommandComplete{})
	fakePg.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	assert.NoError(fakePg.Flush())
	_, err = fakeClient.Receive()
	assert.NoError(err)
	_, err = fakeClient.Receive()
	assert.NoError(err)
	assertLinkReleased(t, session)

	// Force the session to fully exit. closer() blocks on the session's `done`
	// channel (session.Run() having returned), which is the only race-free way
	// to observe that passthroughLoadShedder.Run's defer decrement already ran.
	closer()

	assert.Equal(int64(0), shedder.getActiveCount())
	assert.Equal(int64(1), shedder.getRunCount())
}

// TestRIFSlotHeldExactlyForCheckoutDuration_TerminateMidCheckout verifies
// scenario (b): a Terminate received while a link is held (inside
// handleTransaction's inner loop) closes the session and releases the slot.
func TestRIFSlotHeldExactlyForCheckoutDuration_TerminateMidCheckout(t *testing.T) {
	assert := assert.New(t)
	session, fakeClient, fakePg, backend, closer := newRunningTestSessionWithBackend(t)
	defer closer()
	shedder := backend.loadShedder

	fakeClient.Send(&pgproto3.Query{String: "BEGIN"})
	assert.NoError(fakeClient.Flush())
	_, err := fakePg.Receive()
	assert.NoError(err)
	assert.Equal(int64(1), shedder.getActiveCount())

	fakePg.Send(&pgproto3.CommandComplete{})
	fakePg.Send(&pgproto3.ReadyForQuery{TxStatus: 'T'})
	assert.NoError(fakePg.Flush())
	_, err = fakeClient.Receive()
	assert.NoError(err)
	_, err = fakeClient.Receive()
	assert.NoError(err)
	assert.NotNil(session.CurrentLink())
	assert.Equal(int64(1), shedder.getActiveCount())

	// Terminate mid-checkout: interceptBypassingMessage detects it inside
	// handleTransaction's inner loop, calls s.close (releasing the link), and
	// returns an error wrapping io.EOF that unwinds handleTransaction.
	fakeClient.Send(&pgproto3.Terminate{})
	assert.NoError(fakeClient.Flush())

	// closer() sends a second (harmless, unread) Terminate and waits for
	// session.Run() to return, which is strictly after the mock's defer
	// decrement for this checkout.
	closer()

	assert.Equal(int64(0), shedder.getActiveCount())
	assert.Equal(int64(1), shedder.getRunCount())
}

// TestRIFSlotHeldExactlyForCheckoutDuration_ClientDisconnectMidCheckout verifies
// scenario (c): a client disconnect detected mid-checkout (clientDisconnected set by the
// liveness probe, and the underlying conn subsequently returning an error/EOF)
// causes handleTransaction's inner loop to close the session, releasing the slot.
func TestRIFSlotHeldExactlyForCheckoutDuration_ClientDisconnectMidCheckout(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	clientConn := newScriptableClientConn()
	pgConn, fakePg, _ := newFrontendBackendPair()
	shedder := &passthroughLoadShedder{}
	probe := &fakeLivenessProbe{}
	backend := frontendBackend("testcluster", pgConn)
	backend.loadShedder = shedder
	session := newPortableSession(context.Background(), pgproto3.NewBackend(clientConn, clientConn), clientConn, backend, func(cfg *Config) {
		cfg.LivenessProbe = probe
	})

	done := make(chan error, 1)
	go func() { done <- session.Run() }()

	encode := func(m pgproto3.FrontendMessage) []byte {
		b, err := m.Encode(nil)
		require.NoError(err)
		return b
	}

	// Open a transaction: BEGIN is forwarded and PG reports 'T', so the
	// checkout (link) stays open.
	clientConn.feed(encode(&pgproto3.Query{String: "BEGIN"}))
	_, err := fakePg.Receive()
	require.NoError(err)
	// fakePg.Receive() returning proves the checkout is mid-flight.
	assert.Equal(int64(1), shedder.getActiveCount())

	fakePg.Send(&pgproto3.CommandComplete{})
	fakePg.Send(&pgproto3.ReadyForQuery{TxStatus: 'T'})
	require.NoError(fakePg.Flush())

	// Trigger the disconnect condition. clientDisconnected must be true AND the next
	// client Read must fail for handleTransaction's inner-loop receiveMessage
	// error path to classify the resulting error as a client disconnect and
	// close the session.
	probe.triggerDisconnect()
	// The client monitor polls every 100ms (see client_monitor.go). Polling here
	// (bounded by a generous timeout, not a bare sleep) only affects *when*
	// clientDisconnected flips; the actual RIF-release assertion below is gated on
	// session.Run() returning via `done`, not on this wait.
	require.Eventually(func() bool { return session.ClientDisconnected() }, 2*time.Second, 10*time.Millisecond)

	clientConn.Close() // next Read() on the client conn returns io.EOF.

	select {
	case runErr := <-done:
		assert.Error(runErr)
	case <-time.After(2 * time.Second):
		t.Fatal("session did not exit after client disconnect mid-checkout")
	}

	// session.Run() having returned (received from `done`) happens-after the
	// mock's defer decrement for this checkout.
	assert.Equal(int64(0), shedder.getActiveCount())
	assert.Equal(int64(1), shedder.getRunCount())
}

// TestRIFSlotHeldExactlyForCheckoutDuration_StateRemapMidCheckout verifies
// scenario (d): a pending state remap observed inside handleTransaction's inner
// loop force-releases the link (ending the checkout without dispatching the
// triggering message), releasing the slot on the *old* state's shedder.
func TestRIFSlotHeldExactlyForCheckoutDuration_StateRemapMidCheckout(t *testing.T) {
	assert := assert.New(t)
	session, fakeClient, fakePg, backend, closer := newRunningTestSessionWithBackend(t)
	session.SetLivenessProbe(&fakeLivenessProbe{})
	defer closer()
	oldShedder := backend.loadShedder

	fakeClient.Send(&pgproto3.Query{String: "BEGIN"})
	assert.NoError(fakeClient.Flush())
	_, err := fakePg.Receive()
	assert.NoError(err)
	assert.Equal(int64(1), oldShedder.getActiveCount())

	fakePg.Send(&pgproto3.EmptyQueryResponse{})
	fakePg.Send(&pgproto3.ReadyForQuery{TxStatus: 'T'})
	assert.NoError(fakePg.Flush())
	_, err = fakeClient.Receive()
	assert.NoError(err)
	_, err = fakeClient.Receive()
	assert.NoError(err)
	assert.NotNil(session.CurrentLink())

	newShedder := &passthroughLoadShedder{}
	newBackend := &fixedBackend{backendLink: &testLink{}, loadShedder: newShedder}
	session.RemapBackend(newBackend)

	// SELECT 1 is received inside handleTransaction's inner loop, which calls
	// handleStateRemapping: it force-releases the (old-state) link and returns
	// true, so this message is never dispatched and the checkout ends here.
	fakeClient.Send(&pgproto3.Query{String: "SELECT 1"})
	assert.NoError(fakeClient.Flush())

	txErr, err := fakeClient.Receive()
	assert.NoError(err)
	assert.Equal(buildTransactionRollbackErrorResponse(), txErr)
	rfq, err := fakeClient.Receive()
	assert.NoError(err)
	assert.Equal(buildReadyForQueryIdleMessage(), rfq)
	assert.Equal(newBackend, session.Backend())
	assertLinkReleased(t, session)

	// Force full shutdown to synchronize on the session goroutine having
	// returned from handleTransaction before reading activeCount.
	closer()

	assert.Equal(int64(0), oldShedder.getActiveCount())
	assert.Equal(int64(1), oldShedder.getRunCount())
	assert.Equal(int64(0), newShedder.getRunCount(), "the new shedder must never be invoked: the remap ends the checkout without starting a new one")
}

// TestRIFSlotHeldExactlyForCheckoutDuration_HandlerErrorMidCheckout verifies
// scenario (e): a genuine (non-disconnect) message-handling error raised inside
// handleTransaction's inner loop closes the session and releases the slot.
//
// The error is induced deterministically: after BEGIN opens a held link, PG
// replies to a subsequent simple Query with a message type the proxy does not
// expect for that reply (ParseComplete instead of CommandComplete/ReadyForQuery).
// DecidePgMessageAction rejects the mismatch, so flushPostgres -> handleAction ->
// handleClientMessage -> handleOneMessage all propagate the error, and
// handleOneMessage calls s.close() before returning it.
func TestRIFSlotHeldExactlyForCheckoutDuration_HandlerErrorMidCheckout(t *testing.T) {
	assert := assert.New(t)
	session, fakeClient, fakePg, backend, closer := newRunningTestSessionWithBackend(t)
	defer closer()
	shedder := backend.loadShedder

	fakeClient.Send(&pgproto3.Query{String: "BEGIN"})
	assert.NoError(fakeClient.Flush())
	_, err := fakePg.Receive()
	assert.NoError(err)
	assert.Equal(int64(1), shedder.getActiveCount())

	fakePg.Send(&pgproto3.CommandComplete{})
	fakePg.Send(&pgproto3.ReadyForQuery{TxStatus: 'T'})
	assert.NoError(fakePg.Flush())
	_, err = fakeClient.Receive()
	assert.NoError(err)
	_, err = fakeClient.Receive()
	assert.NoError(err)
	assert.NotNil(session.CurrentLink())
	assert.Equal(int64(1), shedder.getActiveCount())

	fakeClient.Send(&pgproto3.Query{String: "SELECT 1"})
	assert.NoError(fakeClient.Flush())
	_, err = fakePg.Receive()
	assert.NoError(err)

	// Deliberately the wrong reply type for a QueryReply expectation.
	fakePg.Send(&pgproto3.ParseComplete{})
	assert.NoError(fakePg.Flush())

	// s.close() sends a FATAL ErrorResponse to the client before the session
	// goroutine returns from handleOneMessage's error path.
	msg, err := fakeClient.Receive()
	assert.NoError(err)
	errResp, ok := msg.(*pgproto3.ErrorResponse)
	assert.True(ok, "expected ErrorResponse from s.close(), got %T", msg)
	assert.Equal("FATAL", errResp.Severity)

	// closer() waits for session.Run() to fully return, which happens strictly
	// after the mock's defer decrement for this checkout.
	closer()

	assert.Equal(int64(0), shedder.getActiveCount())
	assert.Equal(int64(1), shedder.getRunCount())
}

// TestRIFSlotReacquiredPerCheckout_SequentialCheckouts verifies scenario (f):
// two independent, sequential checkouts on the same session each acquire and
// release the slot without leaking it.
//
// This needs a custom harness rather than newRunningTestSession: its fixedBackend
// only permits a single link acquisition (it errors on the second), which cannot
// model two sequential checkouts. Here the fake backend returns a fresh link on
// every checkout.
//
// Note on what is (and isn't) asserted "between" the two checkouts: the mock's
// activeCount decrement for checkout 1 and its increment for checkout 2 happen
// in program order inside the same session goroutine, so the second
// fakePg.Receive() — which can only unblock once the session goroutine forwards
// the second query from inside fn(), itself strictly after checkout 2's
// increment, itself strictly after checkout 1's decrement — proves checkout 1
// released its slot before checkout 2 acquired its own. There is no
// synchronization primitive in this harness whose completion is ordered
// strictly *between* those two events, so this test does not sample
// getActiveCount()==0 in that gap (that would be an unsynchronized, racy read).
// The "never leaked" property is instead established by (1) the mock's panic
// guard, which fires on any double-release, and (2) the final assertion that
// activeCount is back to 0 (with runCount==2) after both checkouts and teardown.
func TestRIFSlotReacquiredPerCheckout_SequentialCheckouts(t *testing.T) {
	assert := assert.New(t)

	fakeClient, clientConn, clientNetConn := newFrontendBackendPair()
	pgConn, fakePg, _ := newFrontendBackendPair()
	shedder := &passthroughLoadShedder{}

	// A fresh link per checkout (no single-link guard), reusing the same PG side
	// since the two checkouts are strictly sequential.
	backend := frontendBackend("testcluster", pgConn)
	backend.loadShedder = shedder
	session := newPortableSession(context.Background(), clientConn, clientNetConn, backend, nil)

	done := make(chan error, 1)
	go func() { done <- session.Run() }()

	// Checkout 1.
	fakeClient.Send(&pgproto3.Query{String: "SELECT 1"})
	assert.NoError(fakeClient.Flush())
	_, err := fakePg.Receive()
	assert.NoError(err)
	// fakePg.Receive() returning proves checkout 1 is mid-flight (slot held).
	assert.Equal(int64(1), shedder.getActiveCount())
	assert.Equal(int64(1), shedder.getRunCount())

	fakePg.Send(&pgproto3.EmptyQueryResponse{})
	fakePg.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	assert.NoError(fakePg.Flush())
	_, err = fakeClient.Receive()
	assert.NoError(err)
	_, err = fakeClient.Receive()
	assert.NoError(err)

	// Checkout 2: independent simple query, new link acquired.
	fakeClient.Send(&pgproto3.Query{String: "SELECT 2"})
	assert.NoError(fakeClient.Flush())
	_, err = fakePg.Receive()
	assert.NoError(err)
	// Slot re-acquired for a second, distinct checkout (runCount advanced to 2,
	// exactly one slot held).
	assert.Equal(int64(1), shedder.getActiveCount())
	assert.Equal(int64(2), shedder.getRunCount())

	fakePg.Send(&pgproto3.EmptyQueryResponse{})
	fakePg.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	assert.NoError(fakePg.Flush())
	_, err = fakeClient.Receive()
	assert.NoError(err)
	_, err = fakeClient.Receive()
	assert.NoError(err)

	// Terminate winds the session down; joining on done happens-after the mock's
	// defer decrement for checkout 2.
	fakeClient.Send(&pgproto3.Terminate{})
	assert.NoError(fakeClient.Flush())
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("session did not exit after Terminate")
	}

	assert.Equal(int64(2), shedder.getRunCount())
	assert.Equal(int64(0), shedder.getActiveCount())
}
