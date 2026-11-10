// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package session

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"

	pgproxy "github.com/DataDog/pg-proxy"
	"github.com/DataDog/pg-proxy/pkg/link"
	"github.com/DataDog/pg-proxy/pkg/prepared"
	"github.com/DataDog/pg-proxy/pkg/query"
)

type testClient struct {
	sent     []pgproto3.BackendMessage
	events   *[]string
	flushErr error
}

func (*testClient) Receive() (pgproto3.FrontendMessage, error) { return nil, context.Canceled }
func (c *testClient) Send(msg pgproto3.BackendMessage) {
	c.sent = append(c.sent, msg)
	if c.events != nil {
		*c.events = append(*c.events, "send")
	}
}
func (c *testClient) Flush() error {
	if c.events != nil {
		*c.events = append(*c.events, "flush")
	}
	return c.flushErr
}

type flushRecordingHooks struct {
	events  *[]string
	flushed chan struct{}
}

func (h flushRecordingHooks) hooks() Hooks {
	return Hooks{
		BackendMessage:       h.observeBackend,
		ClientFlushSucceeded: h.clientFlushSucceeded,
	}
}

func (h flushRecordingHooks) observeBackend(pgproto3.BackendMessage) {
	if h.events != nil {
		*h.events = append(*h.events, "observe")
	}
}

func (h flushRecordingHooks) clientFlushSucceeded() {
	if h.events != nil {
		*h.events = append(*h.events, "flush_succeeded")
	}
	if h.flushed != nil {
		close(h.flushed)
	}
}

type flushPostgresTestLink struct {
	link.Link
	expectation query.ReplyExpectation
	reply       pgproto3.BackendMessage
}

func (*flushPostgresTestLink) Flush() error { return nil }

func (l *flushPostgresTestLink) Receive() (pgproto3.BackendMessage, error) {
	return l.reply, nil
}

func (l *flushPostgresTestLink) NextExpectation() query.ReplyExpectation {
	return l.expectation
}

func (l *flushPostgresTestLink) ResolveNextExpectation() {
	l.expectation = query.NoReply
}

func (*flushPostgresTestLink) RecordNewNamedPreparedStmt(prepared.PreparedStatement) error {
	return nil
}

type writeRecordingConn struct {
	net.Conn
	writes   int
	written  []byte
	writeErr error
	events   *[]string
}

func (c *writeRecordingConn) Write(p []byte) (int, error) {
	c.writes++
	if c.events != nil {
		*c.events = append(*c.events, "client_write")
	}
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	c.written = append(c.written, p...)
	return len(p), nil
}

type blockingWriteConn struct {
	net.Conn
	started chan struct{}
	release <-chan struct{}
	err     error
}

func (c *blockingWriteConn) Write(p []byte) (int, error) {
	close(c.started)
	<-c.release
	if c.err != nil {
		return 0, c.err
	}
	return len(p), nil
}

// Keep this literal independent of drainMessage to catch changes to the
// prefix that deployed OrgStore clients recognize.
const orgstoreClientDrainPrefix = "Pg-proxy session has been terminated"

func TestRunDrainsCanceledIdleSession(t *testing.T) {
	metrics := &recordingMetrics{}
	restore := pgproxy.Bootstrap(pgproxy.Dependencies{Metrics: metrics})
	t.Cleanup(restore)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client := &testClient{}
	s := New(Config{
		Context:         ctx,
		User:            "user",
		Database:        "database",
		LogicalDatabase: "logical",
		PID:             42,
		SecretKey:       []byte{1, 2, 3, 4},
		Service:         "service",
		Client:          client,
	})

	require.NoError(t, s.Run())
	require.Equal(t, []metricCall{{
		name:  "datadog.pg_proxy.sessions.drained_connections",
		value: 1,
		tags:  []string{"user:user", "database:database", "client_service_account:service", "passthrough_user:false"},
	}}, metrics.counts)
	require.Equal(t, "42", s.Identifier())
	require.Equal(t, "logical", s.LogicalDatabaseID())
	require.Equal(t, []byte{1, 2, 3, 4}, s.SecretKey())
	require.Len(t, client.sent, 1)
	response, ok := client.sent[0].(*pgproto3.ErrorResponse)
	require.True(t, ok)
	require.Equal(t, "57P01", response.Code)
	require.True(t, strings.HasPrefix(response.Message, orgstoreClientDrainPrefix), response.Message)
}

func TestProtocolErrorResponsesRemainStable(t *testing.T) {
	require.Equal(t, "40000", buildTransactionRollbackErrorResponse().Code)
	require.Equal(t, "42P05", buildPreparedStatementsAlreadyExistsErrorResponse("name").Code)
	require.Equal(t, "26000", buildPreparedStatementsDoesNotExistErrorResponse("name").Code)
	require.Equal(t, "42601", buildFailedToParseQueryErrorResponse("select").Code)
}

func TestSessionHooksObserveSuccessfulClientDelivery(t *testing.T) {
	t.Run("buffered action", func(t *testing.T) {
		flushErr := errors.New("flush failed")
		for _, test := range []struct {
			name      string
			flushErr  error
			want      []string
			wantError bool
		}{
			{name: "success", want: []string{"observe", "send", "flush", "flush_succeeded"}},
			{name: "failure", flushErr: flushErr, want: []string{"observe", "send", "flush"}, wantError: true},
		} {
			t.Run(test.name, func(t *testing.T) {
				var events []string
				client := &testClient{events: &events, flushErr: test.flushErr}
				hooks := flushRecordingHooks{events: &events}.hooks()
				session := New(Config{Client: client, Hooks: hooks})
				err := session.handleAction(ClientMessageAction{ReplyMsgToClient: &pgproto3.ReadyForQuery{TxStatus: 'I'}})
				require.Equal(t, test.wantError, err != nil)
				require.Equal(t, test.want, events)
			})
		}
	})

	t.Run("Postgres stream", func(t *testing.T) {
		writeErr := errors.New("client write failed")
		for _, test := range []struct {
			name     string
			writeErr error
		}{
			{name: "success"},
			{name: "failure", writeErr: writeErr},
		} {
			t.Run(test.name, func(t *testing.T) {
				writeStarted := make(chan struct{})
				writeRelease := make(chan struct{})
				clientConn := &blockingWriteConn{
					started: writeStarted,
					release: writeRelease,
					err:     test.writeErr,
				}
				recorder := flushRecordingHooks{flushed: make(chan struct{})}
				session := New(Config{
					Client:     &testClient{},
					ClientConn: clientConn,
					InitialLink: &flushPostgresTestLink{
						expectation: query.BindReply,
						reply:       &pgproto3.BindComplete{},
					},
					Hooks: recorder.hooks(),
				})

				flushDone := make(chan error, 1)
				go func() {
					_, err := session.flushPostgres()
					flushDone <- err
				}()

				select {
				case <-writeStarted:
				case <-time.After(time.Second):
					t.Fatal("Postgres response did not reach the client socket write")
				}
				select {
				case <-recorder.flushed:
					t.Fatal("session hook was notified before the client socket write completed")
				default:
				}

				close(writeRelease)
				select {
				case err := <-flushDone:
					if test.writeErr == nil {
						require.NoError(t, err)
					} else {
						require.ErrorIs(t, err, test.writeErr)
					}
				case <-time.After(time.Second):
					t.Fatal("Postgres flush did not finish after the client socket write was released")
				}

				select {
				case <-recorder.flushed:
					if test.writeErr != nil {
						t.Fatal("session hook was notified after a failed client socket write")
					}
				default:
					if test.writeErr == nil {
						t.Fatal("session hook was not notified after a successful client socket write")
					}
				}
			})
		}
	})

	t.Run("no backend reply expected", func(t *testing.T) {
		clientConn := &writeRecordingConn{}
		backendLink := &testLink{}
		flushSucceeded := 0
		session := New(Config{
			Client: &testClient{}, ClientConn: clientConn, InitialLink: backendLink,
			Hooks: Hooks{ClientFlushSucceeded: func() { flushSucceeded++ }},
		})

		_, err := session.flushPostgres()
		require.NoError(t, err)
		require.Zero(t, clientConn.writes)
		require.Zero(t, flushSucceeded)
		backendLink.assertCalls(t, testLinkCalls{flush: 1, nextExpectation: 1})
	})

	t.Run("duplicate named Parse", func(t *testing.T) {
		writeErr := errors.New("client write failed")
		for _, test := range []struct {
			name       string
			writeErr   error
			wantEvents []string
		}{
			{name: "success", wantEvents: []string{"postgres_flush", "observe", "client_write", "flush_succeeded"}},
			{name: "failure", writeErr: writeErr, wantEvents: []string{"postgres_flush", "observe", "client_write"}},
		} {
			t.Run(test.name, func(t *testing.T) {
				var events []string
				clientConn := &writeRecordingConn{events: &events, writeErr: test.writeErr}
				statement := prepared.CreatePreparedStatement("client-name", "proxy-name", "select 1", nil)
				backendLink := &testLink{
					hasExisting: func(candidate prepared.PreparedStatement) bool { return candidate == statement },
					flush:       func() error { events = append(events, "postgres_flush"); return nil },
				}
				session := New(Config{
					Client: pgproto3.NewBackend(clientConn, clientConn), ClientConn: clientConn,
					InitialLink: backendLink, Hooks: flushRecordingHooks{events: &events}.hooks(),
				})

				proxyToPostgres, err := session.handleRecordNewPreparedStmt(ClientMessageAction{RecordNewPreparedStmt: statement})
				require.False(t, proxyToPostgres)
				if test.writeErr != nil {
					require.ErrorIs(t, err, test.writeErr)
				} else {
					require.NoError(t, err)
					encoded, err := (&pgproto3.ParseComplete{}).Encode(nil)
					require.NoError(t, err)
					require.Equal(t, encoded, clientConn.written)
				}
				require.Equal(t, test.wantEvents, events)
				require.Equal(t, 1, clientConn.writes)
				backendLink.assertCalls(t, testLinkCalls{hasExisting: 1, send: 1, flush: 1, nextExpectation: 1})
			})
		}
	})

	t.Run("suppressed Postgres response", func(t *testing.T) {
		clientConn := &writeRecordingConn{}
		flushSucceeded := 0
		session := New(Config{
			Client:     &testClient{},
			ClientConn: clientConn,
			InitialLink: &flushPostgresTestLink{
				expectation: query.ParseReply,
				reply:       &pgproto3.ParseComplete{},
			},
			Hooks: Hooks{ClientFlushSucceeded: func() { flushSucceeded++ }},
		})
		session.pStmtState.EnqueuePendingStmt(
			prepared.CreatePreparedStatement("client-name", "proxy-name", "select 1", nil),
			true,
		)

		_, err := session.flushPostgres()
		require.NoError(t, err)
		require.Zero(t, clientConn.writes)
		require.Zero(t, flushSucceeded)
	})
}

func TestSessionDecorationIsIndependentOfRouteNotification(t *testing.T) {
	for _, test := range []struct {
		name     string
		decorate bool
		notify   bool
	}{
		{name: "neither"},
		{name: "notification only", notify: true},
		{name: "decoration only", decorate: true},
		{name: "both", decorate: true, notify: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			backendLink := &testLink{}
			backend := &namedBackend{acquire: func(_ context.Context, request LinkRequest) (link.Link, error) {
				require.Equal(t, test.decorate, request.DecorateLink)
				return backendLink, nil
			}}
			notifications := 0
			var hooks Hooks
			if test.notify {
				hooks.RoutedLinkChanged = func(routedLink link.Link) {
					require.Same(t, backendLink, routedLink)
					notifications++
				}
			}
			session := New(Config{Backend: backend, Hooks: hooks, DecorateLink: test.decorate})
			require.NoError(t, session.handleProxyMsgToPg(ClientMessageAction{ProxyMsgToPg: &pgproto3.Query{String: "select 1"}}))
			require.EqualValues(t, 1, backend.acquisitions.Load())
			require.Equal(t, test.notify, notifications == 1)
			backendLink.assertCalls(t, testLinkCalls{send: 1})
		})
	}
}

func TestFrontendHookSkipsDiscardedMessages(t *testing.T) {
	var observed []pgproto3.FrontendMessage
	session := New(Config{
		Client:                  &testClient{},
		InitialDiscardUntilSync: true,
		Hooks: Hooks{FrontendMessage: func(msg pgproto3.FrontendMessage) {
			observed = append(observed, msg)
		}},
	})

	require.NoError(t, session.handleClientMessage(&pgproto3.Parse{Name: "discarded", Query: "select 1"}))
	require.Empty(t, observed)

	require.NoError(t, session.handleClientMessage(&pgproto3.Sync{}))
	require.Len(t, observed, 1)
	require.IsType(t, &pgproto3.Sync{}, observed[0])
}

func TestClosedHookRunsOnce(t *testing.T) {
	var events []string
	backendLink := &testLink{release: func() error {
		events = append(events, "release")
		return nil
	}}
	session := New(Config{
		Client:      &testClient{},
		InitialLink: backendLink,
		Hooks: Hooks{Closed: func() {
			events = append(events, "closed")
		}},
	})

	session.close(nil)
	session.close(nil)

	require.Equal(t, []string{"release", "closed"}, events)
}
