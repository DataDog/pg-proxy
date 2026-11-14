// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

//go:build integration && unix

package integration

import (
	"context"
	"fmt"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/DataDog/pg-proxy/internal/itest"
)

// TestStuckClientDoesNotHoldPooledBackend checks that a client that stops
// reading cannot keep the only backend connection out of the pool. Client A asks
// for a reply far larger than its socket buffers and reads none of it, so the
// proxy's write of that reply blocks after Postgres has finished. Client B must
// still get the backend, and A must get its whole reply once it reads again.
func TestStuckClientDoesNotHoldPooledBackend(t *testing.T) {
	const (
		rows     = 32
		rowBytes = 1 << 20
		// About 4 MiB can sit in the proxy's send buffer and A's pinned receive
		// buffer; the rest of the 32 MiB reply blocks the proxy's write.
		receiveBuffer = 64 << 10
	)
	// The buffer holds the whole reply, so the proxy writes it in one piece after
	// Postgres has finished and the backend is idle. With the default buffer, a
	// reply this large is written while Postgres is still sending it, and a client
	// that stops reading then holds the backend whatever the proxy does.
	cfg := proxyConfig()
	cfg.Proxy.PostgresMsgBufferSize = 2 * rows * rowBytes
	proxy := itest.MustStartProxy(t, cfg)
	connStr := proxy.URL(dbPooled, itest.PostgresUser)
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	clientB, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err, "client B connect")
	defer clientB.Close(context.Background())
	var backendPID uint32
	require.NoError(t, clientB.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&backendPID))

	direct, err := backend.Connect(ctx, "postgres")
	require.NoError(t, err, "connect to Postgres directly")
	defer direct.Close(context.Background())
	backendActivity := func() (state string, queryStart time.Time, err error) {
		err = direct.QueryRow(ctx, "SELECT state, query_start FROM pg_stat_activity WHERE pid = $1", backendPID).Scan(&state, &queryStart)
		return state, queryStart, err
	}
	_, before, err := backendActivity()
	require.NoError(t, err)

	clientA := connectWithReceiveBuffer(t, ctx, connStr, receiveBuffer)
	defer clientA.Close(context.Background())
	pgConnA := clientA.PgConn()
	frontendA := pgConnA.Frontend()
	frontendA.Send(&pgproto3.Query{String: fmt.Sprintf("SELECT repeat('x', %d) FROM generate_series(1, %d)", rowBytes, rows)})
	require.NoError(t, frontendA.Flush())

	// B sends nothing until Postgres has run A's query and sent all of it: the
	// backend has started a query since `before` and is idle again.
	require.Eventually(t, func() bool {
		state, queryStart, err := backendActivity()
		return err == nil && queryStart.After(before) && state == "idle"
	}, 20*time.Second, 5*time.Millisecond, "Postgres did not finish client A's query")

	// A has read nothing, and B must still get the only backend. A proxy that
	// keeps the backend until the write finishes makes B wait for A indefinitely;
	// the bound leaves room for a slow machine and the race detector.
	bctx, bcancel := context.WithTimeout(ctx, 2*time.Second)
	defer bcancel()
	start := time.Now()
	var pid uint32
	require.NoError(t, clientB.QueryRow(bctx, "SELECT pg_backend_pid()").Scan(&pid),
		"client B did not get the backend while client A was not reading its reply")
	t.Logf("client B got the backend %v after Postgres finished client A's query", time.Since(start))
	require.Equal(t, backendPID, pid, "client B should reuse the only pooled backend")

	// A reads again and gets the whole reply.
	require.NoError(t, pgConnA.Conn().SetReadDeadline(time.Now().Add(30*time.Second)))
	gotRows := 0
	for ready := false; !ready; {
		msg, err := frontendA.Receive()
		require.NoError(t, err, "client A read")
		switch msg := msg.(type) {
		case *pgproto3.DataRow:
			require.Len(t, msg.Values, 1)
			require.Len(t, msg.Values[0], rowBytes)
			require.Equal(t, byte('x'), msg.Values[0][rowBytes-1])
			gotRows++
		case *pgproto3.ErrorResponse:
			t.Fatalf("client A got an error instead of its reply: %s", msg.Message)
		case *pgproto3.ReadyForQuery:
			require.Equal(t, byte('I'), msg.TxStatus)
			ready = true
		}
	}
	require.Equal(t, rows, gotRows)

	// A's session is still usable.
	require.NoError(t, pgConnA.Conn().SetReadDeadline(time.Time{}))
	var one int
	require.NoError(t, clientA.QueryRow(ctx, "SELECT 1").Scan(&one))
	require.Equal(t, 1, one)
}

// connectWithReceiveBuffer connects through the proxy with SO_RCVBUF pinned to
// size, which stops the kernel growing the buffer to hold an unread reply.
func connectWithReceiveBuffer(t *testing.T, ctx context.Context, connStr string, size int) *pgx.Conn {
	t.Helper()
	cfg, err := pgx.ParseConfig(connStr)
	require.NoError(t, err)
	dialer := &net.Dialer{Control: func(_, _ string, raw syscall.RawConn) error {
		var sockErr error
		if err := raw.Control(func(fd uintptr) {
			sockErr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_RCVBUF, size)
		}); err != nil {
			return err
		}
		return sockErr
	}}
	cfg.DialFunc = dialer.DialContext
	conn, err := pgx.ConnectConfig(ctx, cfg)
	require.NoError(t, err, "client A connect")
	return conn
}
