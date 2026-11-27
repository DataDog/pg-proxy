// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package fakepg

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

// The pgx paths a pooled link uses (connect, a simple query, Deallocate) work
// against a server that answers inside Write, and leave nothing queued.
func TestServerAnswersPgxInsideWrite(t *testing.T) {
	var received []byte
	server := NewServer(func(msg []byte) []byte {
		received = append(received, msg[0])
		return Answer(msg)
	})
	pool := NewPool(t, server)
	conn, err := pool.Acquire(context.Background())
	require.NoError(t, err)
	t.Cleanup(conn.Release)

	_, err = conn.Exec(context.Background(), "RESET ALL")
	require.NoError(t, err)
	require.NoError(t, conn.Conn().PgConn().Deallocate(context.Background(), "stmt_1"))
	require.Equal(t, []byte{'Q', 'C', 'S'}, received, "one simple query, then Deallocate's Close and Sync")
	server.mu.Lock()
	defer server.mu.Unlock()
	require.Empty(t, server.in)
}

// A reply the server never sends fails the read after ReplyWait rather than
// hanging, and pgx closes the conn.
func TestMissingReplyFailsTheRead(t *testing.T) {
	server := NewServer(nil)
	server.ReplyWait = 10 * time.Millisecond
	pool := NewPool(t, server)
	conn, err := pool.Acquire(context.Background())
	require.NoError(t, err)
	t.Cleanup(conn.Release)

	_, err = conn.Exec(context.Background(), "RESET ALL")
	require.Error(t, err)
	require.True(t, conn.Conn().IsClosed())
}

// pgx's background reader can call Read before the Write whose reply it awaits;
// that Read must wait for the reply rather than fail and break the conn.
func TestReadWaitsForTheReplyToALaterWrite(t *testing.T) {
	server := NewServer(Answer)
	server.started = true
	got := make(chan error, 1)
	go func() {
		buf := make([]byte, 64)
		n, err := server.Read(buf)
		if err == nil && n != len(synced) {
			err = ErrPartialMessage
		}
		got <- err
	}()
	time.Sleep(20 * time.Millisecond) // let the Read start waiting, as a descheduled writer would
	syncMsg, err := (&pgproto3.Sync{}).Encode(nil)
	require.NoError(t, err)
	_, err = server.Write(syncMsg)
	require.NoError(t, err)
	require.NoError(t, <-got)
}

// Close wakes a Read that is waiting, so a background reader cannot outlive its pool.
func TestCloseWakesAWaitingRead(t *testing.T) {
	server := NewServer(nil)
	got := make(chan error, 1)
	go func() {
		_, err := server.Read(make([]byte, 8))
		got <- err
	}()
	time.Sleep(20 * time.Millisecond)
	require.NoError(t, server.Close())
	require.ErrorIs(t, <-got, io.EOF)
}
