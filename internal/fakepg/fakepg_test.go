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
	require.Zero(t, server.Pending())
}

// A reply the server never sends fails the read after ReplyWait rather than
// hanging. pgx then closes the conn and reports that, not the read error, so Err
// keeps the read error, which names the unanswered write.
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
	require.ErrorIs(t, server.Err(), ErrExhausted)
	require.ErrorContains(t, server.Err(), `the last write was &pgproto3.Query{String:"RESET ALL"}`)
}

// The error names a write's messages up to the one describedWrite cuts off, which
// it names by type.
func TestFrontendMessagesNamesACutOffMessage(t *testing.T) {
	write := Encode[pgproto3.FrontendMessage](t,
		&pgproto3.Query{String: "SELECT 1"},
		&pgproto3.Bind{Parameters: [][]byte{make([]byte, describedWrite)}},
	)
	require.Equal(t, `&pgproto3.Query{String:"SELECT 1"}, a 'B' message`, frontendMessages(write[:describedWrite]))
}

// A server with ServerVersion set reports it in the startup handshake.
func TestServerReportsServerVersion(t *testing.T) {
	server := NewServer(nil)
	server.ServerVersion = "16.4"
	conn, err := NewPool(t, server).Acquire(context.Background())
	require.NoError(t, err)
	t.Cleanup(conn.Release)
	require.Equal(t, "16.4", conn.Conn().PgConn().ParameterStatus("server_version"))
}

// A queued script is read in place and never written to, so one encoding serves
// every cycle even when replies are appended behind it.
func TestQueueLeavesScriptIntact(t *testing.T) {
	script := make([]byte, 0, 64)
	script = append(script, Encode[pgproto3.BackendMessage](t, &pgproto3.ParseComplete{})...)
	want := append([]byte(nil), script...)
	server := NewServer(Answer)
	server.started = true
	server.Queue(script)
	_, err := server.Write(Encode[pgproto3.FrontendMessage](t, &pgproto3.Sync{}))
	require.NoError(t, err)
	require.Equal(t, want, script[:len(want)])
	require.Equal(t, want, script[:cap(script)][:len(want)], "the spare capacity behind the script is untouched")
	require.Equal(t, len(want)+len(synced), server.Pending())
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
	_, err := server.Write(Encode[pgproto3.FrontendMessage](t, &pgproto3.Sync{}))
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
