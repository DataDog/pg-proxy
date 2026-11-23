// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package session

import (
	"bytes"
	"fmt"
	"net"
	"strconv"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"

	"github.com/DataDog/pg-proxy/pkg/query"
)

const testReplyFlushSize = 10_240 // the default PostgresMessageBufferSize

// rangeScanReply returns a reply like the reply to
// SELECT aid, bid, abalance, filler FROM pgbench_accounts: one DataRow for each row,
// each with a filler of fillerLen bytes.
func rangeScanReply(rows, fillerLen int) []pgproto3.BackendMessage {
	return filledReply(rows, fillerLen, 'x')
}

// filledReply is rangeScanReply with fillers made of the fill byte.
func filledReply(rows, fillerLen int, fill byte) []pgproto3.BackendMessage {
	reply := []pgproto3.BackendMessage{&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
		{Name: []byte("aid")}, {Name: []byte("bid")}, {Name: []byte("abalance")}, {Name: []byte("filler")},
	}}}
	filler := bytes.Repeat([]byte{fill}, fillerLen)
	for i := range rows {
		reply = append(reply, &pgproto3.DataRow{Values: [][]byte{[]byte(strconv.Itoa(i + 1)), []byte("1"), []byte("0"), filler}})
	}
	return append(reply,
		&pgproto3.CommandComplete{CommandTag: []byte("SELECT " + strconv.Itoa(rows))},
		&pgproto3.ReadyForQuery{TxStatus: 'I'})
}

// reusableReplyLink returns a link that sends the messages of reply in order, and a function
// that rewinds it to the first message.
func reusableReplyLink(reply []pgproto3.BackendMessage) (*testLink, func()) {
	next := 0
	backendLink := &testLink{
		nextExpectation: func() query.ReplyExpectation { return query.QueryReply },
		receive: func() (pgproto3.BackendMessage, error) {
			msg := reply[next]
			next++
			return msg, nil
		},
	}
	return backendLink, func() { next = 0 }
}

// encodeReply returns the encoded reply and the length of each client write that flushPostgres
// makes for it: one write each time the buffered bytes reach flushSize, and one for the rest.
func encodeReply(t *testing.T, reply []pgproto3.BackendMessage, flushSize int) ([]byte, []int) {
	t.Helper()
	var encoded []byte
	var writes []int
	pending := 0
	for _, msg := range reply {
		before := len(encoded)
		var err error
		encoded, err = msg.Encode(encoded)
		require.NoError(t, err)
		pending += len(encoded) - before
		if pending >= flushSize {
			writes = append(writes, pending)
			pending = 0
		}
	}
	if pending > 0 {
		writes = append(writes, pending)
	}
	return encoded, writes
}

// Replies of every size reach the client with the same bytes in the same writes, also when the
// next replies on the session reuse its buffers. The session's retained buffer stays at
// maxRetainedReplyBufCap or smaller.
func TestFlushPostgresReplySizes(t *testing.T) {
	largeFirstRow := rangeScanReply(20, 84)
	largeFirstRow[1] = &pgproto3.DataRow{Values: [][]byte{[]byte("1"), []byte("1"), []byte("0"), bytes.Repeat([]byte("y"), 4<<10)}}
	for _, test := range []struct {
		name  string
		reply []pgproto3.BackendMessage
	}{
		{name: "small reply", reply: rangeScanReply(1, 84)},
		{name: "reply larger than half of the retained buffer", reply: rangeScanReply(6, 84)},
		{name: "reply larger than the flush size", reply: rangeScanReply(100, 84)},
		{name: "one row larger than the retained buffer", reply: rangeScanReply(1, 4<<10)},
		{name: "large first row, then small rows", reply: largeFirstRow},
		{name: "one row larger than a pooled buffer", reply: rangeScanReply(1, pooledReplyBufCap+1)},
		{name: "one row larger than the pool limit", reply: rangeScanReply(1, maxPooledReplyBufCap+1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			wantBytes, wantWrites := encodeReply(t, test.reply, testReplyFlushSize)
			clientConn := &writeRecordingConn{}
			s := New(Config{Client: &testClient{}, ClientConn: clientConn, PostgresMsgBufferSize: testReplyFlushSize})
			for round := range 3 {
				clientConn.written, clientConn.writeLens = nil, nil
				backendLink, _ := reusableReplyLink(test.reply)
				s.linkMu.Lock()
				s.setLink(backendLink)
				s.linkMu.Unlock()

				_, err := s.flushPostgres()
				require.NoError(t, err, "round %d", round)
				require.True(t, bytes.Equal(wantBytes, clientConn.written), "round %d: the client got other bytes", round)
				require.Equal(t, wantWrites, clientConn.writeLens, "round %d", round)
				require.LessOrEqual(t, cap(s.clientReplyBuf), maxRetainedReplyBufCap, "round %d", round)
			}
		})
	}
}

// When an error stops a reply that is in a pooled buffer, the rows that came before the error
// still reach the client before the buffer goes back to the pool.
func TestFlushPostgresLargeReplyDeliversRowsBeforeError(t *testing.T) {
	rows := rangeScanReply(20, 84)
	rows = rows[:len(rows)-2] // without CommandComplete and ReadyForQuery
	wantRows, _ := encodeReply(t, rows, testReplyFlushSize)
	require.Greater(t, len(wantRows), maxRetainedReplyBufCap)
	require.Less(t, len(wantRows), testReplyFlushSize, "the rows must stay in the buffer until the error")

	reply := append(rows[:len(rows):len(rows)], &pgproto3.ErrorResponse{
		Severity: "FATAL", Code: "57P01", Message: "terminating connection due to administrator command",
	})
	errorLink, _ := reusableReplyLink(reply)
	clientConn := &writeRecordingConn{}
	s := New(Config{
		Client: &testClient{}, ClientConn: clientConn, InitialLink: errorLink,
		PostgresMsgBufferSize: testReplyFlushSize,
	})

	_, err := s.flushPostgres()
	require.Error(t, err)
	require.True(t, bytes.HasPrefix(clientConn.written, wantRows), "the rows before the error did not reach the client")
	require.LessOrEqual(t, cap(s.clientReplyBuf), maxRetainedReplyBufCap)
}

// poisonConn takes a buffer from replyBufPool, overwrites all of it, and returns it before it
// records each write. If flushPostgres returned its buffer to the pool before its last write,
// the pool would give that buffer to poisonConn, and the write would contain the poison bytes.
type poisonConn struct {
	writeRecordingConn
}

func (c *poisonConn) Write(p []byte) (int, error) {
	buf := replyBufPool.Get().(*[]byte)
	all := (*buf)[:cap(*buf)]
	for i := range all {
		all[i] = 0xEE
	}
	replyBufPool.Put(buf)
	return c.writeRecordingConn.Write(p)
}

// flushPostgres returns a pooled buffer only after its last write, also when an error ends the
// reply.
func TestFlushPostgresReturnsPooledBufferAfterLastWrite(t *testing.T) {
	complete := rangeScanReply(20, 84)
	rows := complete[: len(complete)-2 : len(complete)-2]
	failed := append(rows, &pgproto3.ErrorResponse{Severity: "FATAL", Code: "57P01", Message: "terminating connection"})
	for _, test := range []struct {
		name    string
		reply   []pgproto3.BackendMessage
		want    []pgproto3.BackendMessage
		wantErr bool
	}{
		{name: "complete reply", reply: complete, want: complete},
		{name: "reply that an error ends", reply: failed, want: rows, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			want, _ := encodeReply(t, test.want, testReplyFlushSize)
			clientConn := &poisonConn{}
			backendLink, _ := reusableReplyLink(test.reply)
			s := New(Config{Client: &testClient{}, ClientConn: clientConn, InitialLink: backendLink, PostgresMsgBufferSize: testReplyFlushSize})

			_, err := s.flushPostgres()
			if test.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.True(t, bytes.HasPrefix(clientConn.written, want), "a write contained bytes from another use of the buffer")
		})
	}
}

// Sessions that forward replies at the same time get only their own bytes, so no buffer is in
// two replies at once.
func TestFlushPostgresConcurrentSessionsKeepTheirOwnBytes(t *testing.T) {
	const sessions, rounds = 16, 300
	type expected struct {
		reply []pgproto3.BackendMessage
		bytes []byte
	}
	all := make([][]expected, sessions)
	for i := range sessions {
		fill := byte('a' + i)
		for _, reply := range [][]pgproto3.BackendMessage{
			filledReply(1, 84, fill), filledReply(8, 84, fill), filledReply(100, 84, fill), filledReply(1, 4<<10, fill),
		} {
			want, _ := encodeReply(t, reply, testReplyFlushSize)
			all[i] = append(all[i], expected{reply: reply, bytes: want})
		}
	}
	errs := make(chan error, sessions)
	var wg sync.WaitGroup
	for i := range sessions {
		wg.Add(1)
		go func(replies []expected) {
			defer wg.Done()
			clientConn := &writeRecordingConn{}
			s := New(Config{Client: &testClient{}, ClientConn: clientConn, PostgresMsgBufferSize: testReplyFlushSize})
			for round := range rounds {
				r := replies[round%len(replies)]
				clientConn.written, clientConn.writeLens = nil, nil
				backendLink, _ := reusableReplyLink(r.reply)
				s.linkMu.Lock()
				s.setLink(backendLink)
				s.linkMu.Unlock()
				if _, err := s.flushPostgres(); err != nil {
					errs <- err
					return
				}
				if !bytes.Equal(r.bytes, clientConn.written) {
					errs <- fmt.Errorf("round %d: the client got bytes of another reply", round)
					return
				}
			}
		}(all[i])
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

type discardConn struct{ net.Conn }

func (discardConn) Write(p []byte) (int, error) { return len(p), nil }

// allocsPerReply returns the allocations that flushPostgres makes to forward reply, after one
// reply has set up the session's buffers.
func allocsPerReply(t *testing.T, reply []pgproto3.BackendMessage) float64 {
	t.Helper()
	backendLink, rewind := reusableReplyLink(reply)
	s := New(Config{Client: &testClient{}, ClientConn: discardConn{}, PostgresMsgBufferSize: testReplyFlushSize})
	forward := func() {
		rewind()
		s.setLink(backendLink)
		if _, err := s.flushPostgres(); err != nil {
			t.Fatal(err)
		}
	}
	forward()
	return testing.AllocsPerRun(100, forward)
}

// A reply larger than the flush size makes no more allocations than a small reply: it grows
// neither the session's retained buffer nor a new buffer each time.
func TestFlushPostgresLargeReplyAllocations(t *testing.T) {
	if raceEnabled {
		t.Skip("with the race detector, sync.Pool drops some buffers, so the allocations vary")
	}
	small := allocsPerReply(t, rangeScanReply(1, 84))
	large := allocsPerReply(t, rangeScanReply(100, 84))
	require.LessOrEqual(t, large, small, "allocations for a 100-row reply")
}

// BenchmarkFlushPostgresReply forwards one reply for each iteration. Its allocation counts show
// that a reply of many rows does not grow a new buffer each time.
func BenchmarkFlushPostgresReply(b *testing.B) {
	for _, bench := range []struct {
		name            string
		rows, fillerLen int
	}{
		{name: "1 row", rows: 1, fillerLen: 84},
		{name: "5 rows", rows: 5, fillerLen: 84},
		{name: "100 rows", rows: 100, fillerLen: 84},
		{name: "1 row of 4 KiB", rows: 1, fillerLen: 4 << 10},
		{name: "1 row of 1 MiB", rows: 1, fillerLen: 1 << 20},
	} {
		b.Run(bench.name, func(b *testing.B) {
			backendLink, rewind := reusableReplyLink(rangeScanReply(bench.rows, bench.fillerLen))
			s := New(Config{Client: &testClient{}, ClientConn: discardConn{}, PostgresMsgBufferSize: testReplyFlushSize})
			b.ReportAllocs()
			for b.Loop() {
				rewind()
				s.setLink(backendLink)
				if _, err := s.flushPostgres(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
