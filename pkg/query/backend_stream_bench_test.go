// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package query

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

// benchFlushThreshold matches the default client flush threshold.
const benchFlushThreshold = 8192

// resultSetStream encodes a simple-query reply: RowDescription, rows DataRows,
// CommandComplete and ReadyForQuery. Each row mixes fixed-width, NULL and
// variable-width text columns, like SELECT id, uuid, name, note, created_at, n FROM items.
func resultSetStream(tb testing.TB, rows int) []byte {
	tb.Helper()
	rng := rand.New(rand.NewSource(1))
	names := []string{"id", "uuid", "name", "note", "created_at", "n"}
	fields := make([]pgproto3.FieldDescription, len(names))
	for i, n := range names {
		fields[i] = pgproto3.FieldDescription{Name: []byte(n), DataTypeOID: 25, DataTypeSize: -1}
	}
	var buf []byte
	var err error
	if buf, err = (&pgproto3.RowDescription{Fields: fields}).Encode(buf); err != nil {
		tb.Fatal(err)
	}
	text := bytes.Repeat([]byte("abcdefghij"), 50)
	for i := range rows {
		var note []byte
		if rng.Intn(4) != 0 {
			note = text[:rng.Intn(len(text))]
		}
		row := &pgproto3.DataRow{Values: [][]byte{
			fmt.Appendf(nil, "%d", i),
			[]byte("2b3c6c1e-6f0a-4d6e-9a43-1b2f5c7d8e90"),
			text[:4+rng.Intn(28)],
			note, // NULL a quarter of the time, otherwise 0-499 bytes
			[]byte("2026-01-02 03:04:05.678901+00"),
			fmt.Appendf(nil, "%d", rng.Intn(1_000_000)),
		}}
		if buf, err = row.Encode(buf); err != nil {
			tb.Fatal(err)
		}
	}
	if buf, err = (&pgproto3.CommandComplete{CommandTag: fmt.Appendf(nil, "SELECT %d", rows)}).Encode(buf); err != nil {
		tb.Fatal(err)
	}
	return append(buf, 'Z', 0, 0, 0, 5, 'I')
}

// decodeEncodeForward mirrors the flushPostgres loop: decode each frame with
// pgproto3.Frontend, decide its action, and re-encode it into replyBuf.
func decodeEncodeForward(fe *pgproto3.Frontend, ts *TraceState, replyBuf []byte, w io.Writer) ([]byte, error) {
	for {
		msg, err := fe.Receive()
		if err != nil {
			return replyBuf, err
		}
		action, err := DecidePgMessageAction(msg, QueryReply, ts)
		if err != nil {
			return replyBuf, err
		}
		encoded, err := action.ProxyMsgToClient.Encode(replyBuf)
		if err != nil {
			return replyBuf, err
		}
		if _, ok := msg.(*pgproto3.DataRow); ok {
			ts.AddRowsSize(len(encoded) - len(replyBuf))
		}
		replyBuf = encoded
		if len(replyBuf) >= benchFlushThreshold {
			_, _ = w.Write(replyBuf)
			replyBuf = replyBuf[:0]
		}
		if action.SetTxStatus != NoTxStatus {
			_, _ = w.Write(replyBuf)
			return replyBuf[:0], nil
		}
	}
}

// rawDataRowForward is the prototype middle ground: it frames the stream
// itself, copies DataRow frames verbatim, and decodes only the other types,
// which the proxy inspects or rewrites. pgproto3.Frontend has no raw-frame
// API, so production would need a framer of its own on the backend conn.
func rawDataRowForward(r *bufio.Reader, ts *TraceState, replyBuf []byte, w io.Writer) ([]byte, error) {
	var hdr [5]byte
	var other []byte
	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return replyBuf, err
		}
		bodyLen := int(binary.BigEndian.Uint32(hdr[1:])) - 4
		if hdr[0] == 'D' {
			start := len(replyBuf)
			replyBuf = append(replyBuf, hdr[:]...)
			replyBuf = append(replyBuf, make([]byte, bodyLen)...)
			if _, err := io.ReadFull(r, replyBuf[start+5:]); err != nil {
				return replyBuf, err
			}
			ts.AddRowsSize(5 + bodyLen)
			ts.rowsNumber++
		} else {
			other = append(other[:0], make([]byte, bodyLen)...)
			if _, err := io.ReadFull(r, other); err != nil {
				return replyBuf, err
			}
			var msg pgproto3.BackendMessage
			switch hdr[0] {
			case 'T':
				msg = &pgproto3.RowDescription{}
			case 'C':
				msg = &pgproto3.CommandComplete{}
			case 'Z':
				msg = &pgproto3.ReadyForQuery{}
			default:
				return replyBuf, fmt.Errorf("unexpected message type %c", hdr[0])
			}
			if err := msg.Decode(other); err != nil {
				return replyBuf, err
			}
			action, err := DecidePgMessageAction(msg, QueryReply, ts)
			if err != nil {
				return replyBuf, err
			}
			if replyBuf, err = action.ProxyMsgToClient.Encode(replyBuf); err != nil {
				return replyBuf, err
			}
			if action.SetTxStatus != NoTxStatus {
				_, _ = w.Write(replyBuf)
				return replyBuf[:0], nil
			}
		}
		if len(replyBuf) >= benchFlushThreshold {
			_, _ = w.Write(replyBuf)
			replyBuf = replyBuf[:0]
		}
	}
}

func TestRawDataRowForwardMatchesDecodeEncode(t *testing.T) {
	stream := resultSetStream(t, 500)
	var want, got bytes.Buffer
	if _, err := decodeEncodeForward(pgproto3.NewFrontend(bytes.NewReader(stream), nil), &TraceState{}, nil, &want); err != nil {
		t.Fatal(err)
	}
	if _, err := rawDataRowForward(bufio.NewReaderSize(bytes.NewReader(stream), 8192), &TraceState{}, nil, &got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(want.Bytes(), stream) || !bytes.Equal(got.Bytes(), stream) {
		t.Fatal("forwarded bytes differ from the backend stream")
	}
}

func BenchmarkBackendStream(b *testing.B) {
	for _, rows := range []int{10, 1000} {
		stream := resultSetStream(b, rows)
		b.Run(fmt.Sprintf("decode-encode/rows=%d", rows), func(b *testing.B) {
			src := bytes.NewReader(stream)
			fe := pgproto3.NewFrontend(src, nil)
			ts := &TraceState{}
			var replyBuf []byte
			b.SetBytes(int64(len(stream)))
			b.ReportAllocs()
			for b.Loop() {
				src.Reset(stream)
				var err error
				if replyBuf, err = decodeEncodeForward(fe, ts, replyBuf, io.Discard); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("raw-datarow/rows=%d", rows), func(b *testing.B) {
			src := bytes.NewReader(stream)
			r := bufio.NewReaderSize(src, 8192)
			ts := &TraceState{}
			var replyBuf []byte
			b.SetBytes(int64(len(stream)))
			b.ReportAllocs()
			for b.Loop() {
				src.Reset(stream)
				r.Reset(src)
				var err error
				if replyBuf, err = rawDataRowForward(r, ts, replyBuf, io.Discard); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkEncode shows that Encode cost depends on dst: appending into a
// reused buffer does not allocate, while a nil dst allocates and grows.
func BenchmarkEncode(b *testing.B) {
	stream := resultSetStream(b, 1)
	fe := pgproto3.NewFrontend(bytes.NewReader(stream), nil)
	if _, err := fe.Receive(); err != nil { // RowDescription
		b.Fatal(err)
	}
	row, err := fe.Receive()
	if err != nil {
		b.Fatal(err)
	}
	msgs := []pgproto3.BackendMessage{
		row,
		&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1000")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
		&pgproto3.ErrorResponse{Severity: "ERROR", Code: "42P01", Message: `relation "items" does not exist`, Position: 15, File: "parse_relation.c", Line: 1449, Routine: "parserOpenTable"},
	}
	for _, m := range msgs {
		name := fmt.Sprintf("%T", m)[len("*pgproto3."):]
		b.Run(name+"/nil-dst", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := m.Encode(nil); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(name+"/reused-dst", func(b *testing.B) {
			buf := make([]byte, 0, 4096)
			b.ReportAllocs()
			for b.Loop() {
				var err error
				if buf, err = m.Encode(buf[:0]); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
