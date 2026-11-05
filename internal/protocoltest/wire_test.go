package protocoltest

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

func testServer(t *testing.T, handle func(net.Conn) error) Endpoint {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	var lock sync.Mutex
	var active net.Conn
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		lock.Lock()
		active = conn
		lock.Unlock()
		defer conn.Close()
		done <- handle(conn)
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		lock.Lock()
		if active != nil {
			_ = active.Close()
		}
		lock.Unlock()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, net.ErrClosed) {
				t.Errorf("fake server: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("fake server did not stop")
		}
	})
	return Endpoint{Address: listener.Addr().String(), User: "postgres", Database: "fixture"}
}

func trustStartup(conn net.Conn) (*pgproto3.Backend, error) {
	backend := pgproto3.NewBackend(conn, conn)
	if _, err := backend.ReceiveStartupMessage(); err != nil {
		return nil, err
	}
	backend.Send(&pgproto3.AuthenticationOk{})
	backend.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
	backend.Send(&pgproto3.BackendKeyData{ProcessID: 17, SecretKey: []byte{1, 2, 3, 4}})
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	return backend, backend.Flush()
}

func selectScenario() Scenario {
	return Scenario{Version: 1, Name: "select_one", Steps: []Step{
		{Send: []Message{{Type: "Query", SQL: "SELECT 1"}}},
		{Flush: true},
		{Checkpoint: "result", Receive: []Expectation{{Type: "RowDescription"}, {Type: "DataRow", Values: []*string{stringPointer("31")}}, {Type: "CommandComplete", Tag: "SELECT 1"}, {Type: "ReadyForQuery", Status: "I"}}},
	}}
}

func selectResponses() []pgproto3.BackendMessage {
	return []pgproto3.BackendMessage{
		&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{Name: []byte("one"), DataTypeOID: 23, DataTypeSize: 4, TypeModifier: -1}}},
		&pgproto3.DataRow{Values: [][]byte{[]byte("1")}},
		&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	}
}

func selectServer(conn net.Conn) error {
	backend, err := trustStartup(conn)
	if err != nil {
		return err
	}
	message, err := backend.Receive()
	if err != nil {
		return err
	}
	if query, ok := message.(*pgproto3.Query); !ok || query.String != "SELECT 1" {
		return fmt.Errorf("unexpected query %#v", message)
	}
	for _, response := range selectResponses() {
		raw, err := response.Encode(nil)
		if err != nil {
			return err
		}
		// Split both headers and bodies. The runner must use framed reads.
		for _, b := range raw {
			if _, err := conn.Write([]byte{b}); err != nil {
				return err
			}
		}
	}
	message, err = backend.Receive()
	if err != nil {
		return err
	}
	if _, ok := message.(*pgproto3.Terminate); !ok {
		return fmt.Errorf("expected Terminate, got %#v", message)
	}
	return nil
}

func TestRunDirectControlWithFragmentedTransport(t *testing.T) {
	first, err := Run(context.Background(), testServer(t, selectServer), selectScenario(), Options{WriteChunkSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	second, err := Run(context.Background(), testServer(t, selectServer), selectScenario(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := Compare(first, second, nil, nil); err != nil {
		t.Fatal(err)
	}
	if !first.Completed || len(first.Events) != 6 {
		t.Fatalf("incomplete transcript: %+v", first)
	}
	if first.StartupParameters["client_encoding"] != "UTF8" {
		t.Fatal("missing startup settings")
	}
	exported, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(exported), "01020304") {
		t.Fatal("cancellation secret escaped into artifact")
	}
}

func TestRunReceiveDeadlineClosesAndJoinsReader(t *testing.T) {
	endpoint := testServer(t, func(conn net.Conn) error {
		backend, err := trustStartup(conn)
		if err != nil {
			return err
		}
		if _, err := backend.Receive(); err != nil {
			return err
		}
		_, err = io.Copy(io.Discard, conn)
		return err
	})
	start := time.Now()
	transcript, err := Run(context.Background(), endpoint, selectScenario(), Options{StepTimeout: 30 * time.Millisecond})
	if err == nil || !strings.Contains(err.Error(), "deadline") {
		t.Fatalf("want bounded receive failure, got %v", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("deadline did not bound run")
	}
	if transcript.Completed || len(transcript.Events) == 0 || transcript.Error == "" {
		t.Fatalf("missing partial failure: %+v", transcript)
	}
}

func TestRunContextCancellationClosesReader(t *testing.T) {
	endpoint := testServer(t, func(conn net.Conn) error {
		_, err := io.Copy(io.Discard, conn)
		return err
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := Run(ctx, endpoint, selectScenario(), Options{StepTimeout: time.Second}); err == nil {
		t.Fatal("cancelled startup succeeded")
	}
}

func TestRunTrailingMessageFails(t *testing.T) {
	endpoint := testServer(t, func(conn net.Conn) error {
		backend, err := trustStartup(conn)
		if err != nil {
			return err
		}
		if _, err := backend.Receive(); err != nil {
			return err
		}
		for _, response := range selectResponses() {
			backend.Send(response)
		}
		backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		if err := backend.Flush(); err != nil {
			return err
		}
		_, err = backend.Receive()
		return err
	})
	transcript, err := Run(context.Background(), endpoint, selectScenario(), Options{})
	if err == nil || !strings.Contains(err.Error(), "unexpected trailing") {
		t.Fatalf("trailing message not detected: %v", err)
	}
	if transcript.Events[len(transcript.Events)-1].Type != "ReadyForQuery" {
		t.Fatal("trailing reply missing from artifact")
	}
}

func TestRunPartialFrameRetained(t *testing.T) {
	partial := []byte{'D', 0, 0, 0, 20, 0, 1, 0}
	endpoint := testServer(t, func(conn net.Conn) error {
		backend, err := trustStartup(conn)
		if err != nil {
			return err
		}
		if _, err := backend.Receive(); err != nil {
			return err
		}
		_, err = conn.Write(partial)
		return err
	})
	transcript, err := Run(context.Background(), endpoint, selectScenario(), Options{})
	if err == nil {
		t.Fatal("partial response succeeded")
	}
	last := transcript.Events[len(transcript.Events)-1]
	if last.Type != "partial" || last.Hex != hex.EncodeToString(partial) {
		t.Fatalf("partial bytes lost: %+v", last)
	}
}

func TestRunUnknownMessageRetained(t *testing.T) {
	raw := []byte{'?', 0, 0, 0, 4}
	endpoint := testServer(t, func(conn net.Conn) error {
		backend, err := trustStartup(conn)
		if err != nil {
			return err
		}
		if _, err := backend.Receive(); err != nil {
			return err
		}
		_, err = conn.Write(raw)
		return err
	})
	transcript, err := Run(context.Background(), endpoint, selectScenario(), Options{})
	if err == nil {
		t.Fatal("unknown response succeeded")
	}
	if transcript.Events[len(transcript.Events)-1].Hex != hex.EncodeToString(raw) {
		t.Fatal("unknown frame lost")
	}
}

func TestReadFrameRejectsUnboundedLength(t *testing.T) {
	for _, length := range []uint32{0, 3, maxFrameBytes, 0xffffffff} {
		raw := []byte{'D', 0, 0, 0, 0}
		binary.BigEndian.PutUint32(raw[1:], length)
		if _, err := readFrame(strings.NewReader(string(raw))); err == nil {
			t.Fatalf("accepted length %d", length)
		}
	}
}

func TestRunRejectsUnflushedReceive(t *testing.T) {
	endpoint := testServer(t, func(conn net.Conn) error {
		if _, err := trustStartup(conn); err != nil {
			return err
		}
		_, err := io.Copy(io.Discard, conn)
		return err
	})
	scenario := selectScenario()
	scenario.Steps = append(scenario.Steps[:1], scenario.Steps[2:]...)
	if _, err := Run(context.Background(), endpoint, scenario, Options{}); err == nil || !strings.Contains(err.Error(), "unflushed") {
		t.Fatalf("unflushed receive accepted: %v", err)
	}
}

func TestRunRejectsNonTrustStartup(t *testing.T) {
	endpoint := testServer(t, func(conn net.Conn) error {
		backend := pgproto3.NewBackend(conn, conn)
		if _, err := backend.ReceiveStartupMessage(); err != nil {
			return err
		}
		backend.Send(&pgproto3.AuthenticationCleartextPassword{})
		if err := backend.Flush(); err != nil {
			return err
		}
		_, err := io.Copy(io.Discard, conn)
		return err
	})
	if _, err := Run(context.Background(), endpoint, selectScenario(), Options{}); err == nil || !strings.Contains(err.Error(), "trust authentication") {
		t.Fatalf("unsupported authentication accepted: %v", err)
	}
}

func TestUnexpectedCancellationKeyIsAlsoRedacted(t *testing.T) {
	endpoint := testServer(t, func(conn net.Conn) error {
		backend, err := trustStartup(conn)
		if err != nil {
			return err
		}
		if _, err := backend.Receive(); err != nil {
			return err
		}
		backend.Send(&pgproto3.BackendKeyData{ProcessID: 23, SecretKey: []byte{0xde, 0xad, 0xbe, 0xef}})
		if err := backend.Flush(); err != nil {
			return err
		}
		_, err = io.Copy(io.Discard, conn)
		return err
	})
	transcript, err := Run(context.Background(), endpoint, selectScenario(), Options{})
	if err == nil {
		t.Fatal("unexpected BackendKeyData accepted")
	}
	exported, err := json.Marshal(transcript)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(exported), "deadbeef") {
		t.Fatal("unexpected cancellation key escaped into artifact")
	}
}

func TestRunRequiresTerminalEOF(t *testing.T) {
	endpoint := testServer(t, func(conn net.Conn) error {
		backend, err := trustStartup(conn)
		if err != nil {
			return err
		}
		if _, err := backend.Receive(); err != nil {
			return err
		}
		for _, response := range selectResponses() {
			backend.Send(response)
		}
		if err := backend.Flush(); err != nil {
			return err
		}
		if _, err := backend.Receive(); err != nil {
			return err
		}
		// A peer that stays silent after Terminate must not make the run pass.
		_, err = io.Copy(io.Discard, conn)
		return err
	})
	transcript, err := Run(context.Background(), endpoint, selectScenario(), Options{StepTimeout: 30 * time.Millisecond})
	if err == nil || !strings.Contains(err.Error(), "terminate: expected EOF") {
		t.Fatalf("silent termination accepted: %v", err)
	}
	if transcript.Completed {
		t.Fatal("silent peer marked complete")
	}
}

func TestReassignmentFailureClosesReader(t *testing.T) {
	endpoint := testServer(t, func(conn net.Conn) error {
		if _, err := trustStartup(conn); err != nil {
			return err
		}
		_, err := io.Copy(io.Discard, conn)
		return err
	})
	scenario := Scenario{Version: 1, Name: "reassignment_error", Steps: []Step{{Reassign: true}}}
	_, err := Run(context.Background(), endpoint, scenario, Options{Reassign: func(context.Context) error { return errors.New("fixture barrier failed") }})
	if err == nil || !strings.Contains(err.Error(), "fixture barrier failed") {
		t.Fatalf("barrier error lost: %v", err)
	}
}
