package protocoltest

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

const (
	maxFrameBytes      = 4 << 20
	maxTranscriptBytes = 32 << 20
	maxMessages        = 16384
)

// Endpoint selects a disposable, trust-authenticated server. Authentication and
// TLS negotiation are intentionally outside this post-authentication harness.
type Endpoint struct {
	Address  string `json:"address"`
	User     string `json:"user"`
	Database string `json:"database"`
}

// Options controls deterministic transport boundaries and fixture barriers.
type Options struct {
	StepTimeout    time.Duration
	WriteChunkSize int
	Reassign       func(context.Context) error
	// BeforeStep is a bounded scheduling barrier, called only after startup.
	BeforeStep func(context.Context, int) error
}

// Frame owns all its data. Hex preserves exact bytes, including NULL versus
// empty values and invalid UTF-8. Decoded bypasses pgproto3's display marshalers.
type Frame struct {
	Direction  string          `json:"direction"`
	Step       int             `json:"step"`
	Checkpoint string          `json:"checkpoint,omitempty"`
	Type       string          `json:"type"`
	Hex        string          `json:"hex"`
	Decoded    json.RawMessage `json:"decoded"`
}

// Transcript separates startup (endpoint-local identities) from the compared
// conversation. Startup secret keys are redacted before anything is exported.
type Transcript struct {
	Startup           []Frame           `json:"startup"`
	StartupParameters map[string]string `json:"startup_parameters"`
	Events            []Frame           `json:"events"`
	Completed         bool              `json:"completed"`
	Error             string            `json:"error,omitempty"`
}

type received struct {
	frame   Frame
	message pgproto3.BackendMessage
	err     error
}

// Run executes one conversation without retries. Every failure returns the
// partial transcript. The socket and reader are closed and joined on all paths.
func Run(ctx context.Context, endpoint Endpoint, scenario Scenario, options Options) (transcript Transcript, runErr error) {
	if err := scenario.Validate(); err != nil {
		return transcript, err
	}
	if options.StepTimeout < 0 || options.WriteChunkSize < 0 {
		return transcript, errors.New("negative runner option")
	}
	if options.StepTimeout == 0 {
		options.StepTimeout = 5 * time.Second
	}
	// A per-step deadline must not let an arbitrarily long conversation run forever.
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	conn, err := (&net.Dialer{Timeout: options.StepTimeout}).DialContext(ctx, "tcp", endpoint.Address)
	if err != nil {
		return transcript, fmt.Errorf("connect: %w", err)
	}
	stopCancel := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stopCancel()
	responses := make(chan received, maxMessages+1)
	readerDone := make(chan struct{})
	go readResponses(conn, responses, readerDone)
	activeStep := -1
	startup := true
	defer func() {
		_ = conn.Close()
		<-readerDone
		// Preserve replies that arrived before a failure but were never consumed by
		// its receive checkpoint. This is diagnostic data, never a successful drain.
		for response := range responses {
			if response.frame.Hex != "" {
				response.frame.Step = activeStep
				response.frame.Checkpoint = "unconsumed"
				if startup {
					transcript.Startup = append(transcript.Startup, redactKey(response.frame, response.message))
				} else {
					transcript.Events = append(transcript.Events, response.frame)
				}
			}
		}
		if runErr != nil {
			transcript.Error = runErr.Error()
		}
	}()
	receive := func() (received, error) {
		timer := time.NewTimer(options.StepTimeout)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return received{}, ctx.Err()
		case <-timer.C:
			return received{}, fmt.Errorf("receive deadline after %s", options.StepTimeout)
		case response, ok := <-responses:
			if !ok {
				return received{}, io.EOF
			}
			return response, response.err
		}
	}
	write := func(raw []byte) error {
		deadline := time.Now().Add(options.StepTimeout)
		if parent, ok := ctx.Deadline(); ok && parent.Before(deadline) {
			deadline = parent
		}
		if err := conn.SetWriteDeadline(deadline); err != nil {
			return err
		}
		for len(raw) > 0 {
			chunk := len(raw)
			if options.WriteChunkSize > 0 && chunk > options.WriteChunkSize {
				chunk = options.WriteChunkSize
			}
			n, err := conn.Write(raw[:chunk])
			raw = raw[n:]
			if err != nil {
				return err
			}
			if n == 0 {
				return io.ErrShortWrite
			}
		}
		return nil
	}
	startupBytes, err := (&pgproto3.StartupMessage{ProtocolVersion: 196608, Parameters: map[string]string{"user": endpoint.User, "database": endpoint.Database, "client_encoding": "UTF8"}}).Encode(nil)
	if err != nil {
		return transcript, err
	}
	if err := write(startupBytes); err != nil {
		return transcript, fmt.Errorf("startup write: %w", err)
	}
	transcript.StartupParameters = map[string]string{}
	authenticated, keySeen := false, false
	for count := 0; ; count++ {
		if count >= 256 {
			return transcript, errors.New("startup exceeded 256 messages")
		}
		response, err := receive()
		if response.frame.Hex != "" {
			transcript.Startup = append(transcript.Startup, redactKey(response.frame, response.message))
		}
		if err != nil {
			return transcript, fmt.Errorf("startup receive: %w", err)
		}
		switch msg := response.message.(type) {
		case *pgproto3.AuthenticationOk:
			if authenticated {
				return transcript, errors.New("duplicate AuthenticationOk")
			}
			authenticated = true
		case *pgproto3.BackendKeyData:
			if !authenticated || keySeen {
				return transcript, errors.New("unexpected BackendKeyData")
			}
			keySeen = true
		case *pgproto3.ParameterStatus:
			if _, exists := transcript.StartupParameters[msg.Name]; exists {
				return transcript, fmt.Errorf("duplicate startup parameter %q", msg.Name)
			}
			transcript.StartupParameters[msg.Name] = msg.Value
		case *pgproto3.NoticeResponse:
		case *pgproto3.ReadyForQuery:
			if !authenticated || !keySeen || msg.TxStatus != 'I' {
				return transcript, errors.New("startup did not finish authenticated and idle")
			}
			startup = false
		case *pgproto3.ErrorResponse:
			return transcript, fmt.Errorf("startup error %s: %s", msg.Code, msg.Message)
		default:
			return transcript, fmt.Errorf("unsupported startup message %s (fixture requires trust authentication)", response.frame.Type)
		}
		if !startup {
			break
		}
	}
	var pending []byte
	frontendBytes := 0
	for stepIndex, step := range scenario.Steps {
		activeStep = stepIndex
		if options.BeforeStep != nil {
			if err := options.BeforeStep(ctx, stepIndex); err != nil {
				return transcript, fmt.Errorf("step %d scheduling barrier: %w", stepIndex, err)
			}
		}
		switch {
		case len(step.Send) > 0:
			for _, msg := range step.Send {
				raw, err := msg.encode()
				if err != nil {
					return transcript, err
				}
				frontendBytes += len(raw)
				if frontendBytes > maxTranscriptBytes || len(transcript.Events) >= maxMessages {
					return transcript, errors.New("frontend transcript limit exceeded")
				}
				pending = append(pending, raw...)
				// Decode from the immutable bytes, rather than the scenario representation,
				// so diagnostics report exactly what the client sent.
				message, err := pgproto3.NewBackend(bytes.NewReader(raw), io.Discard).Receive()
				if err != nil {
					return transcript, fmt.Errorf("decode encoded frontend: %w", err)
				}
				frame, err := snapshot("frontend", raw, message)
				if err != nil {
					return transcript, err
				}
				frame.Step = stepIndex
				transcript.Events = append(transcript.Events, frame)
			}
		case step.Flush:
			if err := write(pending); err != nil {
				return transcript, fmt.Errorf("step %d transport flush: %w", stepIndex, err)
			}
			pending = nil
		case step.Reassign:
			if len(pending) > 0 {
				return transcript, fmt.Errorf("step %d reassign with unflushed bytes", stepIndex)
			}
			if options.Reassign == nil {
				return transcript, errors.New("scenario requires a backend reassignment fixture")
			}
			barrierCtx, barrierCancel := context.WithTimeout(ctx, options.StepTimeout)
			err := options.Reassign(barrierCtx)
			barrierCancel()
			if err != nil {
				return transcript, fmt.Errorf("step %d reassign: %w", stepIndex, err)
			}
		case len(step.Receive) > 0:
			if len(pending) > 0 {
				return transcript, fmt.Errorf("step %d receive with unflushed bytes", stepIndex)
			}
			for expectedIndex, want := range step.Receive {
				response, err := receive()
				if response.frame.Hex != "" {
					response.frame.Step = stepIndex
					response.frame.Checkpoint = step.Checkpoint
					transcript.Events = append(transcript.Events, response.frame)
				}
				if err != nil {
					return transcript, fmt.Errorf("step %d checkpoint %q response %d: %w", stepIndex, step.Checkpoint, expectedIndex, err)
				}
				if err := checkExpectation(want, response); err != nil {
					return transcript, fmt.Errorf("step %d checkpoint %q response %d: %w", stepIndex, step.Checkpoint, expectedIndex, err)
				}
			}
		}
	}
	if len(pending) > 0 {
		return transcript, errors.New("conversation ended with unflushed bytes")
	}
	activeStep = len(scenario.Steps)
	if options.BeforeStep != nil {
		if err := options.BeforeStep(ctx, activeStep); err != nil {
			return transcript, fmt.Errorf("termination scheduling barrier: %w", err)
		}
	}
	raw, _ := (&pgproto3.Terminate{}).Encode(nil)
	frame, _ := snapshot("frontend", raw, &pgproto3.Terminate{})
	frame.Step = activeStep
	transcript.Events = append(transcript.Events, frame)
	if err := write(raw); err != nil {
		return transcript, fmt.Errorf("terminate write: %w", err)
	}
	// EOF is a real terminal boundary. Silence is never accepted as proof that
	// the final checkpoint consumed every message.
	response, err := receive()
	if response.frame.Hex != "" {
		response.frame.Step = activeStep
		response.frame.Checkpoint = "after final checkpoint"
		transcript.Events = append(transcript.Events, response.frame)
		return transcript, fmt.Errorf("unexpected trailing %s", response.frame.Type)
	}
	if !errors.Is(err, io.EOF) {
		return transcript, fmt.Errorf("terminate: expected EOF, got %v", err)
	}
	transcript.Completed = true
	return transcript, nil
}

func readResponses(conn net.Conn, out chan<- received, done chan<- struct{}) {
	defer close(done)
	defer close(out)
	total := 0
	for count := 0; count < maxMessages; count++ {
		raw, err := readFrame(conn)
		if err != nil {
			var partial Frame
			if len(raw) > 0 {
				partial = Frame{Direction: "backend", Type: "partial", Hex: hex.EncodeToString(raw), Decoded: json.RawMessage(`{}`)}
			}
			out <- received{frame: redactKey(partial, nil), err: err}
			return
		}
		total += len(raw)
		if total > maxTranscriptBytes {
			_ = conn.Close()
			out <- received{err: errors.New("backend transcript exceeds 32 MiB")}
			return
		}
		message, err := pgproto3.NewFrontend(bytes.NewReader(raw), io.Discard).Receive()
		if err != nil {
			_ = conn.Close()
			frame := Frame{Direction: "backend", Type: fmt.Sprintf("unknown(%q)", raw[0]), Hex: hex.EncodeToString(raw)}
			out <- received{frame: redactKey(frame, nil), err: fmt.Errorf("decode backend: %w", err)}
			return
		}
		frame, err := snapshot("backend", raw, message)
		out <- received{frame: redactKey(frame, message), message: message, err: err}
		if err != nil {
			_ = conn.Close()
			return
		}
	}
	_ = conn.Close()
	out <- received{err: errors.New("backend transcript exceeds message limit")}
}

func readFrame(reader io.Reader) ([]byte, error) {
	header := make([]byte, 5)
	if n, err := io.ReadFull(reader, header); err != nil {
		return header[:n], err
	}
	length := binary.BigEndian.Uint32(header[1:])
	if length < 4 || length > maxFrameBytes-1 {
		return header, fmt.Errorf("invalid backend frame length %d", length)
	}
	raw := make([]byte, int(length)+1)
	copy(raw, header)
	if n, err := io.ReadFull(reader, raw[5:]); err != nil {
		return raw[:5+n], err
	}
	return raw, nil
}

func snapshot(direction string, raw []byte, message any) (Frame, error) {
	encoded, err := json.Marshal(plainValue(reflect.ValueOf(message)))
	if err != nil {
		return Frame{}, err
	}
	return Frame{Direction: direction, Type: reflect.TypeOf(message).Elem().Name(), Hex: hex.EncodeToString(raw), Decoded: encoded}, nil
}

// plainValue preserves every exported protocol field without invoking display
// marshalers. All byte slices are hex strings; nil slices remain JSON null.
func plainValue(value reflect.Value) any {
	switch value.Kind() {
	case reflect.Pointer, reflect.Interface:
		if value.IsNil() {
			return nil
		}
		return plainValue(value.Elem())
	case reflect.Struct:
		result := map[string]any{}
		for i := 0; i < value.NumField(); i++ {
			field := value.Type().Field(i)
			if field.IsExported() {
				result[field.Name] = plainValue(value.Field(i))
			}
		}
		return result
	case reflect.Slice:
		if value.IsNil() {
			return nil
		}
		if value.Type().Elem().Kind() == reflect.Uint8 {
			return hex.EncodeToString(value.Bytes())
		}
		result := make([]any, value.Len())
		for i := range result {
			result[i] = plainValue(value.Index(i))
		}
		return result
	case reflect.Map:
		if value.IsNil() {
			return nil
		}
		result := map[string]any{}
		for _, key := range value.MapKeys() {
			result[fmt.Sprint(key.Interface())] = plainValue(value.MapIndex(key))
		}
		return result
	case reflect.String:
		return value.String()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return value.Uint()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return value.Int()
	case reflect.Bool:
		return value.Bool()
	default:
		panic("unsupported protocol snapshot kind: " + value.Kind().String())
	}
}

func redactKey(frame Frame, message pgproto3.BackendMessage) Frame {
	raw, _ := hex.DecodeString(frame.Hex)
	_, isKey := message.(*pgproto3.BackendKeyData)
	if isKey || (len(raw) > 0 && raw[0] == 'K') {
		// Header + PID remain diagnostic. Never persist cancellation credentials.
		for i := 9; i < len(raw); i++ {
			raw[i] = 0
		}
		frame.Hex = hex.EncodeToString(raw)
		decoded := map[string]any{}
		_ = json.Unmarshal(frame.Decoded, &decoded)
		if decoded == nil {
			decoded = map[string]any{}
		}
		decoded["SecretKey"] = "<redacted>"
		frame.Decoded, _ = json.Marshal(decoded)
	}
	return frame
}

func checkExpectation(want Expectation, response received) error {
	if want.Type != response.frame.Type {
		return fmt.Errorf("message type: want %s, got %s", want.Type, response.frame.Type)
	}
	switch msg := response.message.(type) {
	case *pgproto3.CopyData:
		if want.Data != nil && *want.Data != hex.EncodeToString(msg.Data) {
			return fmt.Errorf("CopyData.Data: want %s, got %x", *want.Data, msg.Data)
		}
	case *pgproto3.CopyInResponse:
		return checkCopyFormat(want, msg.OverallFormat, msg.ColumnFormatCodes)
	case *pgproto3.CopyOutResponse:
		return checkCopyFormat(want, msg.OverallFormat, msg.ColumnFormatCodes)
	case *pgproto3.ErrorResponse:
		if want.Code != "" && want.Code != msg.Code {
			return fmt.Errorf("ErrorResponse.Code: want %s, got %s", want.Code, msg.Code)
		}
	case *pgproto3.CommandComplete:
		if want.Tag != "" && want.Tag != string(msg.CommandTag) {
			return fmt.Errorf("CommandComplete.CommandTag: want %q, got %q", want.Tag, msg.CommandTag)
		}
	case *pgproto3.ReadyForQuery:
		if want.Status != "" && want.Status[0] != msg.TxStatus {
			return fmt.Errorf("ReadyForQuery.TxStatus: want %s, got %s", want.Status, strconv.QuoteRune(rune(msg.TxStatus)))
		}
	case *pgproto3.DataRow:
		if want.Values != nil {
			if len(want.Values) != len(msg.Values) {
				return fmt.Errorf("DataRow.Values count: want %d, got %d", len(want.Values), len(msg.Values))
			}
			for i, value := range want.Values {
				if value == nil {
					if msg.Values[i] != nil {
						return fmt.Errorf("DataRow.Values[%d]: want NULL, got %x", i, msg.Values[i])
					}
					continue
				}
				decoded, _ := hex.DecodeString(*value)
				if msg.Values[i] == nil || !bytes.Equal(decoded, msg.Values[i]) {
					return fmt.Errorf("DataRow.Values[%d]: want %s, got %x (NULL=%t)", i, *value, msg.Values[i], msg.Values[i] == nil)
				}
			}
		}
	}
	return nil
}

func checkCopyFormat(want Expectation, format byte, columns []uint16) error {
	if want.Format != nil && *want.Format != format {
		return fmt.Errorf("%s.OverallFormat: want %d, got %d", want.Type, *want.Format, format)
	}
	if want.ColumnFormats != nil && !reflect.DeepEqual(want.ColumnFormats, columns) {
		return fmt.Errorf("%s.ColumnFormatCodes: want %v, got %v", want.Type, want.ColumnFormats, columns)
	}
	return nil
}
