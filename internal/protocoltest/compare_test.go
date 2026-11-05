package protocoltest

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

func backendFrame(t *testing.T, message pgproto3.BackendMessage) Frame {
	t.Helper()
	raw, err := message.Encode(nil)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := pgproto3.NewFrontend(bytes.NewReader(raw), nil).Receive()
	if err != nil {
		t.Fatal(err)
	}
	frame, err := snapshot("backend", raw, decoded)
	if err != nil {
		t.Fatal(err)
	}
	return frame
}
func backendTranscript(t *testing.T, messages ...pgproto3.BackendMessage) Transcript {
	t.Helper()
	result := Transcript{Completed: true}
	for _, message := range messages {
		result.Events = append(result.Events, backendFrame(t, message))
	}
	return result
}

func TestOracleMutationsRemainVisible(t *testing.T) {
	tests := []struct {
		name, path  string
		left, right pgproto3.BackendMessage
	}{
		{"transaction status", "ReadyForQuery.TxStatus", &pgproto3.ReadyForQuery{TxStatus: 'I'}, &pgproto3.ReadyForQuery{TxStatus: 'E'}},
		{"command tag", "CommandComplete.CommandTag", &pgproto3.CommandComplete{CommandTag: []byte("INSERT 0 1")}, &pgproto3.CommandComplete{CommandTag: []byte("INSERT 0 2")}},
		{"SQLSTATE", "ErrorResponse.Code", &pgproto3.ErrorResponse{Code: "42601"}, &pgproto3.ErrorResponse{Code: "26000"}},
		{"error detail", "ErrorResponse.Detail", &pgproto3.ErrorResponse{Code: "42601", Detail: "first"}, &pgproto3.ErrorResponse{Code: "42601", Detail: "second"}},
		{"error source", "ErrorResponse.Routine", &pgproto3.ErrorResponse{Routine: "one"}, &pgproto3.ErrorResponse{Routine: "two"}},
		{"unknown error field", "ErrorResponse.UnknownFields", &pgproto3.ErrorResponse{UnknownFields: map[byte]string{'!': "one"}}, &pgproto3.ErrorResponse{UnknownFields: map[byte]string{'!': "two"}}},
		{"NULL empty", "DataRow.Values[0]", &pgproto3.DataRow{Values: [][]byte{nil}}, &pgproto3.DataRow{Values: [][]byte{{}}}},
		{"binary byte", "DataRow.Values[0]", &pgproto3.DataRow{Values: [][]byte{{0xff, 0xfe}}}, &pgproto3.DataRow{Values: [][]byte{{0xff, 0xfd}}}},
		{"parameter OID", "ParameterDescription.ParameterOIDs[0]", &pgproto3.ParameterDescription{ParameterOIDs: []uint32{23}}, &pgproto3.ParameterDescription{ParameterOIDs: []uint32{25}}},
		{"row format", "RowDescription.Fields[0].Format", &pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{Format: 0}}}, &pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{Format: 1}}}},
		{"attribute number", "RowDescription.Fields[0].TableAttributeNumber", &pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{TableAttributeNumber: 1}}}, &pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{TableAttributeNumber: 2}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			left, right := backendTranscript(t, test.left), backendTranscript(t, test.right)
			if err := Compare(left, left, nil, nil); err != nil {
				t.Fatalf("self comparison: %v", err)
			}
			if err := Compare(left, right, nil, nil); err == nil || !strings.Contains(err.Error(), test.path) {
				t.Fatalf("want %s mismatch, got %v", test.path, err)
			}
		})
	}
}

func TestOracleCountsAndOrder(t *testing.T) {
	first := backendFrame(t, &pgproto3.ParseComplete{})
	second := backendFrame(t, &pgproto3.BindComplete{})
	left := Transcript{Completed: true, Events: []Frame{first, second}}
	for _, events := range [][]Frame{{first}, {first, second, second}, {second, first}} {
		if err := Compare(left, Transcript{Completed: true, Events: events}, nil, nil); err == nil {
			t.Fatal("lost/duplicated/reordered reply passed")
		}
	}
	incomplete := left
	incomplete.Completed = false
	if err := Compare(left, incomplete, nil, nil); err == nil {
		t.Fatal("incomplete transcript passed")
	}
}

func TestOracleRelationNormalizationIsNarrow(t *testing.T) {
	row := func(oid uint32, datatype uint32) Transcript {
		return backendTranscript(t, &pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{Name: []byte("id"), TableOID: oid, TableAttributeNumber: 1, DataTypeOID: datatype, DataTypeSize: 4, TypeModifier: -1, Format: 0}}})
	}
	left, right := row(100, 23), row(200, 23)
	a, b := map[uint32]string{100: "public.events"}, map[uint32]string{200: "public.events"}
	if err := Compare(left, right, a, b); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		right   Transcript
		mapping map[uint32]string
	}{
		{right, nil}, {right, map[uint32]string{200: "public.other"}}, {row(0, 23), b}, {row(200, 25), b},
	} {
		if err := Compare(left, test.right, a, test.mapping); err == nil {
			t.Fatal("invalid normalization passed")
		}
	}
	// OID-looking data bytes are values, never relation identifiers.
	if err := Compare(backendTranscript(t, &pgproto3.DataRow{Values: [][]byte{[]byte("100")}}), backendTranscript(t, &pgproto3.DataRow{Values: [][]byte{[]byte("200")}}), a, b); err == nil {
		t.Fatal("data bytes were normalized")
	}
}

func TestOracleDiagnosticOrderingOnly(t *testing.T) {
	left := backendTranscript(t, &pgproto3.ErrorResponse{Severity: "ERROR", Code: "42601", Message: "syntax"})
	raw := []byte{'E', 0, 0, 0, 0, 'M', 's', 'y', 'n', 't', 'a', 'x', 0, 'C', '4', '2', '6', '0', '1', 0, 'S', 'E', 'R', 'R', 'O', 'R', 0, 0}
	binary.BigEndian.PutUint32(raw[1:5], uint32(len(raw)-1))
	message, err := pgproto3.NewFrontend(bytes.NewReader(raw), nil).Receive()
	if err != nil {
		t.Fatal(err)
	}
	frame, err := snapshot("backend", raw, message)
	if err != nil {
		t.Fatal(err)
	}
	if err := Compare(left, Transcript{Completed: true, Events: []Frame{frame}}, nil, nil); err != nil {
		t.Fatal(err)
	}
	raw = append(raw[:len(raw)-1], []byte{'C', '4', '2', '6', '0', '1', 0, 0}...)
	binary.BigEndian.PutUint32(raw[1:5], uint32(len(raw)-1))
	message, err = pgproto3.NewFrontend(bytes.NewReader(raw), nil).Receive()
	if err != nil {
		t.Fatal(err)
	}
	frame, err = snapshot("backend", raw, message)
	if err != nil {
		t.Fatal(err)
	}
	if err := Compare(Transcript{Completed: true, Events: []Frame{frame}}, Transcript{Completed: true, Events: []Frame{frame}}, nil, nil); err == nil {
		t.Fatal("duplicate diagnostic field passed")
	}
}

func TestSnapshotPreservesBinaryAndDoesNotAlias(t *testing.T) {
	data := []byte{0xff, 0xfe}
	message := &pgproto3.DataRow{Values: [][]byte{data, nil, {}}}
	frame := backendFrame(t, message)
	original := frame.Hex
	data[0] = 0
	if frame.Hex != original || !bytes.Contains(frame.Decoded, []byte(`"fffe"`)) {
		t.Fatalf("snapshot aliased or lost bytes: %s", frame.Decoded)
	}
	var decoded map[string]any
	if err := json.Unmarshal(frame.Decoded, &decoded); err != nil {
		t.Fatal(err)
	}
	values := decoded["Values"].([]any)
	if values[1] != nil || values[2] != "" {
		t.Fatalf("NULL/empty changed: %#v", values)
	}
}

func TestOracleRejectsTrailingPayload(t *testing.T) {
	frame := backendFrame(t, &pgproto3.ReadyForQuery{TxStatus: 'I'})
	raw, _ := hex.DecodeString(frame.Hex)
	raw = append(raw, 0)
	binary.BigEndian.PutUint32(raw[1:5], uint32(len(raw)-1))
	frame.Hex = hex.EncodeToString(raw)
	transcript := Transcript{Completed: true, Events: []Frame{frame}}
	if err := Compare(transcript, transcript, nil, nil); err == nil {
		t.Fatal("trailing payload passed")
	}
}

func TestOracleZeroColumnDescription(t *testing.T) {
	transcript := backendTranscript(t, &pgproto3.RowDescription{})
	if err := Compare(transcript, transcript, nil, nil); err != nil {
		t.Fatal(err)
	}
}

func TestOracleCopyPayloadAndFormats(t *testing.T) {
	for _, pair := range [][2]pgproto3.BackendMessage{
		{&pgproto3.CopyData{Data: []byte{0, 1, 0xff}}, &pgproto3.CopyData{Data: []byte{0, 2, 0xff}}},
		{&pgproto3.CopyInResponse{OverallFormat: 0, ColumnFormatCodes: []uint16{0}}, &pgproto3.CopyInResponse{OverallFormat: 1, ColumnFormatCodes: []uint16{1}}},
		{&pgproto3.CopyOutResponse{ColumnFormatCodes: []uint16{0}}, &pgproto3.CopyOutResponse{ColumnFormatCodes: []uint16{1}}},
	} {
		if err := Compare(backendTranscript(t, pair[0]), backendTranscript(t, pair[1]), nil, nil); err == nil {
			t.Fatal("changed COPY payload or format passed")
		}
	}
}
