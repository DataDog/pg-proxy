package protocoltest

import (
	"encoding/hex"
	"strings"
	"testing"
)

func stringPointer(value string) *string { return &value }

func TestScenarioRejectsWeakOrAmbiguousInput(t *testing.T) {
	good := `{"version":1,"name":"example","steps":[{"send":[{"type":"Query","sql":""}]},{"flush_transport":true},{"receive":[{"type":"EmptyQueryResponse"},{"type":"ReadyForQuery","status":"I"}]}]}`
	if _, err := DecodeScenario([]byte(good)); err != nil {
		t.Fatal(err)
	}
	cases := map[string]string{
		"unknown root":            strings.Replace(good, `"version":1`, `"version":1,"ignore_errors":true`, 1),
		"unknown message":         strings.Replace(good, `"sql":""`, `"query":""`, 1),
		"unknown expectation":     strings.Replace(good, `"status":"I"`, `"status":"I","ignore":true`, 1),
		"duplicate field":         strings.Replace(good, `"version":1`, `"version":1,"version":1`, 1),
		"nested duplicate":        strings.Replace(good, `"sql":""`, `"sql":"bad","sql":""`, 1),
		"trailing JSON":           good + ` {}`,
		"wrong version":           strings.Replace(good, `"version":1`, `"version":2`, 1),
		"multiple actions":        strings.Replace(good, `"flush_transport":true`, `"flush_transport":true,"reassign_backend":true`, 1),
		"unused field":            strings.Replace(good, `"sql":""`, `"sql":"","portal":"p"`, 1),
		"wrong expectation field": strings.Replace(good, `"status":"I"`, `"status":"I","code":"42601"`, 1),
		"invalid status":          strings.Replace(good, `"status":"I"`, `"status":"X"`, 1),
		"empty step":              strings.Replace(good, `{"flush_transport":true}`, `{}`, 1),
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeScenario([]byte(data)); err == nil {
				t.Fatal("invalid scenario accepted")
			}
		})
	}
}

func TestMessageReferenceFrames(t *testing.T) {
	cases := []struct {
		message Message
		hex     string
	}{
		{Message{Type: "Query", SQL: "SELECT 1"}, "510000000d53454c454354203100"},
		{Message{Type: "Sync"}, "5300000004"},
		{Message{Type: "CopyData", Data: stringPointer("000aff")}, "6400000007000aff"},
		{Message{Type: "CopyDone"}, "6300000004"},
		{Message{Type: "CopyFail", Message: "stopped"}, "660000000c73746f7070656400"},
		{Message{Type: "Flush"}, "4800000004"},
		{Message{Type: "Parse", SQL: "SELECT 1"}, "50000000100053454c4543542031000000"},
	}
	for _, test := range cases {
		t.Run(test.message.Type, func(t *testing.T) {
			raw, err := test.message.encode()
			if err != nil {
				t.Fatal(err)
			}
			if got := hex.EncodeToString(raw); got != test.hex {
				t.Fatalf("got %s, want %s", got, test.hex)
			}
		})
	}
}

func TestMessageValidation(t *testing.T) {
	for _, message := range []Message{
		{Type: "Bind", Parameters: []*string{nil, stringPointer(""), stringPointer("00ff")}, ParameterFormats: []int16{1}},
		{Type: "Parse", Name: "", SQL: ""},
		{Type: "Describe", Name: "", Object: "S"},
		{Type: "Close", Name: "", Object: "P"},
	} {
		if _, err := message.encode(); err != nil {
			t.Fatal(err)
		}
	}
	for _, message := range []Message{
		{Type: "Bind", Parameters: []*string{stringPointer("not hex")}},
		{Type: "Bind", Parameters: []*string{nil}, ParameterFormats: []int16{0, 1}},
		{Type: "Bind", ResultFormats: []int16{2}},
		{Type: "Describe", Object: "X"},
		{Type: "Flush", SQL: "SELECT 1"},
		{Type: "Parse", Name: "bad\x00name"},
		{Type: "Unknown"},
		{Type: "CopyData"},
		{Type: "CopyData", Data: stringPointer("zz")},
		{Type: "CopyFail", Message: "bad\x00reason"},
		{Type: "Sync", Data: stringPointer("")},
	} {
		if _, err := message.encode(); err == nil {
			t.Fatalf("accepted invalid %+v", message)
		}
	}
}
