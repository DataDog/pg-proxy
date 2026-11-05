// Package protocoltest executes bounded PostgreSQL conversations and compares
// complete responses. It deliberately does not import PgProxy protocol logic.
package protocoltest

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5/pgproto3"
)

// Scenario is a reproducible, versioned conversation. Setup and Effects belong
// to the fixture; Run executes only Steps on one existing, isolated database.
type Scenario struct {
	Version int      `json:"version"`
	Name    string   `json:"name"`
	Setup   []string `json:"setup,omitempty"`
	Steps   []Step   `json:"steps"`
	Effects []Effect `json:"effects,omitempty"`
}

// Step performs exactly one operation. Sending queues bytes; Flush writes them.
// A receive checkpoint describes every expected message, including notices.
type Step struct {
	Send       []Message     `json:"send,omitempty"`
	Flush      bool          `json:"flush_transport,omitempty"`
	Receive    []Expectation `json:"receive,omitempty"`
	Checkpoint string        `json:"checkpoint,omitempty"`
	Reassign   bool          `json:"reassign_backend,omitempty"`
}

// Message describes a frontend message. Parameters contain hex bytes; a null
// element is SQL NULL, while an empty string is a non-NULL zero-length value.
type Message struct {
	Type             string    `json:"type"`
	SQL              string    `json:"sql,omitempty"`
	Name             string    `json:"name,omitempty"`
	Statement        string    `json:"statement,omitempty"`
	Portal           string    `json:"portal,omitempty"`
	Object           string    `json:"object,omitempty"`
	ParameterOIDs    []uint32  `json:"parameter_oids,omitempty"`
	ParameterFormats []int16   `json:"parameter_formats,omitempty"`
	Parameters       []*string `json:"parameters,omitempty"`
	ResultFormats    []int16   `json:"result_formats,omitempty"`
	MaxRows          uint32    `json:"max_rows,omitempty"`
	Data             *string   `json:"data,omitempty"`
	Message          string    `json:"message,omitempty"`
}

// Expectation adds independent assertions to differential equality. Values use
// the same hex/NULL representation as Message.Parameters.
type Expectation struct {
	Type          string    `json:"type"`
	Code          string    `json:"code,omitempty"`
	Tag           string    `json:"tag,omitempty"`
	Status        string    `json:"status,omitempty"`
	Values        []*string `json:"values,omitempty"`
	Data          *string   `json:"data,omitempty"`
	Format        *uint8    `json:"format,omitempty"`
	ColumnFormats []uint16  `json:"column_formats,omitempty"`
}

// Effect is a sorted control query and its expected text rows after the run.
// Null cells represent SQL NULL. Effects must not depend on query ordering.
type Effect struct {
	SQL  string      `json:"sql"`
	Rows [][]*string `json:"rows"`
}

var scenarioName = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// DecodeScenario rejects unknown/duplicate fields, trailing JSON and ambiguous
// steps. A typo must fail instead of silently weakening a scenario.
func DecodeScenario(data []byte) (Scenario, error) {
	if len(data) > 4<<20 {
		return Scenario{}, errors.New("scenario exceeds 4 MiB")
	}
	if err := uniqueJSON(data); err != nil {
		return Scenario{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var scenario Scenario
	if err := decoder.Decode(&scenario); err != nil {
		return scenario, err
	}
	if err := scenario.Validate(); err != nil {
		return scenario, err
	}
	return scenario, nil
}

// Validate checks a scenario constructed either from JSON or directly in Go.
func (s Scenario) Validate() error {
	if s.Version != 1 {
		return fmt.Errorf("unsupported scenario version %d", s.Version)
	}
	if !scenarioName.MatchString(s.Name) {
		return fmt.Errorf("invalid scenario name %q", s.Name)
	}
	if len(s.Steps) == 0 || len(s.Steps) > 4096 {
		return errors.New("scenario must have 1..4096 steps")
	}
	for i, step := range s.Steps {
		operations := 0
		if len(step.Send) > 0 {
			operations++
		}
		if step.Flush {
			operations++
		}
		if len(step.Receive) > 0 {
			operations++
		}
		if step.Reassign {
			operations++
		}
		if operations != 1 {
			return fmt.Errorf("step %d must have exactly one operation", i)
		}
		if step.Checkpoint != "" && len(step.Receive) == 0 {
			return fmt.Errorf("step %d: checkpoint requires receive", i)
		}
		if len(step.Send) > 4096 || len(step.Receive) > 4096 {
			return fmt.Errorf("step %d exceeds message limit", i)
		}
		for j, msg := range step.Send {
			if _, err := msg.encode(); err != nil {
				return fmt.Errorf("step %d send %d: %w", i, j, err)
			}
		}
		for j, want := range step.Receive {
			if err := want.validate(); err != nil {
				return fmt.Errorf("step %d receive %d: %w", i, j, err)
			}
		}
	}
	for i, effect := range s.Effects {
		if effect.SQL == "" || effect.Rows == nil {
			return fmt.Errorf("effect %d requires SQL and explicit rows (use [] for none)", i)
		}
	}
	return nil
}

func (m Message) encode() ([]byte, error) {
	for _, value := range []string{m.SQL, m.Name, m.Statement, m.Portal, m.Message} {
		if strings.ContainsRune(value, 0) {
			return nil, errors.New("message strings cannot contain NUL")
		}
	}
	// Reject fields irrelevant to the selected message, including common typos
	// such as using Name instead of Statement in a Bind.
	allowed := map[string]string{
		"Query": "sql", "Parse": "sql,name,parameter_oids", "Bind": "statement,portal,parameter_formats,parameters,result_formats",
		"Describe": "name,object", "Execute": "portal,max_rows", "Close": "name,object", "Flush": "", "Sync": "", "CopyData": "data", "CopyDone": "", "CopyFail": "message",
	}
	fields, ok := allowed[m.Type]
	if !ok {
		return nil, fmt.Errorf("unsupported frontend message %q", m.Type)
	}
	present := map[string]bool{
		"sql": m.SQL != "", "name": m.Name != "", "statement": m.Statement != "", "portal": m.Portal != "", "object": m.Object != "",
		"parameter_oids": m.ParameterOIDs != nil, "parameter_formats": m.ParameterFormats != nil, "parameters": m.Parameters != nil,
		"result_formats": m.ResultFormats != nil, "max_rows": m.MaxRows != 0, "data": m.Data != nil, "message": m.Message != "",
	}
	for field, exists := range present {
		if exists && !strings.Contains(","+fields+",", ","+field+",") {
			return nil, fmt.Errorf("%s does not accept %s", m.Type, field)
		}
	}
	var message pgproto3.FrontendMessage
	switch m.Type {
	case "CopyData":
		if m.Data == nil {
			return nil, errors.New("CopyData requires explicit hex data")
		}
		data, err := hex.DecodeString(*m.Data)
		if err != nil {
			return nil, fmt.Errorf("CopyData.data: %w", err)
		}
		message = &pgproto3.CopyData{Data: data}
	case "CopyDone":
		message = &pgproto3.CopyDone{}
	case "CopyFail":
		message = &pgproto3.CopyFail{Message: m.Message}
	case "Query":
		message = &pgproto3.Query{String: m.SQL}
	case "Parse":
		message = &pgproto3.Parse{Name: m.Name, Query: m.SQL, ParameterOIDs: m.ParameterOIDs}
	case "Bind":
		parameters := make([][]byte, len(m.Parameters))
		for i, value := range m.Parameters {
			if value != nil {
				decoded, err := hex.DecodeString(*value)
				if err != nil {
					return nil, fmt.Errorf("parameter %d: %w", i, err)
				}
				parameters[i] = decoded
			}
		}
		for _, code := range append(append([]int16{}, m.ParameterFormats...), m.ResultFormats...) {
			if code != 0 && code != 1 {
				return nil, fmt.Errorf("invalid format code %d", code)
			}
		}
		if n := len(m.ParameterFormats); n != 0 && n != 1 && n != len(parameters) {
			return nil, errors.New("parameter format count must be 0, 1, or parameter count")
		}
		message = &pgproto3.Bind{DestinationPortal: m.Portal, PreparedStatement: m.Statement, Parameters: parameters, ParameterFormatCodes: m.ParameterFormats, ResultFormatCodes: m.ResultFormats}
	case "Describe", "Close":
		if m.Object != "S" && m.Object != "P" {
			return nil, errors.New("object must be S or P")
		}
		if m.Type == "Describe" {
			message = &pgproto3.Describe{ObjectType: m.Object[0], Name: m.Name}
		} else {
			message = &pgproto3.Close{ObjectType: m.Object[0], Name: m.Name}
		}
	case "Execute":
		message = &pgproto3.Execute{Portal: m.Portal, MaxRows: m.MaxRows}
	case "Flush":
		message = &pgproto3.Flush{}
	case "Sync":
		message = &pgproto3.Sync{}
	}
	raw, err := message.Encode(nil)
	if err == nil && len(raw) > maxFrameBytes {
		return nil, errors.New("frontend frame exceeds limit")
	}
	return raw, err
}

func (e Expectation) validate() error {
	switch e.Type {
	case "ParseComplete", "BindComplete", "CloseComplete", "CommandComplete", "EmptyQueryResponse", "RowDescription", "DataRow", "ParameterDescription", "NoData", "PortalSuspended", "ReadyForQuery", "ErrorResponse", "NoticeResponse", "ParameterStatus", "CopyInResponse", "CopyOutResponse", "CopyData", "CopyDone":
	default:
		return fmt.Errorf("unsupported expected message %q", e.Type)
	}
	if e.Code != "" && e.Type != "ErrorResponse" {
		return errors.New("code requires ErrorResponse")
	}
	if e.Tag != "" && e.Type != "CommandComplete" {
		return errors.New("tag requires CommandComplete")
	}
	if e.Status != "" && (e.Type != "ReadyForQuery" || (e.Status != "I" && e.Status != "T" && e.Status != "E")) {
		return errors.New("status requires ReadyForQuery and I, T, or E")
	}
	if e.Values != nil && e.Type != "DataRow" {
		return errors.New("values requires DataRow")
	}
	if e.Data != nil {
		if e.Type != "CopyData" {
			return errors.New("data requires CopyData")
		}
		if _, err := hex.DecodeString(*e.Data); err != nil {
			return fmt.Errorf("expected CopyData.data: %w", err)
		}
	}
	if e.Format != nil || e.ColumnFormats != nil {
		if e.Type != "CopyInResponse" && e.Type != "CopyOutResponse" {
			return errors.New("format and column_formats require CopyInResponse or CopyOutResponse")
		}
		if e.Format != nil && *e.Format > 1 {
			return errors.New("COPY format must be 0 or 1")
		}
		for _, format := range e.ColumnFormats {
			if format > 1 {
				return errors.New("COPY column format must be 0 or 1")
			}
		}
	}
	for _, value := range e.Values {
		if value != nil {
			if _, err := hex.DecodeString(*value); err != nil {
				return fmt.Errorf("expected value: %w", err)
			}
		}
	}
	return nil
}

func uniqueJSON(data []byte) error {
	d := json.NewDecoder(bytes.NewReader(data))
	var value func() error
	value = func() error {
		token, err := d.Token()
		if err != nil {
			return err
		}
		delimiter, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delimiter {
		case '{':
			seen := map[string]bool{}
			for d.More() {
				key, err := d.Token()
				if err != nil {
					return err
				}
				name := key.(string)
				if seen[name] {
					return fmt.Errorf("duplicate JSON field %q", name)
				}
				seen[name] = true
				if err := value(); err != nil {
					return err
				}
			}
		case '[':
			for d.More() {
				if err := value(); err != nil {
					return err
				}
			}
		default:
			return errors.New("unexpected JSON delimiter")
		}
		_, err = d.Token()
		return err
	}
	if err := value(); err != nil {
		return err
	}
	if _, err := d.Token(); !errors.Is(err, io.EOF) {
		if err != nil {
			return err
		}
		return errors.New("trailing JSON value")
	}
	return nil
}
