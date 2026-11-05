package protocoltest

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgproto3"
)

// Compare checks complete conversations in execution order. Its only
// normalization is catalog-verified relation OIDs in RowDescription and the
// protocol-unspecified ordering of ErrorResponse/NoticeResponse fields.
// Startup is separate: keys identify different endpoints, and authentication
// is not part of this post-authentication oracle.
func Compare(left, right Transcript, leftRelations, rightRelations map[uint32]string) error {
	if !left.Completed || !right.Completed || left.Error != "" || right.Error != "" {
		return errors.New("cannot compare incomplete or failed transcripts")
	}
	for _, relations := range []map[uint32]string{leftRelations, rightRelations} {
		seen := map[string]bool{}
		for oid, name := range relations {
			if oid == 0 || name == "" || seen[name] {
				return errors.New("relation map must contain unique nonzero OIDs and nonempty identities")
			}
			seen[name] = true
		}
	}
	if len(left.Events) != len(right.Events) {
		return fmt.Errorf("message count: direct %d, proxy %d", len(left.Events), len(right.Events))
	}
	for i, a := range left.Events {
		b := right.Events[i]
		prefix := fmt.Sprintf("event %d step %d checkpoint %q", i, a.Step, a.Checkpoint)
		if a.Direction != b.Direction || a.Type != b.Type || a.Step != b.Step || a.Checkpoint != b.Checkpoint {
			return fmt.Errorf("%s: message order differs: direct %s/%s, proxy %s/%s (step %d checkpoint %q)", prefix, a.Direction, a.Type, b.Direction, b.Type, b.Step, b.Checkpoint)
		}
		av, araw, err := comparisonFrame(a, leftRelations)
		if err != nil {
			return fmt.Errorf("%s direct: %w", prefix, err)
		}
		bv, braw, err := comparisonFrame(b, rightRelations)
		if err != nil {
			return fmt.Errorf("%s proxy: %w", prefix, err)
		}
		if difference := firstDifference(a.Type, av, bv); difference != "" {
			return fmt.Errorf("%s: %s", prefix, difference)
		}
		if !bytes.Equal(araw, braw) {
			return fmt.Errorf("%s: %s wire bytes differ after permitted normalization: direct %x, proxy %x", prefix, a.Type, araw, braw)
		}
	}
	return nil
}

func comparisonFrame(frame Frame, relations map[uint32]string) (any, []byte, error) {
	raw, err := hex.DecodeString(frame.Hex)
	if err != nil {
		return nil, nil, err
	}
	if len(raw) < 5 || int(binary.BigEndian.Uint32(raw[1:5])) != len(raw)-1 {
		return nil, nil, errors.New("invalid stored frame length")
	}
	var message any
	switch frame.Direction {
	case "frontend":
		message, err = pgproto3.NewBackend(bytes.NewReader(raw), io.Discard).Receive()
	case "backend":
		message, err = pgproto3.NewFrontend(bytes.NewReader(raw), io.Discard).Receive()
	default:
		return nil, nil, fmt.Errorf("unknown direction %q", frame.Direction)
	}
	if err != nil {
		return nil, nil, err
	}
	// A decoder can accept a valid prefix and leave trailing payload bytes.
	// Reject that shape even when both lanes happen to contain it.
	switch message.(type) {
	case *pgproto3.ErrorResponse, *pgproto3.NoticeResponse:
	default:
		encoded, encodeErr := message.(interface{ Encode([]byte) ([]byte, error) }).Encode(nil)
		if encodeErr != nil || !bytes.Equal(encoded, raw) {
			return nil, nil, errors.New("noncanonical or trailing wire bytes")
		}
	}
	actual, err := snapshot(frame.Direction, raw, message)
	if err != nil {
		return nil, nil, err
	}
	if actual.Type != frame.Type {
		return nil, nil, fmt.Errorf("stored type %s does not match wire type %s", frame.Type, actual.Type)
	}
	// Re-derive the oracle input from raw bytes. Decoded is only a diagnostic
	// snapshot; corruption of either representation must never silently pass.
	a, err := decodeJSON(actual.Decoded)
	if err != nil {
		return nil, nil, err
	}
	saved, err := decodeJSON(frame.Decoded)
	if err != nil {
		return nil, nil, err
	}
	if !reflect.DeepEqual(a, saved) {
		return nil, nil, errors.New("decoded snapshot does not match wire bytes")
	}
	switch msg := message.(type) {
	case *pgproto3.RowDescription:
		var fields []any
		if len(msg.Fields) > 0 {
			fields = a.(map[string]any)["Fields"].([]any)
		}
		position := 7 // header followed by field count
		for i, field := range msg.Fields {
			end := bytes.IndexByte(raw[position:], 0)
			if end < 0 {
				return nil, nil, errors.New("invalid column name")
			}
			position += end + 1
			if name, ok := relations[field.TableOID]; ok && field.TableOID != 0 {
				fields[i].(map[string]any)["TableOID"] = "relation:" + name
				binary.BigEndian.PutUint32(raw[position:position+4], 0)
			}
			position += 18
		}
	case *pgproto3.ErrorResponse, *pgproto3.NoticeResponse:
		// Error fields have no specified wire ordering. Sort by tag, but retain
		// every byte, including unknown fields and source file/line/routine.
		raw, err = canonicalError(raw)
		if err != nil {
			return nil, nil, err
		}
	}
	return a, raw, nil
}

func canonicalError(raw []byte) ([]byte, error) {
	fields := map[byte][]byte{}
	for position := 5; ; {
		if position >= len(raw) {
			return nil, errors.New("unterminated diagnostic fields")
		}
		tag := raw[position]
		position++
		if tag == 0 {
			if position != len(raw) {
				return nil, errors.New("trailing diagnostic bytes")
			}
			break
		}
		if _, ok := fields[tag]; ok {
			return nil, fmt.Errorf("duplicate diagnostic field %q", tag)
		}
		end := bytes.IndexByte(raw[position:], 0)
		if end < 0 {
			return nil, errors.New("unterminated diagnostic value")
		}
		fields[tag] = bytes.Clone(raw[position : position+end])
		position += end + 1
	}
	result := bytes.Clone(raw[:5])
	tags := make([]int, 0, len(fields))
	for tag := range fields {
		tags = append(tags, int(tag))
	}
	sort.Ints(tags)
	for _, tag := range tags {
		result = append(result, byte(tag))
		result = append(result, fields[byte(tag)]...)
		result = append(result, 0)
	}
	return append(result, 0), nil
}

func decodeJSON(data []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value any
	err := decoder.Decode(&value)
	return value, err
}

func firstDifference(path string, left, right any) string {
	if reflect.DeepEqual(left, right) {
		return ""
	}
	switch a := left.(type) {
	case map[string]any:
		b, ok := right.(map[string]any)
		if !ok {
			break
		}
		keys := map[string]bool{}
		for k := range a {
			keys[k] = true
		}
		for k := range b {
			keys[k] = true
		}
		sorted := make([]string, 0, len(keys))
		for k := range keys {
			sorted = append(sorted, k)
		}
		sort.Strings(sorted)
		for _, key := range sorted {
			av, aok := a[key]
			bv, bok := b[key]
			if !aok || !bok {
				return fmt.Sprintf("%s.%s presence: direct %t, proxy %t", path, key, aok, bok)
			}
			if diff := firstDifference(path+"."+key, av, bv); diff != "" {
				return diff
			}
		}
	case []any:
		b, ok := right.([]any)
		if !ok {
			break
		}
		if len(a) != len(b) {
			return fmt.Sprintf("%s length: direct %d, proxy %d", path, len(a), len(b))
		}
		for i := range a {
			if diff := firstDifference(fmt.Sprintf("%s[%d]", path, i), a[i], b[i]); diff != "" {
				return diff
			}
		}
	}
	if strings.HasSuffix(path, ".TxStatus") {
		char := func(v any) string {
			n, err := strconv.Atoi(fmt.Sprint(v))
			if err != nil {
				return fmt.Sprint(v)
			}
			return strconv.QuoteRune(rune(n))
		}
		return fmt.Sprintf("%s: direct %s, proxy %s", path, char(left), char(right))
	}
	return fmt.Sprintf("%s: direct %v, proxy %v", path, left, right)
}
