// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package setdetect

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
)

// Labels a corpus case can carry.
const (
	LabelDirty  = "dirty"
	LabelClean  = "clean"
	LabelEither = "acceptable-either"
)

// Case is one corpus entry: the full text of one Query or Parse message.
type Case struct {
	ID string `json:"id"`
	// SQL is the message text. It may hold several statements.
	SQL   string `json:"sql"`
	Label string `json:"label"`
	// Category is one of the P* (dirty) or N* (clean) categories in
	// categories.go.
	Category string `json:"category"`
	// Source names where the text came from, e.g. "pg-regress:horology.sql".
	Source string `json:"source"`
	// SCSOff marks text written for standard_conforming_strings = off. The
	// oracle runs it with that setting, and detectors get scsOn = false.
	SCSOff bool `json:"scs_off,omitempty"`
	// Mix is the OLTP traffic bucket (select, update, insert, other) for
	// cases drawn from application traffic, used for the weighted false
	// positive rate. Empty for cases outside the traffic sample.
	Mix string `json:"mix,omitempty"`
	// Exec is false when the oracle should not run the text, for example
	// because it needs objects only its source file creates.
	Exec bool `json:"exec"`
	// Context names the oracle fixture group whose statements must run on
	// the same session first (for example a PREPARE before an EXECUTE).
	Context []string `json:"context,omitempty"`
	// Note explains a hand label.
	Note string `json:"note,omitempty"`
}

// LoadCorpus reads a JSONL corpus file.
func LoadCorpus(path string) ([]Case, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var cases []Case
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for line := 1; sc.Scan(); line++ {
		var c Case
		if err := json.Unmarshal(sc.Bytes(), &c); err != nil {
			return nil, fmt.Errorf("%s:%d: %w", path, line, err)
		}
		cases = append(cases, c)
	}
	return cases, sc.Err()
}

// WriteJSONL writes one JSON value per line.
func WriteJSONL[T any](path string, rows []T) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	for _, r := range rows {
		if err := enc.Encode(r); err != nil {
			f.Close()
			return err
		}
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
