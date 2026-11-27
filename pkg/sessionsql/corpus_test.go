// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package sessionsql

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"flag"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var update = flag.Bool("update", false, "rewrite testdata/corpus.jsonl.gz with the current verdicts")

const corpusPath = "testdata/corpus.jsonl.gz"

// corpusCase is one Query or Parse text. Label and Category come from running
// the text on PostgreSQL 14 and 16 (see the set-detection-bench branch), or
// from hand labels for categories added since. Changes is the scanner's
// recorded verdict, so any change in behavior shows up as a diff.
type corpusCase struct {
	ID string `json:"id"`
	// SQL may hold several statements.
	SQL string `json:"sql"`
	// Label is dirty, clean or acceptable-either.
	Label string `json:"label"`
	// Category is P1-P9 for dirty text and N1-N7 for clean text. P8 is held
	// cursors. P9 is session changes made inside functions or triggers, out of
	// reach of a text scanner.
	Category string `json:"category"`
	// SCSOff marks text written for standard_conforming_strings = off.
	SCSOff  bool `json:"scs_off,omitempty"`
	Changes bool `json:"changes"`
}

func loadCorpus(t testing.TB) []corpusCase {
	t.Helper()
	f, err := os.Open(corpusPath)
	require.NoError(t, err)
	defer f.Close()
	gz, err := gzip.NewReader(f)
	require.NoError(t, err)
	var cases []corpusCase
	sc := bufio.NewScanner(gz)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var c corpusCase
		require.NoError(t, json.Unmarshal(sc.Bytes(), &c))
		cases = append(cases, c)
	}
	require.NoError(t, sc.Err())
	require.NotEmpty(t, cases)
	return cases
}

func writeCorpus(t testing.TB, cases []corpusCase) {
	t.Helper()
	f, err := os.Create(corpusPath)
	require.NoError(t, err)
	gz, err := gzip.NewWriterLevel(f, gzip.BestCompression)
	require.NoError(t, err)
	w := bufio.NewWriter(gz)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	for _, c := range cases {
		require.NoError(t, enc.Encode(c))
	}
	require.NoError(t, w.Flush())
	require.NoError(t, gz.Close())
	require.NoError(t, f.Close())
}

// TestCorpus requires every in-scope dirty text to be flagged and pins every
// verdict. After an intended change, rerun with -update and review the diff of
// the decompressed file.
func TestCorpus(t *testing.T) {
	cases := loadCorpus(t)
	var clean, flaggedClean, outOfScope, missedOutOfScope int
	for i, c := range cases {
		changes := Scan(c.SQL, !c.SCSOff)
		if c.Label == "dirty" {
			if c.Category == "P9" {
				outOfScope++
				if !changes {
					missedOutOfScope++
				}
			} else {
				require.True(t, changes, "in-scope dirty text scanned clean: %s %q", c.ID, c.SQL)
			}
		}
		if c.Label == "clean" {
			clean++
			if changes {
				flaggedClean++
			}
		}
		if *update {
			cases[i].Changes = changes
			continue
		}
		require.Equal(t, c.Changes, changes, "changes verdict moved for %s %q; rerun with -update if intended", c.ID, c.SQL)
	}
	if *update {
		writeCorpus(t, cases)
	}
	t.Logf("%d cases; clean texts flagged: %d of %d (%.1f%%); out-of-scope dirty texts missed: %d of %d",
		len(cases), flaggedClean, clean, 100*float64(flaggedClean)/float64(clean), missedOutOfScope, outOfScope)
}
