// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

//go:build integration

package integration

import (
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

// Known COPY bugs. Each case that hits one still runs against PostgreSQL
// directly; remove proxySkip to reproduce it through the proxy.
const (
	copyInUnderExecute = "TODO: proxy rejects CopyInResponse while it expects ExecuteReply " +
		"and closes the client connection without an ErrorResponse (#12 reported 08006)"
	copyFailNotFlushed = "TODO: proxy does not flush CopyFail to PostgreSQL, " +
		"so the client waits for the ErrorResponse until its read deadline"
)

// TestCopy verifies COPY FROM STDIN and COPY TO STDOUT in the simple and
// extended protocols, including errors during copy-in and the recovery after
// them. Each case runs against PostgreSQL directly and through the proxy.
func TestCopy(t *testing.T) {
	setup := []string{
		"DROP TABLE IF EXISTS copy_events",
		"CREATE TABLE copy_events (id integer PRIMARY KEY, label text)",
	}
	copyFrom := "COPY copy_events FROM STDIN"
	copyTo := "COPY (SELECT id, label FROM copy_events ORDER BY id) TO STDOUT"
	countQuery := &pgproto3.Query{String: "SELECT count(*) FROM copy_events"}
	// Rows 1, 2 and 3 split across CopyData messages mid-row, with a NULL and
	// an empty label.
	rows := []pgproto3.FrontendMessage{
		&pgproto3.CopyData{Data: []byte("1\tone\n2\t")}, &pgproto3.CopyData{Data: []byte("\\N\n3\t\n")},
	}
	badInt := &pgproto3.CopyData{Data: []byte("1\tone\nnot_an_int\tbad\n")}
	duplicate := &pgproto3.CopyData{Data: []byte("1\tone\n1\tduplicate\n")}
	clientFail := []pgproto3.FrontendMessage{
		&pgproto3.CopyData{Data: []byte("1\tone\n")}, &pgproto3.CopyFail{Message: "client stopped COPY"},
	}
	// Messages after an error up to Sync must be ignored, so the insert never runs.
	ignoredInsert := []pgproto3.FrontendMessage{
		&pgproto3.Parse{Query: "INSERT INTO copy_events VALUES (99, 'ignored')"},
		&pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{},
	}
	extendedCopyFrom := []pgproto3.FrontendMessage{
		&pgproto3.Parse{Query: copyFrom}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Flush{},
	}
	selectRows := ready(&pgproto3.Query{String: "SELECT id, label FROM copy_events ORDER BY id"})
	countRows := ready(countQuery)
	textCopyIn := []string{"CopyInResponse 0 [0 0]"}
	extendedCopyIn := []string{"ParseComplete", "BindComplete", "CopyInResponse 0 [0 0]"}
	threeRows := []string{"RowDescription", "DataRow 1|one", "DataRow 2|NULL", "DataRow 3|", "CommandComplete SELECT 3", "ReadyForQuery I"}
	noRows := []string{"RowDescription", "CommandComplete SELECT 0", "ReadyForQuery I"}
	zeroCount := []string{"RowDescription", "DataRow 0", "CommandComplete SELECT 1", "ReadyForQuery I"}
	copiedOut := []string{"CopyData \"1\\tone\\n\"", "CopyData \"2\\t\\\\N\\n\"", "CopyData \"3\\t\\n\"", "CopyDone", "CommandComplete COPY 3"}

	runProtocolCases(t, startProxy(t), []protocolCase{{
		name:  "simple success",
		setup: setup,
		steps: []step{
			copyIn(&pgproto3.Query{String: copyFrom}),
			ready(append(rows, &pgproto3.CopyDone{})...),
			ready(&pgproto3.Query{String: copyTo}),
			selectRows,
		},
		check: func(t *testing.T, replies [][]pgproto3.BackendMessage) {
			requireReplies(t, replies,
				textCopyIn,
				[]string{"CommandComplete COPY 3", "ReadyForQuery I"},
				append(append([]string{"CopyOutResponse 0 [0 0]"}, copiedOut...), "ReadyForQuery I"),
				threeRows,
			)
		},
	}, {
		name:  "binary input",
		setup: setup,
		steps: []step{
			copyIn(&pgproto3.Query{String: "COPY copy_events FROM STDIN (FORMAT binary)"}),
			// The signature, flags and header extension length, then rows (7, NULL)
			// and (8, ''), then the trailer. The first message ends inside the signature.
			ready(
				&pgproto3.CopyData{Data: []byte("PGCOPY\n\xff\r")},
				&pgproto3.CopyData{Data: []byte("\n\x00" + "\x00\x00\x00\x00" + "\x00\x00\x00\x00" +
					"\x00\x02" + "\x00\x00\x00\x04\x00\x00\x00\x07" + "\xff\xff\xff\xff" +
					"\x00\x02" + "\x00\x00\x00\x04\x00\x00\x00\x08" + "\x00\x00\x00\x00" +
					"\xff\xff")},
				&pgproto3.CopyDone{},
			),
			selectRows,
		},
		check: func(t *testing.T, replies [][]pgproto3.BackendMessage) {
			requireReplies(t, replies,
				[]string{"CopyInResponse 1 [1 1]"},
				[]string{"CommandComplete COPY 2", "ReadyForQuery I"},
				[]string{"RowDescription", "DataRow 7|NULL", "DataRow 8|", "CommandComplete SELECT 2", "ReadyForQuery I"},
			)
		},
	}, {
		// Each error ends the COPY and the rows it copied, and the CopyData and
		// CopyDone the client still sends are ignored.
		name:      "simple errors",
		setup:     setup,
		proxySkip: copyFailNotFlushed,
		steps: []step{
			copyIn(&pgproto3.Query{String: copyFrom}),
			ready(clientFail...),
			ready(&pgproto3.CopyData{Data: []byte("leftover\n")}, &pgproto3.CopyDone{},
				countQuery),
			copyIn(&pgproto3.Query{String: copyFrom}),
			ready(badInt, &pgproto3.CopyDone{}),
			countRows,
			copyIn(&pgproto3.Query{String: copyFrom}),
			ready(duplicate, &pgproto3.CopyDone{}),
			countRows,
		},
		check: func(t *testing.T, replies [][]pgproto3.BackendMessage) {
			requireReplies(t, replies,
				textCopyIn, []string{"ErrorResponse 57014", "ReadyForQuery I"}, zeroCount,
				textCopyIn, []string{"ErrorResponse 22P02", "ReadyForQuery I"}, zeroCount,
				textCopyIn, []string{"ErrorResponse 23505", "ReadyForQuery I"}, zeroCount,
			)
		},
	}, {
		// A Sync during copy-in is ignored; the one after CopyDone ends the batch.
		name:      "extended success",
		setup:     setup,
		proxySkip: copyInUnderExecute,
		steps: []step{
			copyIn(&pgproto3.Parse{Query: copyFrom}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{}),
			ready(append(append([]pgproto3.FrontendMessage{&pgproto3.Sync{}}, rows...),
				&pgproto3.CopyDone{}, &pgproto3.Sync{})...),
			quiet(&pgproto3.Parse{Query: copyTo},
				&pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Flush{}),
			ready(&pgproto3.Sync{}),
			selectRows,
		},
		check: func(t *testing.T, replies [][]pgproto3.BackendMessage) {
			requireReplies(t, replies,
				extendedCopyIn,
				[]string{"CommandComplete COPY 3", "ReadyForQuery I"},
				append([]string{"ParseComplete", "BindComplete", "CopyOutResponse 0 [0 0]"}, copiedOut...),
				[]string{"ReadyForQuery I"},
				threeRows,
			)
		},
	}, {
		name:      "extended errors",
		setup:     setup,
		proxySkip: copyInUnderExecute,
		steps: []step{
			copyIn(extendedCopyFrom...),
			failing(clientFail...),
			ready(ignoredInsert...),
			copyIn(extendedCopyFrom...),
			failing(badInt, &pgproto3.CopyDone{}),
			ready(ignoredInsert...),
			selectRows,
		},
		check: func(t *testing.T, replies [][]pgproto3.BackendMessage) {
			requireReplies(t, replies,
				extendedCopyIn, []string{"ErrorResponse 57014"}, []string{"ReadyForQuery I"},
				extendedCopyIn, []string{"ErrorResponse 22P02"}, []string{"ReadyForQuery I"},
				noRows,
			)
		},
	}, {
		name:      "error in an explicit transaction",
		setup:     setup,
		proxySkip: copyInUnderExecute,
		steps: []step{
			ready(&pgproto3.Query{String: "BEGIN"}),
			copyIn(extendedCopyFrom...),
			failing(duplicate, &pgproto3.CopyDone{}),
			ready(ignoredInsert...),
			ready(&pgproto3.Query{String: "SELECT 1"}),
			ready(&pgproto3.Query{String: "ROLLBACK"}),
			selectRows,
		},
		check: func(t *testing.T, replies [][]pgproto3.BackendMessage) {
			requireReplies(t, replies,
				[]string{"CommandComplete BEGIN", "ReadyForQuery T"},
				extendedCopyIn,
				[]string{"ErrorResponse 23505"},
				[]string{"ReadyForQuery E"},
				[]string{"ErrorResponse 25P02", "ReadyForQuery E"},
				[]string{"CommandComplete ROLLBACK", "ReadyForQuery I"},
				noRows,
			)
		},
	}})
}
