// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

//go:build integration

package integration

import (
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLocalErrorTransaction checks that locally detectable statement errors
// have the same transaction effects as backend errors, including savepoints.
func TestLocalErrorTransaction(t *testing.T) {
	proxy := startProxy(t)
	for _, kind := range []string{"missing_bind", "missing_describe", "duplicate_parse"} {
		for _, mode := range []string{"implicit", "explicit", "savepoint", "flush", "pipeline"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				var observed int
				observe := func(t *testing.T) {
					conn, err := backend.Connect(t.Context(), "postgres")
					require.NoError(t, err)
					defer conn.Close(t.Context())
					require.NoError(t, conn.QueryRow(t.Context(), "SELECT count(*) FROM local_error_events").Scan(&observed))
				}
				insert := []pgproto3.FrontendMessage{
					&pgproto3.Parse{Name: "write", Query: "INSERT INTO local_error_events VALUES (1)"},
					&pgproto3.Bind{PreparedStatement: "write"}, &pgproto3.Execute{},
				}
				var bad pgproto3.FrontendMessage
				code := "26000"
				switch kind {
				case "missing_bind":
					bad = &pgproto3.Bind{PreparedStatement: "missing"}
				case "missing_describe":
					bad = &pgproto3.Describe{ObjectType: 'S', Name: "missing"}
				case "duplicate_parse":
					bad = &pgproto3.Parse{Name: "existing", Query: "SELECT 12"}
					code = "42P05"
				}
				steps := []step{ready(&pgproto3.Parse{Name: "existing", Query: "SELECT 11"}, &pgproto3.Sync{})}
				want := [][]string{{"ParseComplete", "ReadyForQuery I"}}
				if mode == "explicit" || mode == "savepoint" {
					steps = append(steps, simpleQuery("BEGIN"))
					want = append(want, []string{"CommandComplete BEGIN", "ReadyForQuery T"})
				}
				if mode == "savepoint" {
					steps = append(steps, simpleQuery("INSERT INTO local_error_events VALUES (10)"), simpleQuery("SAVEPOINT before_error"))
					want = append(want, []string{"CommandComplete INSERT 0 1", "ReadyForQuery T"}, []string{"CommandComplete SAVEPOINT", "ReadyForQuery T"})
				}
				batch := append(insert, bad)
				errorReply := []string{"ParseComplete", "BindComplete", "CommandComplete INSERT 0 1", "ErrorResponse " + code}
				ignored := &pgproto3.Parse{Name: "ignored", Query: "INSERT INTO local_error_events VALUES (2)"}
				status := "I"
				if mode == "explicit" || mode == "savepoint" {
					status = "E"
				}
				if mode == "flush" {
					steps = append(steps, failing(append(batch, &pgproto3.Flush{})...), quiet(ignored, &pgproto3.Bind{PreparedStatement: "ignored"}, &pgproto3.Execute{}, &pgproto3.Flush{}), ready(&pgproto3.Sync{}))
					want = append(want, errorReply, []string{}, []string{"ReadyForQuery I"})
				} else {
					batch = append(batch, ignored, &pgproto3.Bind{PreparedStatement: "ignored"}, &pgproto3.Execute{}, &pgproto3.Sync{})
					if mode == "pipeline" {
						batch = append(batch, &pgproto3.Query{String: "SELECT 1"})
					}
					steps = append(steps, ready(batch...))
					want = append(want, append(errorReply, "ReadyForQuery "+status))
					if mode == "pipeline" {
						steps = append(steps, ready())
						want = append(want, []string{"RowDescription", "DataRow 1", "CommandComplete SELECT 1", "ReadyForQuery I"})
					}
				}
				if mode == "explicit" || mode == "savepoint" {
					steps = append(steps, simpleQuery("SELECT 1"))
					want = append(want, []string{"ErrorResponse 25P02", "ReadyForQuery E"})
					if mode == "savepoint" {
						steps = append(steps, simpleQuery("ROLLBACK TO SAVEPOINT before_error"), simpleQuery("SELECT count(*) FROM local_error_events"))
						want = append(want, []string{"CommandComplete ROLLBACK", "ReadyForQuery T"}, []string{"RowDescription", "DataRow 1", "CommandComplete SELECT 1", "ReadyForQuery T"})
					}
					steps = append(steps, simpleQuery("COMMIT"))
					commit := "ROLLBACK"
					if mode == "savepoint" {
						commit = "COMMIT"
					}
					want = append(want, []string{"CommandComplete " + commit, "ReadyForQuery I"})
				}
				reuse := simpleQuery("SELECT 1")
				reuse.before = observe
				steps = append(steps, reuse)
				want = append(want, []string{"RowDescription", "DataRow 1", "CommandComplete SELECT 1", "ReadyForQuery I"})
				runProtocolCases(t, proxy, []protocolCase{{
					name: "rollback", setup: []string{"DROP TABLE IF EXISTS local_error_events", "CREATE TABLE local_error_events (id int)"},
					steps: steps,
					check: func(t *testing.T, replies [][]pgproto3.BackendMessage) {
						count := 0
						if mode == "savepoint" {
							count = 1
						}
						assert.Equal(t, count, observed, "committed rows seen by an independent connection")
						requireReplies(t, replies, want...)
					},
				}})
			})
		}
	}
}

// An earlier backend error wins over the local error that triggers the drain.
func TestLocalErrorOrdering(t *testing.T) {
	runProtocolCases(t, startProxy(t), []protocolCase{{
		name: "backend error before missing bind",
		steps: []step{
			ready(&pgproto3.Parse{Query: "SELECT 1 / 0"}, &pgproto3.Bind{}, &pgproto3.Execute{},
				&pgproto3.Bind{PreparedStatement: "missing"}, &pgproto3.Sync{}),
			simpleQuery("SELECT 1"),
		},
		check: func(t *testing.T, replies [][]pgproto3.BackendMessage) {
			requireReplies(t, replies,
				[]string{"ParseComplete", "ErrorResponse 22012", "ReadyForQuery I"},
				[]string{"RowDescription", "DataRow 1", "CommandComplete SELECT 1", "ReadyForQuery I"})
		},
	}, {
		name: "duplicate pending parse",
		steps: []step{
			ready(&pgproto3.Parse{Name: "existing", Query: "SELECT 11"},
				&pgproto3.Parse{Name: "existing", Query: "SELECT 12"}, &pgproto3.Sync{}),
			ready(&pgproto3.Bind{PreparedStatement: "existing"}, &pgproto3.Execute{}, &pgproto3.Sync{}),
		},
		check: func(t *testing.T, replies [][]pgproto3.BackendMessage) {
			requireReplies(t, replies,
				[]string{"ParseComplete", "ErrorResponse 42P05", "ReadyForQuery I"},
				[]string{"BindComplete", "DataRow 11", "CommandComplete SELECT 1", "ReadyForQuery I"})
		},
	}})
}

// The internal error must not replace the client's unnamed statement. Use only
// extended messages for recovery, since a simple Query would itself drop it.
func TestLocalErrorPreservesUnnamedStatement(t *testing.T) {
	runProtocolCases(t, startProxy(t), []protocolCase{{
		name: "savepoint recovery retains unnamed statement",
		steps: []step{
			simpleQuery("BEGIN"), simpleQuery("SAVEPOINT before_error"),
			ready(&pgproto3.Parse{Query: "SELECT 7"}, &pgproto3.Sync{}),
			ready(&pgproto3.Describe{ObjectType: 'S', Name: "missing"}, &pgproto3.Sync{}),
			ready(&pgproto3.Parse{Name: "recover", Query: "ROLLBACK TO SAVEPOINT before_error"},
				&pgproto3.Bind{PreparedStatement: "recover"}, &pgproto3.Execute{}, &pgproto3.Sync{}),
			ready(&pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{}),
			simpleQuery("COMMIT"),
		},
		check: func(t *testing.T, replies [][]pgproto3.BackendMessage) {
			requireReplies(t, replies,
				[]string{"CommandComplete BEGIN", "ReadyForQuery T"},
				[]string{"CommandComplete SAVEPOINT", "ReadyForQuery T"},
				[]string{"ParseComplete", "ReadyForQuery T"},
				[]string{"ErrorResponse 26000", "ReadyForQuery E"},
				[]string{"ParseComplete", "BindComplete", "CommandComplete ROLLBACK", "ReadyForQuery T"},
				[]string{"BindComplete", "DataRow 7", "CommandComplete SELECT 1", "ReadyForQuery T"},
				[]string{"CommandComplete COMMIT", "ReadyForQuery I"})
		},
	}})
}
