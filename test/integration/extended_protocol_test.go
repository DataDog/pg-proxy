// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

//go:build integration

package integration

import (
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

// TestExtendedProtocol verifies extended-protocol flows the proxy must relay
// unchanged: binary parameters and results, portals suspended by a row limit,
// Flush before Sync, and several Sync-terminated segments sent together. Each
// case runs against PostgreSQL directly and through the proxy.
func TestExtendedProtocol(t *testing.T) {
	runProtocolCases(t, startProxy(t), []protocolCase{{
		name: "binary values",
		steps: []step{ready(
			&pgproto3.Parse{
				Name:          "binary",
				Query:         "SELECT $1::int4 AS n, $2::bytea AS payload, $3::bytea AS null_value, $4::bytea AS empty_value",
				ParameterOIDs: []uint32{23, 17, 17, 17},
			},
			&pgproto3.Describe{ObjectType: 'S', Name: "binary"},
			&pgproto3.Bind{
				PreparedStatement:    "binary",
				DestinationPortal:    "p",
				ParameterFormatCodes: []int16{1},
				Parameters:           [][]byte{{0, 0, 0, 42}, {0x00, 0xff, 0x80, 0x01}, nil, {}},
				ResultFormatCodes:    []int16{1},
			},
			&pgproto3.Describe{ObjectType: 'P', Name: "p"},
			&pgproto3.Execute{Portal: "p"},
			&pgproto3.Sync{},
		)},
		check: func(t *testing.T, replies [][]pgproto3.BackendMessage) {
			require.Equal(t, []string{
				"ParseComplete", "ParameterDescription", "RowDescription", "BindComplete", "RowDescription",
				"DataRow", "CommandComplete SELECT 1", "ReadyForQuery I",
			}, summarizeTypes(replies[0]))
			require.Equal(t, [][]byte{{0, 0, 0, 42}, {0x00, 0xff, 0x80, 0x01}, nil, {}}, replies[0][5].(*pgproto3.DataRow).Values)
		},
	}, {
		// The portal survives Sync inside a transaction and resumes where it stopped.
		name: "suspended portal",
		steps: []step{
			ready(&pgproto3.Query{String: "BEGIN"}),
			quiet(
				&pgproto3.Parse{Name: "s", Query: "SELECT n FROM generate_series(1, 3) AS n ORDER BY n"},
				&pgproto3.Bind{PreparedStatement: "s", DestinationPortal: "p"},
				&pgproto3.Describe{ObjectType: 'P', Name: "p"},
				&pgproto3.Execute{Portal: "p", MaxRows: 1},
				&pgproto3.Flush{},
			),
			ready(&pgproto3.Sync{}),
			ready(
				&pgproto3.Execute{Portal: "p"},
				&pgproto3.Close{ObjectType: 'P', Name: "p"},
				&pgproto3.Close{ObjectType: 'S', Name: "s"},
				&pgproto3.Sync{},
			),
			ready(&pgproto3.Query{String: "COMMIT"}),
		},
		check: func(t *testing.T, replies [][]pgproto3.BackendMessage) {
			requireReplies(t, replies,
				[]string{"CommandComplete BEGIN", "ReadyForQuery T"},
				[]string{"ParseComplete", "BindComplete", "RowDescription", "DataRow 1", "PortalSuspended"},
				[]string{"ReadyForQuery T"},
				[]string{"DataRow 2", "DataRow 3", "CommandComplete SELECT 2", "CloseComplete", "CloseComplete", "ReadyForQuery T"},
				[]string{"CommandComplete COMMIT", "ReadyForQuery I"},
			)
		},
	}, {
		name: "flush before sync",
		steps: []step{
			quiet(&pgproto3.Parse{Name: "s", Query: "SELECT 10 AS value"}, &pgproto3.Flush{}),
			quiet(
				&pgproto3.Bind{PreparedStatement: "s", DestinationPortal: "p"},
				&pgproto3.Describe{ObjectType: 'P', Name: "p"},
				&pgproto3.Execute{Portal: "p"},
				&pgproto3.Flush{},
			),
			ready(&pgproto3.Sync{}),
			ready(&pgproto3.Close{ObjectType: 'S', Name: "s"}, &pgproto3.Sync{}),
		},
		check: func(t *testing.T, replies [][]pgproto3.BackendMessage) {
			requireReplies(t, replies,
				[]string{"ParseComplete"},
				[]string{"BindComplete", "RowDescription", "DataRow 10", "CommandComplete SELECT 1"},
				[]string{"ReadyForQuery I"},
				[]string{"CloseComplete", "ReadyForQuery I"},
			)
		},
	}, {
		name: "segments sent together",
		steps: []step{
			ready(append(append(namedSelect("1"), namedSelect("2")...), namedSelect("3")...)...),
			ready(),
			ready(),
		},
		check: func(t *testing.T, replies [][]pgproto3.BackendMessage) {
			segment := func(value string) []string {
				return []string{"ParseComplete", "BindComplete", "RowDescription", "DataRow " + value, "CommandComplete SELECT 1", "ReadyForQuery I"}
			}
			requireReplies(t, replies, segment("1"), segment("2"), segment("3"))
		},
	}})
}

// TestExtendedErrors verifies that an error in Bind, Execute or a portal
// lookup ends the batch: PostgreSQL skips every message up to Sync and rolls
// back the implicit transaction, and the session stays usable. Each case runs
// against PostgreSQL directly and through the proxy.
func TestExtendedErrors(t *testing.T) {
	setup := []string{
		"DROP TABLE IF EXISTS extended_events",
		"CREATE TABLE extended_events (id integer PRIMARY KEY)",
		"DROP TABLE IF EXISTS extended_denominator",
		"CREATE TABLE extended_denominator (id integer)",
		"INSERT INTO extended_denominator VALUES (0)",
	}
	insert := func(name, id string) []pgproto3.FrontendMessage {
		return []pgproto3.FrontendMessage{
			&pgproto3.Parse{Name: name, Query: "INSERT INTO extended_events VALUES (" + id + ")"},
			&pgproto3.Bind{PreparedStatement: name}, &pgproto3.Execute{},
		}
	}
	insertThree := ready(&pgproto3.Query{String: "INSERT INTO extended_events VALUES (3)"})
	selectIDs := ready(&pgproto3.Query{String: "SELECT id FROM extended_events ORDER BY id"})
	onlyThree := []string{"RowDescription", "DataRow 3", "CommandComplete SELECT 1", "ReadyForQuery I"}

	runProtocolCases(t, startProxy(t), []protocolCase{{
		// The bind error also rolls back the insert that completed before it.
		name:  "bind error",
		setup: setup,
		steps: []step{
			ready(append(append(append(insert("write", "1"),
				&pgproto3.Parse{Name: "typed", Query: "SELECT $1::int4", ParameterOIDs: []uint32{23}},
				&pgproto3.Bind{PreparedStatement: "typed", Parameters: [][]byte{[]byte("wrong")}},
				&pgproto3.Execute{}),
				insert("ignored", "2")...), &pgproto3.Sync{})...),
			insertThree,
			selectIDs,
		},
		check: func(t *testing.T, replies [][]pgproto3.BackendMessage) {
			requireReplies(t, replies,
				[]string{"ParseComplete", "BindComplete", "CommandComplete INSERT 0 1", "ParseComplete", "ErrorResponse 22P02", "ReadyForQuery I"},
				[]string{"CommandComplete INSERT 0 1", "ReadyForQuery I"},
				onlyThree,
			)
		},
	}, {
		name:  "execute error in a transaction",
		setup: setup,
		steps: []step{
			ready(&pgproto3.Query{String: "BEGIN"}),
			ready(append(append([]pgproto3.FrontendMessage{
				&pgproto3.Parse{Name: "bad", Query: "SELECT 1 / id FROM extended_denominator"},
				&pgproto3.Bind{PreparedStatement: "bad"}, &pgproto3.Execute{},
			}, insert("ignored", "2")...), &pgproto3.Sync{})...),
			ready(&pgproto3.Query{String: "SELECT 1"}),
			ready(&pgproto3.Query{String: "ROLLBACK"}),
			insertThree,
			selectIDs,
		},
		check: func(t *testing.T, replies [][]pgproto3.BackendMessage) {
			requireReplies(t, replies,
				[]string{"CommandComplete BEGIN", "ReadyForQuery T"},
				[]string{"ParseComplete", "BindComplete", "ErrorResponse 22012", "ReadyForQuery E"},
				[]string{"ErrorResponse 25P02", "ReadyForQuery E"},
				[]string{"CommandComplete ROLLBACK", "ReadyForQuery I"},
				[]string{"CommandComplete INSERT 0 1", "ReadyForQuery I"},
				onlyThree,
			)
		},
	}, {
		name:  "portal errors",
		setup: setup,
		steps: []step{
			ready(append(append([]pgproto3.FrontendMessage{&pgproto3.Execute{Portal: "missing"}},
				insert("ignored", "2")...), &pgproto3.Sync{})...),
			ready(&pgproto3.Query{String: "BEGIN"}),
			ready(
				&pgproto3.Parse{Name: "s", Query: "SELECT 1"},
				&pgproto3.Bind{PreparedStatement: "s", DestinationPortal: "p"},
				&pgproto3.Bind{PreparedStatement: "s", DestinationPortal: "p"},
				&pgproto3.Execute{Portal: "p"},
				&pgproto3.Sync{},
			),
			ready(&pgproto3.Query{String: "ROLLBACK"}),
			selectIDs,
		},
		check: func(t *testing.T, replies [][]pgproto3.BackendMessage) {
			requireReplies(t, replies,
				[]string{"ErrorResponse 34000", "ReadyForQuery I"},
				[]string{"CommandComplete BEGIN", "ReadyForQuery T"},
				[]string{"ParseComplete", "BindComplete", "ErrorResponse 42P03", "ReadyForQuery E"},
				[]string{"CommandComplete ROLLBACK", "ReadyForQuery I"},
				[]string{"RowDescription", "CommandComplete SELECT 0", "ReadyForQuery I"},
			)
		},
	}})
}

// namedSelect returns a Sync-terminated segment that selects value through
// statement and portal names derived from it.
func namedSelect(value string) []pgproto3.FrontendMessage {
	return []pgproto3.FrontendMessage{
		&pgproto3.Parse{Name: "s" + value, Query: "SELECT " + value + " AS value"},
		&pgproto3.Bind{PreparedStatement: "s" + value, DestinationPortal: "p" + value},
		&pgproto3.Describe{ObjectType: 'P', Name: "p" + value},
		&pgproto3.Execute{Portal: "p" + value},
		&pgproto3.Sync{},
	}
}

// summarizeTypes is summarize without DataRow values, for binary results.
func summarizeTypes(reply []pgproto3.BackendMessage) []string {
	lines := summarize(reply)
	for i, msg := range reply {
		if _, ok := msg.(*pgproto3.DataRow); ok {
			lines[i] = "DataRow"
		}
	}
	return lines
}
