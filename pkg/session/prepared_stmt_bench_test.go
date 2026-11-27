// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package session

import (
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

// BenchmarkNamedParseOfStatementOnConnection measures a client's Parse of a named
// statement its backend connection already holds, which the session answers
// itself, then a Close of it, so each cycle can parse the same name again.
// BenchmarkProtocolCycle covers a Bind of a held statement (named_statement_reuse)
// and the unnamed pipeline (one_shot_extended).
func BenchmarkNamedParseOfStatementOnConnection(b *testing.B) {
	h := newCycleHarness(b, false)
	require.NoError(b, h.newClient().run(namedStatementScript(b, h, true, true))) // the connection now holds it
	client := h.newClient()
	script := h.script(b,
		[]pgproto3.FrontendMessage{
			&pgproto3.Parse{Name: cycleStmtName, Query: cycleExtendedSQL, ParameterOIDs: cycleParamOIDs},
			&pgproto3.Close{ObjectType: 'S', Name: cycleStmtName},
			&pgproto3.Sync{},
		},
		[]pgproto3.BackendMessage{cycleReadyForQuery()})
	b.ReportAllocs()
	for b.Loop() {
		if err := client.run(script); err != nil {
			b.Fatal(err)
		}
	}
}
