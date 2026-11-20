// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package session

import (
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

// Socket writes, backend round trips, and the release reset are the largest
// per-statement costs of forwarding. Unlike timings, their counts per cycle are
// deterministic, so these tests pin them. A change that removes a write, such
// as skipping the release reset or sending it with the cycle's flush, should
// lower the expected counts here in the same change.

func TestProtocolCycleRoundTripBudgets(t *testing.T) {
	for _, withRole := range []bool{false, true} {
		for _, pc := range protocolCycles {
			t.Run(fmt.Sprintf("%s/role=%t", pc.name, withRole), func(t *testing.T) {
				h := newCycleHarness(t, withRole)
				client, script := pc.prepare(t, h)
				h.resetCounts(client)
				require.NoError(t, client.run(script))
				// One flush to Postgres, one write to the client, and one checkout
				// whose release runs one reset query, whatever the cycle's length.
				want := cycleCounts{
					backendWrites: 2, backendFlushes: 1, backendStatements: pc.statements, releaseResets: 1,
					clientWrites: 1, checkouts: 1,
				}
				if withRole {
					want.roleSets, want.roleResets = 1, 1
				}
				require.Equal(t, want, h.counts(client))
			})
		}
	}
}

// A client's first Parse of a named statement that its backend connection
// already has, for example from another session, is not forwarded. The session
// answers ParseComplete itself, but only after it sends Postgres a Flush and
// reads the replies to the messages before the Parse, in case one failed. Here
// the Parse starts the cycle, so the Flush carries nothing and gets no reply.
// The cycle costs one more backend write and one more client write than a
// forwarded Parse. Lower these counts if the session skips the Flush when no
// message precedes the Parse, or adds ParseComplete to the cycle's client write.
func TestNamedParseOfStatementOnConnectionRoundTrips(t *testing.T) {
	h := newCycleHarness(t, false)
	require.NoError(t, h.newClient().run(preparedStatementScript(t, h)))

	client := h.newClient()
	script := h.script(t,
		[]pgproto3.FrontendMessage{
			&pgproto3.Parse{Name: cycleStmtName, Query: cycleExtendedSQL, ParameterOIDs: cycleParamOIDs},
			cycleBind(cycleStmtName), &pgproto3.Describe{ObjectType: 'P'}, &pgproto3.Execute{}, &pgproto3.Sync{},
		},
		append(append([]pgproto3.BackendMessage{&pgproto3.BindComplete{}}, cycleRowReplies()...), cycleReadyForQuery()),
	)
	h.resetCounts(client)
	require.NoError(t, client.run(script))
	require.Equal(t, cycleCounts{
		backendWrites: 3, backendFlushes: 2, backendStatements: 1, releaseResets: 1, clientWrites: 2, checkouts: 1,
	}, h.counts(client))
}
