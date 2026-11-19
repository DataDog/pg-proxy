// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package session

import (
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/DataDog/pg-proxy/pkg/actions"
	"github.com/DataDog/pg-proxy/pkg/bindparams"
)

const (
	// maxDescribedStmts bounds the named statements whose described types a
	// session remembers. Each keeps at most bindparams.MaxParams types, so the
	// cache holds at most 32 KiB of them, and a few KB for typical statements.
	// When it is full an arbitrary entry is evicted; that statement's binary
	// values render as hex until it is described again.
	maxDescribedStmts = 256
	// maxPendingDescribes bounds the Describe messages awaiting a reply in one
	// cycle; replies beyond it are not recorded.
	maxPendingDescribes = 64
)

// bindParamTypes remembers the parameter types the backend describes, so that
// traced binary Bind values can be decoded. Clients such as pgx send Parse without
// types, Describe the statement in one cycle, and Bind it in binary in a later
// one, so the backend's ParameterDescription must outlive the trace that
// carried it.
type bindParamTypes struct {
	// described holds named and auto-cached statements' types by proxy name, a
	// hash of the SQL and declared types, so a key always denotes the same
	// statement.
	described map[string][]uint32
	// unnamed holds the unnamed statement's types. invalidateUnnamedStmt
	// clears them when an unnamed Parse, Close or simple Query replaces or
	// destroys the statement.
	unnamed []uint32
	// describing holds this cycle's statement Describes, in send order; the
	// first answered of them have had their ParameterDescription.
	describing []describedStmt
	answered   int
}

// describedStmt names where a statement's described types are kept: the
// unnamed slot of generation gen, or the map entry for key.
type describedStmt struct {
	key     string
	unnamed bool
	gen     uint64
}

// observesClientMessage reports whether observeClientMessage needs msg. It is
// small enough to inline, so the Parse, Execute and Sync messages around each
// Bind, the bulk of extended-protocol traffic, skip the call.
func observesClientMessage(msg pgproto3.FrontendMessage) bool {
	switch msg.(type) {
	case *pgproto3.Describe, *pgproto3.Bind:
		return true
	}
	return false
}

// observeClientMessage queues statement Describes for their replies and copies
// the values of a sampled trace's first Bind. It runs before the actions layer
// renames statements, so statement names are the client's; state is the one
// the actions layer decides with.
func (s *Session) observeClientMessage(msg pgproto3.FrontendMessage, state actions.State) {
	switch m := msg.(type) {
	case *pgproto3.Describe:
		if m.ObjectType != 'S' {
			return
		}
		// Sessions that never describe a statement carry no type state.
		if s.paramTypes == nil {
			s.paramTypes = &bindParamTypes{}
		}
		t := s.paramTypes
		if len(t.describing) >= maxPendingDescribes {
			return
		}
		if stmt, _, ok := s.findStmt(m.Name, state); ok {
			t.describing = append(t.describing, stmt)
		}
	case *pgproto3.Bind:
		traceState := s.obsVals.traceState
		if !traceState.WantsBindParameters() {
			return
		}
		// Only binary values are decoded by type, so a Bind of text values, the
		// common case, skips the statement lookup. An unknown statement is the
		// zero value, which has no types.
		var parseOIDs, describedOIDs []uint32
		if bindparams.NeedsTypes(m) {
			stmt, oids, _ := s.findStmt(m.PreparedStatement, state)
			parseOIDs, describedOIDs = oids, s.describedOIDs(stmt)
		}
		traceState.RecordBindParameters(m, parseOIDs, describedOIDs)
	}
}

// observeBackendMessage pairs ParameterDescription replies with the Describe
// messages that asked for them. Callers skip it while none is queued.
func (s *Session) observeBackendMessage(msg pgproto3.BackendMessage) {
	t := s.paramTypes
	switch m := msg.(type) {
	case *pgproto3.ParameterDescription:
		if t.answered < len(t.describing) {
			t.answered++
			s.remember(t.describing[t.answered-1], m.ParameterOIDs)
		}
	case *pgproto3.ReadyForQuery:
		// Describes that failed get no reply; forget them at the end of the cycle.
		t.describing = t.describing[:0]
		t.answered = 0
	}
}

// describedOIDs returns the described types of a statement findStmt just
// returned, so an unnamed one is the current statement.
func (s *Session) describedOIDs(stmt describedStmt) []uint32 {
	t := s.paramTypes
	if t == nil {
		return nil
	}
	if stmt.unnamed {
		return t.unnamed
	}
	return t.described[stmt.key]
}

// remember records the types a ParameterDescription reports for a statement.
// pgproto3 reuses the message's slice, so the types are copied, into the slice
// already held if there is one: readers copy them out. Capture reads only the
// first bindparams.MaxParams types, so only those are kept.
func (s *Session) remember(stmt describedStmt, oids []uint32) {
	t := s.paramTypes
	oids = oids[:min(len(oids), bindparams.MaxParams)]
	if stmt.unnamed {
		// A reply can describe an unnamed statement that a later Parse has
		// already replaced; it must not be taken for the new one.
		if stmt.gen == s.unnamedGen {
			t.unnamed = append(t.unnamed[:0], oids...)
		}
		return
	}
	known, ok := t.described[stmt.key]
	if t.described == nil {
		t.described = make(map[string][]uint32)
	}
	if !ok && len(t.described) >= maxDescribedStmts {
		for key := range t.described { // map order is random, so this evicts an arbitrary entry
			delete(t.described, key)
			break
		}
	}
	t.described[stmt.key] = append(known[:0], oids...)
}

// findStmt returns where the described types of the statement a client Bind or
// Describe names are kept, and its declared types, resolving names as the
// actions layer does with state. The unnamed statement's latest Parse is
// pending until Postgres confirms it, then s.unnamedStmt.
func (s *Session) findStmt(clientName string, state actions.State) (describedStmt, []uint32, bool) {
	if state.UsesUnnamedStmt(clientName) {
		stmt := state.PreparedStatements.FindPendingNamedStmtFromClientName(clientName)
		if stmt == nil {
			stmt = s.unnamedStmt
		}
		var parseOIDs []uint32
		if stmt != nil {
			parseOIDs = stmt.GetParamOIDs()
		}
		return describedStmt{unnamed: true, gen: s.unnamedGen}, parseOIDs, true
	}
	stmt, _ := actions.LookupPreparedStatement(clientName, state.PreparedStatements)
	if stmt == nil {
		return describedStmt{}, nil, false
	}
	return describedStmt{key: stmt.GetProxyName()}, stmt.GetParamOIDs(), true
}
