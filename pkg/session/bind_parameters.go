// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package session

import (
	"slices"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/DataDog/pg-proxy/pkg/bindparams"
	"github.com/DataDog/pg-proxy/pkg/prepared"
)

const (
	// maxDescribedStmts bounds the named statements whose described types a
	// session remembers. Each keeps at most bindparams.MaxParams types, so the
	// cache holds at most 256 KiB of them, and a few KB for typical statements.
	// When it is full an arbitrary entry is evicted; that statement renders as
	// unknown types until it is described again.
	maxDescribedStmts = 256
	// maxPendingDescribes bounds the Describe messages awaiting a reply in one
	// cycle; replies beyond it are not recorded.
	maxPendingDescribes = 64
)

// bindParamTypes remembers the parameter types the backend describes, so that
// traced Bind values can be decoded. Clients such as pgx send Parse without
// types, Describe the statement in one cycle, and Bind it in binary in a later
// one, so the backend's ParameterDescription must outlive the trace that
// carried it.
type bindParamTypes struct {
	// described holds named and auto-cached statements' types by proxy name, a
	// hash of the SQL and declared types, so a key always denotes the same
	// statement.
	described map[string][]uint32
	// unnamed holds the unnamed statement's types while the session's
	// unnamedGen, which every unnamed Parse, Close or simple Query advances,
	// still equals unnamedGen.
	unnamed    []uint32
	unnamedGen uint64
	// describing holds this cycle's statement Describes, in send order; the
	// first answered of them have had their ParameterDescription.
	describing []describedStmt
	answered   int
	// recorded is the statement of the Bind the trace captured, or the zero
	// value, which no Describe has, if it is unknown. A client can Parse without
	// types, Describe and Bind in one flush, so a ParameterDescription for it in
	// the same cycle fills in the types the capture lacked.
	recorded describedStmt
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
// renames statements, so statement names are the client's.
func (s *Session) observeClientMessage(msg pgproto3.FrontendMessage) {
	t := &s.paramTypes
	switch m := msg.(type) {
	case *pgproto3.Describe:
		if m.ObjectType != 'S' || len(t.describing) >= maxPendingDescribes {
			return
		}
		if stmt, _, ok := s.findStmt(m.Name); ok {
			t.describing = append(t.describing, stmt)
		}
	case *pgproto3.Bind:
		traceState := s.obsVals.traceState
		if !traceState.WantsBindParameters() {
			return
		}
		stmt, parseOIDs, ok := s.findStmt(m.PreparedStatement)
		var describedOIDs []uint32
		if ok {
			describedOIDs = s.describedOIDs(stmt)
		}
		traceState.RecordBindParameters(m, parseOIDs, describedOIDs)
		t.recorded = stmt // the zero value when !ok
	}
}

// observeBackendMessage pairs ParameterDescription replies with the Describe
// messages that asked for them. Callers skip it while none is queued.
func (s *Session) observeBackendMessage(msg pgproto3.BackendMessage) {
	t := &s.paramTypes
	switch m := msg.(type) {
	case *pgproto3.ParameterDescription:
		if t.answered < len(t.describing) {
			t.answered++
			stmt := t.describing[t.answered-1]
			s.remember(stmt, m.ParameterOIDs)
			if stmt == t.recorded {
				s.obsVals.traceState.FillBindParameterTypes(m.ParameterOIDs)
			}
		}
	case *pgproto3.ReadyForQuery:
		// Describes that failed get no reply; forget them at the end of the cycle.
		t.describing = t.describing[:0]
		t.answered = 0
		t.recorded = describedStmt{}
	}
}

func (s *Session) describedOIDs(stmt describedStmt) []uint32 {
	t := &s.paramTypes
	if stmt.unnamed {
		if stmt.gen != t.unnamedGen {
			return nil
		}
		return t.unnamed
	}
	return t.described[stmt.key]
}

// remember records the types a ParameterDescription reports for a statement.
// Clients describe the same statements over and over, so an unchanged
// description keeps the copy already held. pgproto3 reuses the message's
// slice, so a new description is copied. Capture reads only the first
// bindparams.MaxParams types, so only those are kept.
func (s *Session) remember(stmt describedStmt, oids []uint32) {
	t := &s.paramTypes
	oids = oids[:min(len(oids), bindparams.MaxParams)]
	if stmt.unnamed {
		// A reply can describe an unnamed statement that a later Parse has
		// already replaced; it must not be taken for the new one.
		if stmt.gen == s.unnamedGen && (t.unnamedGen != stmt.gen || !slices.Equal(t.unnamed, oids)) {
			t.unnamed, t.unnamedGen = slices.Clone(oids), stmt.gen
		}
		return
	}
	known, ok := t.described[stmt.key]
	if ok && slices.Equal(known, oids) {
		return
	}
	if t.described == nil {
		t.described = make(map[string][]uint32)
	}
	if !ok && len(t.described) >= maxDescribedStmts {
		for key := range t.described { // map order is random, so this evicts an arbitrary entry
			delete(t.described, key)
			break
		}
	}
	t.described[stmt.key] = slices.Clone(oids)
}

// findStmt returns where the described types of the statement a client Bind or
// Describe names are kept, and its declared types. The unnamed statement's
// latest Parse is pending until Postgres confirms it, then s.unnamedStmt.
func (s *Session) findStmt(clientName string) (describedStmt, []uint32, bool) {
	hasProxyName := prepared.IsNamedPreparedStmt(clientName) || s.autoPreparedStatementsEnabled()
	stmt := s.pStmtState.FindPendingNamedStmtFromClientName(clientName)
	if stmt == nil && hasProxyName {
		stmt = s.pStmtState.FindConfirmedNamedStmtFromClientName(clientName)
	}
	if !hasProxyName {
		if stmt == nil {
			stmt = s.unnamedStmt
		}
		var parseOIDs []uint32
		if stmt != nil {
			parseOIDs = stmt.GetParamOIDs()
		}
		return describedStmt{unnamed: true, gen: s.unnamedGen}, parseOIDs, true
	}
	if stmt == nil {
		return describedStmt{}, nil, false
	}
	return describedStmt{key: stmt.GetProxyName()}, stmt.GetParamOIDs(), true
}
