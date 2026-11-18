// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package session

import (
	"slices"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/DataDog/pg-proxy/pkg/prepared"
)

const (
	// maxDescribedStmts bounds the named statements whose described types a
	// session remembers, about 20 KB at most. When it is full an arbitrary entry
	// is evicted; that statement renders as unknown types until it is described
	// again.
	maxDescribedStmts = 256
	// maxPendingDescribes bounds the Describe messages awaiting a reply in one
	// cycle; replies beyond it are not recorded.
	maxPendingDescribes = 64
)

// bindParamTypes remembers parameter types so that traced Bind values can be
// decoded. Clients such as pgx send Parse without types, Describe the statement
// in one cycle, and Bind it in binary in a later one, so the backend's
// ParameterDescription must outlive the trace that carried it.
//
// Statements with a proxy name are keyed by it: a hash of the SQL and declared
// types, so a key always denotes the same statement. The unnamed statement,
// when it is not auto-cached, is replaced by each unnamed Parse or simple
// Query; unnamedGen counts those, so that a reply describing a replaced
// statement is dropped instead of being taken for the current one.
type bindParamTypes struct {
	described map[string][]uint32
	// describing holds this cycle's statement Describes, in send order; the
	// first answered of them have had their ParameterDescription.
	describing []pendingDescribe
	answered   int

	unnamed          []uint32 // declared types of the unnamed statement
	unnamedDescribed []uint32 // its types as the backend described them
	unnamedGen       uint64
}

// pendingDescribe is a statement Describe awaiting its ParameterDescription.
// declared counts the leading parameters Parse gave a type, decided when the
// Describe is sent so the queue stays small.
type pendingDescribe struct {
	stmt     describedStmt
	declared int
}

// declaredPrefix returns how many leading parameters parseOIDs gives a type.
func declaredPrefix(parseOIDs []uint32) int {
	if i := slices.Index(parseOIDs, 0); i >= 0 {
		return i
	}
	return len(parseOIDs)
}

// describedStmt names where a statement's described types are kept: the
// unnamed slot of generation gen, or the map entry for key.
type describedStmt struct {
	key     string
	unnamed bool
	gen     uint64
}

// observesClientMessage reports whether observeClientMessage needs msg. It is
// the only gate on Bind: unsampled Binds are filtered here, not again in
// observeClientMessage. It is small enough to inline, so unsampled Binds and
// the Execute, Sync and Flush messages around them, the bulk of
// extended-protocol traffic, skip the call.
func observesClientMessage(msg pgproto3.FrontendMessage, sampled bool) bool {
	switch msg.(type) {
	case *pgproto3.Parse, *pgproto3.Query, *pgproto3.Describe:
		return true
	case *pgproto3.Bind:
		return sampled
	}
	return false
}

// observeClientMessage records type information from msg and copies a Bind's
// values; callers filter with observesClientMessage. It runs before the actions
// layer renames statements, so statement names are the client's.
func (s *Session) observeClientMessage(msg pgproto3.FrontendMessage) {
	t := &s.paramTypes
	switch m := msg.(type) {
	case *pgproto3.Parse:
		if !s.stmtHasProxyName(m.Name) {
			t.replaceUnnamed(m.ParameterOIDs) // pgproto3 allocates it per message
		}
	case *pgproto3.Query:
		// A simple query destroys the unnamed statement.
		if !s.autoPreparedStatementsEnabled() {
			t.replaceUnnamed(nil)
		}
	case *pgproto3.Describe:
		if m.ObjectType != 'S' || len(t.describing) >= maxPendingDescribes {
			return
		}
		if stmt, parseOIDs, ok := s.findStmt(m.Name); ok {
			t.describing = append(t.describing, pendingDescribe{stmt: stmt, declared: declaredPrefix(parseOIDs)})
		}
	case *pgproto3.Bind:
		stmt, parseOIDs, ok := s.findStmt(m.PreparedStatement)
		var describedOIDs []uint32
		if ok {
			describedOIDs = t.describedOIDs(stmt)
		}
		s.obsVals.traceState.RecordBindParameters(m, parseOIDs, describedOIDs)
	}
}

// observeBackendMessage pairs ParameterDescription replies with the Describe
// messages that asked for them.
func (s *Session) observeBackendMessage(msg pgproto3.BackendMessage) {
	t := &s.paramTypes
	switch m := msg.(type) {
	case *pgproto3.ParameterDescription:
		if t.answered >= len(t.describing) {
			return
		}
		pending := &t.describing[t.answered]
		t.answered++
		t.remember(pending, m.ParameterOIDs)
	case *pgproto3.ReadyForQuery:
		// Describes that failed get no reply; forget them at the end of the cycle.
		t.describing = t.describing[:0]
		t.answered = 0
	}
}

func (t *bindParamTypes) replaceUnnamed(parseOIDs []uint32) {
	t.unnamed = parseOIDs
	t.unnamedDescribed = nil
	t.unnamedGen++
}

func (t *bindParamTypes) describedOIDs(stmt describedStmt) []uint32 {
	if stmt.unnamed {
		if stmt.gen != t.unnamedGen {
			return nil
		}
		return t.unnamedDescribed
	}
	return t.described[stmt.key]
}

// remember records the types a ParameterDescription reports for a statement.
func (t *bindParamTypes) remember(pending *pendingDescribe, oids []uint32) {
	stmt := pending.stmt
	// Declared types win, so there is nothing to keep if all are declared.
	if pending.declared >= len(oids) {
		return
	}
	// Transaction-pooled clients describe the same statements over and over;
	// keep the copy already held.
	if known := t.describedOIDs(stmt); known != nil && slices.Equal(known, oids) {
		return
	}
	// pgproto3 reuses the message's slice, so keep a copy.
	oids = slices.Clone(oids)
	if stmt.unnamed {
		if stmt.gen == t.unnamedGen {
			t.unnamedDescribed = oids
		}
		return
	}
	if t.described == nil {
		t.described = make(map[string][]uint32)
	}
	if _, ok := t.described[stmt.key]; !ok && len(t.described) >= maxDescribedStmts {
		for key := range t.described { // map order is random, so this evicts an arbitrary entry
			delete(t.described, key)
			break
		}
	}
	t.described[stmt.key] = oids
}

// findStmt returns where the described types of the statement a client Bind or
// Describe names are kept, and its declared types.
func (s *Session) findStmt(clientName string) (stmt describedStmt, parseOIDs []uint32, ok bool) {
	if !s.stmtHasProxyName(clientName) {
		return describedStmt{unnamed: true, gen: s.paramTypes.unnamedGen}, s.paramTypes.unnamed, true
	}
	found := s.pStmtState.FindPendingNamedStmtFromClientName(clientName)
	if found == nil {
		found = s.pStmtState.FindConfirmedNamedStmtFromClientName(clientName)
	}
	if found == nil {
		return describedStmt{}, nil, false
	}
	return describedStmt{key: found.GetProxyName()}, found.GetParamOIDs(), true
}

func (s *Session) stmtHasProxyName(clientName string) bool {
	return prepared.IsNamedPreparedStmt(clientName) || s.autoPreparedStatementsEnabled()
}
