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
	// maxDescribedStmts bounds the parameter types a session remembers. When it
	// is full the cache starts over; a forgotten statement only renders as
	// unknown types until it is described again.
	maxDescribedStmts = 512
	// maxPendingDescribes bounds the Describe messages awaiting a reply in one
	// cycle; replies beyond it are not recorded.
	maxPendingDescribes = 64
)

// bindParamTypes remembers parameter types so that traced Bind values can be
// decoded. Clients such as pgx send Parse without types, Describe the statement
// in one cycle, and Bind it in binary in a later one, so the backend's
// ParameterDescription must outlive the trace that carried it.
//
// Statements are keyed by proxy name, a hash of the SQL and declared types, so
// a key always denotes the same statement. A session's unnamed statement, when
// it is not auto-cached, has the empty key and is forgotten when replaced.
type bindParamTypes struct {
	described map[string][]uint32
	// describing holds the keys of this cycle's statement Describes, in send
	// order; the first answered of them have had their ParameterDescription.
	describing []string
	answered   int
	unnamed    []uint32 // declared types of the last unnamed Parse
}

// observesClientMessage reports whether observeClientMessage needs msg. It is
// small enough to inline, so unsampled Binds and the Execute, Sync and Flush
// messages around them, the bulk of extended-protocol traffic, skip the call.
func observesClientMessage(msg pgproto3.FrontendMessage, sampled bool) bool {
	switch msg.(type) {
	case *pgproto3.Parse, *pgproto3.Query, *pgproto3.Describe:
		return true
	case *pgproto3.Bind:
		return sampled
	}
	return false
}

// observeClientMessage records type information from msg and copies a sampled
// trace's Bind values. It runs before the actions layer renames statements, so
// statement names are the client's.
func (s *Session) observeClientMessage(msg pgproto3.FrontendMessage, sampled bool) {
	t := &s.paramTypes
	switch m := msg.(type) {
	case *pgproto3.Parse:
		if !s.stmtHasProxyName(m.Name) {
			t.unnamed = m.ParameterOIDs // pgproto3 allocates it per message
			delete(t.described, "")
		}
	case *pgproto3.Query:
		// A simple query destroys the unnamed statement.
		if !s.autoPreparedStatementsEnabled() {
			t.unnamed = nil
			delete(t.described, "")
		}
	case *pgproto3.Describe:
		if m.ObjectType != 'S' || len(t.describing) >= maxPendingDescribes {
			return
		}
		if key, _, ok := s.bindStmt(m.Name); ok {
			t.describing = append(t.describing, key)
		}
	case *pgproto3.Bind:
		if !sampled {
			return
		}
		key, parseOIDs, _ := s.bindStmt(m.PreparedStatement)
		s.obsVals.traceState.RecordBindParameters(m, parseOIDs, t.described[key])
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
		key := t.describing[t.answered]
		t.answered++
		// Transaction-pooled clients describe the same statements over and
		// over; keep the copy they already have.
		if known, ok := t.described[key]; ok && slices.Equal(known, m.ParameterOIDs) {
			return
		}
		if t.described == nil || len(t.described) >= maxDescribedStmts {
			t.described = make(map[string][]uint32)
		}
		// pgproto3 reuses the message's slice, so keep a copy.
		t.described[key] = slices.Clone(m.ParameterOIDs)
	case *pgproto3.ReadyForQuery:
		// Describes that failed get no reply; forget them at the end of the cycle.
		t.describing = t.describing[:0]
		t.answered = 0
	}
}

// bindStmt returns the cache key and declared parameter types of the statement
// a client Bind or Describe names.
func (s *Session) bindStmt(clientName string) (key string, parseOIDs []uint32, ok bool) {
	if !s.stmtHasProxyName(clientName) {
		return "", s.paramTypes.unnamed, true
	}
	stmt := s.pStmtState.FindPendingNamedStmtFromClientName(clientName)
	if stmt == nil {
		stmt = s.pStmtState.FindConfirmedNamedStmtFromClientName(clientName)
	}
	if stmt == nil {
		return "", nil, false
	}
	return stmt.GetProxyName(), stmt.GetParamOIDs(), true
}

func (s *Session) stmtHasProxyName(clientName string) bool {
	return prepared.IsNamedPreparedStmt(clientName) || s.autoPreparedStatementsEnabled()
}
