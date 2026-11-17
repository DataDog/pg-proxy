// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package setdetect

// Pre-optimization versions of the detectors, kept to benchmark before
// against after in one run and to fuzz the optimized versions for equal
// verdicts. Each is a copy of the code as first written, renamed. Rule fixes
// made later (SET local.x) are applied to both, so only speed differs.

import (
	"hash/maphash"
	"math/bits"
	"strings"
	"sync"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/proto"
)

func lexDirtyV0(sql string, scsOn bool) bool {
	n := len(sql)
	state := stStart
	for i := 0; i < n; {
		c := sql[i]
		switch lexClass[c] {
		case clsSpace:
			i++
		case clsSemi:
			state = stStart
			i++
		case clsDash:
			if i+1 < n && sql[i+1] == '-' {
				i += 2
				for i < n && sql[i] != '\n' && sql[i] != '\r' {
					i++
				}
				continue
			}
			state = leaveStart(state)
			i++
		case clsSlash:
			if i+1 < n && sql[i+1] == '*' {
				j, ok := skipBlockComment(sql, i)
				if !ok {
					return true
				}
				i = j
				continue
			}
			state = leaveStart(state)
			i++
		case clsQuote:
			j, ok := skipString(sql, i, !scsOn)
			if !ok {
				return true
			}
			i = j
			if afterSet(state) {
				return true
			}
			state = stBody
		case clsDQuote:
			j, ok := skipQuotedIdent(sql, i)
			if !ok {
				return true
			}
			if isSetterName(sql[i+1 : j-1]) {
				return true
			}
			if afterSet(state) {
				// SET "search_path" = ...
				return true
			}
			state = stBody
			i = j
		case clsDollar:
			j, ok, isQuote := skipDollar(sql, i)
			if !ok {
				return true
			}
			if isQuote && afterSet(state) {
				return true
			}
			if isQuote {
				state = stBody
			}
			i = j
		case clsIdentStart:
			j := i + 1
			for j < n && isIdentByte(sql[j]) {
				j++
			}
			word := sql[i:j]
			if j < n {
				switch {
				case len(word) == 1 && (c == 'E' || c == 'e') && sql[j] == '\'':
					k, ok := skipString(sql, j, true)
					if !ok {
						return true
					}
					i = k
					if afterSet(state) {
						return true
					}
					state = stBody
					continue
				case len(word) == 1 && (c == 'U' || c == 'u') && sql[j] == '&' && j+1 < n:
					switch sql[j+1] {
					case '"':
						// A U&"..." identifier can spell any name.
						return true
					case '\'':
						k, ok := skipString(sql, j+1, false)
						if !ok {
							return true
						}
						i = k
						if afterSet(state) {
							return true
						}
						state = stBody
						continue
					}
				}
			}
			switch state {
			case stStart:
				switch {
				case eqFold(word, "set"):
					state = stAfterSet
				case eqFold(word, "do"), eqFold(word, "call"), eqFold(word, "execute"):
					return true
				default:
					state = stBody
				}
			case stAfterSet:
				switch {
				case eqFold(word, "local") || eqFold(word, "transaction") || eqFold(word, "constraints"):
					state = stAfterScope
				case eqFold(word, "session"):
					state = stAfterSession
				default:
					return true
				}
			case stAfterSession:
				// SET SESSION TRANSACTION is SET TRANSACTION; anything else
				// (a setting, CHARACTERISTICS, AUTHORIZATION) persists.
				if !eqFold(word, "transaction") {
					return true
				}
				state = stAfterScope
			case stAfterScope:
				if eqFold(word, "to") {
					return true
				}
				state = stBody
			default:
				if isSetterName(word) || eqFold(word, "execute") {
					return true
				}
			}
			i = j
		case clsDigit:
			j := i + 1
			for j < n && isIdentByte(sql[j]) {
				j++
			}
			state = leaveStart(state)
			i = j
		default:
			if state == stAfterScope {
				if scopeNameDirty(c) {
					return true
				}
				state = stBody
			}
			state = leaveStart(state)
			i++
		}
	}
	// A statement that ends right after SET is a syntax error; the server
	// rejects it, but say dirty rather than reason about it.
	return state == stAfterSet || state == stAfterSession
}

func astDirtyV0(sql string) bool {
	tree, err := pg_query.Parse(sql)
	if err != nil {
		return true
	}
	for _, raw := range tree.GetStmts() {
		if astStmtDirtyV0(raw.GetStmt()) {
			return true
		}
	}
	return false
}

func astStmtDirtyV0(node *pg_query.Node) bool {
	switch n := node.GetNode().(type) {
	case *pg_query.Node_VariableSetStmt:
		v := n.VariableSetStmt
		switch v.GetKind() {
		case pg_query.VariableSetKind_VAR_SET_VALUE:
			return !v.GetIsLocal()
		case pg_query.VariableSetKind_VAR_SET_MULTI:
			return v.GetName() == "SESSION CHARACTERISTICS"
		}
		return false
	case *pg_query.Node_DoStmt, *pg_query.Node_ExecuteStmt, *pg_query.Node_CallStmt:
		return true
	}
	dirty := false
	WalkAST(node, func(m proto.Message) bool {
		switch x := m.(type) {
		case *pg_query.FuncCall:
			if n := LastName(x); n == "set_config" || n == "set_limit" {
				dirty = true
			}
		case *pg_query.UpdateStmt:
			if x.GetRelation().GetRelname() == "pg_settings" {
				dirty = true
			}
		case *pg_query.ExecuteStmt:
			dirty = true // EXPLAIN ANALYZE EXECUTE, CREATE TABLE AS EXECUTE
		}
		return !dirty
	})
	return dirty
}

func proxysqlCaughtV0(raw string) bool {
	dig := proxysqlDigest(raw)
	switch {
	case hasPrefixFold(dig, "SET "):
		if proxysqlMulti(dig) {
			return true // locked
		}
		nq := proxysqlVersionedSet.ReplaceAllString(raw, "SET$1")
		nq = proxysqlInlineComment.ReplaceAllString(nq, "")
		nq = strings.TrimRight(nq, " ;")
		if proxysqlSetRE.MatchString(dig) {
			if proxysqlTZInterval.MatchString(nq) {
				return true
			}
			vars, ok := proxysqlParseSet(nq)
			if !ok || len(vars) == 0 {
				return false // forwarded without a lock
			}
			// Tracked, ignored-and-answered-locally, or locked: all safe.
			return true
		}
		if proxysqlIsSetTransaction(dig) && proxysqlIsSetTransaction(raw) {
			return false
		}
		return true
	case hasPrefixFold(dig, "RESET "), hasPrefixFold(dig, "DISCARD "):
		if hasPrefixFold(dig, "DISCARD ") {
			nq := proxysqlInlineComment.ReplaceAllString(raw, "")
			nq = proxysqlNonWord.ReplaceAllString(proxysqlDiscardWord.ReplaceAllString(nq, ""), "")
			if !hasPrefixFold(nq, "ALL") && !hasPrefixFold(nq, "PLANS") {
				return false
			}
		}
		// Tracked locally or locked; either way the backend is safe.
		return true
	}
	return proxysqlPinned(dig)
}

func stage1LoopV0(sql string) bool {
	n := len(sql)
	for i := 0; i < n; i++ {
		c := sql[i]
		switch {
		case c|0x20 == 'e':
			if i > 0 && sql[i-1]|0x20 == 's' && i+1 < n && sql[i+1]|0x20 == 't' {
				return true
			}
			if i+3 < n && sql[i+1]|0x20 == 'x' && sql[i+2]|0x20 == 'e' && sql[i+3]|0x20 == 'c' {
				return true
			}
		case c == '&':
			if i > 0 && sql[i-1]|0x20 == 'u' {
				return true
			}
		case c == ';':
			return true
		}
	}
	return false
}

func stage1SWARV0(sql string) bool {
	n := len(sql)
	i := 0
	for ; i+9 <= n; i += 8 {
		w := load64(sql, i)
		nx := load64(sql, i+1) // byte j+1 under byte j
		f, fn := w|0x2020202020202020, nx|0x2020202020202020
		cand := zeroBytes(f^(0x65*lo8)) & (zeroBytes(fn^(0x74*lo8)) | zeroBytes(fn^(0x78*lo8))) // e then t|x
		cand |= zeroBytes(w ^ (';' * lo8))
		cand |= zeroBytes(nx ^ ('&' * lo8))
		for cand != 0 {
			j := i + bits.TrailingZeros64(cand)/8
			if candidateAt(sql, j) {
				return true
			}
			cand &= cand - 1
		}
	}
	for ; i < n; i++ {
		if candidateAt(sql, i) {
			return true
		}
	}
	return false
}

func pgdogRegexV0(sql string) bool {
	if pgdogIsCommon(sql) {
		return false
	}
	return pgdogSetRE.MatchString(truncateUTF8(sql, pgdogRegexLimit))
}

func pgdogGateV0(sql string) bool {
	return !pgdogIsCommon(sql) && pgdogCmdRE.MatchString(truncateUTF8(sql, pgdogRegexLimit))
}

var _ = proto.Message(nil)
var _ = pg_query.Parse

func wordBoundaryV0(sql string) Verdict {
	n := len(sql)
	for i := 0; i+2 < n; i++ {
		if sql[i]|0x20 != 's' || sql[i+1]|0x20 != 'e' || sql[i+2]|0x20 != 't' {
			continue
		}
		if i > 0 && isIdentByte(sql[i-1]) {
			continue
		}
		if i+3 < n && isIdentByte(sql[i+3]) {
			continue
		}
		return Dirty
	}
	return Clean
}

func prefilterHitV0(sql string) bool {
	n := len(sql)
	for i := 0; i+1 < n; i++ {
		switch sql[i] | 0x20 {
		case 's':
			if i+2 < n && sql[i+1]|0x20 == 'e' && sql[i+2]|0x20 == 't' {
				return true
			}
		case 'u':
			if sql[i+1] == '&' {
				return true
			}
		case 'e':
			if i+3 < n && sql[i+1]|0x20 == 'x' && sql[i+2]|0x20 == 'e' && sql[i+3]|0x20 == 'c' {
				return true
			}
		case 'c':
			if i+3 < n && sql[i+1]|0x20 == 'a' && sql[i+2]|0x20 == 'l' && sql[i+3]|0x20 == 'l' {
				return true
			}
		case 'd':
			if sql[i+1]|0x20 == 'o' {
				return true
			}
		}
	}
	return false
}

// cachedV0 is the mutex-guarded cache as first written.
type cachedV0 struct {
	Inner Detector
	seed  maphash.Seed
	mu    sync.Mutex
	slots []cacheSlot
}

func (c *cachedV0) Classify(sql string, scsOn bool) Verdict {
	h := maphash.String(c.seed, sql)
	if !scsOn {
		h = ^h
	}
	s := &c.slots[h%uint64(len(c.slots))]
	c.mu.Lock()
	if s.full && s.key == h {
		v := s.verdict
		c.mu.Unlock()
		return v
	}
	c.mu.Unlock()
	v := c.Inner.Classify(sql, scsOn)
	c.mu.Lock()
	*s = cacheSlot{key: h, verdict: v, full: true}
	c.mu.Unlock()
	return v
}
