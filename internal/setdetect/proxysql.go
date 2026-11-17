// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package setdetect

import (
	"regexp"
	"strings"
)

// A port of ProxySQL's PostgreSQL session-state handling at v3.0 663e84a,
// under default configuration (query_digests on, set_parser_algorithm 2,
// set_query_lock_on_hostgroup 1, set_config tracking off). ProxySQL never
// resets a backend; it is safe when it either tracks a SET (answers it
// locally and replays it) or locks or pins the session to its backend. Both
// count as Dirty here. Everything it forwards unseen counts as Clean.
//
// Sources: lib/PgSQL_Session.cpp handle_special_commands and
// handle_SET_command, lib/PgSQL_Variables.cpp, lib/PgSQL_Set_Stmt_Parser.cpp
// parse1v2, lib/pgsql_tokenizer.cpp (digest), lib/PgSQL_Connection.cpp
// ProcessQueryAndSetStatusFlags.

// ProxySQL is the detector.
type ProxySQL struct{}

// Name implements Detector.
func (ProxySQL) Name() string { return "proxysql" }

// Classify implements Detector.
func (ProxySQL) Classify(sql string, _ bool) Verdict { return verdict(proxysqlCaught(sql)) }

const proxysqlTrackedVars = "client_encoding|names|DateStyle|IntervalStyle|standard_conforming_strings|TimeZone|TIME ZONE|search_path|allow_in_place_tablespaces|bytea_output|client_min_messages|enable_bitmapscan|enable_hashjoin|enable_indexscan|enable_nestloop|enable_seqscan|enable_sort|escape_string_warning|extra_float_digits|maintenance_work_mem|synchronous_commit|application_name"

var (
	// match_regexes[1], case-insensitive partial match on the digest.
	proxysqlSetRE         = regexp.MustCompile("(?i)^SET(?: +)(|SESSION +)`?(" + proxysqlTrackedVars + ")`?(?:$|[\\s=])")
	proxysqlVersionedSet  = regexp.MustCompile(`^/\*!\d\d\d\d\d SET(.*)\*/`)
	proxysqlInlineComment = regexp.MustCompile(`/\*.*?\*/`) // (?U)/\*.*\*/; . does not match \n
	proxysqlTZInterval    = regexp.MustCompile(`(?i)^\s*SET\s+(?:SESSION\s+|LOCAL\s+)?TIME\s+ZONE\s+INTERVAL\b`)
	proxysqlSetPrefix     = regexp.MustCompile(`(?i)^\s*SET\s+`)
	proxysqlTrailing      = regexp.MustCompile(`(\s|;)+$`)
	proxysqlResetWord     = regexp.MustCompile(`(?i)\bRESET\b`)
	proxysqlDiscardWord   = regexp.MustCompile(`(?i)\bDISCARD\b`)
	proxysqlNonWord       = regexp.MustCompile(`[^\w]*`)
	proxysqlIntervalValue = regexp.MustCompile(`(?i)^INTERVAL\b`)
	// parse1v2, verbatim, anchored for the Consume loop.
	proxysqlParse1v2 = regexp.MustCompile(`(?i)^\s*(?:(?:(SESSION)\s+)?((?:TIME\s+ZONE|TRANSACTION\s+ISOLATION\s+LEVEL|XML\s+OPTION|(?:(?:[^\s=]{1,4}|[^\s=]{6}|[^\s=]{8,}|(?:[^lL][^\s=]{4}|[lL][^oO][^\s=]{3}|[lL][oO][^cC][^\s=]{2}|[lL][oO][cC][^aA][^\s=]|[lL][oO][cC][aA][^lL])|(?:[^sS][^\s=]{6}|[sS][^eE][^\s=]{5}|[sS][eE][^sS][^\s=]{4}|[sS][eE][sS][^sS][^\s=]{3}|[sS][eE][sS][sS][^iI][^\s=]{2}|[sS][eE][sS][sS][iI][^oO][^\s=]|[sS][eE][sS][sS][iI][oO][^nN])))))(?:\s*(=)\s*|\s+(TO)\s+|\s+())(?:([A-Za-z_][\w$\.]*)\s*\(\s*('(?:''|[^'])*'|-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?|[^\s();]+)\s*\)|('(?:''|[^'])*'|-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?|(?:(?:[^\s= T;()][^;()]*|\s+[^=T;()][^;()]*)|=[^;()]+|TO[^;()]+|T[^O;()][^;()]*|T)))\s*;?)\s*$`)
)

var proxysqlKnownVars = map[string]bool{
	"client_encoding": true, "names": true, "datestyle": true, "intervalstyle": true, "search_path": true,
	"standard_conforming_strings": true, "timezone": true, "time zone": true,
	"allow_in_place_tablespaces": true, "bytea_output": true, "client_min_messages": true,
	"enable_bitmapscan": true, "enable_hashjoin": true, "enable_indexscan": true, "enable_nestloop": true,
	"enable_seqscan": true, "enable_sort": true, "escape_string_warning": true, "extra_float_digits": true,
	"maintenance_work_mem": true, "synchronous_commit": true,
	// ignore_vars: answered locally and not forwarded, so never leaked.
	"application_name": true,
}

func proxysqlCaught(raw string) bool {
	if !proxysqlMayMatter(raw) {
		return false
	}
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

// proxysqlMulti is is_multi_statement_command: a ; that is not the last byte.
func proxysqlMulti(dig string) bool {
	i := strings.IndexByte(dig, ';')
	return i >= 0 && i != len(dig)-1
}

// proxysqlPinned is ProcessQueryAndSetStatusFlags: prefixes that disable
// multiplexing, so the backend stays with this client.
func proxysqlPinned(dig string) bool {
	for _, p := range []string{"PREPARE ", "CREATE TEMPORARY TABLE ", "CREATE TEMP TABLE ",
		"CREATE SEQUENCE", "CREATE TEMP SEQUENCE", "CREATE TEMPORARY SEQUENCE",
		"SELECT pg_advisory_lock", "SELECT pg_try_advisory_lock"} {
		if hasPrefixFold(dig, p) {
			return true
		}
	}
	upper := strings.ToUpper(dig)
	if strings.Contains(upper, "LISTEN ") && !strings.Contains(upper, "UNLISTEN *") {
		return true
	}
	if (hasPrefixFold(dig, "DECLARE") || strings.IndexByte(dig, ';') >= 0) && strings.Contains(upper, "CURSOR WITH HOLD") {
		return true
	}
	return false
}

func hasPrefixFold(s, prefix string) bool {
	return len(s) >= len(prefix) && strings.EqualFold(s[:len(prefix)], prefix)
}

// proxysqlParseSet is parse1v2 after set_query's remove_spaces.
func proxysqlParseSet(nq string) (map[string][]string, bool) {
	q := strings.Join(strings.Fields(nq), " ")
	q = proxysqlSetPrefix.ReplaceAllString(q, "")
	q = proxysqlTrailing.ReplaceAllString(q, "")
	vars := map[string][]string{}
	for q != "" {
		m := proxysqlParse1v2.FindStringSubmatchIndex(q)
		if m == nil {
			return nil, false
		}
		group := func(i int) string {
			if m[2*i] < 0 {
				return ""
			}
			return q[m[2*i]:m[2*i+1]]
		}
		if group(6) != "" {
			return nil, true // function-call value: empty result
		}
		name := strings.ToLower(strings.Trim(group(2), "\""))
		val := strings.TrimRight(group(8), " \n\r\t,")
		if !proxysqlKnownVars[name] {
			// unknown_var: locked.
			vars[name] = []string{"<locked>"}
		} else if val == "" || proxysqlIntervalValue.MatchString(val) {
			vars[name] = []string{"<locked>"}
		} else {
			vars[name] = append(vars[name], val)
		}
		if m[1] == 0 {
			break
		}
		q = q[m[1]:]
	}
	return vars, true
}

// proxysqlIsSetTransaction is is_set_transaction_stmt.
func proxysqlIsSetTransaction(text string) bool {
	p := pgsqlSkipLead(text)
	if p < 0 || !hasPrefixFold(text[p:], "SET") {
		return false
	}
	p += 3
	if !proxysqlWordEnds(text, p) {
		return false
	}
	p = pgsqlSkipLeadAt(text, p)
	for _, scope := range []string{"LOCAL", "SESSION"} {
		if p >= 0 && hasPrefixFold(text[p:], scope) {
			p += len(scope)
			if !proxysqlWordEnds(text, p) {
				return false
			}
			p = pgsqlSkipLeadAt(text, p)
			break
		}
	}
	if p < 0 || !hasPrefixFold(text[p:], "TRANSACTION") {
		return false
	}
	p += len("TRANSACTION")
	if p < len(text) && text[p] != ';' && !proxysqlWordEnds(text, p) {
		return false
	}
	for q := strings.IndexByte(text[p:], ';'); q >= 0; {
		at := p + q + 1
		if r := pgsqlSkipLeadAt(text, at); r >= 0 && r < len(text) {
			return false
		}
		next := strings.IndexByte(text[at:], ';')
		if next < 0 {
			break
		}
		p, q = at, next
	}
	return true
}

func proxysqlWordEnds(s string, p int) bool {
	if p >= len(s) {
		return false
	}
	c := s[p]
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v' ||
		strings.HasPrefix(s[p:], "/*") || strings.HasPrefix(s[p:], "--")
}

func pgsqlSkipLead(s string) int { return pgsqlSkipLeadAt(s, 0) }

// pgsqlSkipLeadAt skips whitespace, -- comments and nested block comments.
// It returns len(s) at the end and -1 on an unterminated block comment.
func pgsqlSkipLeadAt(s string, p int) int {
	for p < len(s) {
		switch {
		case lexClass[s[p]] == clsSpace:
			p++
		case strings.HasPrefix(s[p:], "--"):
			for p < len(s) && s[p] != '\n' {
				p++
			}
		case strings.HasPrefix(s[p:], "/*"):
			j, ok := skipBlockComment(s, p)
			if !ok {
				return -1
			}
			p = j
		default:
			return p
		}
	}
	return p
}

// proxysqlDigest approximates pgsql_query_digest_and_first_comment: comments
// removed (a leading one leaves nothing), whitespace collapsed, literals and
// numbers replaced by ?, identifiers and keywords kept with their case,
// trailing spaces and semicolons stripped. Backslash escapes are honored in
// every literal, as the tokenizer does.
func proxysqlDigest(raw string) string {
	var b strings.Builder
	b.Grow(len(raw))
	space := func() {
		if b.Len() > 0 {
			s := b.String()
			if s[len(s)-1] != ' ' {
				b.WriteByte(' ')
			}
		}
	}
	n := len(raw)
	for i := 0; i < n; {
		c := raw[i]
		switch {
		case lexClass[c] == clsSpace:
			space()
			i++
		case c == '-' && i+1 < n && raw[i+1] == '-':
			for i < n && raw[i] != '\n' && raw[i] != '\r' {
				i++
			}
			space()
		case c == '/' && i+1 < n && raw[i+1] == '*':
			j, _ := skipBlockComment(raw, i)
			i = j
			space()
		case c == '\'':
			j, _ := skipString(raw, i, true)
			b.WriteByte('?')
			i = j
		case c == '$' && i+1 < n && (raw[i+1] == '$' || lexClass[raw[i+1]] == clsIdentStart):
			j, ok, isQuote := skipDollar(raw, i)
			if ok && isQuote {
				b.WriteByte('?')
			} else {
				b.WriteString(raw[i:j])
			}
			i = j
		case c == '"':
			j, _ := skipQuotedIdent(raw, i)
			b.WriteString(raw[i:j])
			i = j
		case lexClass[c] == clsDigit:
			j := i + 1
			for j < n && (lexClass[raw[j]] == clsDigit || raw[j] == '.') {
				j++
			}
			b.WriteByte('?')
			i = j
		case lexClass[c] == clsIdentStart:
			j := i + 1
			for j < n && isIdentByte(raw[j]) {
				j++
			}
			w := raw[i:j]
			if j < n && raw[j] == '\'' && len(w) == 1 && strings.ContainsAny(w, "EeBbXxNn") {
				k, _ := skipString(raw, j, true)
				b.WriteByte('?')
				i = k
				continue
			}
			if eqFold(w, "true") || eqFold(w, "false") {
				b.WriteByte('?')
			} else {
				b.WriteString(w)
			}
			i = j
		case c == ':' && i+1 < n && raw[i+1] == ':':
			// ::type casts are dropped.
			j := i + 2
			for j < n && (isIdentByte(raw[j]) || raw[j] == '.') {
				j++
			}
			i = j
		default:
			b.WriteByte(c)
			i++
		}
	}
	return strings.TrimRight(b.String(), " ;")
}

// proxysqlMayMatter is a verdict-preserving shortcut: it is false only when
// no rule can fire, so the digest need not be built. Every rule but two
// keys on the digest's first word, which is the text's first word after the
// whitespace and comments the digest drops; the two others need "listen" or
// "hold" somewhere in the text.
func proxysqlMayMatter(raw string) bool {
	if containsFold(raw, "listen") || containsFold(raw, "hold") {
		return true
	}
	p := proxysqlSkipLead(raw)
	j := p
	for j < len(raw) && identCont[raw[j]] {
		j++
	}
	switch w := raw[p:j]; {
	case eqFold(w, "set"), eqFold(w, "reset"), eqFold(w, "discard"), eqFold(w, "prepare"):
		return true
	case eqFold(w, "select"):
		return containsFold(raw, "advisory")
	case eqFold(w, "create"):
		return containsFold(raw, "temp") || containsFold(raw, "sequence")
	}
	return false
}

// proxysqlSkipLead skips what proxysqlDigest drops before the first token:
// whitespace, -- comments up to \n or \r, nested block comments and ::type
// casts.
func proxysqlSkipLead(s string) int {
	p := 0
	for p < len(s) {
		switch {
		case lexClass[s[p]] == clsSpace:
			p++
		case s[p] == '-' && p+1 < len(s) && s[p+1] == '-':
			for p < len(s) && s[p] != '\n' && s[p] != '\r' {
				p++
			}
		case s[p] == '/' && p+1 < len(s) && s[p+1] == '*':
			p, _ = skipBlockComment(s, p)
		case s[p] == ':' && p+1 < len(s) && s[p+1] == ':':
			// The digest drops ::type casts, even leading ones.
			p += 2
			for p < len(s) && (isIdentByte(s[p]) || s[p] == '.') {
				p++
			}
		default:
			return p
		}
	}
	return p
}

// containsFold reports whether s contains lower (lower-case ASCII letters
// and '_'), ignoring ASCII case, without allocating.
func containsFold(s, lower string) bool {
	first := lower[0]
	for i := 0; i+len(lower) <= len(s); i++ {
		if c := s[i]; (c == first || c|0x20 == first) && eqFold(s[i:i+len(lower)], lower) {
			return true
		}
	}
	return false
}
