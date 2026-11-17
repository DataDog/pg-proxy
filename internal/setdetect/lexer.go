// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package setdetect

// The token-aware lexer: one pass over the text with no allocation. It
// follows the Postgres scanner closely enough to know where comments,
// literals and quoted identifiers start and end, and judges each statement by
// its keywords:
//
//   - A statement that starts with SET is dirty unless the next word is LOCAL,
//     TRANSACTION or CONSTRAINTS, or the next two are SESSION TRANSACTION,
//     and that word is not itself a setting name: SET local.tenant = 1 sets
//     a custom setting, so a following ".", "=" or TO is dirty too.
//   - A statement that starts with DO or CALL is dirty.
//   - The word EXECUTE anywhere is dirty (it also runs a prepared statement
//     under EXPLAIN ANALYZE or CREATE TABLE AS).
//   - The identifiers set_config, set_limit and pg_settings anywhere, in any
//     case or quoted, are dirty, and so is any U&"..." identifier, whose
//     spelling the lexer does not decode.
//   - Anything unterminated (a comment, literal, quoted identifier or dollar
//     quote) is dirty: the lexer cannot tell where it ends, so it fails closed.

const (
	clsOther byte = iota
	clsSpace
	clsIdent // letters, digits, _, $ and non-ASCII bytes after the first
	clsIdentStart
	clsQuote  // '
	clsDQuote // "
	clsDollar // $
	clsDash   // -
	clsSlash  // /
	clsSemi   // ;
	clsDigit  // 0-9
)

var lexClass = func() (t [256]byte) {
	for _, c := range []byte(" \t\n\r\f\v") {
		t[c] = clsSpace
	}
	for c := 'a'; c <= 'z'; c++ {
		t[c] = clsIdentStart
		t[c-32] = clsIdentStart
	}
	t['_'] = clsIdentStart
	for c := 0x80; c < 0x100; c++ {
		t[c] = clsIdentStart
	}
	for c := '0'; c <= '9'; c++ {
		t[c] = clsDigit
	}
	t['\''] = clsQuote
	t['"'] = clsDQuote
	t['$'] = clsDollar
	t['-'] = clsDash
	t['/'] = clsSlash
	t[';'] = clsSemi
	return t
}()

// identCont marks bytes that continue an identifier.
var identCont = func() (t [256]bool) {
	for c := 0; c < 256; c++ {
		k := lexClass[c]
		t[c] = k == clsIdentStart || k == clsDigit || c == '$'
	}
	return t
}()

func isIdentByte(c byte) bool { return identCont[c] }

// boring marks bytes that cannot start a token the rules look at once a
// statement is past its first words: whitespace and punctuation other than
// quotes, $, -, / and ;. Digits start numbers, which the skip loop consumes
// whole.
var boring = func() (t [256]bool) {
	for c := 0; c < 256; c++ {
		switch lexClass[c] {
		case clsSpace:
			t[c] = true
		case clsOther:
			t[c] = c != '&' // keep '&' out so U& is seen at the U
		}
	}
	return t
}()

// Lexer is the token-aware lexer detector.
type Lexer struct{}

// Name implements Detector.
func (Lexer) Name() string { return "lexer" }

// Classify implements Detector.
func (Lexer) Classify(sql string, scsOn bool) Verdict { return verdict(lexDirty(sql, scsOn)) }

// Statement-start states.
const (
	stStart        = iota // no word yet in this statement
	stAfterSet            // the statement started with SET
	stAfterSession        // SET SESSION
	stAfterScope          // SET LOCAL, SET TRANSACTION, SET CONSTRAINTS, SET SESSION TRANSACTION
	stBody                // past the first words
)

func lexDirty(sql string, scsOn bool) bool {
	n := len(sql)
	state := stStart
	for i := 0; i < n; {
		if state == stBody {
			for i < n {
				c := sql[i]
				if boring[c] {
					i++
					continue
				}
				if lexClass[c] == clsDigit {
					// A number runs on through identifier bytes, as in the
					// digit case below.
					for i++; i < n && identCont[sql[i]]; i++ {
					}
					continue
				}
				break
			}
			if i == n {
				break
			}
		}
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
			for j < n && identCont[sql[j]] {
				j++
			}
			if state == stBody && j-i > 1 {
				// Past the first words only setter names and EXECUTE
				// matter, and their lengths are 7, 9, 10 and 11.
				if l := j - i; l >= 7 && l <= 11 && l != 8 && bodyWordDirty(sql[i:j]) {
					return true
				}
				i = j
				continue
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
					return true // SET local TO ...: a setting named local
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

func bodyWordDirty(w string) bool { return isSetterName(w) || eqFold(w, "execute") }

func leaveStart(state int) int {
	if state == stStart {
		return stBody
	}
	return state
}

// scopeNameDirty reports whether the punctuation c after SET LOCAL (or
// TRANSACTION, CONSTRAINTS) makes that word a setting name.
func scopeNameDirty(c byte) bool { return c == '.' || c == '=' }

func afterSet(state int) bool { return state == stAfterSet || state == stAfterSession }

// isSetterName reports whether an identifier names a setter or pg_settings.
// Unquoted names fold case; callers pass quoted names without the quotes,
// and those compare exactly, as Postgres does.
func isSetterName(w string) bool {
	if len(w) != 10 && len(w) != 11 && len(w) != 9 {
		return false
	}
	return eqFold(w, "set_config") || eqFold(w, "pg_settings") || eqFold(w, "set_limit")
}

// eqFold compares w with lower, which must hold only lower-case ASCII
// letters and '_'. Only letters fold: '_' | 0x20 is DEL, not '_'.
func eqFold(w, lower string) bool {
	if len(w) != len(lower) {
		return false
	}
	for i := 0; i < len(w); i++ {
		if c := w[i]; c != lower[i] && (c|0x20 != lower[i] || lower[i] < 'a') {
			return false
		}
	}
	return true
}

// skipBlockComment returns the index after the comment that starts at i,
// honoring nesting.
func skipBlockComment(sql string, i int) (int, bool) {
	depth := 0
	n := len(sql)
	for i+1 < n {
		switch {
		case sql[i] == '/' && sql[i+1] == '*':
			depth++
			i += 2
		case sql[i] == '*' && sql[i+1] == '/':
			depth--
			i += 2
			if depth == 0 {
				return i, true
			}
		default:
			i++
		}
	}
	return n, false
}

// skipString returns the index after the literal whose opening quote is at
// i. backslash says whether \ escapes the next byte (E” strings, and plain
// strings when standard_conforming_strings is off). Adjacent literals
// separated by a newline continue in Postgres; they are lexed as separate
// literals, which ends at the same place.
func skipString(sql string, i int, backslash bool) (int, bool) {
	n := len(sql)
	for i++; i < n; i++ {
		switch sql[i] {
		case '\\':
			if backslash {
				i++
			}
		case '\'':
			if i+1 < n && sql[i+1] == '\'' {
				i++
				continue
			}
			return i + 1, true
		}
	}
	return n, false
}

func skipQuotedIdent(sql string, i int) (int, bool) {
	n := len(sql)
	for i++; i < n; i++ {
		if sql[i] == '"' {
			if i+1 < n && sql[i+1] == '"' {
				i++
				continue
			}
			return i + 1, true
		}
	}
	return n, false
}

// skipDollar handles a $ at the start of a token: $n parameters, $tag$
// quotes, and a lone $ (an operator character in some contexts). isQuote is
// true for a dollar quote.
func skipDollar(sql string, i int) (end int, ok, isQuote bool) {
	n := len(sql)
	j := i + 1
	if j < n && lexClass[sql[j]] == clsDigit {
		for j < n && lexClass[sql[j]] == clsDigit {
			j++
		}
		return j, true, false
	}
	for j < n && sql[j] != '$' {
		if !isIdentByte(sql[j]) || sql[j] == '$' {
			return i + 1, true, false
		}
		j++
	}
	if j >= n {
		return i + 1, true, false
	}
	tag := sql[i : j+1] // $tag$
	body := j + 1
	for k := body; k+len(tag) <= n; k++ {
		if sql[k] == '$' && sql[k:k+len(tag)] == tag {
			return k + len(tag), true, true
		}
	}
	return n, false, true
}
