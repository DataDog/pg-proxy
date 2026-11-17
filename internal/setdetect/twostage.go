// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package setdetect

// Two-stage detectors: a cheap first stage clears most traffic, and the
// lexer runs only on what it cannot clear.
//
// Every lexer rule needs one of these in the text, ignoring ASCII case:
// "set" (SET, set_config, set_limit, pg_settings), "exec" (EXECUTE), "u&"
// (U&"..."), a ";" (a later statement), or a leading DO or CALL keyword. The
// first stage looks for the first four anywhere, comments and literals
// included, and checks the leading keyword. If it finds none, the lexer
// would answer clean too.

// TwoStage uses a plain byte loop for the first stage.
type TwoStage struct{}

// Name implements Detector.
func (TwoStage) Name() string { return "two-stage" }

// Classify implements Detector.
func (TwoStage) Classify(sql string, scsOn bool) Verdict {
	if !stage1Loop(sql) {
		return verdict(leadingDoCall(sql))
	}
	return verdict(lexDirty(sql, scsOn))
}

func stage1Loop(sql string) bool {
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

// leadingDoCall reports whether the first word, after whitespace and
// comments, is DO or CALL. An unterminated comment counts as dirty.
func leadingDoCall(sql string) bool {
	p := skipLeadPG(sql)
	if p < 0 {
		return true
	}
	j := p
	for j < len(sql) && isIdentByte(sql[j]) {
		j++
	}
	w := sql[p:j]
	return eqFold(w, "do") || eqFold(w, "call")
}

// skipLeadPG skips whitespace and comments the way the Postgres scanner
// does: a -- comment ends at \n or \r, and block comments nest. It returns
// -1 on an unterminated block comment. (pgsqlSkipLead is ProxySQL's
// version, whose -- comments end only at \n.)
func skipLeadPG(s string) int {
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
