// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package sessionsql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestScan(t *testing.T) {
	const (
		clean   = false
		changes = true
	)
	cases := []struct {
		sql  string
		want bool
	}{
		{"SELECT 1", clean},
		{"", clean},
		{"SET search_path = app", changes},
		{"set Search_Path to app", changes},
		{`SET "search_path" = app`, changes},
		{"SET ROLE admin", changes},
		{"SET SESSION AUTHORIZATION admin", changes},
		{"SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY", changes},
		{"SET LOCAL statement_timeout = 0", clean},
		{"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", clean},
		{"SET SESSION TRANSACTION READ ONLY", clean},
		{"SET CONSTRAINTS ALL DEFERRED", clean},
		// Found by fuzzing against pg_query: local here is a setting name.
		{"SET local.tenant_id = 1", changes},
		{"SET local = 1", changes},
		{"SET local TO 1", changes},
		{"SET", changes},
		{"SELECT 1; SET work_mem = '1MB'", changes},
		{"SELECT set_config('app.tenant_id', $1, false)", changes},
		{`SELECT "set_config"('a.b', 'c', false)`, changes},
		{`SELECT "Set_Config"('a.b', 'c', false)`, changes}, // over-matches; costs a reset
		{"SELECT pg_catalog.SET_CONFIG('a.b', 'c', false)", changes},
		{"UPDATE pg_settings SET setting = 'x' WHERE name = 'work_mem'", changes},
		{"SELECT set_limit(0.5)", changes},
		{"DO $$ BEGIN PERFORM 1; END $$", changes},
		{"CALL p()", changes},
		// Found by fuzzing a faster variant: \r ends a -- comment too.
		{"--\rCALL a()", changes},
		{"EXECUTE s", changes},
		{"EXPLAIN ANALYZE EXECUTE s", changes},
		{`SELECT U&"\0073et_config"('a.b', 'c', false)`, changes},
		{"DECLARE c CURSOR WITH HOLD FOR SELECT 1", changes},
		{"declare c no scroll cursor for select 1", changes},
		{"BEGIN; DECLARE c CURSOR WITH HOLD FOR SELECT 1; COMMIT", changes},
		{"RESET ALL", clean},
		{"RESET search_path", clean},
		{"DISCARD ALL", clean},
		{"UPDATE t SET x = 1", clean},
		{"INSERT INTO t VALUES (1) ON CONFLICT (id) DO UPDATE SET x = 1", clean},
		{"SELECT 'SET search_path = x'", clean},
		{"SELECT 1 -- SET search_path = x", clean},
		{"SELECT 1 /* SET /* nested */ search_path = x */", clean},
		{"SELECT 1 /*/ SET search_path = x */", clean},
		{"SELECT 1 /* **/; SET search_path = x", changes},
		{"SELECT 1 /*/**/*/; SET search_path = x", changes},
		{"SELECT $tag$ SET search_path = x $tag$", clean},
		{"SELECT current_setting('search_path')", clean},
		{"SELECT 1 OFFSET 10", clean},
		{"SELECT 'unterminated", changes},
		{"SELECT 1 /* unterminated", changes},
		{`SELECT "unterminated`, changes},
		{"SELECT $q$ unterminated", changes},
		{"SELECT currval('s')", clean},
		{"SELECT currval('s'); SET work_mem = '1MB'", changes},
		// A number ends before a $, which may open a dollar quote.
		{"SELECT 1$$a'$$; SET work_mem = '1MB'; --'", changes},
		{"SELECT x FROM t WHERE y = 1 AND z = 1$$a'$$; SET work_mem = '1MB'; --'", changes},
		{"SELECT a1$ FROM t", clean},
		// A letter after the digits starts an identifier, which takes in a $:
		// before PostgreSQL 15, 1a$$ is 1 AS a$$.
		{"SELECT 1a$$; SET search_path = s; SELECT 1 AS b$$", changes},
		{"SELECT 1 FROM t WHERE x = 1e2a$$; SET search_path = s; SELECT 1 AS b$$", changes},
		{"SELECT 1_a$$; SET search_path = s; SELECT 1 AS b$$", changes},
		{"SELECT 1a$$ FROM t", clean},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, Scan(c.sql, true), "%q", c.sql)
	}
}

func TestScanStandardConformingStrings(t *testing.T) {
	// With the setting on, the backslash is a plain byte and the literal ends
	// before the SET. With it off, the backslash escapes the quote, and the SET
	// is inside the literal.
	sql := `SELECT 'a\'; SET search_path = x; --'`
	assert.True(t, Scan(sql, true))
	assert.False(t, Scan(sql, false))
	assert.True(t, ScanEither(sql), "the verdict holds under either value")

	escaped := `SELECT E'a\'; SET search_path = x; --'`
	assert.False(t, Scan(escaped, true), "E'' literals always take backslash escapes")
	assert.False(t, ScanEither("SELECT 1"))
}

func TestScanContinuedLiterals(t *testing.T) {
	// A literal continued after a newline keeps its first part's escape rule:
	// backslash escapes for E'', none for U&'', whatever the setting says.
	for _, scsOn := range []bool{true, false} {
		for _, c := range []struct {
			sql  string
			want bool
		}{
			{"SELECT E'a'\n'\\'' ; SET search_path = x; --'", true},
			{"SELECT E'a' \t\r\n\f\v'\\'' ; SET search_path = x; --'", true},
			{"SELECT U&'b'\n'\\' UESCAPE '!'; SET search_path = x; --'", true},
			// Found in review: each part fools one setting value, so together
			// they hid the SET from ScanEither.
			{"SELECT E'a'\n'\\'', U&'b'\n'\\' UESCAPE '!'; SET search_path = x; --'", true},
			// The continuation holds the SET text, so it does not run.
			{"SELECT E'a'\n'\\'; SET search_path = x; --'", false},
			{"SELECT E'a'\n''", false},
			{"SELECT E'a'\nFROM t", false},
			{"SELECT E'a'\n-- comment\nFROM t", false},
			// PostgreSQL 17 does not continue a literal across a comment, but
			// the scanner cannot tell, so it counts the text.
			{"SELECT E'a'\n-- comment\n'b'", true},
			{"SELECT E'a' -- comment\n'b'", true},
		} {
			assert.Equal(t, c.want, Scan(c.sql, scsOn), "scsOn=%v %q", scsOn, c.sql)
		}
	}
}

func TestScanAllocatesNothing(t *testing.T) {
	sql := "SELECT a, b FROM t WHERE id = $1 AND name = 'x' -- trailing\n; SELECT currval('s')"
	allocs := testing.AllocsPerRun(100, func() { _ = Scan(sql, true) })
	assert.Zero(t, allocs)
}

func BenchmarkScan(b *testing.B) {
	for _, bc := range []struct{ name, sql string }{
		{"select1", "SELECT 1"},
		{"oltp", "SELECT o.id, o.total FROM orders o JOIN customers c ON c.id = o.customer_id WHERE c.tenant_id = $1 AND o.created_at > $2 ORDER BY o.created_at DESC LIMIT 50"},
		{"commented_oltp", "/*application='orders-api',traceparent='00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01'*/ " +
			"SELECT o.id, o.total FROM orders o JOIN customers c ON c.id = o.customer_id WHERE c.tenant_id = $1 AND o.created_at > $2 ORDER BY o.created_at DESC LIMIT 50"},
		{"in_list_10KB", "SELECT * FROM t WHERE id IN (" + strings.Repeat("123456, ", 1250) + "1)"},
		{"escape_literals", `SELECT * FROM t WHERE a = E'x\ty' AND b = U&'d\0061t' AND c IN (E'a', E'b', E'c')`},
		{"dollar_quote_10KB", "SELECT $fn$" + strings.Repeat("x = $1 ", 1430) + "$fn$"},
	} {
		b.Run(bc.name, func(b *testing.B) {
			b.SetBytes(int64(len(bc.sql)))
			for b.Loop() {
				_ = Scan(bc.sql, true)
			}
		})
	}
}
