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
		clean   = Flags(0)
		changes = failClosed
		reads   = MayReadSequenceState
	)
	cases := []struct {
		sql  string
		want Flags
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
		{"SELECT $tag$ SET search_path = x $tag$", clean},
		{"SELECT current_setting('search_path')", clean},
		{"SELECT 1 OFFSET 10", clean},
		{"SELECT 'unterminated", changes},
		{"SELECT 1 /* unterminated", changes},
		{`SELECT "unterminated`, changes},
		{"SELECT $q$ unterminated", changes},
		{"SELECT currval('s')", reads},
		{"SELECT pg_catalog.lastval()", reads},
		{"INSERT INTO t VALUES (nextval('s')) RETURNING CURRVAL('s')", reads},
		{`SELECT "currval"('s')`, reads},
		{`SELECT "CurrVal"('s')`, reads},
		{"SELECT 'currval'", clean},
		{"SELECT currval('s'); SET work_mem = '1MB'", changes},
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
	assert.Equal(t, failClosed, Scan(sql, true))
	assert.Equal(t, Flags(0), Scan(sql, false))
	assert.Equal(t, failClosed, ScanEither(sql), "the verdict holds under either value")

	escaped := `SELECT E'a\'; SET search_path = x; --'`
	assert.Equal(t, Flags(0), Scan(escaped, true), "E'' literals always take backslash escapes")
	assert.Equal(t, Flags(0), ScanEither("SELECT 1"))
}

func TestFlagsHas(t *testing.T) {
	assert.True(t, failClosed.Has(MayChangeSession))
	assert.True(t, failClosed.Has(MayChangeSession|MayReadSequenceState))
	assert.False(t, MayReadSequenceState.Has(MayChangeSession))
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
		{"in_list_10KB", "SELECT * FROM t WHERE id IN (" + strings.Repeat("123456, ", 1250) + "1)"},
	} {
		b.Run(bc.name, func(b *testing.B) {
			b.SetBytes(int64(len(bc.sql)))
			for b.Loop() {
				_ = Scan(bc.sql, true)
			}
		})
	}
}
