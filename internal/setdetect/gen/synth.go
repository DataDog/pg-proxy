// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package main

import (
	"fmt"
	"math/rand"
	"strings"

	sd "github.com/DataDog/pg-proxy/internal/setdetect"
)

// Hand-labeled cases. Each helper below writes cases for one category; the
// label is the author's, from the category rules, and the oracle checks the
// executable ones against Postgres. Objects they use are created by
// oracle_fixture.sql.

type synth struct {
	cases []sd.Case
	n     map[string]int
}

func (s *synth) add(source, sql, label, cat, note, mix string, opts ...func(*sd.Case)) {
	if s.n == nil {
		s.n = map[string]int{}
	}
	s.n[source]++
	c := sd.Case{
		ID:       fmt.Sprintf("%s-%04d", source, s.n[source]),
		SQL:      sql,
		Label:    label,
		Category: cat,
		Source:   source,
		Mix:      mix,
		Exec:     true,
		Note:     note,
	}
	for _, o := range opts {
		o(&c)
	}
	s.cases = append(s.cases, c)
}

func scsOff(c *sd.Case) { c.SCSOff = true }
func noExec(c *sd.Case) { c.Exec = false }
func ctx(stmts ...string) func(*sd.Case) {
	return func(c *sd.Case) { c.Context = stmts }
}

const (
	D = sd.LabelDirty
	C = sd.LabelClean
	E = sd.LabelEither
)

// sessionVars are (name, value) pairs for direct SET forms. Values are
// chosen to differ from the defaults, so the oracle sees a change.
var sessionVars = [][2]string{
	{"search_path", "tenant_1, public"},
	{"search_path", "'$user', public, tenant_2"},
	{"statement_timeout", "'5s'"},
	{"statement_timeout", "5000"},
	{"lock_timeout", "'2s'"},
	{"idle_in_transaction_session_timeout", "'30s'"},
	{"work_mem", "'64MB'"},
	{"maintenance_work_mem", "'256MB'"},
	{"application_name", "'billing-worker'"},
	{"DateStyle", "'SQL, DMY'"},
	{"IntervalStyle", "iso_8601"},
	{"extra_float_digits", "3"},
	{"client_min_messages", "'warning'"},
	{"TimeZone", "'America/New_York'"},
	{"timezone", "'UTC'"},
	{"default_transaction_isolation", "'serializable'"},
	{"default_transaction_read_only", "on"},
	{"enable_seqscan", "off"},
	{"enable_nestloop", "false"},
	{"random_page_cost", "1.1"},
	{"synchronous_commit", "off"},
	{"bytea_output", "'escape'"},
	{"plan_cache_mode", "force_custom_plan"},
	{"row_security", "off"},
	{"jit", "off"},
	{"temp_buffers", "'16MB'"},
	{"geqo", "off"},
	{"app.tenant_id", "'42'"},
	{"app.user_id", "'7'"},
	{"myapp.flag", "'on'"},
	{"app.request_id", "'9f86d081'"},
	{"pg_trgm.similarity_threshold", "0.5"},
}

func (s *synth) directSet() {
	for _, v := range sessionVars {
		name, val := v[0], v[1]
		s.add("synth-p1", "SET "+name+" = "+val, D, "P1", "", "")
		s.add("synth-p1", "SET "+name+" TO "+val, D, "P1", "", "")
		s.add("synth-p1", "SET SESSION "+name+" = "+val, D, "P1", "", "")
		s.add("synth-p1", "set "+strings.ToLower(name)+"="+val+";", D, "P1", "", "")
		// Scoped and reset forms of the same setting are clean or either.
		s.add("synth-n4", "SET LOCAL "+name+" = "+val, C, "N4", "SET LOCAL outside a transaction only warns", "")
		s.add("synth-n4", "BEGIN; SET LOCAL "+name+" TO "+val+"; COMMIT", C, "N4", "SET LOCAL ends with the transaction", "")
		s.add("synth-n4", "BEGIN; SET "+name+" = "+val+"; ROLLBACK", E, "N4", "rolled back: clean, but no text detector can know the outcome", "")
		s.add("synth-n5", "RESET "+name, E, "N5", "", "")
		s.add("synth-n5", "SET "+name+" TO DEFAULT", E, "N5", "same as RESET", "")
	}
	special := []string{
		"SET TIME ZONE 'UTC'",
		"SET TIME ZONE 'America/Los_Angeles'",
		"SET TIME ZONE -7",
		"SET TIME ZONE INTERVAL '+02:00' HOUR TO MINUTE",
		"SET SESSION TIME ZONE 'Europe/Paris'",
		"SET NAMES 'LATIN1'",
		"SET NAMES 'SQL_ASCII'",
		"SET SCHEMA 'tenant_1'",
		"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE",
		"SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY",
		"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ WRITE",
		"SET ROLE app_reader",
		"SET ROLE 'app_writer'",
		"SET SESSION ROLE app_reader",
		"SET SESSION AUTHORIZATION app_reader",
		"SET SESSION AUTHORIZATION 'app_writer'",
		"SET XML OPTION DOCUMENT",
		"SET search_path = \"Tenant One\", public",
		"SET datestyle TO postgres, dmy",
	}
	for _, q := range special {
		s.add("synth-p1", q, D, "P1", "", "")
	}
	for _, q := range []string{
		"SET TIME ZONE LOCAL", "SET TIME ZONE DEFAULT", "SET ROLE NONE",
		"SET SESSION AUTHORIZATION DEFAULT", "RESET ROLE", "RESET SESSION AUTHORIZATION",
		"RESET ALL", "RESET TIME ZONE", "DISCARD ALL", "SET statement_timeout FROM CURRENT",
	} {
		s.add("synth-n5", q, E, "N5", "resets to the session default", "")
	}
	for _, q := range []string{
		"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
		"SET TRANSACTION READ ONLY",
		"SET TRANSACTION ISOLATION LEVEL READ COMMITTED, READ WRITE",
		"BEGIN; SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; SELECT 1; COMMIT",
		"SET LOCAL TRANSACTION READ ONLY",
		"SET CONSTRAINTS ALL DEFERRED",
		"SET CONSTRAINTS ALL IMMEDIATE",
		"BEGIN; SET CONSTRAINTS ALL DEFERRED; COMMIT",
		"BEGIN; SET LOCAL ROLE app_reader; SELECT current_user; COMMIT",
		"BEGIN; SET LOCAL search_path = tenant_1; SELECT 1; COMMIT",
	} {
		s.add("synth-n4", q, C, "N4", "", "")
	}
}

// setConfigCalls are set_config spellings with the label each call gives.
var setConfigCalls = []struct{ call, label, note string }{
	{"set_config('app.tenant_id', '42', false)", D, ""},
	{"set_config('search_path', 'tenant_1, public', false)", D, ""},
	{"set_config('statement_timeout', '1s', false)", D, ""},
	{"SET_CONFIG('app.user_id', '7', FALSE)", D, "upper case"},
	{"pg_catalog.set_config('app.tenant_id', '43', false)", D, "schema-qualified"},
	{"\"set_config\"('app.tenant_id', '44', false)", D, "quoted name"},
	{"pg_catalog.\"set_config\"('work_mem', '8MB', false)", D, "quoted, qualified"},
	{"set_config('app.tenant_id', '45', 'false')", D, "is_local as text"},
	{"set_config('app.tenant_id', '46', 'f')", D, ""},
	{"set_config('app.tenant_id', '47', false::boolean)", D, "cast"},
	{"set_config('app.tenant_id', '48', 1 = 0)", D, "is_local expression"},
	{"set_config('app.tenant_id', '49', true)", E, "is_local true: no effect in autocommit"},
	{"set_config('app.tenant_id', '50', 'true')", E, "is_local true"},
	{"set_config('TimeZone', 'Asia/Tokyo', false)", D, "reported setting"},
	{"set_config('role', 'app_reader', false)", D, "role through set_config"},
}

func (s *synth) setConfig() {
	positions := []string{
		"SELECT %s",
		"SELECT %s AS ok",
		"SELECT %s, now()",
		"SELECT 1 WHERE %s IS NOT NULL",
		"WITH c AS (SELECT %s AS v) SELECT v FROM c",
		"WITH c AS MATERIALIZED (SELECT %s AS v) SELECT v FROM c",
		"SELECT * FROM (SELECT %s AS v) s",
		"SELECT (SELECT %s)",
		"SELECT CASE WHEN true THEN %s END",
		"SELECT coalesce(%s, 'x')",
		"VALUES (%s)",
		"SELECT * FROM %s",
		"SELECT id FROM users WHERE id = 1 AND %s IS NOT NULL",
		"UPDATE users SET name = name WHERE id = 1 RETURNING %s",
		"INSERT INTO audit_log (msg) VALUES (%s)",
		"INSERT INTO audit_log (msg) SELECT %s",
		"DELETE FROM audit_log WHERE id = -1 OR %s IS NULL RETURNING id",
		"SELECT lower(%s)",
		"(SELECT %s)",
		"SELECT %s FROM users LIMIT 1",
	}
	for _, c := range setConfigCalls {
		for _, p := range positions {
			s.add("synth-p2", fmt.Sprintf(p, c.call), c.label, "P2", c.note, "")
		}
	}
	// Parameters: the usual tenant pattern, prepared by drivers.
	for _, q := range []string{
		"SELECT set_config('app.tenant_id', $1, false)",
		"SELECT set_config($1, $2, false)",
		"SELECT set_config($1, $2, $3)",
		"SELECT set_config('app.tenant_id', $1::text, $2::boolean)",
		"SELECT pg_catalog.set_config('app.user_id', $1, false), now()",
		"select set_config('request.jwt.claims', $1, false)",
	} {
		s.add("synth-p2", q, D, "P2", "parameterized", "")
	}
	// PostgREST runs these inside its transaction with is_local true.
	s.add("synth-p2", "select set_config('search_path', $1, true), set_config('role', $2, true), set_config('request.jwt.claims', $3, true)", E, "P2", "PostgREST: is_local true", "")
	s.add("synth-p2", "select set_config('role', 'app_reader', true), set_config('request.method', 'GET', true)", E, "P2", "PostgREST", "")
}

func (s *synth) pgSettings() {
	for _, q := range []string{
		"UPDATE pg_settings SET setting = '1s' WHERE name = 'statement_timeout'",
		"update pg_settings set setting = 'tenant_1' where name = 'search_path'",
		"UPDATE pg_catalog.pg_settings SET setting = '32MB' WHERE name = 'work_mem'",
		"UPDATE \"pg_settings\" SET setting = 'off' WHERE name = 'enable_seqscan'",
		"UPDATE pg_settings s SET setting = '3' WHERE s.name = 'extra_float_digits'",
		"UPDATE pg_settings SET setting = reset_val WHERE name = 'work_mem'",
		"UPDATE pg_settings SET setting = 'warning' WHERE name = 'client_min_messages' RETURNING name, setting",
		"WITH u AS (UPDATE pg_settings SET setting = '2s' WHERE name = 'lock_timeout' RETURNING name) SELECT * FROM u",
		"UPDATE  pg_settings\n   SET setting = 'UTC'\n WHERE name = 'TimeZone'",
		"/* admin */ UPDATE pg_settings SET setting = 'off' WHERE name = 'jit'",
		"UPDATE pg_settings SET setting = '1' WHERE name = 'random_page_cost'; SELECT 1",
		"SELECT 1; UPDATE pg_settings SET setting = '2' WHERE name = 'random_page_cost'",
		"UPDATE PG_SETTINGS SET SETTING = 'on' WHERE NAME = 'default_transaction_read_only'",
		"UPDATE pg_settings SET setting = '64MB' WHERE name IN ('work_mem')",
	} {
		s.add("synth-p3", q, D, "P3", "", "")
	}
	for _, q := range []string{
		"SELECT name, setting FROM pg_settings WHERE name = 'work_mem'",
		"SELECT * FROM pg_catalog.pg_settings WHERE source = 'session'",
		"SELECT setting::int FROM pg_settings WHERE name = 'max_connections'",
		"SELECT count(*) FROM pg_settings",
		"SELECT current_setting('search_path')",
		"SELECT current_setting('app.tenant_id', true)",
		"SELECT current_setting('server_version_num')::int",
		"SHOW search_path",
		"SHOW ALL",
		"SHOW TIME ZONE",
		"SHOW transaction isolation level",
		"SELECT show_limit()",
		"SELECT name FROM pg_settings WHERE name LIKE 'enable%' ORDER BY name",
	} {
		s.add("synth-n6", q, C, "N6", "", "select")
	}
}

func (s *synth) indirect() {
	for _, q := range []string{
		"DO $$BEGIN PERFORM set_config('app.tenant_id', '1', false); END$$",
		"DO $$ BEGIN EXECUTE 'SET search_path = tenant_1'; END $$",
		"DO 'BEGIN SET statement_timeout = 1000; END'",
		"DO LANGUAGE plpgsql $x$ BEGIN SET work_mem = '16MB'; END $x$",
		"DO $$ BEGIN EXECUTE format('SET %I = %L', 'app.tenant_id', '9'); END $$",
		"DO $$ DECLARE t text := 'tenant_2'; BEGIN EXECUTE 'SET search_path TO ' || quote_ident(t); END $$",
		"DO $body$\nBEGIN\n  UPDATE pg_settings SET setting = 'off' WHERE name = 'jit';\nEND\n$body$",
		"do $$ begin perform pg_catalog.set_config('app.user_id', '3', false); end $$",
		"DO $$ BEGIN IF true THEN SET SESSION lock_timeout = '3s'; END IF; END $$",
		"DO $$ BEGIN EXECUTE 'SET ROLE app_reader'; END $$",
		"DO $$ BEGIN EXECUTE 'se' || 't application_name = ''x'''; END $$",
		"DO $$ BEGIN PERFORM set_limit(0.4); END $$",
	} {
		s.add("synth-p4", q, D, "P4", "DO block", "")
	}
	for _, q := range []string{
		"DO $$ BEGIN RAISE NOTICE 'settings ok'; END $$",
		"DO $$ BEGIN SET LOCAL work_mem = '8MB'; END $$",
		"DO $$ BEGIN PERFORM set_config('app.tenant_id', '1', true); END $$",
		"DO $$ DECLARE n int; BEGIN SELECT count(*) INTO n FROM users; END $$",
		"DO $$ BEGIN UPDATE users SET name = name WHERE id = 1; END $$",
	} {
		s.add("synth-n7", q, C, "N7", "DO block that changes no setting", "other")
	}
	prep := "PREPARE p_cfg(text) AS SELECT set_config('app.tenant_id', $1, false)"
	prepSet := "PREPARE p_upd AS UPDATE pg_settings SET setting = '7s' WHERE name = 'lock_timeout'"
	prepSel := "PREPARE p_sel AS SELECT 1"
	s.add("synth-p4", "EXECUTE p_cfg('7')", D, "P4", "prepared set_config", "", ctx(prep))
	s.add("synth-p4", "execute p_cfg ( 'x' )", D, "P4", "prepared set_config", "", ctx(prep))
	s.add("synth-p4", "EXECUTE p_upd", D, "P4", "prepared UPDATE pg_settings", "", ctx(prepSet))
	s.add("synth-p4", prep+"; EXECUTE p_cfg('8')", D, "P4", "PREPARE and EXECUTE in one message", "")
	s.add("synth-p4", "EXPLAIN ANALYZE EXECUTE p_cfg('9')", D, "P4", "EXPLAIN ANALYZE runs the prepared statement", "", ctx(prep))
	s.add("synth-p4", "EXPLAIN (ANALYZE, COSTS OFF) EXECUTE p_upd", D, "P4", "", "", ctx(prepSet))
	s.add("synth-p4", "CREATE TEMP TABLE t_exec AS EXECUTE p_cfg('10')", D, "P4", "CREATE TABLE AS runs the prepared statement", "", ctx(prep))
	s.add("synth-p4", "EXPLAIN EXECUTE p_cfg('11')", E, "P4", "EXPLAIN without ANALYZE runs nothing", "", ctx(prep))
	s.add("synth-p4", "PREPARE q AS SELECT set_config('app.user_id', '1', false); EXECUTE q", D, "P4", "", "")
	s.add("synth-n7", "EXECUTE p_sel", C, "N7", "prepared SELECT 1", "other", ctx(prepSel))
	s.add("synth-p4", prep, E, "P4", "PREPARE alone runs nothing", "")
	s.add("synth-p4", prepSet, E, "P4", "PREPARE alone runs nothing", "")
	for _, q := range []string{
		"EXPLAIN ANALYZE SELECT set_config('app.tenant_id', '1', false)",
		"EXPLAIN (ANALYZE) SELECT set_config('app.tenant_id', '2', false)",
		"EXPLAIN (ANALYZE true, BUFFERS) SELECT set_config('work_mem', '9MB', false)",
		"EXPLAIN (ANALYZE, COSTS OFF) UPDATE pg_settings SET setting = '4s' WHERE name = 'lock_timeout'",
		"explain analyze select pg_catalog.set_config('app.user_id', '4', false)",
		"EXPLAIN ANALYZE VERBOSE SELECT * FROM (SELECT set_config('app.tenant_id', '3', false)) s",
		"COPY (SELECT set_config('app.tenant_id', '5', false)) TO STDOUT",
		"COPY (SELECT set_config('search_path', 'tenant_1', false)) TO STDOUT WITH (FORMAT csv)",
		"copy (update pg_settings set setting = '5s' where name = 'lock_timeout' returning name) to stdout",
		"CALL proc_set_tenant('t1')",
		"CALL public.proc_set_tenant('t2')",
		"call proc_tenant('t3')",
		"CALL proc_path('tenant_1')",
	} {
		s.add("synth-p4", q, D, "P4", "", "")
	}
	for _, q := range []string{
		"EXPLAIN SELECT set_config('app.tenant_id', '1', false)",
		"EXPLAIN (COSTS OFF) SELECT set_config('app.tenant_id', '1', false)",
		"EXPLAIN (ANALYZE false) SELECT set_config('app.tenant_id', '1', false)",
	} {
		s.add("synth-p4", q, E, "P4", "EXPLAIN without ANALYZE runs nothing", "")
	}
	s.add("synth-n7", "CALL proc_noop()", C, "N7", "procedure changes no setting", "other")
	s.add("synth-n7", "EXPLAIN ANALYZE SELECT 1", C, "N7", "", "other")
	s.add("synth-n7", "COPY (SELECT id FROM users) TO STDOUT", C, "N7", "", "other")
}

// multiStatement puts a positive after one or more clean statements.
func (s *synth) multiStatement() {
	firsts := []string{
		"SELECT 1",
		"BEGIN",
		"SELECT 'a;b'",
		"SELECT $$;$$",
		"/* c */ SELECT 1",
		"UPDATE users SET name = name WHERE id = 1",
		"SELECT 1; -- trailing ; comment\n",
		"SELECT \";\" FROM (SELECT 1 AS \";\") s",
	}
	seconds := []string{
		"SET search_path = tenant_1",
		"set statement_timeout to '1s'",
		"SELECT set_config('app.tenant_id', '1', false)",
		"UPDATE pg_settings SET setting = '1s' WHERE name = 'lock_timeout'",
		"DO $$BEGIN PERFORM set_config('app.user_id', '2', false); END$$",
		"SET ROLE app_reader",
		"SET SESSION AUTHORIZATION app_writer",
		"SET TIME ZONE 'UTC'",
	}
	for _, a := range firsts {
		for _, b := range seconds {
			sep := "; "
			if strings.HasSuffix(a, "\n") {
				// The first statement ends with its ; before a -- comment.
				sep = ""
			}
			q := a + sep + b
			if a == "BEGIN" {
				q += "; COMMIT"
			}
			s.add("synth-p5", q, D, "P5", "", "")
		}
	}
	for _, q := range []string{
		"SELECT 1;SET search_path=tenant_1",
		"SELECT 1;;SET work_mem='8MB'",
		"SELECT 1; SELECT 2; SELECT 3; SET application_name = 'x'",
		"INSERT INTO audit_log (msg) VALUES ('a'); SET app.tenant_id = '5'",
		"SET LOCAL work_mem = '1MB'; SET work_mem = '2MB'",
		"SET TRANSACTION READ ONLY; SET search_path = tenant_2",
		"SELECT pg_advisory_unlock_all(); CLOSE ALL; UNLISTEN *; SET search_path = tenant_1",
	} {
		s.add("synth-p5", q, D, "P5", "", "")
	}
	// asyncpg's reset sequence and similar resets.
	s.add("synth-driver", "SELECT pg_advisory_unlock_all();\nCLOSE ALL;\nUNLISTEN *;\nRESET ALL;", E, "N5", "asyncpg reset query", "other")
	s.add("synth-driver", "SELECT pg_advisory_unlock_all(); CLOSE ALL; UNLISTEN *; RESET ALL", E, "N5", "asyncpg reset query", "other")
	s.add("synth-n4", "BEGIN; SET LOCAL statement_timeout = '1s'; SELECT 1; COMMIT", C, "N4", "", "other")
	s.add("synth-n7", "SELECT 1; SELECT 2", C, "N7", "", "select")
	s.add("synth-n3", "SELECT 1 -- ; SET search_path = tenant_1", C, "N3", "SET is inside a comment", "select")
	s.add("synth-n3", "SELECT 'x; SET search_path = tenant_1'", C, "N3", "SET is inside a literal", "select")
	s.add("synth-n3", "SELECT 1 /* ; SET search_path = tenant_1 */", C, "N3", "", "select")
	s.add("synth-n3", "SELECT $$; SET search_path = tenant_1; $$", C, "N3", "", "select")
}

// lexical applies tricks to positive base statements.
func (s *synth) lexical() {
	bases := []string{
		"SET search_path = tenant_1",
		"SET statement_timeout TO '2s'",
		"SET SESSION app.tenant_id = '11'",
		"SET ROLE app_reader",
		"SELECT set_config('app.tenant_id', '12', false)",
		"UPDATE pg_settings SET setting = '3s' WHERE name = 'lock_timeout'",
		"SET TIME ZONE 'UTC'",
		"SET SESSION AUTHORIZATION app_writer",
	}
	tricks := []struct {
		name string
		fn   func(string) string
	}{
		{"lower", strings.ToLower},
		{"upper", strings.ToUpper},
		{"mixed", mixedCase},
		{"leading spaces", func(q string) string { return "   \t  " + q }},
		{"leading newlines", func(q string) string { return "\n\n\t" + q }},
		{"line comment first", func(q string) string { return "-- set by the tenant middleware\n" + q }},
		{"two line comments", func(q string) string { return "-- a\n-- b\n" + q }},
		{"block comment first", func(q string) string { return "/* tenant */ " + q }},
		{"block comment glued", func(q string) string { return "/*x*/" + q }},
		{"nested comment first", func(q string) string { return "/* outer /* inner */ still a comment */ " + q }},
		{"dbm comment first", func(q string) string {
			return "/*dddbs='api',dde='prod',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ " + q
		}},
		{"sqlcommenter suffix", func(q string) string { return q + " /*controller='tenants',action='switch'*/" }},
		{"comment inside", func(q string) string { return strings.Replace(q, " ", " /* c */ ", 1) }},
		{"newlines between tokens", func(q string) string { return strings.ReplaceAll(q, " ", "\n") }},
		{"tabs between tokens", func(q string) string { return strings.ReplaceAll(q, " ", "\t") }},
		{"form feed", func(q string) string { return strings.Replace(q, " ", "\f", 1) }},
		{"trailing semicolons", func(q string) string { return q + " ;;" }},
		{"parenthesized", func(q string) string {
			if strings.HasPrefix(q, "SELECT") {
				return "(" + q + ")"
			}
			return q + ";"
		}},
		{"empty statement first", func(q string) string { return "; " + q }},
		{"comment then newline", func(q string) string { return "/* a */\n-- b\n" + q }},
	}
	for _, b := range bases {
		for _, t := range tricks {
			s.add("synth-p6", t.fn(b), D, "P6", t.name, "")
		}
	}
	for _, q := range []string{
		"SET \"search_path\" = tenant_1",
		"SET \"statement_timeout\" TO '1s'",
		"SET \"app\".\"tenant_id\" = '13'",
		"SET U&\"search\\005fpath\" = tenant_1",
		"SELECT U&\"\\0073et_config\"('app.tenant_id', '14', false)",
		"SELECT U&\"set\\005fconfig\"('app.tenant_id', '15', false)",
		"SELECT U&\"!0073et_config\" UESCAPE '!'('app.tenant_id', '16', false)",
		"SELECT pg_catalog . set_config('app.tenant_id', '17', false)",
		"SELECT set_config\n('app.tenant_id', '18', false)",
		"SELECT set_config ( 'app.tenant_id' , '19' , false )",
		"SELECT set_config($q$app.tenant_id$q$, $$20$$, false)",
		"SELECT set_config(E'app.tenant_id', E'21', false)",
		"SELECT set_config(U&'app.tenant\\005fid', '22', false)",
		"SET search_path = $$tenant_1$$",
		"SET app.tenant_id = E'it\\'s'",
		"SET app.tenant_id = U&'\\0034\\0032'",
		"SELECT 'it''s; fine'; SET search_path = tenant_1",
		"SELECT E'\\'; SET x'; SET search_path = tenant_1",
		"SELECT $a$ $b$ ; $b$ $a$; SET search_path = tenant_1",
		"SELECT \"a;b\" FROM (SELECT 1 AS \"a;b\") t; SET search_path = tenant_1",
		"UPDATE pg_catalog . pg_settings SET setting = '4s' WHERE name = 'lock_timeout'",
		"UPDATE U&\"pg\\005fsettings\" SET setting = '5s' WHERE name = 'lock_timeout'",
		"SELECT/**/set_config('app.tenant_id', '23', false)",
		"SELECT-1, set_config('app.tenant_id', '24', false)",
	} {
		s.add("synth-p6", q, D, "P6", "quoting or escapes", "")
	}
	// A -- comment ends at \r as well as \n (found by fuzzing).
	for _, q := range []string{
		"--\rCALL proc_set_tenant('t9')",
		"-- note\rDO $$BEGIN PERFORM set_config('app.tenant_id', '9', false); END$$",
		"-- note\rSET search_path = tenant_1",
		"-- note\r\nEXECUTE p_cfg('12')",
	} {
		s.add("synth-p6", q, D, "P6", "line comment ended by a carriage return", "", ctx("PREPARE p_cfg(text) AS SELECT set_config('app.tenant_id', $1, false)"))
	}
	// Custom settings whose first part is a scope keyword (found by fuzzing).
	for _, q := range []string{
		"SET local.tenant_id = '5'",
		"SET LOCAL . tenant_id = '6'",
		"SET transaction.tag TO 'x'",
		"SET constraints.mode = 'strict'",
		"SET SESSION transaction.tag = 'y'",
		"SET SESSION local.tenant_id = '7'",
	} {
		s.add("synth-p6", q, D, "P6", "custom setting named like a scope keyword", "")
	}
	// standard_conforming_strings: the same text means different things.
	scsPairs := []string{
		`SELECT 'a\'; SET search_path = tenant_1; --'`,
		`SELECT 'x\'; SELECT set_config($$app.tenant_id$$, $$25$$, false) --'`,
		`SELECT 'q\\'; SET search_path = tenant_2`,
	}
	s.add("synth-p6", scsPairs[0], D, "P6", "scs on: the literal ends at the second quote", "")
	s.add("synth-n3", scsPairs[0], C, "N3", "scs off: one literal", "", scsOff)
	s.add("synth-p6", scsPairs[1], D, "P6", "scs on", "")
	s.add("synth-n3", scsPairs[1], C, "N3", "scs off: one literal", "", scsOff)
	s.add("synth-p6", scsPairs[2], D, "P6", "scs on", "")
	s.add("synth-p6", scsPairs[2], D, "P6", "scs off: \\\\ is one backslash", "", scsOff)
	s.add("synth-p6", `SELECT E'a\'; SET search_path = tenant_1; --'; SET search_path = tenant_2`, D, "P6", "E'' always takes backslash escapes", "")
	s.add("synth-n3", `SELECT E'a\'; SET search_path = tenant_1; --'`, C, "N3", "E'' literal hides the SET", "select")
}

func mixedCase(q string) string {
	b := []byte(q)
	inQuote := false
	for i, c := range b {
		if c == '\'' {
			inQuote = !inQuote
		}
		if inQuote || i%2 == 1 {
			continue
		}
		if c >= 'a' && c <= 'z' {
			b[i] = c - 32
		} else if c >= 'A' && c <= 'Z' {
			b[i] = c + 32
		}
	}
	return string(b)
}

func (s *synth) extensionAndServer() {
	for _, q := range []string{
		"SELECT set_limit(0.5)",
		"SELECT public.set_limit(0.4)",
		"select set_limit(0.35)",
		"SELECT name FROM users WHERE set_limit(0.6) > 0 LIMIT 1",
		"SELECT set_limit(0.45), show_limit()",
		"WITH t AS (SELECT set_limit(0.55)) SELECT * FROM t",
	} {
		s.add("synth-p7", q, D, "P7", "pg_trgm set_limit sets pg_trgm.similarity_threshold", "")
	}
	for _, q := range []string{
		"SELECT f_tenant('t1')",
		"SELECT set_tenant('t2')",
		"SELECT id FROM users WHERE id = 1 AND f_tenant('t3') IS NOT NULL",
		"SELECT f_tz()",
		"SELECT f_path()",
		"SELECT f_timeout()",
		"SELECT * FROM f_tenant_rows('t4')",
		"INSERT INTO audit_log (msg) VALUES ('trigger sets app.last_audit')",
		"WITH x AS (SELECT f_tenant('t5')) SELECT * FROM x",
		"SELECT f_role()",
	} {
		s.add("synth-p9", q, D, "P9", "a function or trigger changes the setting", "")
	}
	for _, q := range []string{
		"SELECT f_local('t1')",
		"SELECT f_setclause()",
		"SELECT f_noop()",
	} {
		s.add("synth-n7", q, C, "N7", "function changes no lasting setting", "select")
	}
}

// cleanForms covers N1 to N4 with text that contains "set".
func (s *synth) cleanForms() {
	for _, q := range []string{
		"UPDATE users SET name = 'Ann' WHERE id = 1",
		"UPDATE users SET settings = jsonb_set(settings, '{theme}', '\"dark\"') WHERE id = 2",
		"UPDATE users SET reset_at = now(), status = 'active' WHERE id = 3 RETURNING id",
		"UPDATE assets SET charset = 'utf8' WHERE asset_id = 10",
		"UPDATE datasets SET settings = '{}' WHERE id = 1",
		"update orders set status='shipped' where id = 5",
		"UPDATE orders o SET total = o.total + 1 FROM users u WHERE u.id = o.user_id AND u.id = 1",
		"INSERT INTO app_settings (key, value) VALUES ('theme', 'dark') ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
		"INSERT INTO app_settings (key, value) VALUES ('lang', 'en') ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value RETURNING key",
		"INSERT INTO users (id, email, name) VALUES (100, 'a@x.io', 'A') ON CONFLICT (id) DO UPDATE SET email = EXCLUDED.email, name = EXCLUDED.name",
		"INSERT INTO app_settings (key, value) VALUES ('k', 'v') ON CONFLICT DO NOTHING",
		"MERGE INTO app_settings t USING (VALUES ('m', '1')) v(k, val) ON t.key = v.k WHEN MATCHED THEN UPDATE SET value = v.val WHEN NOT MATCHED THEN INSERT VALUES (v.k, v.val)",
		"UPDATE users SET (name, status) = ('B', 'x') WHERE id = 4",
		"UPDATE users SET name = DEFAULT WHERE id = 5",
	} {
		s.add("synth-n1", q, C, "N1", "", mixFor(q))
	}
	for _, q := range []string{
		"SELECT id FROM orders ORDER BY id OFFSET 20 LIMIT 10",
		"SELECT * FROM datasets",
		"SELECT settings->>'theme' FROM users WHERE id = 1",
		"SELECT asset_id, dataset_id FROM assets WHERE charset = 'utf8'",
		"SELECT reset_at FROM users WHERE reset_at > now() - interval '1 day'",
		"SELECT offset_ms FROM orders OFFSET 5",
		"SELECT 1 AS \"set\"",
		"SELECT 1 AS \"set_config\"",
		"SELECT 1 AS set_config",
		"SELECT jsonb_set('{}'::jsonb, '{a}', '1')",
		"SELECT setval('orders_id_seq', 1000)",
		"SELECT set_masklen('192.168.1.5/24'::inet, 16)",
		"SELECT set_byte('\\x1234'::bytea, 0, 255)",
		"SELECT set_bit('\\x1234'::bytea, 0, 1)",
		"SELECT setseed(0.5)",
		"SELECT * FROM users u JOIN datasets d ON d.id = u.id WHERE u.settings ? 'beta'",
		"SELECT sunset, onset FROM (SELECT 1 AS sunset, 2 AS onset) t",
		"SELECT \"Settings\" FROM (SELECT 1 AS \"Settings\") t",
		"SELECT t.subset FROM (SELECT 1 AS subset) t",
		"SELECT count(*) FROM app_settings",
		"SELECT * FROM pg_catalog.pg_tables WHERE tablename = 'settings'",
		"SELECT x FROM generate_series(1, 3) x OFFSET 1",
		"SELECT 1 FROM users OFFSET $1 LIMIT $2",
		"SELECT set_name FROM (SELECT 'a' AS set_name) s",
		"SELECT name AS reset FROM users LIMIT 1",
	} {
		s.add("synth-n2", q, C, "N2", "", mixFor(q))
	}
	for _, q := range []string{
		"SELECT 'SET search_path = tenant_1'",
		"INSERT INTO audit_log (msg) VALUES ('SET ROLE admin')",
		"SELECT 1 /* SET search_path = x */",
		"SELECT 1 -- SET work_mem = '1GB'",
		"SELECT $$SET search_path TO evil$$",
		"SELECT E'\\nSET x = 1'",
		"SELECT 'set_config(''app.tenant_id'', ''1'', false)'",
		"SELECT 1 /* set_config('app.tenant_id', '1', false) */",
		"/* UPDATE pg_settings SET setting = 1 */ SELECT 1",
		"SELECT 'UPDATE pg_settings SET setting = ''1'''",
		"/*dddbs='settings-api',dde='prod',ddps='settings',ddpv='2.1.0',traceparent='00-0000000000000000c0ffee1234567890-1234567890abcdef-01'*/ SELECT id FROM users WHERE id = 1",
		"SELECT id FROM users WHERE id = 1 /*controller='settings',action='reset',framework='rails'*/",
		"SELECT 'DO $$ BEGIN SET x = 1; END $$'",
		"SELECT U&'SET \\0073earch_path'",
		"COMMENT ON TABLE users IS 'SET search_path before use'",
		"SELECT $tag$ SET ROLE admin $tag$",
		"SELECT B'101' AS set",
	} {
		s.add("synth-n3", q, C, "N3", "", mixFor(q))
	}
	for _, q := range []string{
		"CREATE OR REPLACE FUNCTION f_sp() RETURNS int LANGUAGE sql SET search_path = public AS $$ SELECT 1 $$",
		"CREATE OR REPLACE FUNCTION f_body() RETURNS void LANGUAGE plpgsql AS $$ BEGIN SET LOCAL work_mem = '8MB'; END $$",
		"CREATE OR REPLACE FUNCTION f_body2() RETURNS void LANGUAGE plpgsql AS $$ BEGIN PERFORM set_config('app.x', '1', false); END $$",
		"ALTER FUNCTION f_noop() SET search_path = public",
		"ALTER ROLE app_reader SET statement_timeout = '5s'",
		"ALTER ROLE app_reader IN DATABASE scratch SET work_mem = '8MB'",
		"ALTER DATABASE scratch SET work_mem = '16MB'",
		"ALTER SYSTEM SET work_mem = '4MB'",
		"ALTER TABLE scratch_t SET (fillfactor = 70)",
		"ALTER TABLE scratch_t ALTER COLUMN v SET DEFAULT 0",
		"ALTER TABLE scratch_t ALTER COLUMN v SET STATISTICS 100",
		"ALTER TABLE scratch_t SET UNLOGGED",
		"ALTER TABLE scratch_t SET LOGGED",
		"ALTER TABLE scratch_move SET SCHEMA tenant_1",
		"ALTER SEQUENCE orders_id_seq SET SCHEMA public",
		"ALTER INDEX scratch_idx SET (fillfactor = 80)",
		"ALTER VIEW scratch_v SET (security_barrier = true)",
		"CREATE OR REPLACE VIEW scratch_v2 AS SELECT set_config('app.x', '1', false) AS c",
	} {
		lab := C
		if strings.Contains(q, "VIEW") && strings.Contains(q, "set_config") {
			lab = E
		}
		s.add("synth-n4", q, lab, "N4", "DDL with SET, not a session setting", "")
	}
}

func mixFor(q string) string {
	l := strings.ToLower(strings.TrimSpace(q))
	for strings.HasPrefix(l, "/*") {
		i := strings.Index(l, "*/")
		if i < 0 {
			break
		}
		l = strings.TrimSpace(l[i+2:])
	}
	switch {
	case strings.HasPrefix(l, "select"), strings.HasPrefix(l, "with"), strings.HasPrefix(l, "(select"):
		return "select"
	case strings.HasPrefix(l, "update"), strings.HasPrefix(l, "merge"),
		strings.HasPrefix(l, "insert") && strings.Contains(l, "do update"):
		return "update"
	case strings.HasPrefix(l, "insert"):
		return "insert"
	}
	return "other"
}

// drivers covers connection bootstrap and framework statements.
func (s *synth) drivers() {
	dirtyQ := []struct{ q, note string }{
		{"SET extra_float_digits = 3", "pgjdbc"},
		{"SET application_name = 'PostgreSQL JDBC Driver'", "pgjdbc"},
		{"SET client_min_messages TO 'warning'", "Rails"},
		{"SET standard_conforming_strings = on", "Rails"},
		{"SET intervalstyle = iso_8601", "Rails"},
		{"SET SESSION timezone TO 'UTC'", "Rails"},
		{"SET SESSION statement_timeout TO 5000", "Rails variables:"},
		{"SET TIME ZONE 'UTC'", "Django/psycopg"},
		{"SET TIME ZONE 'America/Chicago'", "Django"},
		{"SET default_transaction_isolation TO 'repeatable read'", "psycopg2 set_session"},
		{"SET default_transaction_read_only TO 'on'", "psycopg2 set_session"},
		{"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED", "SQLAlchemy"},
		{"SET search_path TO \"tenant1\", \"public\"", "Rails apartment"},
		{"SET search_path = 'tenant1','public'", "django-tenants"},
		{"SET search_path TO tenant_1,public", "Hibernate multitenancy"},
		{"SET app.current_tenant = '9'", "RLS middleware"},
		{"SELECT set_config('app.current_tenant', $1, false)", "RLS middleware, prepared"},
		{"SET ROLE app_reader", "pool role switch"},
		{"SET datestyle TO 'ISO'", "Npgsql / libpq"},
		{"SET client_encoding TO 'UTF8'", "psycopg"},
		{"SET lock_timeout = '1s'", "migrations (strong_migrations)"},
		{"SET statement_timeout = '30s'", "migrations"},
	}
	for _, d := range dirtyQ {
		cat := "P1"
		if strings.Contains(d.q, "set_config") {
			cat = "P2"
		}
		s.add("synth-driver", d.q, D, cat, d.note, "other")
	}
	eitherQ := []struct{ q, note string }{
		{"DISCARD ALL", "Npgsql / PgBouncer reset"},
		{"RESET ALL", "pool reset"},
		{"SET LOCAL ROLE authenticated", "Supabase"},
		{"SET LOCAL \"hasura.user\" = '{\"x-hasura-role\":\"user\"}'", "Hasura"},
	}
	for _, d := range eitherQ {
		cat := "N5"
		lab := E
		if strings.HasPrefix(d.q, "SET LOCAL") {
			cat, lab = "N4", C
		}
		s.add("synth-driver", d.q, lab, cat, d.note, "other")
	}
	cleanQ := []struct{ q, note string }{
		{"SELECT t.oid, t.typname, t.typelem, t.typdelim, t.typinput, r.rngsubtype, t.typtype, t.typbasetype FROM pg_type as t LEFT JOIN pg_range as r ON oid = rngtypid WHERE t.typname IN ('int2', 'int4', 'int8', 'oid', 'float4', 'float8', 'numeric', 'bool', 'timestamp', 'timestamptz')", "Rails type map"},
		{"SHOW TIME ZONE", "Rails"},
		{"SELECT current_setting('server_version_num')::integer", "Prisma"},
		{"SELECT version()", "many"},
		{"SELECT 1", "health check"},
		{"-- ping", "pgx ShouldPing"},
		{"select nextval ('hibernate_sequence')", "Hibernate"},
		{"SELECT count(*) FROM information_schema.tables WHERE table_schema = CURRENT_SCHEMA() AND table_name = $1 AND table_type = $2", "GORM HasTable"},
		{"SELECT CURRENT_DATABASE()", "GORM"},
		{"SELECT \"users\".* FROM \"users\" WHERE \"users\".\"id\" = $1 LIMIT $2", "Rails"},
		{"SELECT \"django_session\".\"session_key\", \"django_session\".\"session_data\", \"django_session\".\"expire_date\" FROM \"django_session\" WHERE (\"django_session\".\"expire_date\" > $1 AND \"django_session\".\"session_key\" = $2) LIMIT 21", "Django"},
		{"SELECT \"public\".\"User\".\"id\", \"public\".\"User\".\"email\" FROM \"public\".\"User\" WHERE \"public\".\"User\".\"id\" = $1 LIMIT $2 OFFSET $3", "Prisma"},
		{"select u1_0.id,u1_0.email,u1_0.name from users u1_0 where u1_0.id=$1", "Hibernate 6"},
		{"-- name: GetUser :one\nSELECT id, email FROM users WHERE id = $1 LIMIT 1", "sqlc"},
		{"-- name: UpdateUserSettings :exec\nUPDATE users SET settings = $2 WHERE id = $1", "sqlc"},
		{"select oid, typname from pg_type where typname = any($1)", "pgx LoadTypes"},
		{"BEGIN ISOLATION LEVEL READ COMMITTED", "pgx BeginTx"},
		{"BEGIN READ ONLY", "pgx"},
		{"COMMIT", ""},
		{"ROLLBACK", ""},
		{"SAVEPOINT active_record_1", "Rails"},
		{"RELEASE SAVEPOINT active_record_1", "Rails"},
		{"SELECT pg_try_advisory_lock($1)", "migrations"},
		{"SELECT pg_advisory_unlock($1)", "migrations"},
		{"LISTEN events", ""},
		{"SELECT \"schema_migrations\".\"version\" FROM \"schema_migrations\" ORDER BY \"schema_migrations\".\"version\" ASC", "Rails"},
		{"SELECT a.attname, format_type(a.atttypid, a.atttypmod), pg_get_expr(d.adbin, d.adrelid), a.attnotnull, a.atttypid, a.atttypmod, c.collname, col_description(a.attrelid, a.attnum) AS comment, attidentity AS identity, attgenerated as attgenerated FROM pg_attribute a LEFT JOIN pg_attrdef d ON a.attrelid = d.adrelid AND a.attnum = d.adnum LEFT JOIN pg_type t ON a.atttypid = t.oid LEFT JOIN pg_collation c ON a.attcollation = c.oid AND a.attcollation <> t.typcollation WHERE a.attrelid = '\"users\"'::regclass AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum", "Rails columns"},
	}
	for _, d := range cleanQ {
		cat := "N7"
		if strings.Contains(strings.ToLower(d.q), "set") {
			cat = "N2"
			if strings.Contains(d.q, "UPDATE") {
				cat = "N1"
			}
		}
		if strings.Contains(d.q, "current_setting") {
			cat = "N6"
		}
		opt := func(*sd.Case) {}
		if strings.Contains(d.q, "django_session") || strings.Contains(d.q, "schema_migrations") || strings.Contains(d.q, "\"User\"") || strings.Contains(d.q, "hibernate_sequence") {
			opt = noExec
		}
		s.add("synth-driver", d.q, C, cat, d.note, mixFor(d.q), opt)
	}
}

// oltp synthesizes application traffic. Text is deterministic (fixed seed).
func (s *synth) oltp(n int) {
	r := rand.New(rand.NewSource(20261116))
	comment := func() string {
		switch r.Intn(5) {
		case 0:
			return fmt.Sprintf("/*dddbs='%s',dde='prod',ddps='%s',ddpv='1.%d.0',traceparent='00-%032x-%016x-01'*/ ",
				pick(r, "orders-api", "settings-svc", "asset-store", "dataset-sync"), pick(r, "web", "worker"), r.Intn(20), r.Uint64(), r.Uint64())
		case 1:
			return ""
		}
		return ""
	}
	suffix := func() string {
		if r.Intn(6) == 0 {
			return fmt.Sprintf(" /*controller='%s',action='%s',framework='rails'*/", pick(r, "users", "orders", "settings", "assets"), pick(r, "show", "index", "update", "reset"))
		}
		return ""
	}
	inList := func(k int) string {
		ids := make([]string, k)
		for i := range ids {
			ids[i] = fmt.Sprint(r.Intn(100000))
		}
		return strings.Join(ids, ", ")
	}
	param := r.Intn(2) == 0
	v := func(lit string, i int) string {
		if param {
			return fmt.Sprintf("$%d", i)
		}
		return lit
	}
	selects := []func() string{
		func() string { return "SELECT id, email, name FROM users WHERE id = " + v(fmt.Sprint(r.Intn(1000)), 1) },
		func() string {
			return "SELECT u.id, u.name, o.total FROM users u JOIN orders o ON o.user_id = u.id WHERE u.id = " + v(fmt.Sprint(r.Intn(1000)), 1) + " ORDER BY o.created_at DESC LIMIT 20"
		},
		func() string { return "SELECT * FROM orders WHERE id IN (" + inList(1+r.Intn(30)) + ")" },
		func() string {
			return fmt.Sprintf("SELECT id, status FROM orders WHERE user_id = %s ORDER BY id LIMIT %d OFFSET %d", v("1", 1), 10+r.Intn(40), r.Intn(500))
		},
		func() string {
			return "SELECT asset_id, dataset_id, charset FROM assets WHERE dataset_id = " + v(fmt.Sprint(r.Intn(50)), 1)
		},
		func() string { return "SELECT settings FROM users WHERE id = " + v("2", 1) },
		func() string {
			return "SELECT count(*) FROM orders WHERE status = " + v("'open'", 1) + " AND created_at > now() - interval '7 days'"
		},
		func() string {
			return "WITH recent AS (SELECT user_id, sum(total) AS t FROM orders WHERE created_at > now() - interval '30 days' GROUP BY user_id) SELECT u.email, r.t FROM recent r JOIN users u ON u.id = r.user_id ORDER BY r.t DESC LIMIT 10"
		},
		func() string {
			return "SELECT d.id, d.name FROM datasets d WHERE d.settings ->> 'visibility' = " + v("'public'", 1)
		},
		func() string {
			return "SELECT id FROM users WHERE email = " + v("'a@example.com'", 1) + " AND reset_at IS NULL"
		},
		func() string {
			return "SELECT o.id FROM orders o WHERE EXISTS (SELECT 1 FROM users u WHERE u.id = o.user_id AND u.status = 'active') LIMIT 50"
		},
		func() string { return "SELECT 1" },
		func() string { return "SELECT now()" },
		func() string { return "SELECT key, value FROM app_settings WHERE key = " + v("'theme'", 1) },
		func() string {
			return "SELECT e.payload->>'kind' AS kind, count(*) FROM events e WHERE e.ts > now() - interval '1 hour' GROUP BY 1 ORDER BY 2 DESC"
		},
	}
	updates := []func() string{
		func() string { return "UPDATE users SET name = " + v("'Bo'", 1) + " WHERE id = " + v("3", 2) },
		func() string {
			return "UPDATE orders SET status = " + v("'paid'", 1) + ", updated_at = now() WHERE id = " + v("4", 2) + " RETURNING id, status"
		},
		func() string {
			return "UPDATE users SET settings = settings || '{\"beta\": true}' WHERE id = " + v("5", 1)
		},
		func() string {
			return "UPDATE accounts SET balance = balance - " + v("10", 1) + " WHERE id = " + v("1", 2)
		},
		func() string {
			return "INSERT INTO app_settings (key, value) VALUES (" + v("'k1'", 1) + ", " + v("'v'", 2) + ") ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"
		},
		func() string {
			return "UPDATE assets SET charset = 'utf8', reset_at = now() WHERE asset_id = " + v("11", 1)
		},
	}
	inserts := []func() string{
		func() string { return "INSERT INTO audit_log (msg) VALUES (" + v("'login'", 1) + ")" },
		func() string {
			return fmt.Sprintf("INSERT INTO orders (user_id, total, status) VALUES (%s, %s, 'open') RETURNING id", v("1", 1), v("9.99", 2))
		},
		func() string {
			return "INSERT INTO events (payload) VALUES ('{\"kind\": \"click\"}'), ('{\"kind\": \"view\"}')"
		},
		func() string {
			return "INSERT INTO app_settings (key, value) VALUES (" + v("'x'", 1) + ", 'y') ON CONFLICT DO NOTHING"
		},
		func() string {
			return "INSERT INTO assets (dataset_id, charset, name) VALUES (1, 'latin1', 'logo.png') RETURNING asset_id"
		},
	}
	others := []func() string{
		func() string { return "DELETE FROM audit_log WHERE id = " + v("-1", 1) },
		func() string { return "BEGIN" },
		func() string { return "COMMIT" },
		func() string { return "ROLLBACK" },
		func() string { return "SELECT pg_advisory_xact_lock(" + v("42", 1) + ")" },
		func() string { return "DELETE FROM events WHERE ts < now() - interval '90 days'" },
		func() string { return "TRUNCATE scratch_t" },
	}
	for i := 0; i < n; i++ {
		param = r.Intn(3) == 0
		var q, mix string
		switch x := r.Intn(100); {
		case x < 70:
			q, mix = selects[r.Intn(len(selects))](), "select"
		case x < 85:
			q, mix = updates[r.Intn(len(updates))](), "update"
		case x < 95:
			q, mix = inserts[r.Intn(len(inserts))](), "insert"
		default:
			q, mix = others[r.Intn(len(others))](), "other"
		}
		q = comment() + q + suffix()
		lab, ok := labelSQL(q)
		if !ok {
			panic("synthesized OLTP text does not parse: " + q)
		}
		s.add("synth-oltp", q, C, lab.Category, "", mix)
	}
}

func pick(r *rand.Rand, xs ...string) string { return xs[r.Intn(len(xs))] }
