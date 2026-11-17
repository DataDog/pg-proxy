// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

//go:build integration && oracle

package setdetect

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/testcontainers/testcontainers-go"

	"github.com/DataDog/pg-proxy/internal/itest"
)

//go:embed oracle_fixture.sql
var oracleFixture string

// OracleResult is what running one case did to session state.
type OracleResult struct {
	ID string `json:"id"`
	// Changed lists the session state that differs after the statement:
	// pg_settings names with source = 'session', current_user,
	// session_user, and custom:<name> placeholders.
	Changed []string `json:"changed,omitempty"`
	// Reported lists ParameterStatus keys whose value changed, which is what
	// PgBouncer-style tracking sees.
	Reported []string `json:"reported,omitempty"`
	Error    string   `json:"error,omitempty"`
}

// reportedKeys are the GUC_REPORT settings of PG14 to PG18.
var reportedKeys = []string{
	"application_name", "client_encoding", "DateStyle", "default_transaction_read_only",
	"in_hot_standby", "integer_datetimes", "IntervalStyle", "is_superuser", "scram_iterations",
	"search_path", "server_encoding", "server_version", "session_authorization",
	"standard_conforming_strings", "TimeZone",
}

// customNameRE finds custom setting names: dotted names after SET [SESSION|LOCAL]
// or as set_config's first argument, and dotted words elsewhere.
var customNameRE = regexp.MustCompile(`(?i)(?:\bset\s+(?:session\s+|local\s+)?"?|set_config\s*\(\s*'|\b)((?:[a-z_][a-z0-9_$]*)(?:\.[a-z_][a-z0-9_$]*)+)`)

// TestOracle runs the executable corpus on a real server and writes
// corpus/oracle-<tag>.jsonl. Run it once per server version:
//
//	ORACLE_IMAGE=<image> ORACLE_TAG=pg16 ORACLE_REGRESS=<postgresql>/src/test/regress \
//	    go test -tags integration,oracle -run TestOracle -timeout 60m ./internal/setdetect
func TestOracle(t *testing.T) {
	image, tag := os.Getenv("ORACLE_IMAGE"), os.Getenv("ORACLE_TAG")
	if image == "" || tag == "" {
		t.Skip("set ORACLE_IMAGE and ORACLE_TAG")
	}
	cases, err := LoadCorpus("corpus/corpus.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	// Trust auth: a fresh session per case, without a SCRAM exchange each.
	pg, err := itest.StartPostgres(ctx, itest.WithImage(image),
		itest.WithContainerOptions(testcontainers.WithEnv(map[string]string{"POSTGRES_HOST_AUTH_METHOD": "trust"})))
	if err != nil {
		t.Fatal(err)
	}
	defer pg.Terminate(context.Background())

	customs := customNames(cases)
	snap := snapshotSQL(customs)
	admin := connect(t, pg.URL("postgres"))
	for _, stmt := range []string{"CREATE DATABASE fixture", "CREATE DATABASE regression", "CREATE DATABASE scratch"} {
		mustExec(t, admin, stmt)
	}
	admin.Close(ctx)
	fix := connect(t, pg.URL("fixture"))
	mustExec(t, fix, oracleFixture)
	fix.Close(ctx)

	// ORACLE_INCREMENTAL=1 keeps the existing file's rows and runs only
	// non-regression cases it lacks.
	var results []OracleResult
	have := map[string]bool{}
	incremental := os.Getenv("ORACLE_INCREMENTAL") != ""
	if incremental {
		old, err := LoadOracleFile(filepath.Join("corpus", "oracle-"+tag+".jsonl"))
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range old {
			if strings.HasPrefix(r.Error, "context: ") {
				continue // run again
			}
			have[r.ID] = true
			results = append(results, r)
		}
	}
	regress := map[string]map[int]string{} // file -> statement index -> case ID
	var single []Case
	for _, c := range cases {
		if !c.Exec {
			continue
		}
		if file, idx, ok := regressRef(c.Source); ok {
			if regress[file] == nil {
				regress[file] = map[int]string{}
			}
			regress[file][idx] = c.ID
			continue
		}
		if !have[c.ID] {
			single = append(single, c)
		}
	}
	// Cases run in their own sessions, so they can run in parallel, except
	// DDL that changes the fixture: those run alone, afterwards.
	var mu sync.Mutex
	work := make(chan Case)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for c := range work {
				r := runOne(t, pg.URL("fixture"), c, snap)
				mu.Lock()
				results = append(results, r)
				if len(results)%1000 == 0 {
					t.Logf("%d cases run", len(results))
				}
				mu.Unlock()
			}
		}()
	}
	var serial []Case
	for _, c := range single {
		if ddlRE.MatchString(c.SQL) {
			serial = append(serial, c)
			continue
		}
		work <- c
	}
	close(work)
	wg.Wait()
	for _, c := range serial {
		results = append(results, runOne(t, pg.URL("fixture"), c, snap))
	}
	if dir := os.Getenv("ORACLE_REGRESS"); dir != "" && !incremental {
		results = append(results, runRegress(t, pg.URL("regression"), dir, regress, snap)...)
	}
	sort.Slice(results, func(i, j int) bool { return results[i].ID < results[j].ID })
	if err := WriteJSONL(filepath.Join("corpus", "oracle-"+tag+".jsonl"), results); err != nil {
		t.Fatal(err)
	}
	changed := 0
	for _, r := range results {
		if len(r.Changed) > 0 {
			changed++
		}
	}
	t.Logf("%s: ran %d cases, %d changed session state", tag, len(results), changed)
}

// LoadOracleFile reads an oracle JSONL file.
func LoadOracleFile(path string) ([]OracleResult, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var out []OracleResult
	for _, line := range strings.Split(string(data), "\n") {
		if line == "" {
			continue
		}
		var r OracleResult
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, nil
}

func regressRef(source string) (string, int, bool) {
	rest, ok := strings.CutPrefix(source, "pg-regress:")
	if !ok {
		return "", 0, false
	}
	file, idx, ok := strings.Cut(rest, "#")
	if !ok {
		return "", 0, false
	}
	var n int
	if _, err := fmt.Sscan(idx, &n); err != nil {
		return "", 0, false
	}
	return file, n, true
}

func customNames(cases []Case) []string {
	set := map[string]bool{"app.last_audit": true}
	for _, c := range cases {
		for _, m := range customNameRE.FindAllStringSubmatch(c.SQL, -1) {
			name := strings.ToLower(m[1])
			// Skip schema-qualified objects; anything else dotted is a
			// possible placeholder, and an extra name costs nothing.
			if strings.HasPrefix(name, "pg_catalog.") || strings.HasPrefix(name, "public.") || strings.HasPrefix(name, "information_schema.") {
				continue
			}
			set[name] = true
		}
	}
	names := make([]string, 0, len(set))
	for n := range set {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// snapshotSQL reads the session state the reset restores, one row per item.
// A custom placeholder that a SET LOCAL created keeps the value ” after the
// transaction, and RESET ALL leaves it so too; ” therefore counts as unset.
// The transaction_* settings are left out: every transaction derives them
// from default_transaction_*, and SET TRANSACTION in autocommit leaves them
// marked source = 'session' with the default value.
func snapshotSQL(customs []string) string {
	quoted := make([]string, len(customs))
	for i, n := range customs {
		quoted[i] = "'" + n + "'"
	}
	return `SELECT name, setting FROM pg_catalog.pg_settings WHERE source = 'session'
  AND name NOT IN ('transaction_isolation', 'transaction_read_only', 'transaction_deferrable')
UNION ALL SELECT 'current_user', current_user::text
UNION ALL SELECT 'session_user', session_user::text
UNION ALL SELECT 'custom:' || n, COALESCE(NULLIF(pg_catalog.current_setting(n, true), ''), '<unset>')
  FROM pg_catalog.unnest(ARRAY[` + strings.Join(quoted, ",") + `]::text[]) n`
}

type state struct {
	rows     map[string]string
	reported map[string]string
}

func snapshot(ctx context.Context, conn *pgconn.PgConn, snap string) (state, error) {
	s := state{rows: map[string]string{}, reported: map[string]string{}}
	res := conn.ExecParams(ctx, snap, nil, nil, nil, nil).Read()
	if res.Err != nil {
		return s, res.Err
	}
	for _, row := range res.Rows {
		s.rows[string(row[0])] = string(row[1])
	}
	for _, k := range reportedKeys {
		s.reported[k] = conn.ParameterStatus(k)
	}
	return s, nil
}

func diffState(a, b state) (changed, reported []string) {
	for k, v := range b.rows {
		if a.rows[k] != v {
			changed = append(changed, k)
		}
	}
	for k := range a.rows {
		if _, ok := b.rows[k]; !ok {
			changed = append(changed, k)
		}
	}
	for _, k := range reportedKeys {
		if a.reported[k] != b.reported[k] {
			reported = append(reported, k)
		}
	}
	sort.Strings(changed)
	return changed, reported
}

var paramRE = regexp.MustCompile(`\$(\d+)`)

// execute runs sql as one Query message, or as Parse/Bind/Execute with text
// parameters "1" when it has $n placeholders.
func execute(ctx context.Context, conn *pgconn.PgConn, sql string) error {
	if ms := paramRE.FindAllStringSubmatch(sql, -1); len(ms) > 0 && !strings.Contains(sql, "$$") {
		n := 0
		for _, m := range ms {
			var i int
			fmt.Sscan(m[1], &i)
			n = max(n, i)
		}
		params := make([][]byte, n)
		for i := range params {
			params[i] = []byte("1")
		}
		return conn.ExecParams(ctx, sql, params, nil, nil, nil).Read().Err
	}
	if strings.Contains(strings.ToLower(sql), "to stdout") {
		_, err := conn.CopyTo(ctx, discard{}, sql)
		return err
	}
	_, err := conn.Exec(ctx, sql).ReadAll()
	return err
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }

func runOne(t *testing.T, url string, c Case, snap string) OracleResult {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	r := OracleResult{ID: c.ID}
	conn, err := pgconn.Connect(ctx, url)
	if err != nil {
		t.Fatalf("%s: connect: %v", c.ID, err)
	}
	defer conn.Close(context.Background())
	pre := append(append([]string{}, preload...), c.Context...)
	if c.SCSOff {
		pre = append([]string{"SET standard_conforming_strings = off"}, pre...)
	}
	for _, p := range pre {
		// Context statements (LOAD, PREPARE ... AS ... $1) always go as a
		// plain Query: a $n there is the prepared statement's parameter.
		if _, err := conn.Exec(ctx, p).ReadAll(); err != nil {
			r.Error = "context: " + err.Error()
			return r
		}
	}
	before, err := snapshot(ctx, conn, snap)
	if err != nil {
		t.Fatalf("%s: snapshot: %v", c.ID, err)
	}
	if err := execute(ctx, conn, c.SQL); err != nil {
		r.Error = err.Error()
	}
	finishTransaction(ctx, conn)
	after, err := snapshot(ctx, conn, snap)
	if err != nil {
		r.Error = strings.TrimSpace(r.Error + "; snapshot after: " + err.Error())
		return r
	}
	r.Changed, r.Reported = diffState(before, after)
	return r
}

var ddlRE = regexp.MustCompile(`(?i)^\s*(create|alter|drop|truncate|comment)\b`)

// preload loads the libraries whose first use defines settings
// (plpgsql.*, pg_trgm.similarity_threshold), so that a statement which
// merely loads one does not look like a change.
var preload = []string{"LOAD 'plpgsql'", "LOAD 'pg_trgm'"}

var copyStdinRE = regexp.MustCompile(`(?is)^\s*(?:--[^\n]*\n\s*)*copy\b.*\bfrom\s+stdin\b`)

var txControl = regexp.MustCompile(`(?i)^(begin|start\s+transaction|commit|end|rollback|abort|savepoint|release|prepare\s+transaction)\b`)

// isTxControl matches transaction control after leading comments.
func isTxControl(stmt string) bool {
	p := pgsqlSkipLead(stmt)
	return p >= 0 && txControl.MatchString(stmt[p:])
}

// finishTransaction commits a transaction the statement left open, as the
// client would before the backend goes back to the pool: the state that
// matters is the state at release. A failed transaction rolls back.
func finishTransaction(ctx context.Context, conn *pgconn.PgConn) {
	if conn.IsClosed() || conn.TxStatus() == 'I' {
		return
	}
	_, _ = conn.Exec(ctx, "COMMIT").ReadAll()
}

// runRegress runs every regression script in schedule order, one session
// per script, statement by statement in autocommit. Transaction control is
// skipped so every statement's effect is its autocommit effect. Corpus
// statements are bracketed by snapshots.
func runRegress(t *testing.T, url, dir string, want map[string]map[int]string, snap string) []OracleResult {
	names, err := RegressSchedule(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []OracleResult
	for _, name := range names {
		file := name + ".sql"
		data, err := os.ReadFile(filepath.Join(dir, "sql", file))
		if err != nil {
			continue
		}
		t.Logf("regress %s", file)
		ids := want[file]
		var conn *pgconn.PgConn
		reconnect := func() {
			if conn != nil {
				conn.Close(context.Background())
			}
			conn, err = pgconn.Connect(context.Background(), url)
			if err != nil {
				t.Fatalf("%s: connect: %v", file, err)
			}
			for _, p := range preload {
				if err := execute(context.Background(), conn, p); err != nil {
					t.Fatalf("%s: %s: %v", file, p, err)
				}
			}
		}
		reconnect()
		for i, stmt := range RegressStatements(string(data)) {
			id, inCorpus := ids[i]
			if UsesPsqlVar(stmt) || isTxControl(stmt) || copyStdinRE.MatchString(stmt) {
				// COPY FROM STDIN would wait for the data psql sends, which
				// RegressStatements dropped; it cannot change settings.
				if inCorpus {
					out = append(out, OracleResult{ID: id, Error: "skipped: transaction control or COPY FROM STDIN"})
				}
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			var before state
			if inCorpus {
				before, err = snapshot(ctx, conn, snap)
				if err != nil {
					// The session is unusable (for example a role without
					// access); start a new one.
					reconnect()
					before, err = snapshot(ctx, conn, snap)
				}
			}
			execErr := execute(ctx, conn, stmt)
			finishTransaction(ctx, conn)
			if inCorpus {
				r := OracleResult{ID: id}
				if execErr != nil {
					r.Error = execErr.Error()
				}
				if err == nil {
					after, aerr := snapshot(ctx, conn, snap)
					if aerr == nil {
						r.Changed, r.Reported = diffState(before, after)
					} else {
						r.Error = strings.TrimSpace(r.Error + "; snapshot after: " + aerr.Error())
					}
				}
				out = append(out, r)
			}
			cancel()
			if conn.IsClosed() || errors.Is(execErr, context.DeadlineExceeded) {
				reconnect()
			}
		}
		conn.Close(context.Background())
	}
	return out
}

func connect(t *testing.T, url string) *pgconn.PgConn {
	conn, err := pgconn.Connect(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	return conn
}

func mustExec(t *testing.T, conn *pgconn.PgConn, sql string) {
	if _, err := conn.Exec(context.Background(), sql).ReadAll(); err != nil {
		t.Fatalf("%.80s: %v", sql, err)
	}
}

// TestServerReference measures, against a local container, the round trips
// the detectors are weighed against: the release reset and the
// pg_settings probe (a reference point, not a candidate). Writes
// results/server-reference.json.
//
//	ORACLE_IMAGE=<image> go test -tags integration,oracle -run TestServerReference ./internal/setdetect
func TestServerReference(t *testing.T) {
	image := os.Getenv("ORACLE_IMAGE")
	if image == "" || os.Getenv("ORACLE_TAG") != "" {
		t.Skip("set ORACLE_IMAGE only")
	}
	ctx := context.Background()
	pg, err := itest.StartPostgres(ctx, itest.WithImage(image),
		itest.WithContainerOptions(testcontainers.WithEnv(map[string]string{"POSTGRES_HOST_AUTH_METHOD": "trust"})))
	if err != nil {
		t.Fatal(err)
	}
	defer pg.Terminate(context.Background())
	conn := connect(t, pg.URL("postgres"))
	defer conn.Close(ctx)
	probe := snapshotSQL([]string{"app.tenant_id", "app.user_id"})
	type row struct {
		Query    string  `json:"query"`
		MedianUS float64 `json:"median_us"`
		P99US    float64 `json:"p99_us"`
	}
	var rows []row
	for _, q := range []struct{ name, sql string }{
		{"SELECT 1", "SELECT 1"},
		{"RESET ALL", "RESET ALL"},
		{"RESET SESSION AUTHORIZATION; RESET ROLE; RESET ALL", "RESET SESSION AUTHORIZATION; RESET ROLE; RESET ALL"},
		{"verify probe (pg_settings + users + 2 custom)", probe},
	} {
		run := func() {
			if _, err := conn.Exec(ctx, q.sql).ReadAll(); err != nil {
				t.Fatal(err)
			}
		}
		for i := 0; i < 500; i++ {
			run()
		}
		samples := make([]float64, 5000)
		for i := range samples {
			t0 := time.Now()
			run()
			samples[i] = float64(time.Since(t0).Nanoseconds()) / 1000
		}
		sort.Float64s(samples)
		rows = append(rows, row{q.name, samples[len(samples)/2], samples[len(samples)*99/100]})
		t.Logf("%-55s median %6.1f µs  p99 %6.1f µs", q.name, samples[len(samples)/2], samples[len(samples)*99/100])
	}
	data, _ := json.MarshalIndent(rows, "", "  ")
	if err := os.WriteFile("results/server-reference.json", append(data, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}
