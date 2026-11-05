//go:build integration

// Package integration checks client-visible conversations against PostgreSQL and PgProxy.
package integration

import (
	"bytes"
	"context"
	"embed"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	pgproxy "github.com/DataDog/pg-proxy"
	"github.com/DataDog/pg-proxy/internal/protocoltest"
)

//go:embed testdata/*.json
var corpus embed.FS

var replayFile = flag.String("protocol-replay", "", "replay the exact scenario and configuration saved in report.json")

func scenarios(t *testing.T) []protocoltest.Scenario {
	t.Helper()
	paths, err := corpus.ReadDir("testdata")
	if err != nil {
		t.Fatal(err)
	}
	var result []protocoltest.Scenario
	names := map[string]bool{}
	for _, path := range paths {
		data, err := corpus.ReadFile("testdata/" + path.Name())
		if err != nil {
			t.Fatal(err)
		}
		scenario, err := protocoltest.DecodeScenario(data)
		if err != nil {
			t.Fatalf("%s: %v", path.Name(), err)
		}
		if names[scenario.Name] {
			t.Fatalf("duplicate scenario %q", scenario.Name)
		}
		names[scenario.Name] = true
		result = append(result, scenario)
	}
	return result
}

func TestCorpus(t *testing.T) {
	if n := len(scenarios(t)); n != 10 {
		t.Fatalf("expected ten reviewed conversations, got %d", n)
	}
}

func TestDifferential(t *testing.T) {
	if *replayFile != "" {
		saved, err := loadReplay(*replayFile)
		if err != nil {
			t.Fatal(err)
		}
		pgMetadata, _ := saved.Metadata["postgres"].(map[string]any)
		image, _ := pgMetadata["postgres_image"].(string)
		if image == "external disposable server" {
			if os.Getenv("PGPROXY_TEST_ADMIN_DSN") == "" {
				t.Fatal("replay requires the disposable external server options recorded in the report")
			}
		} else if image != "" {
			if os.Getenv("PGPROXY_TEST_ADMIN_DSN") != "" {
				t.Fatal("clear PGPROXY_TEST_ADMIN_DSN to replay the saved container fixture")
			}
			t.Setenv("PGPROXY_TEST_POSTGRES_IMAGE", image)
		}
		pg := startPostgres(t)
		if expected, ok := pgMetadata["postgres_version"].(string); ok && pg.Metadata["postgres_version"] != expected {
			t.Fatalf("replay PostgreSQL version differs: got %v, want %s", pg.Metadata["postgres_version"], expected)
		}
		t.Run(saved.Scenario.Name, func(t *testing.T) { testConversation(t, pg, saved.Scenario, saved.BufferSize, saved.WriteChunkSize) })
		return
	}
	cases := scenarios(t)
	pg := startPostgres(t)
	for _, scenario := range cases {
		t.Run(scenario.Name, func(t *testing.T) {
			for _, variant := range []struct{ buffer, chunk int }{{64, 0}, {64, 7}, {4096, 0}, {4096, 7}} {
				t.Run(fmt.Sprintf("buffer_%d_chunk_%d", variant.buffer, variant.chunk), func(t *testing.T) {
					testConversation(t, pg, scenario, variant.buffer, variant.chunk)
				})
			}
		})
	}
}

// Buffer boundaries are explicit cases, not a randomized retry strategy.
func TestBufferBoundaries(t *testing.T) {
	pg := startPostgres(t)
	for _, size := range []int{64, 4096} {
		for _, delta := range []int{-1, 0, 1} {
			// DataRow header, column count and one field length consume eleven bytes.
			payload := size + delta - 11
			value := hex.EncodeToString(bytes.Repeat([]byte("x"), payload))
			scenario := protocoltest.Scenario{Version: 1, Name: fmt.Sprintf("buffer_%d_%s", size, map[int]string{-1: "below", 0: "equal", 1: "above"}[delta]), Steps: []protocoltest.Step{
				{Send: []protocoltest.Message{{Type: "Query", SQL: fmt.Sprintf("SELECT repeat('x', %d)", payload)}}},
				{Flush: true},
				{Checkpoint: "boundary_row", Receive: []protocoltest.Expectation{{Type: "RowDescription"}, {Type: "DataRow", Values: []*string{&value}}, {Type: "CommandComplete", Tag: "SELECT 1"}, {Type: "ReadyForQuery", Status: "I"}}},
				{Send: []protocoltest.Message{{Type: "Query", SQL: "SELECT 1"}}},
				{Flush: true},
				{Checkpoint: "recovery", Receive: []protocoltest.Expectation{{Type: "RowDescription"}, {Type: "DataRow", Values: []*string{stringPointer("31")}}, {Type: "CommandComplete", Tag: "SELECT 1"}, {Type: "ReadyForQuery", Status: "I"}}},
			}}
			t.Run(scenario.Name, func(t *testing.T) { testConversation(t, pg, scenario, size, 7) })
		}
	}
}

func stringPointer(value string) *string { return &value }

type laneResult struct {
	Name         string                  `json:"name"`
	Transcript   protocoltest.Transcript `json:"transcript"`
	Error        string                  `json:"error,omitempty"`
	Effects      [][][]*string           `json:"effects"`
	CheckoutPIDs []uint32                `json:"checkout_pids,omitempty"`
	CleanupError string                  `json:"cleanup_error,omitempty"`
	Relations    map[uint32]string       `json:"relations"`
}

type report struct {
	Test           string                `json:"test"`
	Scenario       protocoltest.Scenario `json:"scenario"`
	BufferSize     int                   `json:"buffer_size"`
	WriteChunkSize int                   `json:"write_chunk_size"`
	Metadata       map[string]any        `json:"metadata"`
	Results        []laneResult          `json:"results"`
	Differences    []string              `json:"differences,omitempty"`
	ProxyLog       string                `json:"proxy_log"`
	PostgresLog    string                `json:"postgres_log,omitempty"`
}

func testConversation(t *testing.T, pg *postgresFixture, scenario protocoltest.Scenario, buffer, chunk int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	logs := &boundedLog{}
	restore := pgproxy.Bootstrap(pgproxy.Dependencies{LogHandler: slog.NewJSONHandler(logs, nil)})
	defer restore()
	result := report{Test: t.Name(), Scenario: scenario, BufferSize: buffer, WriteChunkSize: chunk, Metadata: map[string]any{"go_version": runtime.Version(), "revision": sourceRevision(), "postgres": pg.Metadata, "schedule": "deterministic; no randomized generation"}}
	var lanes []*lane
	defer func() {
		// Close sessions/pools before observing cleanup and writing the artifact.
		for index, current := range lanes {
			if current == nil {
				continue
			}
			if err := current.Close(); err != nil {
				t.Errorf("%s cleanup: %v", result.Results[index].Name, err)
				result.Results[index].CleanupError = err.Error()
			}
		}
		if !t.Failed() && os.Getenv("PGPROXY_TEST_KEEP_ARTIFACTS") != "1" {
			return
		}
		result.ProxyLog = logs.String()
		logCtx, logCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer logCancel()
		if contents, err := pg.Logs(logCtx); err == nil {
			result.PostgresLog = contents
		} else {
			result.PostgresLog = "log collection failed: " + err.Error()
		}
		path, err := writeReport(result, artifactRoot())
		if err != nil {
			t.Errorf("write failure artifact: %v", err)
		} else {
			t.Logf("protocol artifact: %s", path)
		}
	}()
	for _, mode := range []struct {
		name  string
		proxy bool
	}{{"direct", false}, {"direct_control", false}, {"proxy", true}} {
		result.Results = append(result.Results, laneResult{Name: mode.name})
		record := &result.Results[len(result.Results)-1]
		current, err := newLane(ctx, pg.Admin.Config(), scenario.Setup, mode.proxy, buffer)
		lanes = append(lanes, current)
		if current != nil {
			record.Relations = current.Relations
		}
		if err != nil {
			record.Error = "fixture setup: " + err.Error()
			t.Errorf("%s: %s", mode.name, record.Error)
			return
		}
		transcript, err := protocoltest.Run(ctx, current.Endpoint, scenario, protocoltest.Options{StepTimeout: 5 * time.Second, WriteChunkSize: chunk, Reassign: current.Reassign})
		record.Transcript = transcript
		record.CheckoutPIDs = current.CheckoutPIDs()
		if err != nil {
			record.Error = err.Error()
			t.Errorf("%s: %v", mode.name, err)
		}
		if err := current.CheckReassignment(); err != nil {
			record.Error += "; " + err.Error()
			t.Errorf("%s: %v", mode.name, err)
		}
		for _, effect := range scenario.Effects {
			rows, err := effectRows(ctx, current.Admin, effect.SQL)
			if err != nil {
				t.Errorf("%s effect %q: %v", mode.name, effect.SQL, err)
				record.Error += "; effect: " + err.Error()
			}
			record.Effects = append(record.Effects, rows)
			if err == nil && !reflect.DeepEqual(rows, effect.Rows) {
				t.Errorf("%s effect %q: got %v want %v", mode.name, effect.SQL, rows, effect.Rows)
				record.Error += "; effect differs from expected rows"
			}
		}
	}
	for index := 1; index < len(result.Results); index++ {
		left, right := result.Results[0], result.Results[index]
		if err := protocoltest.Compare(left.Transcript, right.Transcript, left.Relations, right.Relations); err != nil {
			difference := fmt.Sprintf("direct vs %s: %v", right.Name, err)
			result.Differences = append(result.Differences, difference)
			t.Error(difference)
		}
		// Startup maps have unspecified emission order, but values must match.
		if !reflect.DeepEqual(left.Transcript.StartupParameters, right.Transcript.StartupParameters) {
			difference := fmt.Sprintf("direct vs %s: startup parameters differ: %v / %v", right.Name, left.Transcript.StartupParameters, right.Transcript.StartupParameters)
			result.Differences = append(result.Differences, difference)
			t.Error(difference)
		}
		if !reflect.DeepEqual(left.Effects, right.Effects) {
			difference := "direct vs " + right.Name + ": database effects differ"
			result.Differences = append(result.Differences, difference)
			t.Error(difference)
		}
	}
}

func effectRows(ctx context.Context, conn *pgx.Conn, sql string) ([][]*string, error) {
	rows, err := conn.Query(ctx, sql, pgx.QueryExecModeSimpleProtocol)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([][]*string, 0)
	for rows.Next() {
		values := make([]*string, len(rows.RawValues()))
		for index, value := range rows.RawValues() {
			if value != nil {
				values[index] = stringPointer(string(value))
			}
		}
		result = append(result, values)
	}
	return result, rows.Err()
}

func sourceRevision() string {
	if revision := os.Getenv("PGPROXY_TEST_REVISION"); revision != "" {
		return revision
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" {
				return setting.Value
			}
		}
	}
	return "unknown; set PGPROXY_TEST_REVISION when invoking go test"
}

func artifactRoot() string {
	for _, key := range []string{"PGPROXY_TEST_ARTIFACT_DIR", "TEST_UNDECLARED_OUTPUTS_DIR"} {
		if path := os.Getenv(key); path != "" {
			return path
		}
	}
	return ""
}

func writeReport(result report, root string) (string, error) {
	if root != "" {
		if err := os.MkdirAll(root, 0700); err != nil {
			return "", err
		}
	}
	directory, err := os.MkdirTemp(root, "pgproxy-protocol-")
	if err != nil {
		return "", err
	}
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return directory, err
	}
	if err = os.WriteFile(filepath.Join(directory, "report.json"), data, 0600); err != nil {
		return directory, err
	}
	scenario, err := json.MarshalIndent(result.Scenario, "", "  ")
	if err != nil {
		return directory, err
	}
	if err = os.WriteFile(filepath.Join(directory, "scenario.json"), scenario, 0600); err != nil {
		return directory, err
	}
	replay := `#!/bin/sh
# Run from a pg-proxy checkout; every replay creates fresh fixture databases.
set -eu
artifact_dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
PGPROXY_TEST_REVISION="$(git rev-parse HEAD)" go test -tags=integration -count=1 -timeout=10m -v ./internal/protocoltest/integration -run '^TestDifferential$' -args -protocol-replay "$artifact_dir/report.json"
`
	if err = os.WriteFile(filepath.Join(directory, "replay.sh"), []byte(replay), 0700); err != nil {
		return directory, err
	}
	return directory, nil
}

func loadReplay(path string) (report, error) {
	var result report
	file, err := os.Open(path)
	if err != nil {
		return result, err
	}
	defer file.Close()
	const limit = 128 << 20
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return result, err
	}
	if len(data) > limit {
		return result, fmt.Errorf("replay report exceeds 128 MiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return result, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return result, fmt.Errorf("replay report contains trailing JSON: %v", err)
	}
	// Use the strict scenario decoder on the saved JSON, rather than allowing
	// encoding/json to silently accept duplicate scenario fields during replay.
	var envelope struct {
		Scenario json.RawMessage `json:"scenario"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return result, err
	}
	result.Scenario, err = protocoltest.DecodeScenario(envelope.Scenario)
	if err != nil {
		return result, err
	}
	if result.BufferSize <= 0 || result.WriteChunkSize < 0 {
		return result, fmt.Errorf("invalid saved buffer or chunk size")
	}
	postgres, ok := result.Metadata["postgres"].(map[string]any)
	if !ok {
		return result, fmt.Errorf("replay report requires PostgreSQL metadata")
	}
	for _, key := range []string{"postgres_image", "postgres_version"} {
		if value, ok := postgres[key].(string); !ok || value == "" {
			return result, fmt.Errorf("replay report requires %s", key)
		}
	}
	return result, nil
}

type boundedLog struct {
	mu   sync.Mutex
	data []byte
}

func (b *boundedLog) Write(value []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	const limit = 1 << 20
	length := len(value)
	if length > limit {
		value = value[length-limit:]
	}
	if len(b.data)+len(value) > limit {
		b.data = b.data[len(b.data)+len(value)-limit:]
	}
	b.data = append(b.data, value...)
	return length, nil
}
func (b *boundedLog) String() string { b.mu.Lock(); defer b.mu.Unlock(); return string(b.data) }

func TestFailureArtifact(t *testing.T) {
	expected := report{
		Test: "TestDifferential/replay/buffer_64_chunk_7", BufferSize: 64, WriteChunkSize: 7,
		Metadata: map[string]any{"postgres": map[string]any{"postgres_image": postgresImage, "postgres_version": "test-version"}},
		Scenario: protocoltest.Scenario{Version: 1, Name: "replay", Steps: []protocoltest.Step{
			{Send: []protocoltest.Message{{Type: "Query", SQL: "SELECT 1"}}},
			{Flush: true},
			{Checkpoint: "idle", Receive: []protocoltest.Expectation{{Type: "RowDescription"}, {Type: "DataRow", Values: []*string{stringPointer("31")}}, {Type: "CommandComplete", Tag: "SELECT 1"}, {Type: "ReadyForQuery", Status: "I"}}},
		}},
		Results:     []laneResult{{Name: "direct", Error: "fixture setup: statement failed"}},
		Differences: []string{"ReadyForQuery.TxStatus: I != E"},
	}
	directory, err := writeReport(expected, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	actual, err := loadReplay(filepath.Join(directory, "report.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("artifact lost evidence: %#v", actual)
	}
	replay, err := os.ReadFile(filepath.Join(directory, "replay.sh"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(replay), "-protocol-replay") || !strings.Contains(string(replay), "$artifact_dir/report.json") {
		t.Fatalf("replay does not execute the saved conversation: %s", replay)
	}
}

// A failed setup still returns ownership of the database to the caller.
func TestLaneSetupFailure(t *testing.T) {
	pg := startPostgres(t)
	current, err := newLane(t.Context(), pg.Admin.Config(), []string{"CREATE TABLE setup_started (id int)", "SELECT * FROM missing_setup_table"}, false, 64)
	if current == nil {
		t.Fatalf("expected partial fixture after setup failure, got %v", err)
	}
	defer func() {
		if err := current.Close(); err != nil {
			t.Errorf("cleanup failed setup: %v", err)
		}
	}()
	if err == nil || !strings.Contains(err.Error(), "fixture setup statement 1") || !strings.Contains(err.Error(), "42P01") {
		t.Fatalf("setup error lost its stage or PostgreSQL cause: %v", err)
	}
	if err := current.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), fixtureTimeout)
	defer cancel()
	var exists bool
	if err := pg.Admin.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)", current.database).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("failed fixture database survived cleanup")
	}
}
