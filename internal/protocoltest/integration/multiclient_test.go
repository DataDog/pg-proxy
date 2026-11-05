//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"runtime"
	"testing"
	"time"

	pgproxy "github.com/DataDog/pg-proxy"
	"github.com/DataDog/pg-proxy/internal/protocoltest"
)

// A compound case saves two ordinary scenarios and an explicit Go-controlled
// turn schedule. Each turn ends at a receive checkpoint before the next starts.
type compoundCase struct {
	Version int                     `json:"version"`
	Name    string                  `json:"name"`
	Clients []protocoltest.Scenario `json:"clients"`
	Turns   []clientTurn            `json:"turns"`
}
type clientTurn struct {
	Client         int  `json:"client"`
	Start          int  `json:"start"`
	End            int  `json:"end"`
	ReassignBefore bool `json:"reassign_before,omitempty"`
}
type turnObservation struct {
	Client     int    `json:"client"`
	Checkpoint string `json:"checkpoint"`
	BackendPID uint32 `json:"backend_pid,omitempty"`
	Reassigned bool   `json:"reassigned,omitempty"`
}

func (c compoundCase) Validate() error {
	if c.Version != 1 || len(c.Clients) != 2 || len(c.Turns) == 0 || len(c.Turns) > 4096 {
		return errors.New("compound case requires version 1, two clients, and 1..4096 turns")
	}
	next := [2]int{}
	for _, client := range c.Clients {
		if err := client.Validate(); err != nil {
			return err
		}
		if len(client.Setup) > 0 || len(client.Effects) > 0 {
			return errors.New("compound clients must use the shared empty fixture")
		}
	}
	for _, turn := range c.Turns {
		if turn.Client < 0 || turn.Client > 1 || turn.Start != next[turn.Client] || turn.End < turn.Start || turn.End >= len(c.Clients[turn.Client].Steps) {
			return errors.New("compound turns must cover each client's steps once in order")
		}
		if len(c.Clients[turn.Client].Steps[turn.End].Receive) == 0 {
			return errors.New("compound turn must end at a receive checkpoint")
		}
		next[turn.Client] = turn.End + 1
	}
	for i, client := range c.Clients {
		if next[i] != len(client.Steps) {
			return errors.New("compound schedule leaves client steps uncovered")
		}
	}
	return nil
}

func preparedIsolation(reassign, sameSQL bool) compoundCase {
	c := compoundCase{Version: 1, Name: "prepared_isolation"}
	if reassign {
		c.Name += "_reassignment"
	}
	if sameSQL {
		c.Name += "_same_sql"
	}
	for client := 0; client < 2; client++ {
		s := protocoltest.Scenario{Version: 1, Name: fmt.Sprintf("%s_client_%d", c.Name, client)}
		appendTurn := func(checkpoint string, messages []protocoltest.Message, replies []protocoltest.Expectation) {
			start := len(s.Steps)
			s.Steps = append(s.Steps, protocoltest.Step{Send: messages}, protocoltest.Step{Flush: true}, protocoltest.Step{Checkpoint: checkpoint, Receive: replies})
			c.Turns = append(c.Turns, clientTurn{Client: client, Start: start, End: len(s.Steps) - 1, ReassignBefore: reassign && client == 0 && start == 3})
		}
		sql, oid := "SELECT $1::int4 + 1 AS value", uint32(23)
		params, values := []string{"37", "3130", "37", "39"}, []string{"38", "3131", "3134", "3138"}
		if client == 1 && sameSQL {
			params = []string{"32", "34", "36", "38"}
			values = []string{"33", "35", "37", "39"}
		}
		if client == 1 && !sameSQL {
			sql, oid = "SELECT upper($1::text) AS value", 25
			params = []string{"6f6e65", "74776f", "7468726565", "666f7572"}
			values = []string{"4f4e45", "54574f", "5448524545", "464f5552"}
		}
		for segment := 0; segment < 4; segment++ {
			var msgs []protocoltest.Message
			var replies []protocoltest.Expectation
			if segment == 0 || client == 0 && segment == 2 {
				if segment == 2 {
					msgs = append(msgs, protocoltest.Message{Type: "Close", Object: "S", Name: "shared"})
					replies = append(replies, protocoltest.Expectation{Type: "CloseComplete"})
					sql = "SELECT $1::int4 * 2 AS value"
				}
				msgs = append(msgs, protocoltest.Message{Type: "Parse", Name: "shared", SQL: sql, ParameterOIDs: []uint32{oid}})
				replies = append(replies, protocoltest.Expectation{Type: "ParseComplete"})
			}
			msgs = append(msgs, protocoltest.Message{Type: "Bind", Statement: "shared", Portal: "same_portal", Parameters: []*string{stringPointer(params[segment])}}, protocoltest.Message{Type: "Execute", Portal: "same_portal"}, protocoltest.Message{Type: "Sync"})
			replies = append(replies, protocoltest.Expectation{Type: "BindComplete"}, protocoltest.Expectation{Type: "DataRow", Values: []*string{stringPointer(values[segment])}}, protocoltest.Expectation{Type: "CommandComplete", Tag: "SELECT 1"}, protocoltest.Expectation{Type: "ReadyForQuery", Status: "I"})
			appendTurn(fmt.Sprintf("segment_%d", segment), msgs, replies)
		}
		appendTurn("recovery", []protocoltest.Message{{Type: "Query", SQL: "SELECT 1 AS recovery"}}, []protocoltest.Expectation{{Type: "RowDescription"}, {Type: "DataRow", Values: []*string{stringPointer("31")}}, {Type: "CommandComplete", Tag: "SELECT 1"}, {Type: "ReadyForQuery", Status: "I"}})
		c.Clients = append(c.Clients, s)
	}
	// Interleave complete segments, preserving both open sessions until all turns finish.
	ordered := make([]clientTurn, 0, len(c.Turns))
	for segment := 0; segment < 5; segment++ {
		ordered = append(ordered, c.Turns[segment], c.Turns[5+segment])
	}
	c.Turns = ordered
	return c
}

func TestPreparedIsolation(t *testing.T) {
	pg := startPostgres(t)
	for _, variant := range []struct{ reassign, sameSQL bool }{{false, false}, {true, false}, {false, true}} {
		c := preparedIsolation(variant.reassign, variant.sameSQL)
		for _, v := range []struct{ buffer, chunk int }{{64, 0}, {64, 7}, {4096, 0}, {4096, 7}} {
			t.Run(fmt.Sprintf("%s/buffer_%d_chunk_%d", c.Name, v.buffer, v.chunk), func(t *testing.T) { testCompoundConversation(t, pg, c, v.buffer, v.chunk) })
		}
	}
}

func runCompound(ctx context.Context, current *lane, c compoundCase, chunk int) (transcripts []protocoltest.Transcript, turns []turnObservation, runErr error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	type arrival struct {
		client, step int
		resume       chan struct{}
	}
	type outcome struct {
		client     int
		transcript protocoltest.Transcript
		err        error
	}
	arrived := make(chan arrival, 2)
	finished := make(chan outcome, 2)
	transcripts = make([]protocoltest.Transcript, 2)
	received := 0
	collect := func(done outcome) {
		transcripts[done.client] = done.transcript
		received++
		if done.err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("client %d: %w", done.client, done.err))
			cancel()
		}
	}
	// Join both runners on every failure path; their sockets/readers follow context cancellation.
	defer func() {
		cancel()
		timer := time.NewTimer(5 * time.Second)
		defer timer.Stop()
		for received < 2 {
			select {
			case done := <-finished:
				collect(done)
			case <-timer.C:
				runErr = errors.Join(runErr, errors.New("compound clients did not stop"))
				return
			}
		}
	}()
	for client := 0; client < 2; client++ {
		go func() {
			starts := map[int]bool{len(c.Clients[client].Steps): true}
			for _, turn := range c.Turns {
				if turn.Client == client {
					starts[turn.Start] = true
				}
			}
			transcript, err := protocoltest.Run(ctx, current.Endpoint, c.Clients[client], protocoltest.Options{StepTimeout: 5 * time.Second, WriteChunkSize: chunk, BeforeStep: func(ctx context.Context, step int) error {
				if !starts[step] {
					return nil
				}
				request := arrival{client: client, step: step, resume: make(chan struct{})}
				select {
				case arrived <- request:
				case <-ctx.Done():
					return ctx.Err()
				}
				select {
				case <-request.resume:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}})
			finished <- outcome{client, transcript, err}
		}()
	}
	pending := map[int]arrival{}
	wait := func(client, step int) error {
		for {
			if request, ok := pending[client]; ok {
				if request.step != step {
					return fmt.Errorf("client %d reached step %d, want %d", client, request.step, step)
				}
				return nil
			}
			select {
			case request := <-arrived:
				if _, exists := pending[request.client]; exists {
					return errors.New("duplicate client barrier")
				}
				pending[request.client] = request
			case done := <-finished:
				collect(done)
				return fmt.Errorf("client %d exited before scheduling completed: %v", done.client, done.err)
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	// Neither client can send a post-authentication message before both reach here.
	for client := 0; client < 2; client++ {
		if err := wait(client, 0); err != nil {
			return transcripts, turns, err
		}
	}
	var lastPID uint32
	for _, turn := range c.Turns {
		if err := wait(turn.Client, turn.Start); err != nil {
			return transcripts, turns, err
		}
		if turn.ReassignBefore {
			if err := current.Reassign(ctx); err != nil {
				return transcripts, turns, err
			}
		}
		before := len(current.CheckoutPIDs())
		close(pending[turn.Client].resume)
		delete(pending, turn.Client)
		if err := wait(turn.Client, turn.End+1); err != nil {
			return transcripts, turns, err
		}
		observation := turnObservation{Client: turn.Client, Checkpoint: c.Clients[turn.Client].Steps[turn.End].Checkpoint, Reassigned: turn.ReassignBefore}
		if current.pool != nil {
			pids := current.CheckoutPIDs()
			if len(pids) != before+1 {
				return transcripts, turns, fmt.Errorf("turn must observe one backend checkout, got %d", len(pids)-before)
			}
			observation.BackendPID = pids[before]
			if lastPID != 0 && (observation.BackendPID != lastPID) != turn.ReassignBefore {
				return transcripts, turns, fmt.Errorf("backend sharing/reassignment mismatch: previous %d current %d", lastPID, observation.BackendPID)
			}
			lastPID = observation.BackendPID
		}
		turns = append(turns, observation)
	}
	for client := 0; client < 2; client++ {
		close(pending[client].resume)
	}
	for received < 2 {
		select {
		case done := <-finished:
			collect(done)
		case <-ctx.Done():
			return transcripts, turns, ctx.Err()
		}
	}
	return transcripts, turns, runErr
}

func testCompoundConversation(t *testing.T, pg *postgresFixture, c compoundCase, buffer, chunk int) {
	t.Helper()
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	logs := &boundedLog{}
	restore := pgproxy.Bootstrap(pgproxy.Dependencies{LogHandler: slog.NewJSONHandler(logs, nil)})
	defer restore()
	result := report{Test: t.Name(), Scenario: c.Clients[0], Compound: &c, BufferSize: buffer, WriteChunkSize: chunk, Metadata: map[string]any{"go_version": runtime.Version(), "revision": sourceRevision(), "postgres": pg.Metadata, "schedule": "compound version 1; post-startup barriers; deterministic client turns"}}
	var lanes []*lane
	defer func() { finishReport(t, pg, &result, logs, lanes) }()
	for _, mode := range []struct {
		name  string
		proxy bool
	}{{"direct", false}, {"direct_control", false}, {"proxy", true}} {
		result.Results = append(result.Results, laneResult{Name: mode.name})
		record := &result.Results[len(result.Results)-1]
		current, err := newLane(ctx, pg.Admin.Config(), nil, mode.proxy, buffer)
		lanes = append(lanes, current)
		if err != nil {
			record.Error = "fixture setup: " + err.Error()
			t.Error(record.Error)
			return
		}
		record.Relations = current.Relations
		record.Clients, record.Turns, err = runCompound(ctx, current, c, chunk)
		record.CheckoutPIDs = current.CheckoutPIDs()
		if err != nil {
			record.Error = err.Error()
			t.Errorf("%s: %v", mode.name, err)
		}
		if err := current.CheckReassignment(); err != nil {
			record.Error += "; " + err.Error()
			t.Error(err)
		}
	}
	for _, right := range result.Results[1:] {
		left := result.Results[0]
		for client := 0; client < 2; client++ {
			if err := protocoltest.Compare(left.Clients[client], right.Clients[client], left.Relations, right.Relations); err != nil {
				difference := fmt.Sprintf("direct vs %s client %d: %v", right.Name, client, err)
				result.Differences = append(result.Differences, difference)
				t.Error(difference)
			}
			if !reflect.DeepEqual(left.Clients[client].StartupParameters, right.Clients[client].StartupParameters) {
				difference := fmt.Sprintf("direct vs %s client %d: startup parameters differ", right.Name, client)
				result.Differences = append(result.Differences, difference)
				t.Error(difference)
			}
		}
	}
}

// Keep scheduling validation independent of Docker and of PgProxy behavior.
func TestCompoundSchedule(t *testing.T) {
	for _, reassign := range []bool{false, true} {
		c := preparedIsolation(reassign, false)
		if err := c.Validate(); err != nil {
			t.Fatal(err)
		}
		c.Turns[2].Start++
		if c.Validate() == nil {
			t.Fatal("out-of-order client schedule accepted")
		}
	}
}
