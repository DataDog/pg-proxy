// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package query

import (
	"errors"
	"fmt"
	"io"
	"testing"

	pgproxy "github.com/DataDog/pg-proxy"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordingSpan records the tags and finish error of a span.
type recordingSpan struct {
	tags     map[string]any
	finished bool
	err      error
}

func (s *recordingSpan) StartChild(string) pgproxy.Span { return &recordingSpan{} }
func (s *recordingSpan) SetTag(key string, value any) {
	if s.tags == nil {
		s.tags = map[string]any{}
	}
	s.tags[key] = value
}
func (s *recordingSpan) Finish(err error) {
	s.finished = true
	s.err = err
}

func TestIsBackendError(t *testing.T) {
	tests := []struct {
		name    string
		code    string
		message string
		want    bool
	}{
		// Switchover signatures.
		{"read-only UPDATE during switchover", "25006", "cannot execute UPDATE in a read-only transaction", true},
		{"read-only DELETE during switchover", "25006", "cannot execute DELETE in a read-only transaction", true},
		{"admin shutdown", "57P01", "terminating connection due to administrator command", true},
		{"shutting down", "57P03", "the database system is shutting down", true},
		// Other infrastructure classes.
		{"too many connections", "53300", "sorry, too many clients already", true},
		{"disk full", "53100", "could not extend file", true},
		{"connection failure", "08006", "connection failure", true},
		{"io error", "58030", "could not read block", true},
		{"internal error", "XX000", "internal error", true},
		{"crash shutdown", "57P02", "terminating connection because of crash of another server process", true},
		// Client query errors.
		{"not null violation", "23502", `null value in column "commits" of relation "prc_prs" violates not-null constraint`, false},
		{"undefined table", "42P01", `relation "asm.ai_guard_past_mistake_raw" does not exist`, false},
		{"deadlock", "40P01", "deadlock detected", false},
		{"serialization failure", "40001", "could not serialize access due to concurrent update", false},
		{"statement timeout", "57014", "canceling statement due to statement timeout", false},
		{"syntax error", "42601", `syntax error at or near "SELEC"`, false},
		{"permission denied", "42501", "permission denied for table foo", false},
		{"read-only without switchover message", "25006", "some other read-only problem", false},
		{"starting up is infra but not a switchover", "57P03", "the database system is starting up", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsBackendError(tt.code, tt.message))
		})
	}
}

func TestErrorResponseSpanError(t *testing.T) {
	tests := []struct {
		name           string
		msg            *pgproto3.ErrorResponse
		wantSpanErr    bool
		wantSwitchover bool
	}{
		{
			name:        "client constraint violation is not a span error",
			msg:         &pgproto3.ErrorResponse{Severity: "ERROR", Code: "23502", Message: "null value in column violates not-null constraint"},
			wantSpanErr: false,
		},
		{
			name:        "client statement timeout is not a span error",
			msg:         &pgproto3.ErrorResponse{Severity: "ERROR", Code: "57014", Message: "canceling statement due to statement timeout"},
			wantSpanErr: false,
		},
		{
			name:           "switchover read-only error is a span error",
			msg:            &pgproto3.ErrorResponse{Severity: "ERROR", Code: "25006", Message: "cannot execute UPDATE in a read-only transaction"},
			wantSpanErr:    true,
			wantSwitchover: true,
		},
		{
			name:        "resource exhaustion is a span error",
			msg:         &pgproto3.ErrorResponse{Severity: "ERROR", Code: "53300", Message: "sorry, too many clients already"},
			wantSpanErr: true,
		},
		{
			name:        "FATAL is left to the root span",
			msg:         &pgproto3.ErrorResponse{Severity: "FATAL", Code: "57P01", Message: "terminating connection due to administrator command"},
			wantSpanErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			span := &recordingSpan{}
			traceState := &TraceState{trace: span}

			_, _ = DecidePgMessageAction(tt.msg, QueryReply, traceState)

			require.True(t, span.finished)
			assert.Equal(t, tt.msg.Code, span.tags["pg.sqlstate"])
			if !tt.wantSpanErr {
				assert.NoError(t, span.err)
				return
			}
			var backendErr *BackendError
			require.ErrorAs(t, span.err, &backendErr)
			assert.Equal(t, tt.msg.Code, backendErr.Code)
			assert.Equal(t, tt.wantSwitchover, backendErr.Switchover)
		})
	}
}

func TestSpanError(t *testing.T) {
	errA := errors.New("a")
	errB := errors.New("b")

	assert.NoError(t, spanError())
	assert.NoError(t, spanError(nil))
	assert.NoError(t, spanError(io.EOF))
	assert.NoError(t, spanError(fmt.Errorf("wrapped: %w", io.ErrUnexpectedEOF)))

	// A single error keeps its concrete type so Error Tracking groups by it.
	be := &BackendError{Code: "25006"}
	assert.Same(t, be, spanError(be))
	assert.Same(t, errA, spanError(nil, errA))

	joined := spanError(errA, errB)
	assert.ErrorIs(t, joined, errA)
	assert.ErrorIs(t, joined, errB)
}
