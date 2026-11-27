// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package resizable

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
)

func TestConnectionTracerDelegatesSupportedInterfaces(t *testing.T) {
	underlying := &recordingTracer{}
	tracer := newConnectionTracer(underlying, testGate(t, 1, nil))
	ctx := context.Background()

	tracer.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{})
	tracer.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{})
	tracer.TraceBatchStart(ctx, nil, pgx.TraceBatchStartData{})
	tracer.TraceBatchQuery(ctx, nil, pgx.TraceBatchQueryData{})
	tracer.TraceBatchEnd(ctx, nil, pgx.TraceBatchEndData{})
	tracer.TraceCopyFromStart(ctx, nil, pgx.TraceCopyFromStartData{})
	tracer.TraceCopyFromEnd(ctx, nil, pgx.TraceCopyFromEndData{})
	tracer.TracePrepareStart(ctx, nil, pgx.TracePrepareStartData{})
	tracer.TracePrepareEnd(ctx, nil, pgx.TracePrepareEndData{})
	tracer.TraceConnectStart(ctx, pgx.TraceConnectStartData{ConnConfig: testConfig(t, 1).ConnConfig})
	tracer.TraceConnectEnd(ctx, pgx.TraceConnectEndData{})

	assert.Equal(t, []string{
		"query-start", "query-end",
		"batch-start", "batch-query", "batch-end",
		"copy-start", "copy-end",
		"prepare-start", "prepare-end",
		"connect-start", "connect-end",
	}, underlying.calls)
}

func TestConnectionTracerSupportsNilAndQueryOnlyTracers(t *testing.T) {
	ctx := context.Background()
	gate := testGate(t, 1, nil)
	tracer := newConnectionTracer(nil, gate)
	assert.Equal(t, ctx, tracer.TraceBatchStart(ctx, nil, pgx.TraceBatchStartData{}))
	assert.Equal(t, ctx, tracer.TraceCopyFromStart(ctx, nil, pgx.TraceCopyFromStartData{}))
	assert.Equal(t, ctx, tracer.TracePrepareStart(ctx, nil, pgx.TracePrepareStartData{}))
	assert.IsType(t, &connectAttempt{}, tracer.TraceConnectStart(ctx, pgx.TraceConnectStartData{ConnConfig: testConfig(t, 1).ConnConfig}).Value(connectKey{}))

	query := &queryOnlyTracer{}
	tracer = newConnectionTracer(query, gate)
	tracer.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{})
	tracer.TraceBatchEnd(ctx, nil, pgx.TraceBatchEndData{})
	assert.True(t, query.called)
}

type queryOnlyTracer struct{ called bool }

func (t *queryOnlyTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	return ctx
}
func (t *queryOnlyTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {
	t.called = true
}

type recordingTracer struct{ calls []string }

func (t *recordingTracer) record(name string) { t.calls = append(t.calls, name) }
func (t *recordingTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	t.record("query-start")
	return ctx
}
func (t *recordingTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {
	t.record("query-end")
}
func (t *recordingTracer) TraceBatchStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceBatchStartData) context.Context {
	t.record("batch-start")
	return ctx
}
func (t *recordingTracer) TraceBatchQuery(context.Context, *pgx.Conn, pgx.TraceBatchQueryData) {
	t.record("batch-query")
}
func (t *recordingTracer) TraceBatchEnd(context.Context, *pgx.Conn, pgx.TraceBatchEndData) {
	t.record("batch-end")
}
func (t *recordingTracer) TraceCopyFromStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceCopyFromStartData) context.Context {
	t.record("copy-start")
	return ctx
}
func (t *recordingTracer) TraceCopyFromEnd(context.Context, *pgx.Conn, pgx.TraceCopyFromEndData) {
	t.record("copy-end")
}
func (t *recordingTracer) TracePrepareStart(ctx context.Context, _ *pgx.Conn, _ pgx.TracePrepareStartData) context.Context {
	t.record("prepare-start")
	return ctx
}
func (t *recordingTracer) TracePrepareEnd(context.Context, *pgx.Conn, pgx.TracePrepareEndData) {
	t.record("prepare-end")
}
func (t *recordingTracer) TraceConnectStart(ctx context.Context, _ pgx.TraceConnectStartData) context.Context {
	t.record("connect-start")
	return ctx
}
func (t *recordingTracer) TraceConnectEnd(context.Context, pgx.TraceConnectEndData) {
	t.record("connect-end")
}
