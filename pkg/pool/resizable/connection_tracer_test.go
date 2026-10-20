package resizable

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
)

func TestConnectionTracerReleasesFailedReservationOnce(t *testing.T) {
	releases := 0
	tracer := newConnectionTracer(nil, func() { releases++ })
	failure := pgx.TraceConnectEndData{Err: errors.New("connect failed")}

	tracer.TraceConnectEnd(context.Background(), failure)
	tracer.TraceConnectEnd(context.Background(), failure)

	assert.Equal(t, 1, releases)
}

func TestConnectionTracerKeepsSuccessfulReservation(t *testing.T) {
	releases := 0
	tracer := newConnectionTracer(nil, func() { releases++ })

	tracer.TraceConnectEnd(context.Background(), pgx.TraceConnectEndData{})

	assert.Zero(t, releases)
}

func TestConnectionTracerDelegatesSupportedInterfaces(t *testing.T) {
	underlying := &recordingTracer{}
	tracer := newConnectionTracer(underlying, func() {})
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
	tracer.TraceConnectStart(ctx, pgx.TraceConnectStartData{})
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
	tracer := newConnectionTracer(nil, func() {})
	assert.Equal(t, ctx, tracer.TraceBatchStart(ctx, nil, pgx.TraceBatchStartData{}))
	assert.Equal(t, ctx, tracer.TraceCopyFromStart(ctx, nil, pgx.TraceCopyFromStartData{}))
	assert.Equal(t, ctx, tracer.TracePrepareStart(ctx, nil, pgx.TracePrepareStartData{}))
	assert.Equal(t, ctx, tracer.TraceConnectStart(ctx, pgx.TraceConnectStartData{}))

	query := &queryOnlyTracer{}
	tracer = newConnectionTracer(query, func() {})
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
