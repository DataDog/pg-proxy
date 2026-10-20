package resizable

import (
	"context"
	"sync"

	"github.com/jackc/pgx/v5"
)

// connectionTracer preserves every pgx tracing interface exposed by an
// existing tracer and releases a reserved gate slot when connection setup
// fails. pgx has no connection-failure lifecycle hook, so ConnectTracer is needed
// to account for failures after BeforeConnect reserves capacity.
type connectionTracer struct {
	query    pgx.QueryTracer
	batch    pgx.BatchTracer
	copyFrom pgx.CopyFromTracer
	prepare  pgx.PrepareTracer
	connect  pgx.ConnectTracer

	onConnectFailure func()
	releaseOnce      sync.Once
}

func newConnectionTracer(underlying pgx.QueryTracer, onConnectFailure func()) *connectionTracer {
	tracer := &connectionTracer{
		query:            underlying,
		onConnectFailure: onConnectFailure,
	}
	tracer.batch, _ = underlying.(pgx.BatchTracer)
	tracer.copyFrom, _ = underlying.(pgx.CopyFromTracer)
	tracer.prepare, _ = underlying.(pgx.PrepareTracer)
	tracer.connect, _ = underlying.(pgx.ConnectTracer)
	return tracer
}

func (t *connectionTracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if t.query != nil {
		return t.query.TraceQueryStart(ctx, conn, data)
	}
	return ctx
}

func (t *connectionTracer) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	if t.query != nil {
		t.query.TraceQueryEnd(ctx, conn, data)
	}
}

func (t *connectionTracer) TraceBatchStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchStartData) context.Context {
	if t.batch != nil {
		return t.batch.TraceBatchStart(ctx, conn, data)
	}
	return ctx
}

func (t *connectionTracer) TraceBatchQuery(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchQueryData) {
	if t.batch != nil {
		t.batch.TraceBatchQuery(ctx, conn, data)
	}
}

func (t *connectionTracer) TraceBatchEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchEndData) {
	if t.batch != nil {
		t.batch.TraceBatchEnd(ctx, conn, data)
	}
}

func (t *connectionTracer) TraceCopyFromStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceCopyFromStartData) context.Context {
	if t.copyFrom != nil {
		return t.copyFrom.TraceCopyFromStart(ctx, conn, data)
	}
	return ctx
}

func (t *connectionTracer) TraceCopyFromEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceCopyFromEndData) {
	if t.copyFrom != nil {
		t.copyFrom.TraceCopyFromEnd(ctx, conn, data)
	}
}

func (t *connectionTracer) TracePrepareStart(ctx context.Context, conn *pgx.Conn, data pgx.TracePrepareStartData) context.Context {
	if t.prepare != nil {
		return t.prepare.TracePrepareStart(ctx, conn, data)
	}
	return ctx
}

func (t *connectionTracer) TracePrepareEnd(ctx context.Context, conn *pgx.Conn, data pgx.TracePrepareEndData) {
	if t.prepare != nil {
		t.prepare.TracePrepareEnd(ctx, conn, data)
	}
}

func (t *connectionTracer) TraceConnectStart(ctx context.Context, data pgx.TraceConnectStartData) context.Context {
	if t.connect != nil {
		return t.connect.TraceConnectStart(ctx, data)
	}
	return ctx
}

func (t *connectionTracer) TraceConnectEnd(ctx context.Context, data pgx.TraceConnectEndData) {
	if data.Err != nil {
		defer t.releaseOnce.Do(t.onConnectFailure)
	}
	if t.connect != nil {
		t.connect.TraceConnectEnd(ctx, data)
	}
}

var (
	_ pgx.QueryTracer    = (*connectionTracer)(nil)
	_ pgx.BatchTracer    = (*connectionTracer)(nil)
	_ pgx.CopyFromTracer = (*connectionTracer)(nil)
	_ pgx.PrepareTracer  = (*connectionTracer)(nil)
	_ pgx.ConnectTracer  = (*connectionTracer)(nil)
)
