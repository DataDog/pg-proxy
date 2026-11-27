// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package resizable

import (
	"context"
	"sync/atomic"

	"github.com/jackc/pgx/v5"
)

// connectionTracer preserves every pgx tracing interface exposed by an
// existing tracer and tags the dial contexts of one pgx.ConnectConfig call.
// pgconn also uses the dialer for out-of-band cancellation sockets.
type connectionTracer struct {
	gate     *resizableGate
	query    pgx.QueryTracer
	batch    pgx.BatchTracer
	copyFrom pgx.CopyFromTracer
	prepare  pgx.PrepareTracer
	connect  pgx.ConnectTracer
}

type connectKey struct{}

// Each ConnectConfig call tries fallback addresses sequentially. Its dialer
// may also be called concurrently by a cancellation handler.
type connectAttempt struct {
	socket      *budgetConn
	established atomic.Bool
}

func newConnectionTracer(underlying pgx.QueryTracer, gate *resizableGate) *connectionTracer {
	tracer := &connectionTracer{query: underlying, gate: gate}
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
	grant, _ := ctx.Value(admissionKey{}).(*admission)
	if t.connect != nil {
		ctx = t.connect.TraceConnectStart(ctx, data)
	}
	if grant != nil && ctx.Value(admissionKey{}) != grant {
		ctx = context.WithValue(ctx, admissionKey{}, grant)
	}
	attempt := new(connectAttempt)
	t.gate.wrapDials(data.ConnConfig, attempt)
	return context.WithValue(ctx, connectKey{}, attempt)
}

func (t *connectionTracer) TraceConnectEnd(ctx context.Context, data pgx.TraceConnectEndData) {
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
