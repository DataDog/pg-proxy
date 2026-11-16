// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

// Package otel connects the proxy to caller-owned OpenTelemetry providers.
package otel

import (
	"context"
	"fmt"
	"unicode/utf8"

	pgproxy "github.com/DataDog/pg-proxy"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// Tracer uses the standard OpenTelemetry tracer and W3C Trace Context carried
// in leading or trailing SQL comments. It does not install global providers.
type Tracer struct{ tracer trace.Tracer }

// NewTracer wraps a tracer from the caller's provider. A nil tracer is a no-op.
func NewTracer(tracer trace.Tracer) *Tracer {
	if tracer == nil {
		tracer = noop.NewTracerProvider().Tracer("pg-proxy")
	}
	return &Tracer{tracer: tracer}
}

func queryContext(sql string) context.Context {
	// Prefer a valid leading context, but ordinary comments and malformed
	// carriers must not hide valid propagation at the end of the query.
	for _, carrier := range sqlComments(sql) {
		ctx := propagation.TraceContext{}.Extract(context.Background(), carrier)
		if trace.SpanContextFromContext(ctx).IsValid() {
			return ctx
		}
	}
	return context.Background()
}
func (t *Tracer) HasQueryContext(sql string) bool {
	return trace.SpanContextFromContext(queryContext(sql)).IsValid()
}
func (t *Tracer) StartRoot(operation, sql string) (pgproxy.Span, bool) {
	parent := queryContext(sql)
	propagated := trace.SpanContextFromContext(parent).IsValid()
	ctx, span := t.tracer.Start(parent, operation, trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(attribute.String("db.system", "postgres"), attribute.String("db.statement", escapeInvalidUTF8(sql))))
	return otelSpan{tracer: t.tracer, ctx: ctx, span: span}, propagated
}
func (t *Tracer) WarnNamedPreparedStatement(name, sql, service string) {
	pgproxy.Logger().Warn("trace context makes named prepared statement SQL unique; avoid per-request comments on reusable named statements", "service", service, "name", name, "query", sql)
}

type otelSpan struct {
	tracer trace.Tracer
	ctx    context.Context
	span   trace.Span
}

func (s otelSpan) StartChild(operation string) pgproxy.Span {
	ctx, child := s.tracer.Start(s.ctx, operation, trace.WithSpanKind(trace.SpanKindInternal), trace.WithAttributes(attribute.String("db.system", "postgres")))
	return otelSpan{tracer: s.tracer, ctx: ctx, span: child}
}
func (s otelSpan) SetTag(key string, value any) {
	var attr attribute.KeyValue
	switch v := value.(type) {
	case string:
		attr = attribute.String(key, escapeInvalidUTF8(v))
	case bool:
		attr = attribute.Bool(key, v)
	case int:
		attr = attribute.Int(key, v)
	case int64:
		attr = attribute.Int64(key, v)
	case float64:
		attr = attribute.Float64(key, v)
	case []string:
		attr = attribute.StringSlice(key, v)
	default:
		attr = attribute.String(key, fmt.Sprint(v))
	}
	s.span.SetAttributes(attr)
}

// Sampled reports whether exporters will receive the span. Non-recording spans
// can still carry a sampled parent context, so both checks are needed.
func (s otelSpan) Sampled() bool {
	return s.span.IsRecording() && s.span.SpanContext().IsSampled()
}

// PostgreSQL text and binary Bind parameters may contain invalid UTF-8. Escape
// only telemetry copies so protobuf encoding cannot drop the entire export batch.
func escapeInvalidUTF8(value string) string {
	if !utf8.ValidString(value) {
		return fmt.Sprintf("%q", value)
	}
	return value
}

func (s otelSpan) Finish(err error) {
	if err != nil {
		message := err.Error()
		if !utf8.ValidString(message) {
			message = escapeInvalidUTF8(message)
			err = fmt.Errorf("%s", message)
		}
		s.span.RecordError(err)
		s.span.SetStatus(codes.Error, message)
	}
	s.span.End()
}
