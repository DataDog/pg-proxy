// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

// Package datadog connects the proxy to the public Datadog SDKs.
package datadog

import (
	"errors"
	"fmt"

	"github.com/DataDog/dd-trace-go/v2/ddtrace/ext"
	"github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"
	pgproxy "github.com/DataDog/pg-proxy"
)

// Tracer preserves the existing Datadog span names, tags and SQL propagation.
// The application must start and stop the Datadog tracer.
type Tracer struct{}

func options() []tracer.StartSpanOption {
	return []tracer.StartSpanOption{tracer.Tag(ext.SpanKind, ext.SpanKindServer), tracer.Tag(ext.DBSystem, "postgres")}
}

func (Tracer) StartRoot(operation, sql string) (pgproxy.Span, bool) {
	opts := append(options(), tracer.Tag(ext.DBStatement, sql))
	propagated := false
	if sql != "" {
		carrier := tracer.SQLCommentCarrier{Query: sql}
		if parent, err := carrier.Extract(); parent != nil {
			opts = append(opts, tracer.WithStartSpanConfig(&tracer.StartSpanConfig{Parent: parent}))
			propagated = true
		} else if !errors.Is(err, tracer.ErrSpanContextNotFound) {
			pgproxy.Logger().Error("extracting trace comment from query", "query", sql, "error", err)
		}
	}
	return span{tracer.StartSpan(operation, opts...)}, propagated
}

func (Tracer) HasQueryContext(sql string) bool {
	carrier := tracer.SQLCommentCarrier{Query: sql}
	parent, _ := carrier.Extract()
	return parent != nil
}

func (Tracer) WarnNamedPreparedStatement(name, sql, service string) {
	pgproxy.Logger().Warn(fmt.Sprintf(`%s is injecting parent trace information in its query, rendering the benefits of named prepared statements null. Please use service-level tracing or disable tracing entirely in your Postgres client. {name: %s, query: %s}`, service, name, sql))
}

type span struct{ span *tracer.Span }

func (s span) StartChild(operation string) pgproxy.Span {
	return span{s.span.StartChild(operation, options()...)}
}
func (s span) SetTag(key string, value any) { s.span.SetTag(key, value) }
func (s span) Finish(err error) {
	if err == nil {
		s.span.Finish()
	} else {
		s.span.Finish(tracer.WithError(err))
	}
}

// Sampled reads the priority the tracer chose when the root started, or the
// one inherited from the SQL comment. An undecided priority counts as kept. A
// nil span means the Datadog tracer is not running, so nothing is recorded.
func (s span) Sampled() bool {
	if s.span == nil {
		return false
	}
	priority, ok := s.span.Context().SamplingPriority()
	return !ok || priority > 0
}
