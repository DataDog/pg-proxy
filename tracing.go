// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package pgproxy

import "time"

// Tracer owns backend-specific span creation and SQL-comment propagation.
// Implementations must be safe for concurrent sessions. The caller owns exporter
// setup and shutdown. A query without application context starts a local trace.
type Tracer interface {
	// StartRoot returns a non-nil span and whether SQL supplied a valid parent.
	StartRoot(operation, sql string) (Span, bool)
	HasQueryContext(sql string) bool
	// WarnNamedPreparedStatement reports the backend's advice when trace IDs
	// make a named prepared statement's SQL unique on each execution.
	WarnNamedPreparedStatement(name, sql, service string)
}

// Span is the small tracing surface used by the protocol engine. It deliberately
// does not expose backend span, context, or options types.
type Span interface {
	StartChild(operation string) Span
	SetTag(key string, value any)
	Finish(err error)
}

// SamplingReporter is optionally implemented by spans that know whether their
// trace will be kept. The proxy skips building costly tag values, such as Bind
// parameters, for traces that report false. Spans that do not implement it are
// treated as kept.
type SamplingReporter interface {
	Sampled() bool
}

// OperationMetricsReporter is an optional root-span capability. The protocol
// engine reports each completed child operation independently of span creation.
// A false KeepChildSpans result suppresses the child spans for that root.
type OperationMetricsReporter interface {
	KeepChildSpans() bool
	RecordOperation(operation string, elapsed time.Duration, outcome OperationOutcome)
}

// OperationOutcome records which side caused a query phase to fail.
type OperationOutcome uint8

const (
	OperationSucceeded OperationOutcome = iota
	OperationProxyError
	OperationBackendError
)

// NoopTracer preserves protocol trace lifecycles without recording or exporting.
type NoopTracer struct{}

func (NoopTracer) StartRoot(string, string) (Span, bool)             { return noopSpan{}, false }
func (NoopTracer) HasQueryContext(string) bool                       { return false }
func (NoopTracer) WarnNamedPreparedStatement(string, string, string) {}

type noopSpan struct{}

func (noopSpan) StartChild(string) Span { return noopSpan{} }
func (noopSpan) SetTag(string, any)     {}
func (noopSpan) Finish(error)           {}
func (noopSpan) Sampled() bool          { return false }
