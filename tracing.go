package pgproxy

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

// NoopTracer preserves protocol trace lifecycles without recording or exporting.
type NoopTracer struct{}

func (NoopTracer) StartRoot(string, string) (Span, bool)             { return noopSpan{}, false }
func (NoopTracer) HasQueryContext(string) bool                       { return false }
func (NoopTracer) WarnNamedPreparedStatement(string, string, string) {}

type noopSpan struct{}

func (noopSpan) StartChild(string) Span { return noopSpan{} }
func (noopSpan) SetTag(string, any)     {}
func (noopSpan) Finish(error)           {}
