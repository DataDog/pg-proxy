// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package pgproxy

import (
	"context"
	"log/slog"
	"sync/atomic"
)

// Dependencies are runtime integrations an embedder supplies. The zero value
// is safe to use: every field is optional, and the accessors below substitute a
// no-op rather than requiring one.
type Dependencies struct {
	// LogHandler receives the library's internal diagnostic records. This is
	// the stdlib slog.Handler rather than a logging interface of our own, so an
	// embedder can pass what it already has — slog.NewJSONHandler, or an
	// adapter over its in-house logger — without implementing our types.
	// Optional: when nil, those records are discarded.
	LogHandler slog.Handler

	// Metrics receives the library's telemetry. Optional: nil discards metrics.
	Metrics MetricsClient

	// MetricNamespace is prepended to every metric name the library emits,
	// for example "datadog." + "pg_proxy.messages". Empty selects
	// DefaultMetricNamespace. Do not also configure a namespace on the client
	// itself (such as statsd.WithNamespace), or names are prefixed twice.
	MetricNamespace string

	// Tracer receives query spans. Optional: nil disables tracing.
	Tracer Tracer
}

// resolved pairs the Dependencies with the values derived from them, so the
// read path is one atomic load rather than a lock plus a nil check. Metrics in
// particular are emitted from per-message code, where an RWMutex would
// contend across every session goroutine.
type resolved struct {
	deps      Dependencies
	logger    *slog.Logger
	metrics   MetricsClient
	namespace string
	tracer    Tracer
}

var current atomic.Pointer[resolved]

func init() {
	// Establishes the no-op defaults, so the accessors work before (or without)
	// any Bootstrap call. Package inits run before any importer's, so no
	// library code can observe an unset value.
	current.Store(resolve(Dependencies{}))
}

// Bootstrap sets the process-wide Dependencies used by every pg-proxy
// component. Call once, as early as possible in the embedder's startup,
// before constructing any Session, TraceState, etc.
//
// Returns a restore func so tests can Bootstrap a fake and undo it, e.g. via
// t.Cleanup. Tests that do this must not run in parallel with each other:
// Bootstrap replaces one process-wide value, so concurrent Bootstrap calls
// race over which one wins.
func Bootstrap(d Dependencies) (restore func()) {
	previous := current.Swap(resolve(d))
	return func() { current.Store(previous) }
}

// Deps returns the current process-wide Dependencies as supplied. Prefer the
// Logger and Metrics accessors, which substitute no-ops for unset fields;
// these are the raw values, so they may be nil.
func Deps() Dependencies { return current.Load().deps }

// Logger returns the logger library code writes its internal diagnostics to.
//
// Never nil, so call sites need no guard: with no LogHandler Bootstrapped it
// discards everything. That matters because methods on a nil *slog.Logger
// panic rather than no-op.
func Logger() *slog.Logger { return current.Load().logger }

type loggerContextKey struct{}

// ContextWithLogger attaches a logger without modifying the parent context.
func ContextWithLogger(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerContextKey{}, logger)
}

// LoggerFromContext returns the request logger, falling back to Logger.
func LoggerFromContext(ctx context.Context) *slog.Logger {
	if logger, ok := ctx.Value(loggerContextKey{}).(*slog.Logger); ok && logger != nil {
		return logger
	}
	return Logger()
}

// Metrics returns the client library code reports telemetry to. Never nil, so
// call sites need no guard: with no Metrics Bootstrapped it discards.
func Metrics() MetricsClient { return current.Load().metrics }

func resolve(d Dependencies) *resolved {
	r := &resolved{
		deps:      d,
		logger:    slog.New(slog.DiscardHandler),
		metrics:   NoopMetrics{},
		namespace: d.MetricNamespace,
		tracer:    NoopTracer{},
	}
	if r.namespace == "" {
		r.namespace = DefaultMetricNamespace
	}
	if d.LogHandler != nil {
		r.logger = slog.New(d.LogHandler)
	}
	if d.Metrics != nil {
		r.metrics = namespacedMetrics{namespace: r.namespace, inner: d.Metrics}
	}
	if d.Tracer != nil {
		r.tracer = d.Tracer
	}
	return r
}

// Tracing returns the configured tracer, or a no-op tracer.
func Tracing() Tracer { return current.Load().tracer }
