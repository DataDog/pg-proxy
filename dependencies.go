package pgproxy

import (
	"log/slog"
	"sync/atomic"

	"github.com/DataDog/datadog-go/v5/statsd"
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

	// Metrics receives the library's telemetry. This is datadog-go's
	// ClientInterface, which stays fetchable outside Datadog, so it needs no
	// interface of our own either. Optional: when nil, nothing is emitted.
	Metrics statsd.ClientInterface
}

// resolved pairs the Dependencies with the values derived from them, so the
// read path is one atomic load rather than a lock plus a nil check. Metrics in
// particular are emitted from per-message code, where an RWMutex would
// contend across every session goroutine.
type resolved struct {
	deps    Dependencies
	logger  *slog.Logger
	metrics statsd.ClientInterface
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

// Metrics returns the client library code reports telemetry to. Never nil, so
// call sites need no guard: with no Metrics Bootstrapped it discards.
func Metrics() statsd.ClientInterface { return current.Load().metrics }

func resolve(d Dependencies) *resolved {
	r := &resolved{
		deps:    d,
		logger:  slog.New(slog.DiscardHandler),
		metrics: &statsd.NoOpClient{},
	}
	if d.LogHandler != nil {
		r.logger = slog.New(d.LogHandler)
	}
	if d.Metrics != nil {
		r.metrics = d.Metrics
	}
	return r
}
