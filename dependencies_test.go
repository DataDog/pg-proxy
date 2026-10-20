package pgproxy

import (
	"io"
	"log/slog"
	"testing"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests replace one process-wide value, so none of them may call
// t.Parallel(): concurrent Bootstraps would race over which one wins.

// TestAccessorsNeverNil pins the invariant the accessors' callers rely on to
// skip nil checks. It matters most for Logger: methods on a nil *slog.Logger
// dereference the handler and panic, so returning nil would turn a missing
// Bootstrap into a crash on the first diagnostic.
func TestAccessorsNeverNil(t *testing.T) {
	require.NotNil(t, Logger())
	require.NotNil(t, Metrics())

	// The no-op substitutes must be usable, not merely non-nil.
	Logger().Debug("must not panic")
	assert.NoError(t, Metrics().Count("test.count", 1, nil, 1))
}

func TestBootstrapInstallsDependencies(t *testing.T) {
	handler := slog.NewTextHandler(io.Discard, nil)
	metrics := &statsd.NoOpClient{}

	restore := Bootstrap(Dependencies{LogHandler: handler, Metrics: metrics})
	t.Cleanup(restore)

	assert.Same(t, handler, Logger().Handler(), "Logger must wrap the supplied handler")
	assert.Same(t, metrics, Metrics(), "Metrics must be the supplied client")
	assert.Same(t, handler, Deps().LogHandler, "Deps must report the raw value as supplied")
}

func TestRestoreRevertsToPreviousDependencies(t *testing.T) {
	before := Logger()

	restore := Bootstrap(Dependencies{LogHandler: slog.NewTextHandler(io.Discard, nil)})
	require.NotSame(t, before, Logger(), "Bootstrap must have taken effect")
	restore()

	assert.Same(t, before, Logger(), "restore must reinstate the prior logger")
}

// TestPartialDependenciesFallBack covers the mixed case: one field supplied,
// the other left unset, which must not disturb the unset one's no-op.
func TestPartialDependenciesFallBack(t *testing.T) {
	metrics := &statsd.NoOpClient{}

	restore := Bootstrap(Dependencies{Metrics: metrics})
	t.Cleanup(restore)

	assert.Same(t, metrics, Metrics())
	require.NotNil(t, Logger(), "an unset LogHandler must still yield a usable logger")
	Logger().Debug("must not panic")
}
