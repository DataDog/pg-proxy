// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package session

import (
	"context"
	"runtime"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	pgproxy "github.com/DataDog/pg-proxy"
	proxyotel "github.com/DataDog/pg-proxy/pkg/otel"
)

const (
	largeBindPayload = 64 << 20 // one binary bytea parameter
	// Reading the Bind and re-encoding it for Postgres each hold one copy of the payload.
	largeBindAmplificationBudget = 2.1
	// Tracing may add span state, but not another copy of the payload.
	largeBindTraceBudget = 64 << 10
	largeBindSQL         = "INSERT INTO files (data) VALUES ($1) /*traceparent='00-0000000000000000000000000000002a-000000000000000b-01'*/"
)

// TestLargeBindAmplification bounds the heap the session allocates to forward
// one Bind with a 64 MiB parameter, so that memory use stays proportional to the
// message. The sampled-trace case adds the cost of trace parameters.
func TestLargeBindAmplification(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates several hundred MiB")
	}
	if raceEnabled {
		t.Skip("the race detector changes allocation sizes")
	}
	h := newCycleHarness(t, false)
	client := h.newClient(func(cfg *Config) { cfg.EnableTraceParameters = true })
	small, large := largeBindScript(t, h, 1), largeBindScript(t, h, largeBindPayload)

	require.NoError(t, client.run(small)) // first-cycle session state
	untraced := cycleHeapBytes(t, client, large)
	t.Logf("forwarding a %d MiB Bind allocated %.2fx the payload", largeBindPayload>>20, float64(untraced)/largeBindPayload)
	require.LessOrEqual(t, float64(untraced), largeBindAmplificationBudget*largeBindPayload)

	t.Run("sampled trace with parameters", func(t *testing.T) {
		recorder := tracetest.NewSpanRecorder()
		provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder), sdktrace.WithSampler(sdktrace.AlwaysSample()))
		t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
		t.Cleanup(pgproxy.Bootstrap(pgproxy.Dependencies{Tracer: proxyotel.NewTracer(provider.Tracer("test"))}))

		require.NoError(t, client.run(small)) // first traced cycle
		traced := cycleHeapBytes(t, client, large)
		t.Logf("with a sampled trace and trace parameters, it allocated %.2fx the payload, %d bytes more",
			float64(traced)/largeBindPayload, int64(traced)-int64(untraced))
		foundParameters := false
		for _, span := range recorder.Ended() {
			for _, attr := range span.Attributes() {
				foundParameters = foundParameters || attr.Key == "db.parameters.0"
			}
		}
		require.True(t, foundParameters, "the trace must carry the Bind parameters")
		require.LessOrEqual(t, traced, untraced+largeBindTraceBudget)
	})
}

// largeBindScript inserts one binary parameter of payload bytes.
func largeBindScript(tb testing.TB, h *cycleHarness, payload int) cycleScript {
	return h.script(tb,
		[]pgproto3.FrontendMessage{
			&pgproto3.Parse{Query: largeBindSQL, ParameterOIDs: []uint32{17}},
			&pgproto3.Bind{ParameterFormatCodes: []int16{1}, Parameters: [][]byte{make([]byte, payload)}},
			&pgproto3.Execute{}, &pgproto3.Sync{},
		},
		[]pgproto3.BackendMessage{
			&pgproto3.ParseComplete{}, &pgproto3.BindComplete{},
			&pgproto3.CommandComplete{CommandTag: []byte("INSERT 0 1")}, cycleReadyForQuery(),
		})
}

// cycleHeapBytes returns the heap bytes allocated while client runs script.
func cycleHeapBytes(t *testing.T, client *cycleClient, script cycleScript) uint64 {
	t.Helper()
	// Two collections empty sync.Pools, so the cycle cannot reuse a large
	// buffer that pgproto3 pooled during an earlier cycle.
	runtime.GC()
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	require.NoError(t, client.run(script))
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}
