package otel_test

import (
	"context"
	"log/slog"
	"testing"

	pgproxy "github.com/DataDog/pg-proxy"
	proxyotel "github.com/DataDog/pg-proxy/pkg/otel"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	otellog "go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
)

type logExporter struct{ records []sdklog.Record }

func (e *logExporter) Export(_ context.Context, records []sdklog.Record) error {
	for _, r := range records {
		e.records = append(e.records, r.Clone())
	}
	return nil
}
func (*logExporter) Shutdown(context.Context) error   { return nil }
func (*logExporter) ForceFlush(context.Context) error { return nil }
func TestLogsReachOTelSDK(t *testing.T) {
	exporter := &logExporter{}
	provider := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewSimpleProcessor(exporter)))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
	handler := proxyotel.NewLogHandler(provider.Logger("pg-proxy"))
	t.Cleanup(pgproxy.Bootstrap(pgproxy.Dependencies{LogHandler: handler}))
	pgproxy.Logger().With("cluster", "orders").WithGroup("pool").Warn("connection failed", "attempts", 3, slog.Group("target", "port", 5432))
	pgproxy.Logger().Info("ready")
	require.Len(t, exporter.records, 2)
	record := exporter.records[0]
	require.Equal(t, "connection failed", record.Body().AsString())
	require.Equal(t, otellog.SeverityWarn, record.Severity())
	require.False(t, record.Timestamp().IsZero())
	attrs := map[string]attribute.Value{}
	record.WalkAttributes(func(a attribute.KeyValue) bool { attrs[string(a.Key)] = a.Value; return true })
	require.Equal(t, "orders", attrs["cluster"].AsString())
	require.Equal(t, int64(3), attrs["pool.attempts"].AsInt64())
	require.Equal(t, int64(5432), attrs["pool.target.port"].AsInt64())
	require.Equal(t, 0, exporter.records[1].AttributesLen(), "derived handlers must not mutate their parent")
}
