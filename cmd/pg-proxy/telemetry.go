// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	pgproxy "github.com/DataDog/pg-proxy"
	proxydatadog "github.com/DataDog/pg-proxy/pkg/datadog"
	proxyotel "github.com/DataDog/pg-proxy/pkg/otel"
)

// runWithTelemetry owns provider lifetime and error reporting, including failed
// proxy startup. Runtime failures must be logged before the exporters flush.
func runWithTelemetry(ctx context.Context, cfg config) (err error) {
	deps, shutdown, err := setupTelemetry(ctx, cfg.telemetry)
	if err != nil {
		slog.Error("telemetry setup failed", "error", err)
		return err
	}
	restore := pgproxy.Bootstrap(deps)
	defer restore()
	defer func() {
		if err != nil {
			pgproxy.Logger().Error("pg-proxy stopped", "error", err)
		}
		if shutdownErr := shutdown(); shutdownErr != nil {
			// Exporters have stopped, but the handler still writes local diagnostics.
			pgproxy.Logger().Error("telemetry shutdown failed", "error", shutdownErr)
			err = errors.Join(err, shutdownErr)
		}
	}()
	for _, warning := range cfg.telemetryWarnings {
		pgproxy.Logger().Warn(warning)
	}
	return run(ctx, cfg)
}

// setupTelemetry creates only the selected providers. It never installs global
// OTel providers, and local diagnostic logging stays enabled with OTLP export.
func setupTelemetry(ctx context.Context, cfg telemetryConfig) (deps pgproxy.Dependencies, shutdown func() error, err error) {
	if err := cfg.validate(); err != nil {
		return deps, nil, err
	}
	deps = pgproxy.Dependencies{LogHandler: slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: cfg.LogLevel}), Tracer: pgproxy.NoopTracer{}}
	var stops []func(context.Context) error
	var stopNative func() error
	shutdown = func() error {
		result := shutdownOTelProviders(stops)
		if stopNative != nil {
			// Native SDK shutdown is synchronous and does not accept a context.
			result = errors.Join(result, stopNative())
		}
		return result
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, shutdown())
		}
	}()
	if cfg.Traces == traceDatadog || cfg.Metrics == metricDogStatsD || cfg.Logs == logsJSON {
		// Supply disabled integrations so the helper creates only selected ones.
		native := proxydatadog.Options{
			Dependencies: pgproxy.Dependencies{Tracer: deps.Tracer, Metrics: pgproxy.NoopMetrics{}, LogHandler: deps.LogHandler},
			LogLevel:     cfg.LogLevel,
		}
		if cfg.Traces == traceDatadog {
			native.Dependencies.Tracer = nil
			// The SDK's start option takes precedence over the environment.
			if _, configured := os.LookupEnv("DD_TRACER_EXPERIMENTAL_SPAN_POOL_ENABLED"); !configured {
				native.TracerOptions = []tracer.StartOption{tracer.WithSpanPool(true)}
			}
		}
		if cfg.Metrics == metricDogStatsD {
			native.Dependencies.Metrics = nil
		}
		if cfg.Logs == logsJSON {
			native.Dependencies.LogHandler = nil
		}
		deps, stopNative, err = proxydatadog.Start(native)
		if err != nil {
			return deps, shutdown, err
		}
	}
	if cfg.Traces != traceOTel && cfg.Metrics != metricOTel && cfg.Logs != logsOTel {
		return deps, shutdown, nil
	}
	res, err := resource.New(ctx, resource.WithAttributes(attribute.String("service.name", "pg-proxy")), resource.WithFromEnv())
	if err != nil {
		return deps, shutdown, errors.New("invalid OpenTelemetry resource configuration")
	}
	if cfg.Traces == traceOTel {
		exporter, err := otlptracehttp.New(ctx)
		if err != nil {
			return deps, shutdown, errors.New("could not initialize OpenTelemetry trace exporter")
		}
		provider := sdktrace.NewTracerProvider(sdktrace.WithResource(res), sdktrace.WithBatcher(exporter))
		stops = append(stops, provider.Shutdown)
		deps.Tracer = proxyotel.NewTracer(provider.Tracer("pg-proxy"))
	}
	if cfg.Metrics == metricOTel {
		exporter, err := otlpmetrichttp.New(ctx)
		if err != nil {
			return deps, shutdown, errors.New("could not initialize OpenTelemetry metric exporter")
		}
		provider := sdkmetric.NewMeterProvider(sdkmetric.WithResource(res), sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter)))
		stops = append(stops, provider.Shutdown)
		deps.Metrics = proxyotel.NewMetrics(provider.Meter("pg-proxy"))
	}
	if cfg.Logs == logsOTel {
		exporter, err := otlploghttp.New(ctx)
		if err != nil {
			return deps, shutdown, errors.New("could not initialize OpenTelemetry log exporter")
		}
		provider := sdklog.NewLoggerProvider(sdklog.WithResource(res), sdklog.WithProcessor(sdklog.NewBatchProcessor(exporter)))
		stops = append(stops, provider.Shutdown)
		deps.LogHandler = fanoutLogHandler{
			level:    cfg.LogLevel,
			handlers: []slog.Handler{deps.LogHandler, proxyotel.NewLogHandler(provider.Logger("pg-proxy"))},
		}
	}
	return deps, shutdown, nil
}

// Independent providers share one fresh deadline, including after cancellation
// or partial startup failure. An error must not cancel another provider's flush.
func shutdownOTelProviders(stops []func(context.Context) error) error {
	if len(stops) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	errs := make([]error, len(stops))
	var workers sync.WaitGroup
	for i, stop := range stops {
		workers.Go(func() { errs[i] = stop(ctx) })
	}
	workers.Wait()
	return errors.Join(errs...)
}

func validateOTLPProtocol(signal string) error {
	key := "OTEL_EXPORTER_OTLP_" + signal + "_PROTOCOL"
	protocol := os.Getenv(key)
	if protocol == "" {
		key = "OTEL_EXPORTER_OTLP_PROTOCOL"
		protocol = os.Getenv(key)
	}
	if protocol != "" && protocol != "http/protobuf" {
		return fmt.Errorf("%s must be http/protobuf; the OSS binary supports OTLP/HTTP only", key)
	}
	return nil
}

// fanoutLogHandler keeps terminal diagnostics when log export is enabled.
type fanoutLogHandler struct {
	level    slog.Level
	handlers []slog.Handler
}

func (h fanoutLogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	if level < h.level {
		return false
	}
	for _, handler := range h.handlers {
		if handler.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

func (h fanoutLogHandler) Handle(ctx context.Context, record slog.Record) error {
	if record.Level < h.level {
		return nil
	}
	var err error
	for _, handler := range h.handlers {
		if handler.Enabled(ctx, record.Level) {
			err = errors.Join(err, handler.Handle(ctx, record.Clone()))
		}
	}
	return err
}

func (h fanoutLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	next := make([]slog.Handler, len(h.handlers))
	for i, handler := range h.handlers {
		next[i] = handler.WithAttrs(attrs)
	}
	return fanoutLogHandler{level: h.level, handlers: next}
}

func (h fanoutLogHandler) WithGroup(name string) slog.Handler {
	next := make([]slog.Handler, len(h.handlers))
	for i, handler := range h.handlers {
		next[i] = handler.WithGroup(name)
	}
	return fanoutLogHandler{level: h.level, handlers: next}
}
