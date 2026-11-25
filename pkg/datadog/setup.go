// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package datadog

import (
	"errors"
	"io"
	"log/slog"
	"net"
	"net/url"
	"os"
	"strings"
	"sync"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"

	pgproxy "github.com/DataDog/pg-proxy"
)

// Options accepts existing integrations and the public SDKs' own options.
type Options struct {
	// Supplied integrations are reused and never started or stopped here.
	// Set Tracer to Tracer{} when the application already owns the global tracer.
	Dependencies  pgproxy.Dependencies
	TracerOptions []tracer.StartOption
	StatsdOptions []statsd.Option
	// ChildSpanMode enables experimental DogStatsD phase metrics. It requires
	// this helper to own the tracer. With sampled mode, callers must omit
	// single-span rules and late sampling overrides from TracerOptions.
	ChildSpanMode ChildSpanMode
	// LogWriter defaults to stderr. Ignored when Dependencies.LogHandler is set.
	LogWriter io.Writer
	// LogLevel defaults to INFO. Ignored when Dependencies.LogHandler is set.
	LogLevel slog.Level
}

// Start creates missing integrations using DD_* environment settings. Call once
// at process startup, then install the returned dependencies with pgproxy.Bootstrap.
// With no supplied Tracer, this starts the process-global Datadog tracer, replacing
// any running instance. Applications that already own it must supply Tracer{}.
// After sessions stop, shutdown flushes only the clients created here. JSON logs
// require Agent log collection; this helper does not send logs over the network.
func Start(opts Options) (pgproxy.Dependencies, func() error, error) {
	deps := opts.Dependencies
	if err := opts.ChildSpanMode.Validate(); err != nil {
		return deps, nil, err
	}
	if opts.ChildSpanMode.enabled() && deps.Tracer != nil {
		return deps, nil, errors.New("child span mode requires this helper to own the native tracer")
	}
	if _, noMetrics := deps.Metrics.(pgproxy.NoopMetrics); opts.ChildSpanMode.enabled() && noMetrics {
		return deps, nil, errors.New("child span mode requires a metrics client")
	}
	if deps.Tracer == nil {
		if err := ValidateTracingEnvironment(); err != nil {
			return deps, nil, err
		}
	}
	service := os.Getenv("DD_SERVICE")
	if service == "" {
		service = "pg-proxy"
	}
	if deps.LogHandler == nil {
		writer := opts.LogWriter
		if writer == nil {
			writer = os.Stderr
		}
		handler := slog.NewJSONHandler(writer, &slog.HandlerOptions{
			Level: opts.LogLevel,
			ReplaceAttr: func(groups []string, attr slog.Attr) slog.Attr {
				if len(groups) == 0 {
					switch attr.Key {
					case slog.MessageKey:
						attr.Key = "message"
					case slog.LevelKey:
						attr.Key = "status"
					}
				}
				return attr
			},
		})
		attrs := []slog.Attr{slog.String("service", service)}
		for _, key := range []string{"env", "version"} {
			envKey := "DD_ENV"
			if key == "version" {
				envKey = "DD_VERSION"
			}
			if value := os.Getenv(envKey); value != "" {
				attrs = append(attrs, slog.String(key, value))
			}
		}
		deps.LogHandler = handler.WithAttrs(attrs)
	}
	var client *statsd.Client
	if deps.Metrics == nil {
		address := ""
		if os.Getenv("DD_AGENT_HOST") == "" && os.Getenv("DD_DOGSTATSD_URL") == "" {
			port := os.Getenv("DD_DOGSTATSD_PORT")
			if port == "" {
				port = "8125"
			}
			address = net.JoinHostPort("127.0.0.1", port)
		}
		// Needs Datadog Agent 7.25.0 or later. StatsdOptions can override it.
		options := []statsd.Option{statsd.WithExtendedClientSideAggregation()}
		if os.Getenv("DD_SERVICE") == "" {
			options = append(options, statsd.WithTags([]string{"service:" + service}))
		}
		options = append(options, opts.StatsdOptions...)
		var err error
		client, err = statsd.New(address, options...)
		if err != nil {
			return deps, nil, errors.New("could not initialize Datadog metrics client")
		}
		deps.Metrics = client
	}
	ownsTracer := deps.Tracer == nil
	if ownsTracer {
		options := []tracer.StartOption{tracer.WithService(service)}
		// dd-trace-go expects host:port here, while the public StatsD client
		// accepts the standard udp:// URL. Keep the tracer's health/runtime
		// metrics working with the same DD_DOGSTATSD_URL as proxy metrics.
		if destination, err := url.Parse(os.Getenv("DD_DOGSTATSD_URL")); err == nil && destination.Scheme == "udp" && destination.Hostname() != "" {
			port := destination.Port()
			if port == "" {
				port = "8125"
			}
			options = append(options, tracer.WithDogstatsdAddr(net.JoinHostPort(destination.Hostname(), port)))
		}
		options = append(options, opts.TracerOptions...)
		if err := tracer.Start(options...); err != nil {
			// The SDK can fail after installing the tracer and starting workers.
			tracer.Stop()
			if client != nil {
				_ = client.Close()
			}
			return deps, nil, errors.New("could not initialize Datadog tracer")
		}
		deps.Tracer = Tracer{}
		if opts.ChildSpanMode.enabled() {
			deps.Tracer = operationTracer{mode: opts.ChildSpanMode}
		}
	}
	shutdown := sync.OnceValue(func() error {
		if ownsTracer {
			tracer.Stop()
		}
		if client != nil {
			return client.Close()
		}
		return nil
	})
	return deps, shutdown, nil
}

// ValidateTracingEnvironment checks native tracing's exporter-selection
// constraints without starting clients or replacing the process-global tracer.
// Callers that supply their own tracer do not need these environment settings.
func ValidateTracingEnvironment() error {
	// The public SDK also reads OTel exporter selection. Reject conflicts rather
	// than silently disabling native tracing or routing it to OTLP.
	switch strings.ToLower(strings.TrimSpace(os.Getenv("OTEL_TRACES_EXPORTER"))) {
	case "otlp":
		if os.Getenv("DD_TRACE_AGENT_PROTOCOL_VERSION") == "" {
			return errors.New("native Datadog tracing requires OTEL_TRACES_EXPORTER to be unset or DD_TRACE_AGENT_PROTOCOL_VERSION to be set")
		}
	case "none":
		if os.Getenv("DD_TRACE_ENABLED") == "" {
			return errors.New("native Datadog tracing requires OTEL_TRACES_EXPORTER to be unset or DD_TRACE_ENABLED to be set")
		}
	}
	return nil
}
