---
title: "Go library configuration"
description: "Go types and callbacks for configuring PG Proxy in your application."
---

When building your own Go application with the PG Proxy library, use the types and callbacks below to configure its listener, authentication, routing, and telemetry.

If you are running the supplied executable, see [local development settings](../../../how-to/local-development/#settings) for the options it exposes.

## Process dependencies

Call `pgproxy.Bootstrap(pgproxy.Dependencies{...})` early in application startup, before constructing proxy components.

| Field | Type | When omitted |
| --- | --- | --- |
| `LogHandler` | `slog.Handler` | Library diagnostic records are discarded. |
| `Metrics` | `pgproxy.MetricsClient` | Metrics sent through the global client are discarded. |
| `MetricNamespace` | `string` | Metric names use `pgproxy.DefaultMetricNamespace`, `datadog.`. See [Metric names](../../observability/metrics/#metric-names). |
| `Tracer` | `pgproxy.Tracer` | Query spans are not recorded. |

These dependencies are process-wide. `Bootstrap` returns a restore function for tests; tests that replace these dependencies must not run in parallel with one another. Providers and exporters are owned by the application.

## Listener and server

`server.Config` composes the listener and session runtime.

| Field | Purpose |
| --- | --- |
| `ListenAddress` | TCP address to bind. |
| `TLSConfig`, `RequireTLS` | Client-side TLS configuration and requirement. |
| `Admission` | Session admission policy. |
| `Authenticate` | Authentication callback supplied by the application. |
| `LoadShedder` | Load-shedding integration. |
| `Sessions` | Required session registry for lookup and lifecycle management. |
| `Runtime` | Routing inventory and session integrations. |

`server.NewServer` requires both `Sessions` and `Runtime.Inventory`. Construction binds the listener socket.

## Routing and session runtime

| `server.RuntimeConfig` field | Purpose |
| --- | --- |
| `Inventory` | Required lookup of logical databases and backend state. |
| `Settings` | `topology.RuntimeConfig`, including `PostgresMessageBufferSize`. |
| `ParameterStatuses` | Optional callback to update a private copy of backend parameters. The callback must not retain the map. |
| `SessionExtensions` | Optional hooks and link decoration for each authenticated session. |
| `RandomInt` | Random-number function used by routing. |
| `Mirroring` | Session traffic-mirroring integration. |
| `Metrics` | Optional session metric implementation; otherwise sessions use the global metric client. |
| `EnableTraceParameters` | Set to `true` to capture raw bind values in Datadog and OTel query traces. Defaults to `false`; SQL text is unaffected. Also available in `session.Config`. |

`topology.DefaultPostgresMessageBufferSize` is `10_240`. The local executable supplies it explicitly.

`PostgresMessageBufferSize` controls when buffered replies are flushed to the client. It does not cap incoming message sizes; see [Client-message size limitations](../../limitations/#client-messages-have-no-configured-size-limit).

## Logical databases

`topology.LogicalDatabase.Cluster` selects the primary cluster. `ReadClusters` maps read-cluster names to their runtime weights. The application supplies configuration and backend discovery through the inventory interfaces.

## Telemetry settings

Install telemetry dependencies with `pgproxy.Bootstrap` before creating proxy components. Omitted integrations are disabled. Your application keeps providers and clients alive until sessions stop, then flushes and shuts them down.

### OpenTelemetry

Create the providers and exporters in your application, then adapt their instruments:

| Dependency | Adapter |
| --- | --- |
| `Tracer` | `pkg/otel.NewTracer(tracerProvider.Tracer("pg-proxy"))` |
| `Metrics` | `pkg/otel.NewMetrics(meterProvider.Meter("pg-proxy"))` |
| `LogHandler` | `pkg/otel.NewLogHandler(loggerProvider.Logger("pg-proxy"))` |

Pass these as `pgproxy.Dependencies` to `Bootstrap`. The adapters do not create exporters or install global OTel providers. Configure export, identity, and sampling on the providers your application owns.

### Datadog

Call `pkg/datadog.Start(Options{})` to create integrations from `DD_*` settings. Handle its error, pass the returned dependencies to `Bootstrap`, and call its returned shutdown function after sessions stop.

| `Options` field | Purpose |
| --- | --- |
| `Dependencies` | Reuse existing integrations. The helper creates only missing ones and never starts or stops supplied clients. |
| `TracerOptions` | Native Datadog tracer options. |
| `StatsdOptions` | Native DogStatsD client options. |
| `LogWriter` | JSON log destination; defaults to stderr. Ignored when a log handler is supplied. |
| `LogLevel` | Minimum JSON log severity as a `slog.Level`; defaults to INFO. Ignored when a log handler is supplied. |

If your application already runs the global Datadog tracer, supply `Tracer: datadog.Tracer{}` in `Options.Dependencies` so the helper reuses it.

See [Telemetry configuration](../../observability/) for backend settings, [Metrics](../../observability/metrics/) for measurements, and [Tracing](../../observability/tracing/) for propagation and recorded data.

## Traffic mirroring

`server.RuntimeConfig.Mirroring` is a `session.Mirroring` value. `Policy` returns the application's current `session.MirrorPolicy`; `AcquireShadow` resolves the named shadow database and acquires a link for replay. Both callbacks are needed for useful replay.

| Policy field | Meaning |
| --- | --- |
| `Enabled` | Enables mirroring for matching sessions. |
| `SourceDatabase` | Must match the session's logical database name. |
| `ShadowDatabase` | Destination passed to `AcquireShadow`; captured when the session's worker starts. |
| `DisableAt` | Required expiry time. A zero or past value makes mirroring inapplicable. |
| `QueueSize` | Per-session capacity in bytes, including estimated message overhead and in-flight replay buffers. |

The server runtime creates a shared byte budget for its sessions. Its default computation uses 5% of the container memory hard limit, with a minimum and fallback of 64 MiB. Small control messages can exceed the budget temporarily so flush and release boundaries are preserved.

The local reference binary does not supply these callbacks. See [Traffic mirroring](../../../explanation/traffic-mirroring/) for replay semantics and failure behavior.
