---
# Unless explicitly stated otherwise all files in this repository are licensed
# under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/)
# Copyright 2026-Present Datadog, Inc.
title: "Go library configuration"
description: "Go types and callbacks for configuring PG Proxy in your application."
---

When building your own Go application with the PG Proxy library, use the types and callbacks below to configure its listener, authentication, routing, and telemetry.

If you run the `pg-proxy` binary, see [configuration file reference](../../../reference/configuration/configuration-file/) for the options it exposes.

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
| `AuthenticationMessageLimit` | Maximum message body size during authentication, in bytes. `0` means no limit. The binary uses 16 KiB. |
| `StartupTimeout` | Time allowed for TLS, startup, and authentication. `0` means no limit. The binary uses 10 seconds. |
| `MaxPendingConnections` | Maximum connections in startup at once; more are closed. `0` means no limit. The binary uses 100. |
| `LoadShedder` | Load-shedding integration. |
| `Sessions` | Required session registry for lookup and lifecycle management. |
| `Runtime` | Routing inventory and session integrations. |

`server.NewServer` requires both `Sessions` and `Runtime.Inventory`. Construction binds the listener socket.

## Routing and session runtime

| `server.RuntimeConfig` field | Purpose |
| --- | --- |
| `Inventory` | Required lookup of logical databases and backend state. |
| `Settings` | `topology.RuntimeConfig`, including `PostgresMessageBufferSize` and `ClientDisconnectTimer`. |
| `ParameterStatuses` | Optional callback to update a private copy of backend parameters. The callback must not retain the map. |
| `SessionExtensions` | Optional hooks and link decoration for each authenticated session. |
| `RandomInt` | Random-number function used by routing. |
| `Mirroring` | Session traffic-mirroring integration. |
| `Metrics` | Optional session metric implementation; otherwise sessions use the global metric client. |
| `EnableTraceParameters` | Set to `true` to capture raw bind values in sampled Datadog and OTel query traces. Defaults to `false`; SQL text is unaffected. Also available in `session.Config`. |

`topology.DefaultPostgresMessageBufferSize` is `10_240`. The `pg-proxy` binary sets it explicitly.

`PostgresMessageBufferSize` controls when buffered replies are flushed to the client. It does not cap incoming message sizes; see [Client-message size limitations](../../limitations/#client-messages-have-no-configured-size-limit).

`ClientDisconnectTimer` selects how each session checks for a client that disconnects during a backend checkout: `false`, the default, starts a goroutine and a ticker for each checkout, and `true` keeps one timer for the session. It is the library form of the `client_disconnect_timer` proxy setting.

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

The `pg-proxy` binary does not support mirroring and fails at startup if the configuration has a `mirroring` section. See [Traffic mirroring](../../../explanation/traffic-mirroring/) for replay semantics and failure behavior.
