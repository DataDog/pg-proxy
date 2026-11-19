---
# Unless explicitly stated otherwise all files in this repository are licensed
# under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/)
# Copyright 2026-Present Datadog, Inc.
title: "Telemetry configuration"
description: "Flags, environment variables, and examples for Datadog and OpenTelemetry."
---

The `pg-proxy` binary reads telemetry settings from command-line flags and environment variables. `proxy.yaml` contains database and listener settings; it has no telemetry fields.

The examples use Bash or Zsh, `pg-proxy` on your PATH, and an existing [database configuration](../../how-to/local-development/#3-configure-and-start-the-proxy). For applications using the Go library, see [Go telemetry configuration](../configuration/library/#telemetry-settings).

## Backend selection

| Flag | Export | Local logs |
| --- | --- | --- |
| `-telemetry otel` | Traces, metrics, and logs over OTLP/HTTP. | Text on stderr |
| `-telemetry datadog` | Traces and DogStatsD metrics to the Datadog Agent. | JSON on stderr |
| `-telemetry none` | None. | Text on stderr |
| Omitted | Choose each OTel signal with [exporter variables](#signal-selection). | Text on stderr |

With no flags or exporter variables set, PG Proxy exports no telemetry and writes text logs to stderr.

`-telemetry` selects all three backends instead of using the `OTEL_*_EXPORTER` selectors. In Datadog mode, the native SDK also reads `OTEL_TRACES_EXPORTER`, so conflicting values can still fail startup; see [Conflicting OTel settings](#conflicting-otel-settings).

At startup, PG Proxy warns when a preset overrides a conflicting exporter selector, naming the ignored variable and selected backend. Unset, empty, and matching settings are quiet. Native Datadog trace conflicts retain the startup checks below rather than being reported as ignored. These warnings follow `-log-level`.

**`-telemetry` and `-tracing` cannot be used together**: startup fails even if `-tracing` is `none`. Without `-telemetry`, `-tracing otel` or `-tracing none` overrides only the trace selector.

## OpenTelemetry

This example exports traces, metrics, and logs to a local OTLP/HTTP receiver. The receiver must have a pipeline for each signal. For a receiver that accepts only traces, use [signal selection](#signal-selection).

```bash
export OTEL_SERVICE_NAME=pg-proxy
export OTEL_RESOURCE_ATTRIBUTES='deployment.environment.name=local'
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318

pg-proxy \
  -config proxy.yaml \
  -telemetry otel
```

### Signal selection

To choose signals independently, omit `-telemetry` and set the variables below. Each accepts `otlp` or `none` and defaults to `none`. Values are case-insensitive and ignore surrounding whitespace. Values such as `stdout` and `console` are rejected.

| Variable | What `otlp` enables |
| --- | --- |
| `OTEL_TRACES_EXPORTER` | [Query spans](tracing/). |
| `OTEL_METRICS_EXPORTER` | [Proxy metrics](metrics/). |
| `OTEL_LOGS_EXPORTER` | Diagnostic log export. Logs also remain on stderr. |

For traces only:

```bash
export OTEL_TRACES_EXPORTER=otlp
export OTEL_METRICS_EXPORTER=none
export OTEL_LOGS_EXPORTER=none
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318

pg-proxy -config proxy.yaml
```

### Destination and transport

| Variable | Default | Purpose |
| --- | --- | --- |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `https://localhost:4318` | Base URL. The SDK appends `/v1/traces`, `/v1/metrics`, or `/v1/logs`. Use `http://` for a receiver without TLS. |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | `http/protobuf` | The only protocol `pg-proxy` supports. Other values fail startup for enabled signals. |

Per-signal settings override shared settings. For example, `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` overrides `OTEL_EXPORTER_OTLP_ENDPOINT` for traces. Use `METRICS` or `LOGS` in place of `TRACES` for the other signals.

A per-signal endpoint must include the signal path:

```bash
export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://127.0.0.1:4318/v1/traces
```

An endpoint configures where data goes; it does not enable export.

### Service identity

| Variable | Default | Purpose |
| --- | --- | --- |
| `OTEL_SERVICE_NAME` | `pg-proxy` | Service name for all three signals. |
| `OTEL_RESOURCE_ATTRIBUTES` | Unset | Comma-separated `key=value` attributes. For example: `deployment.environment.name=local,service.version=dev`. |

### Export timing and sampling

Metrics export every 60 seconds by default. For local development, set `OTEL_METRIC_EXPORT_INTERVAL=1000` to export every second. The binary collects [connection-pool gauges](metrics/#connection-pool-gauges) every 2 seconds. Each export sends their latest values.

The default trace sampler follows an incoming parent's sampling decision and samples new root traces. Application trace context comes from [SQL comments](tracing/#propagation).

### Advanced SDK settings

These variables are read by the OpenTelemetry Go SDK:

| Settings | Reference |
| --- | --- |
| Headers, TLS certificates, compression, request timeouts, and per-signal overrides | [OTLP/HTTP exporter configuration](https://pkg.go.dev/go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp@v1.46.0#pkg-overview) |
| `OTEL_TRACES_SAMPLER`, `OTEL_TRACES_SAMPLER_ARG` | [Trace sampling](https://opentelemetry.io/docs/languages/sdk-configuration/general/#otel_traces_sampler) |
| Trace and log queues and batch delays | [Trace batching](https://opentelemetry.io/docs/specs/otel/configuration/sdk-environment-variables/#batch-span-processor), [log batching](https://opentelemetry.io/docs/specs/otel/configuration/sdk-environment-variables/#batch-logrecord-processor) |
| Metric export intervals and timeouts | [Periodic metric export](https://opentelemetry.io/docs/specs/otel/configuration/sdk-environment-variables/#periodic-exporting-metricreader) |

### OTel metrics sent to Datadog

If your Collector exports to Datadog, use [Send OTel metrics to Datadog](../../how-to/otel-metrics-to-datadog/) to configure delta export and rate conversion. This keeps event counters in the rate format used by DogStatsD. It requires configuration in both the proxy and the Collector.

## Native Datadog

The Datadog Agent must accept traces and DogStatsD metrics. To collect logs, configure it to read the process's stderr or container logs. PG Proxy writes JSON logs but does not upload them directly.

Set the Agent address and service tags, and remove any previous OTel trace selection for this command. This `env` command works in Fish, Bash, and Zsh:

```bash
env -u OTEL_TRACES_EXPORTER \
  DD_SERVICE=pg-proxy \
  DD_ENV=local \
  DD_VERSION=dev \
  DD_AGENT_HOST=127.0.0.1 \
  pg-proxy \
  -config proxy.yaml \
  -telemetry datadog
```

### Identity and destination

| Variable | Default | Purpose |
| --- | --- | --- |
| `DD_SERVICE` | `pg-proxy` | Service name for traces, metrics, and JSON logs. |
| `DD_ENV` | Unset | Environment tag for all three signals. |
| `DD_VERSION` | Unset | Version tag for all three signals. |
| `DD_AGENT_HOST` | Local Agent | Agent hostname for traces and DogStatsD. Set it explicitly for a remote Agent. |
| `DD_TRACE_ENABLED` | `true` | Set to `false` to disable native tracing. |

With no destination configured, metrics go to `127.0.0.1:8125`. Traces use the local Agent socket if available, otherwise `http://localhost:8126`.

For other settings, use the SDK references:

- [Datadog Go tracing configuration](https://docs.datadoghq.com/tracing/trace_collection/library_config/go/): sampling, trace ports and URLs, and tracer options.
- [DogStatsD environment variables](https://github.com/DataDog/datadog-go/tree/v5.9.0#supported-environment-variables): metrics ports and Unix sockets.

### Conflicting OTel settings

The Datadog SDK also reads `OTEL_TRACES_EXPORTER`. Unsetting it, as in the example, avoids changing native tracing through an OTel setting. The checks below also ignore case and surrounding whitespace.

| Value left in `OTEL_TRACES_EXPORTER` | Startup behavior |
| --- | --- |
| `otlp` | Rejected unless `DD_TRACE_AGENT_PROTOCOL_VERSION` explicitly selects the transport. |
| `none` | Rejected unless `DD_TRACE_ENABLED` explicitly sets whether tracing is enabled. |

## Query parameters

Bind parameter capture is **off by default** in both Datadog and OTel modes. To include raw values as `db.query.parameter.0` … `db.query.parameter.n` on the root span, add `-trace-parameters=true` to a command that enables tracing:

```bash
pg-proxy \
  -config proxy.yaml \
  -telemetry otel \
  -trace-parameters=true
```

This flag does not enable tracing by itself or change the parameters sent to PostgreSQL. SQL text, including literal values, is still recorded as `db.statement`; error messages are also unaffected. Parameter capture does not obfuscate values. See [Recorded data](tracing/#recorded-data).

Values are added only to sampled traces, so unsampled traces skip the formatting cost. The decision is the one made when the root span starts: the Datadog sampling priority, or the OTel span's sampled flag. A trace kept later, for example because it contains an error, has no parameter tags. Values are copied on the Bind path and decoded when the trace finishes; see [Recorded data](tracing/#recorded-data) for the format and limits.

For the Go library, the equivalent option is `EnableTraceParameters`, also `false` by default.

## Logging and shutdown

Diagnostic logs remain on stderr in every mode: text for OTel or disabled export, JSON for native Datadog. Native JSON records include `message`, `status`, and service tags. Records without a span context do not automatically receive trace IDs.

`-log-level` sets the minimum severity for proxy logs on stderr and through OTLP. It defaults to `INFO`; use `-log-level DEBUG` to include debug records in both outputs. Standard levels are `DEBUG`, `INFO`, `WARN`, and `ERROR`, matched case-insensitively. SDK diagnostic logs retain their own settings and format.

After sessions stop, the enabled OTel providers flush concurrently under one fresh five-second deadline, including after cancellation or partial startup failure. All shutdown errors are collected without canceling another provider's flush. Native Datadog clients shut down afterward using their SDK's behavior; the five-second deadline applies only to OTel cleanup. Errors before telemetry starts are written only to stderr.
