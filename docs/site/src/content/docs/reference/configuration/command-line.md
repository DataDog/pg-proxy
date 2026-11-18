---
# Unless explicitly stated otherwise all files in this repository are licensed
# under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/)
# Copyright 2026-Present Datadog, Inc.
title: "Flags and environment variables"
description: "Command-line flags and environment variables of the pg-proxy binary."
---

```sh
pg-proxy -config proxy.yaml [-telemetry datadog|otel|none] [-log-level LEVEL] [-trace-parameters]
```

Database, listener, session, and authentication settings are in the [configuration file](../configuration-file/). Telemetry is set only with flags and environment variables.

## Flags

| Flag | Default | Description |
| --- | --- | --- |
| `-config` | Required | Path to the configuration file. Must end in `.yaml`, `.yml`, or `.json`. |
| `-telemetry` | Unset | Telemetry preset for traces, metrics, and logs: `datadog`, `otel`, or `none`. When unset, each signal follows its `OTEL_*_EXPORTER` variable. See [Telemetry configuration](../../observability/). |
| `-tracing` | Unset | `otel` or `none`. Overrides `OTEL_TRACES_EXPORTER` for traces only. Cannot be combined with `-telemetry`. Prefer `-telemetry` or `OTEL_TRACES_EXPORTER`. |
| `-trace-parameters` | `false` | Record raw bind parameter values as `db.parameters` on sampled traces. SQL text is recorded either way. See [Query parameters](../../observability/#query-parameters). |
| `-log-level` | `INFO` | Minimum log level: `DEBUG`, `INFO`, `WARN`, or `ERROR`. Applies to stderr and OTLP logs. |

The binary does not accept positional arguments.

## Environment variables

| Variables | Used for |
| --- | --- |
| `OTEL_TRACES_EXPORTER`, `OTEL_METRICS_EXPORTER`, `OTEL_LOGS_EXPORTER` | Choose `otlp` or `none` for each signal when `-telemetry` is unset. |
| `OTEL_EXPORTER_OTLP_*`, `OTEL_SERVICE_NAME`, `OTEL_RESOURCE_ATTRIBUTES`, `OTEL_METRIC_EXPORT_INTERVAL`, and other `OTEL_*` SDK variables | OTLP destination, identity, and timing. See [OpenTelemetry](../../observability/#opentelemetry). |
| `DD_AGENT_HOST`, `DD_SERVICE`, `DD_ENV`, `DD_VERSION`, `DD_DOGSTATSD_*`, `DD_TRACE_*` | Native Datadog traces and metrics with `-telemetry datadog`. See [Native Datadog](../../observability/#native-datadog). |
| `PGSSLROOTCERT`, `PGSSLMODE`, `PGOPTIONS`, and other libpq `PG*` variables | Defaults for backend connections, for settings the configuration file does not set. See [`databases`](../configuration-file/#databases). |
| Any name in `password_env` or `scram_sha_256_env` | Credentials named in the configuration file. See [credential references](../client-authentication/#credential-references). |

## Signals and exit status

| Event | Behavior |
| --- | --- |
| Ready | Logs `proxy ready` with the listener address after all backend pools connect and the listener opens. |
| `SIGINT` or `SIGTERM` | Stops accepting connections, drains sessions, and closes pools within `proxy.shutdown_grace_period`. See [Graceful shutdown](../../../explanation/graceful-shutdown/). |
| Clean shutdown | Exit status `0`. |
| Invalid flags or configuration, a backend that cannot be reached at startup, or a shutdown that exceeds the grace period | Logs the error and exits with status `1`. |

The binary does not reload its configuration. Restart it to apply changes to the file, credential files, or certificates.
