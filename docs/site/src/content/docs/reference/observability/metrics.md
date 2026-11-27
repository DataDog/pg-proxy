---
# Unless explicitly stated otherwise all files in this repository are licensed
# under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/)
# Copyright 2026-Present Datadog, Inc.
title: "Metrics"
description: "Metric clients, session overrides, and selected metric names."
---

The library uses its own `pgproxy.MetricsClient` interface for its process-wide metric client. Existing StatsD clients implement it directly. Supply it as `pgproxy.Dependencies.Metrics` during `Bootstrap`. A missing client is replaced by a no-op implementation.

`pkg/otel.NewMetrics` adapts a caller-owned OpenTelemetry meter. Counts become
counters, gauges become synchronous gauges, and distributions become histograms.
`Timing` records milliseconds. Names and tag values are preserved; tag keys split
at the first colon. The adapter records every measurement and ignores StatsD
transport sampling hints. The caller owns provider shutdown and export.

## Metric names

Metric names start with a namespace, `datadog.` by default, followed by `pg_proxy.`, for example `datadog.pg_proxy.messages`. Set `pgproxy.Dependencies.MetricNamespace` to replace the namespace. A missing trailing dot is added, and `Bootstrap` panics unless the namespace starts with a letter and contains only letters, digits, `_` and `.`; `pgproxy.NormalizeMetricNamespace` returns the same check as an error. Library code emits bare names, such as `pg_proxy.messages`, and the process-wide client installed by `Bootstrap` adds the namespace exactly once. Do not also set a namespace on the client, such as `statsd.WithNamespace`, or names are prefixed twice.

The exported name constants, such as `pgproxy.MetricMessageLatency` and `session.MetricMessages`, are bare names too. A session metric override receives bare names, so compare them against the constants. Forward them through `pgproxy.Metrics()`, or wrap them with `pgproxy.MetricName(...)` when sending to another client, or they lose the namespace.

The tables below show names with the default namespace.

## Session metrics

`server.RuntimeConfig.Metrics` accepts a `session.Metrics` implementation with these methods:

```go
Count(name string, value int64, tags session.MetricTags)
Distribution(name string, value float64, tags session.MetricTags)
```

These are method signatures, not a complete implementation. `MetricTags.Tags()` returns the tags; `MetricTags.Key()` returns a stable lookup key for that combination. When the session override is omitted, session measurements use the process-wide client. An override affects session measurements, not every metric in the library.

## Selected session measurements

All names below have the prefix `datadog.pg_proxy.`. This is a selected inventory, not a complete list.

| Name after prefix | Type | Measurement |
| --- | --- | --- |
| `messages` | Count | Client protocol messages observed. |
| `messages.latency_us` | Distribution | Message-handling time in microseconds. |
| `messages.queries` | Count | Queries observed. |
| `messages.parse.latency_us` | Distribution | Query-classification time in microseconds. |
| `messages.load_shedding_rejected` | Count | Requests rejected by load shedding. |
| `client_messages.body_length_bytes` | Distribution | Client-message body size estimated from decoded fields, in bytes, excluding the protocol header. |
| `flush_postgres.latency_us` | Distribution | Backend-buffer flush time in microseconds. |
| `sessions.buffer_flush.count` | Distribution | Client-buffer flush count recorded when the reply loop performs its final flush. |

Message measurements add a `message` tag to the session's base tags.

The body-size metric can omit trailing padding accepted by the decoder and does not enforce a size limit. See [Client-message size limitations](../../limitations/#client-messages-have-no-configured-size-limit).

## Connection-pool gauges

`pool.ExportPoolStats` emits these gauges from a `pgxpool.Stat`. It adds `direct_pool:true` or `direct_pool:false` to the supplied tags. All names have the prefix `datadog.pg_proxy.connections.`.

| Name after prefix | Value |
| --- | --- |
| `pool_total` | Total connections. |
| `pool_acquired` | Currently acquired connections. |
| `pool_acquired_total` | Cumulative successful acquisitions, emitted as a gauge. |
| `pool_idle` | Idle connections. |
| `pool_constructing` | Connections being created. |
| `pool_max` | Maximum connections. |

When a metrics backend is configured, the `pg-proxy` binary exports these gauges and `sessions.waiting` every 2 seconds for each cluster's primary pool, tagged `database:<dbname>` and `database_cluster:<cluster>`. It stops at shutdown. Library embedders start the same collection with `backend.Runtime.StartStats`, which `Runtime.Close` stops. Supplying a metric client does not start collection by itself.

## Pool waits and connection reuse

These names use the prefix `datadog.pg_proxy.` and are emitted through the process-wide metric client.

| Name after prefix | Type | Meaning |
| --- | --- | --- |
| `sessions.waiting` | Gauge | Sessions currently waiting for backend checkout, exported every 2 seconds with the [connection-pool gauges](#connection-pool-gauges). |
| `sessions.connect_wait_us` | Distribution | Time through a successful backend acquisition, in microseconds. Failed acquisitions are not included. |
| `sessions.connections.destroyed.release_non_idle` | Count | Backend connections closed because release found a state unsafe for reuse; includes a `state` tag. |
| `sessions.connections.guc_reset_failure` | Count | Failed resets before connection reuse. The reset runs `RESET ALL`; without role management it first runs `RESET SESSION AUTHORIZATION` and `RESET ROLE`. |
| `sessions.connections.reset` | Count | Releases that reach the reset decision, one each, with `outcome:reset` or `outcome:skipped` and a `reason` tag (see below). A release that closes its connection first, such as one left in an unsafe state, is not counted. |
| `sessions.connections.reset.latency_us` | Distribution | Time to send the reset and read its reply, in microseconds, including the `audit` check. Only releases whose reset succeeded; tagged with `reason`, and `check` when present. |
| `sessions.connections.reset.would_leak` | Count | In `audit` mode, settings that an audited release found changed: settings that `dynamic` would have left for the next client. One count per setting and check, tagged `setting:<name>` and `check:list` or `check:full`. On a release with the full check, a setting that both checks find is counted once with each tag. |

The three reset metrics depend on the database's [`reset_mode`](../../../explanation/pooling/#session-reset-modes). `always` emits none of them. In the other modes, each carries a `reset_mode` tag, and `sessions.connections.reset` carries one of these reasons. A release gets the first reason in the table that applies:

| `reason` | Meaning |
| --- | --- |
| `client_sql` | The client's SQL may have changed session state. |
| `prepared_stmt` | A `Bind` ran a prepared statement whose SQL may change session state, or one the proxy could not match to its `Parse`. |
| `parameter_status` | PostgreSQL reported a changed setting with `ParameterStatus`. |
| `server_version` | The server is one of the releases listed in [Reset modes](../../compatibility/#reset-modes) and no role implementation is in use, so the proxy reset a release it would otherwise have skipped. |
| `clean` | `dynamic` found nothing to reset and skipped the reset. |
| `audit` | `audit` reset a release that `dynamic` would have skipped. A `check` tag says what it checked: `none`, `list` for the built-in settings, the role, and `reset_audit_settings`, or `full` for the list and every setting that `pg_settings` shows changed in the session. |
| `off` | `off` skipped the reset. |

`reset_audit_rate` sets the share of `audit`-reason releases that run a check, `0.1` by default. A share of `0.01` of all `audit`-reason releases runs the `full` check, which reads `pg_settings` and costs more; with a `reset_audit_rate` below `0.01`, every checked release runs it. A `would_leak` count names the setting, never its value; `setting:current_user` stands for the role. The proxy also logs a `reset_would_leak` warning with the changed setting names and the backend PID.

A high waiting count together with an acquired count near the pool maximum suggests pool pressure. Low PostgreSQL CPU alone does not rule it out: open transactions can retain connections between statements.

Backend pool metrics use `database` for the configured backend name and `database_cluster` for the cluster key. Session metrics describe the client-facing logical database. Account for proxy instances and `direct_pool` when aggregating pool gauges.

## Traffic mirroring

These counters use the prefix `datadog.pg_proxy.session.` and the process-wide metric client, independently of the session metric override.

| Name after prefix | Meaning |
| --- | --- |
| `message_mirrored` | Message copies admitted to the queue. This does not mean replay succeeded. |
| `message_mirror_dropped` | The first copy in a batch rejected by a capacity limit; `reason:session` or `reason:global` identifies the budget. Later messages in that batch are neither copied nor counted. |
| `mirror.replayed` | Messages forwarded to a shadow link (`kind:msg`), completed flushes (`kind:flush`), or a `Parse` skipped because the shadow connection already had the statement (`kind:parse_existing`; also counted as `kind:msg`). This is not a result-equivalence check. |
| `mirror.replay_errors` | Replay failures, tagged by `op`: `acquire` (could not get a shadow connection), `flush` (flush or drain failed), `stmt_rejected` (an error occurred in a batch that created a prepared statement, so the statement may be missing and the connection was discarded), or `reparse_budget` (the session's queue byte cap or the shared byte budget could not admit a `Parse` that re-creates a missing statement on the shadow connection, so the batch was skipped). Other errors that the shadow returns for replayed statements are counted only in `mirror.replay_error_response`. |
| `mirror.replay_error_response` | Error responses from the shadow, tagged `code:<SQLSTATE>`. After a non-fatal error, the flush still counts as `mirror.replayed` with `kind:flush`, unless the batch created a prepared statement since its last flush; then it counts as `mirror.replay_errors` with `op:stmt_rejected`. A `FATAL` error counts as `op:flush`. |
| `mirror.shadow_reparse` | `Parse` messages the worker sent to re-create a named statement on a shadow connection that lacked it. |
| `mirror.catalog_miss` | A replayed `Bind` or `Describe` named a statement the worker has no definition for, for example because its `Parse` was dropped. The message is still replayed and can fail on the shadow. |
| `mirror.worker.stop_timeout` | Worker teardown exceeded its first waiting period and attempted to force-close its shadow link. |
| `mirror.worker.stop_abandoned` | The worker still had not stopped after the second waiting period. |

These are selected measurements, not an exhaustive count of all lost replay work. A dropped message can cause later messages in its batch to be skipped, and closing a queue discards its unread tail. See [Traffic mirroring](../../../explanation/traffic-mirroring/).

## Local development

Use `-telemetry datadog` for native DogStatsD export. The public StatsD client
implements `MetricsClient` directly, preserving distributions, timing units,
and sampling. `pkg/datadog.Start` can create it or reuse a supplied client.
When `Start` creates the client, it turns on client-side aggregation for
distributions and timings. At each flush, every 2 seconds by default, the client
sends each metric context once with all of its samples.

Set `OTEL_METRICS_EXPORTER=otlp` to enable OTLP/HTTP metric export in the `pg-proxy`
binary. `OTEL_METRIC_EXPORT_INTERVAL` controls the interval in milliseconds
(default `60000`); `1000` is useful for local verification. The provider also
flushes on shutdown. Use a receiver with a metrics pipeline, then run queries
and look for `datadog.pg_proxy.messages.queries` and
`datadog.pg_proxy.messages`.

The binary also collects [connection-pool gauges](#connection-pool-gauges) every 2 seconds. The exporter sends the latest values at each export interval.
See the [telemetry configuration](../) for endpoint settings.
Embedders supply a metric client through `pgproxy.Dependencies.Metrics`, described
in [Go library configuration](../../configuration/library/).
