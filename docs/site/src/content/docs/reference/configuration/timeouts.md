---
# Unless explicitly stated otherwise all files in this repository are licensed
# under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/)
# Copyright 2026-Present Datadog, Inc.
title: "Timeouts"
description: "Configure backend statement timeouts without relying on session affinity."
---

Connection setup, waiting for a pooled connection, and executing SQL are separate operations. A timeout for one does not automatically bound the others.

| Setting | Scope |
| --- | --- |
| Client `connect_timeout` | Establishing the client's connection to the proxy; semantics depend on the driver. |
| Client pool acquire timeout | Waiting for an available connection in the application's own pool. |
| PostgreSQL `statement_timeout` | Execution of a statement on the backend, including time spent waiting for locks. |
| Client query deadline | How long the client waits; cancellation and connection reuse depend on the driver and protocol state. |

The proxy does not limit how long a session waits for a connection from a primary pool. The wait ends when a connection is free, or when the proxy detects that a TCP client disconnected, which it checks every 100 ms during a checkout. On platforms other than Linux, the check sees a socket error or hangup but not a client's orderly close. The proxy then closes the backend connection it acquired, if any. A read routed to a replica waits at most 50 ms before it falls back to the primary; see [Read routing](../../../explanation/read-routing/).

## Backend pool setting

In the `pg-proxy` configuration file, add `connection_options` to the relevant `databases` entry:

```yaml
databases:
  - name: primary
    host: 127.0.0.1
    port: 15432
    dbname: postgres
    username: postgres
    password: postgres
    sslmode: disable
    connection_options:
      statement_timeout: "5s"
```

This is a database-section example; retain the file's `clusters`, `logical_databases`, and `proxy` settings. See [Configuration file](../../../reference/configuration/configuration-file/). The options are supplied when opening backend connections. A unitless statement timeout is measured in milliseconds; `0` disables it. Specify a unit to make the intended duration clear.

These backend options are separate from the `pg_proxy.*` options in a client's connection string. The portable server composition does not forward arbitrary client startup settings to the backend.

For a different limit on a subset of traffic, configure a separate backend entry and logical database with its own `connection_options`. Pools targeting the same physical database still contribute to that database's total connection load.

## Transaction override

Use `SET LOCAL` within a transaction to override the baseline for that transaction:

```sql
BEGIN;
SET LOCAL statement_timeout = '10s';
SHOW statement_timeout;
SELECT 1;
COMMIT;
```

The setting ends with the transaction. A standalone `SET statement_timeout` followed by a later query may use different backend connections and will not reliably configure that query.

`SHOW statement_timeout` reports the setting on the backend handling that statement. With read routing, different backend pools can have different baselines.

## Errors and cancellation

A statement timeout is enforced by PostgreSQL. Inside an explicit transaction, handle the error and roll back before continuing. A client disconnect or timeout may instead leave the client uncertain about the outcome. Reconnecting does not establish whether a write committed; retry only when the operation's semantics make that safe.

PostgreSQL documents the exact [statement timeout semantics](https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-STATEMENT-TIMEOUT) and [SET LOCAL behavior](https://www.postgresql.org/docs/current/sql-set.html).

## Shutdown grace period

In the `pg-proxy` binary, `proxy.shutdown_grace_period` (JSON: `shutdownGracePeriod`) bounds session and pool cleanup after `SIGINT` or `SIGTERM`. It accepts a positive Go duration string, such as `30s` or `2m`. Omitted or empty values use `30s`; zero, negative, invalid, and overflowing durations fail at startup.

This limit starts when shutdown begins, not when a transaction starts. It does not change statement or connection timeouts. Give the service manager a longer stop timeout so it does not kill the process before cleanup and telemetry flushing finish. See [Run in production](../../../how-to/run-in-production/#7-restart-and-roll-out-changes).

## Fixed limits in the binary

These limits are built into the `pg-proxy` binary and cannot be configured.

| Limit | Value | Behavior |
| --- | --- | --- |
| Connection startup | 10 seconds | TLS, startup, and authentication must finish within this time, or the connection is closed. The limit ends once the session starts; it does not limit queries or session length. |
| Pending connections | 100 | At most 100 connections can be in startup at once. More connections are closed at once. Established sessions do not count. |
| Authentication message size | 16 KiB | Larger messages during authentication fail the connection. Messages after authentication have no size limit. |
| Connection reset on release | 5 seconds | If the reset that runs when a backend connection goes back to the pool takes longer, the connection is closed. See [Transaction pooling](../../../explanation/pooling/#session-state-belongs-to-the-backend). |
| Telemetry flush on shutdown | 5 seconds | OpenTelemetry providers have this long to send buffered data after sessions close. |
| Backend check at startup | 1 second | Each primary and replica pool must open a connection and answer a ping within this time, or the binary fails to start. |
| Client TCP keepalive | 15 seconds idle, then 3 probes 15 seconds apart | An idle client that disappears without closing its connection is detected after about 60 seconds, and its session ends. While the proxy has unacknowledged data for the client, TCP retransmission timing applies instead. |

Applications that embed the library set the first three with `server.Config.StartupTimeout`, `MaxPendingConnections`, and `AuthenticationMessageLimit`. Each defaults to `0`, which means no limit. See [Go library configuration](../library/). The connection reset and TCP keepalive limits apply to the library too.
