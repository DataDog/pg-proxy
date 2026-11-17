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

## Backend pool setting

In the OSS binary's configuration file, add `connection_options` to the relevant `databases` entry:

```yaml
databases:
  - name: primary
    host: 127.0.0.1
    port: 5432
    dbname: postgres
    username: postgres
    password: postgres
    sslmode: disable
    connection_options:
      statement_timeout: "5s"
```

This is a database-section example; retain the file's `clusters`, `logical_databases`, and `proxy` settings. See [OSS binary settings](../../../how-to/local-development/#settings). The options are supplied when opening backend connections. A unitless statement timeout is measured in milliseconds; `0` disables it. Specify a unit to make the intended duration clear.

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

The OSS binary's `proxy.shutdown_grace_period` (JSON: `shutdownGracePeriod`) bounds session and pool cleanup after `SIGINT` or `SIGTERM`. It accepts a positive Go duration string, such as `30s` or `2m`. Omitted or empty values use `30s`; zero, negative, invalid, and overflowing durations fail at startup.

This limit starts when shutdown begins, not when a transaction starts. It does not change statement or connection timeouts. Give the service manager a longer stop timeout so it does not kill the process before cleanup and telemetry flushing finish. See [Run in production](../../../how-to/run-in-production/#7-restart-and-roll-out-changes).
