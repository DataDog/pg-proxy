---
# Unless explicitly stated otherwise all files in this repository are licensed
# under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/)
# Copyright 2026-Present Datadog, Inc.
title: "Transaction pooling"
description: "Understand the packages behind shared PostgreSQL connections."
---

PG Proxy provides transaction-level connection pooling. Client sessions can share backend connections across transactions instead of holding one backend connection for their entire lifetime.

## The pieces involved

| Package | Responsibility |
| --- | --- |
| `pkg/pool` | Backend pool lifecycle, instance selection, and health state |
| `pkg/backend` | Connection checkout and session-link construction |
| `pkg/link` | Session-to-Postgres link and connection release |
| `pkg/prepared` | Prepared-statement name mapping for transaction pooling |
| `pkg/session` | Client session protocol and lifecycle |

## Sessions and connections are different limits

In the `pg-proxy` configuration file, `databases[].max_conns` controls a backend pool; client-session admission uses `proxy.max_sessions` (default `100000`), `proxy.default_max_user_sessions` (default `10000`), and optional `users[].max_sessions` overrides. The `users` section requires `proxy.password_auth_enabled: true`. Applications embedding the library supply their own session admission policy, such as the global and per-service caps in `pkg/limiter`.

A client session can stay open without holding a backend connection. Conversely, a client idle inside an unfinished transaction still occupies its backend connection. Many open client sessions therefore do not necessarily mean many active database connections.

## Why transactions matter

A client session can remain open longer than a transaction. Transaction pooling makes those lifetimes separate: the proxy can reuse backend connections across transactions while keeping the client session alive.

An unfinished transaction also matters during shutdown because it can prevent a session and its backend connection from closing within `proxy.shutdown_grace_period`.

## Session state belongs to the backend

When a backend connection goes back to the pool, the proxy can reset it with `RESET SESSION AUTHORIZATION; RESET ROLE; RESET ALL`, so settings from one client do not leak to the next. By default it does so on every release; [session reset modes](#session-reset-modes) can skip it. If the reset fails or takes longer than 5 seconds, the proxy closes that connection instead of reusing it. A connection released in the middle of a protocol exchange, such as an unfinished `COPY`, is also closed.

A later transaction may acquire another connection. Use transaction-scoped settings such as `SET LOCAL`, and avoid relying on temporary tables or session-level locks across transactions. [Transaction-pooling compatibility](../../reference/compatibility/) describes these boundaries.

[Prepared statements](../prepared-statements/) receive special handling: the proxy tracks client statement names and prepares statements on newly selected backend connections as needed.

## Session reset modes

A connection returns to the pool after every transaction and every autocommit statement, and the reset is one more query to PostgreSQL each time, even after a checkout that changed no session state. `databases[].reset_mode` chooses, for each database's pools, when the proxy sends the reset:

| Mode | On release |
| --- | --- |
| `always` | Sends the reset every time. This is the default. |
| `dynamic` | Sends the reset only when the checkout may have changed session state, and skips it otherwise. |
| `audit` | Sends the reset every time, so clients see no difference from `always`. On a sample of the releases that `dynamic` would skip, it also checks which settings the checkout left changed, and reports each one as a setting `dynamic` would have leaked. |
| `off` | Never sends the reset. A client's `SET` carries over to the next client of the connection. |

`dynamic` marks a checkout as one that may have changed session state when:

- The client's SQL may change a setting, the role, or a held cursor. The proxy scans the text of each simple query, and of each prepared statement a `Bind` runs, for statements such as `SET` (but not `SET LOCAL`), `DO`, `CALL`, `DECLARE`, and `EXECUTE`, and for names such as `set_config` and `pg_settings`. `RESET` and `DISCARD` do not count. Text the scanner cannot read to its end counts as a change.
- A `Bind` runs a statement that the proxy cannot match to its `Parse`.
- PostgreSQL sends a `ParameterStatus` message, which reports a change to a setting such as `TimeZone`, even one made inside a function.

The scan errs toward resetting: a false alarm costs one reset, while a missed change leaks a setting to the next client. It cannot see changes that functions or triggers make without the client's SQL naming them, so `dynamic` suits databases where clients change session state only in their own SQL. [Transaction-pooling compatibility](../../reference/compatibility/#reset-modes) lists what `dynamic` can miss. To measure it on your own traffic first, run `audit` and watch the [reset metrics](../../reference/observability/metrics/#pool-waits-and-connection-reuse).

## Related documentation

- [Set connection and session limits](../../how-to/set-pool-limits/) for the practical steps.
- [Configuration file](../../reference/configuration/configuration-file/) for defaults and shutdown behavior.
