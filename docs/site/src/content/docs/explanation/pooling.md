---
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

In the `pg-proxy` configuration file, `databases[].max_conns` controls a backend pool; client-session admission uses `proxy.max_sessions` (default `100000`), `proxy.default_max_user_sessions` (default `10000`), and optional `users[].max_sessions` overrides. Applications embedding the library supply their own session admission policy, such as the global and per-service caps in `pkg/limiter`.

A client session can stay open without holding a backend connection. Conversely, a client idle inside an unfinished transaction still occupies its backend connection. Many open client sessions therefore do not necessarily mean many active database connections.

## Why transactions matter

A client session can remain open longer than a transaction. Transaction pooling makes those lifetimes separate: the proxy can reuse backend connections across transactions while keeping the client session alive.

An unfinished transaction also matters during shutdown because it can prevent a session and its backend connection from closing within `proxy.shutdown_grace_period`.

## Session state belongs to the backend

A later transaction may acquire another connection. Use transaction-scoped settings such as `SET LOCAL`, and avoid relying on temporary tables or session-level locks across transactions. [Transaction-pooling compatibility](../../reference/compatibility/) describes these boundaries.

[Prepared statements](../prepared-statements/) receive special handling: the proxy tracks client statement names and prepares statements on newly selected backend connections as needed.

## Related documentation

- [Set connection and session limits](../../how-to/set-pool-limits/) for the practical steps.
- [Local development settings](../../how-to/local-development/#settings) for defaults and shutdown behavior.
