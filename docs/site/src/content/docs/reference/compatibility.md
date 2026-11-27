---
# Unless explicitly stated otherwise all files in this repository are licensed
# under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/)
# Copyright 2026-Present Datadog, Inc.
title: "Transaction-pooling compatibility"
description: "Session state, prepared statements, and connection reuse boundaries."
---

A client session is not a dedicated PostgreSQL connection. The next transaction may run on another backend connection, even when the application keeps its connection to PG Proxy open.

## Session state

| Feature | Guidance |
| --- | --- |
| Transaction-scoped settings | Use `SET LOCAL` inside an explicit transaction. |
| Session-scoped `SET` | Do not rely on the value carrying into another transaction. |
| Protocol-level named prepared statements | Supported through the proxy's statement mapping without enabling the unnamed-statement option. |
| Unnamed statements reused across transactions | Supported: the proxy parses the client's last unnamed statement again in each transaction that uses it. To cache unnamed statements per backend connection instead, set `pg_proxy.auto_cached_prepared_statements=true` in the client startup options. It defaults to `false`; see [Client connection options](../configuration/client-connections/). |
| SQL `PREPARE` / `EXECUTE` | Not covered by the protocol-level mapping. Do not rely on these statements across transactions. |
| Session-level advisory locks | Do not use them as locks owned by the client session. Use transaction-level advisory locks where appropriate. |
| Temporary tables | Do not rely on a temporary table being available in a later transaction or automatically removed on release. |
| Session-scoped subscriptions such as `LISTEN` | Require connection affinity that transaction pooling does not provide. |

For example, keep a query-specific setting and the query in the same transaction:

```sql
BEGIN;
SET LOCAL statement_timeout = '5s';
SELECT 1;
COMMIT;
```

## Returning a backend connection

Long or idle transactions keep their backend connection, leaving fewer connections available for other clients. End transactions promptly and configure appropriate [Timeouts](../configuration/timeouts/). See [Set connection and session limits](../../how-to/set-pool-limits/) for pool capacity settings.

The link is reusable only after PostgreSQL reports an idle `ReadyForQuery` state and expected replies have been consumed. An open or failed transaction is not an idle connection ready for another client.

On release, the current link implementation resets the connection's configuration and identity as described below, then removes excess cached prepared statements. An unsafe connection or a failed reset causes the connection to be closed. With the default [reset mode](#reset-modes), `always`, the reset runs on every release.

[`RESET ALL`](https://www.postgresql.org/docs/current/sql-reset.html) resets configuration parameters to their baseline values. It does not reset `role` or `session_authorization`, so the proxy resets them separately:

- With application-supplied role management, the role implementation resets the role, and then the proxy runs `RESET ALL`. The role implementation must undo any `SET ROLE` or `SET SESSION AUTHORIZATION` a client issues.
- Otherwise, the proxy runs `RESET SESSION AUTHORIZATION; RESET ROLE; RESET ALL` as one simple query. A client's `SET ROLE` or `SET SESSION AUTHORIZATION` does not carry to the next client. A role assigned at connection time, through `ALTER ROLE ... SET role` or a startup option, is restored.

This is not a complete reset of PostgreSQL session state: in every mode, it does not clear temporary objects, session-level advisory locks, cursors declared `WITH HOLD`, or sequence state such as `lastval()`. The proxy retains prepared statements for reuse rather than running `DISCARD ALL` on every release.

### Reset modes

`databases[].reset_mode` selects when the reset above runs. See [Session reset modes](../../explanation/pooling/#session-reset-modes) for how each mode works.

- `always` and `audit` run it on every release.
- `off` never runs it. Without application-supplied role management, any setting a client changes, including its role, carries over to the next client of that backend connection. With it, the role implementation still resets the role.
- `dynamic` runs it only when the checkout may have changed session state. Its limits:
  - It sees the session changes that the client's SQL names, and the settings PostgreSQL reports with `ParameterStatus`. It does not see other changes made inside functions or triggers, including a change of role. For example, if a function the client calls runs `set_config('statement_timeout', '1s', false)`, the next client of the connection inherits that timeout.
  - Without application-supplied role management, it resets on every release on PostgreSQL 14.0 to 14.13, 15.0 to 15.8, 16.0 to 16.4, and 17.0. These releases lack the fix for CVE-2024-10978, and on them a client's `RESET SESSION AUTHORIZATION` or `DISCARD ALL` can drop a role assigned at connection time without a `ParameterStatus` message. Older releases also lack the fix, but they report the change with `ParameterStatus`, which marks the checkout. The proxy reads the version from the `server_version` that the server sends at connection startup, and treats a version it cannot parse as one without the fix.

The reset is also what discards the unnamed prepared statement. After a release where `dynamic` or `off` skipped the reset, the next checkout of that connection closes the unnamed statement before the client's first request that expects a reply, so a client cannot run the previous client's unnamed statement. A simple query needs no close, because it discards the unnamed statement itself.

Client-issued `DISCARD ALL` or `DEALLOCATE ALL` can remove backend statements independently of the proxy's inventory. Do not use them as a replacement for the driver's protocol-level statement close operation.

## Related reference

- [Prepared statements](../../explanation/prepared-statements/) explains the mapping and cache lifecycle.
- [Timeouts](../configuration/timeouts/) distinguishes backend query limits from client connection limits.
- [Read routing](../../explanation/read-routing/) describes replica consistency boundaries.
