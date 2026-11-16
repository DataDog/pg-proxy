---
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
| Unnamed statements reused across transactions | Set `pg_proxy.auto_cached_prepared_statements=true` in the client startup options. It defaults to `false`; see [Client connection options](../configuration/client-connections/). |
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

On release, the current link implementation resets the connection's role and configuration, then removes excess cached prepared statements. An unsafe connection or a failed reset causes the connection to be closed.

[`RESET ALL`](https://www.postgresql.org/docs/current/sql-reset.html) resets configuration parameters to their baseline values. It does not reset `role` or `session_authorization`, so the proxy resets them separately:

- With application-supplied role management, the role implementation resets the role, and then the proxy runs `RESET ALL`.
- Otherwise, the proxy runs `RESET SESSION AUTHORIZATION; RESET ROLE; RESET ALL` as one simple query. A client's `SET ROLE` or `SET SESSION AUTHORIZATION` does not carry to the next client. A role assigned at connection time, through `ALTER ROLE ... SET role` or a startup option, is restored.

This is not a complete reset of PostgreSQL session state: it does not clear temporary objects or session-level advisory locks. The proxy retains prepared statements for reuse rather than running `DISCARD ALL` on every release.

Client-issued `DISCARD ALL` or `DEALLOCATE ALL` can remove backend statements independently of the proxy's inventory. Do not use them as a replacement for the driver's protocol-level statement close operation.

## Related reference

- [Prepared statements](../../explanation/prepared-statements/) explains the mapping and cache lifecycle.
- [Timeouts](../configuration/timeouts/) distinguishes backend query limits from client connection limits.
- [Read routing](../../explanation/read-routing/) describes replica consistency boundaries.
