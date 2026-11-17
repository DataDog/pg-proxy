---
# Unless explicitly stated otherwise all files in this repository are licensed
# under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/)
# Copyright 2026-Present Datadog, Inc.
title: "Prepared statements"
description: "How prepared statements survive changes of backend connection."
---

A PostgreSQL prepared statement belongs to one backend connection. A client connected through PG Proxy can use several backend connections over its lifetime. Without extra handling, a statement prepared in one transaction may be missing when the next transaction uses it.

## Two sets of state

PG Proxy keeps a mapping for the client session and a separate inventory for each backend connection.

| State | What it remembers |
| --- | --- |
| Client session | The client's statement name, SQL text, parameter types, and whether preparation was confirmed. |
| Backend connection | Which proxy-named statements have been prepared on this connection. |

For named statements, the proxy generates a backend name from the SQL text and parameter type identifiers. Two clients can use the same client-side name for different queries without colliding on a shared backend connection.

## Using a statement on another connection

1. The client sends a protocol `Parse` message. The proxy records the statement as pending and substitutes its backend name.
2. PostgreSQL returns `ParseComplete`, confirming preparation.
3. In a later transaction, the client sends `Bind` or `Describe` using its original name.
4. If the selected backend connection lacks the statement, the proxy prepares it there before forwarding the operation.

The client does not receive extra preparation replies for this internal work. Its statement remains associated with its client session even as backend connections change.

## Unnamed statements

Some drivers use an unnamed statement across separate transactions. PG Proxy provides a client startup option to give these statements proxy-generated names and use the same tracking mechanism:

```text
options='-c pg_proxy.auto_cached_prepared_statements=true'
```

This option is defined by PG Proxy's listener and passed to its session runtime. It is not a built-in PostgreSQL setting or a YAML configuration field. Named protocol statements already use the tracking mechanism without this option.

This option defaults to `false`. It applies to extended-protocol statements; it is not a cache of query results or of arbitrary SQL sent through the simple-query protocol. See [Client connection options](../../reference/configuration/client-connections/) for the exact syntax.

## Closing and retaining statements

A protocol `Close` removes a named statement from the client's mapping. The backend copy can remain available for reuse. When a backend connection is released, the link removes excess cached statements using usage and recency information.

The session runtime supplies a retention threshold of `100` statements per backend connection. Cleanup happens on release, so this is not a hard cap at every point during a transaction.

## Boundaries

This mechanism handles protocol-level prepared statements. SQL commands such as `PREPARE`, `EXECUTE`, and `DEALLOCATE` sent as query text do not use the same name mapping. Do not rely on SQL-level prepared statements surviving across transactions.

Changing SQL comments for every execution also changes the statement's identity. Per-query trace comments on named statements reduce reuse; see [Tracing](../../reference/observability/tracing/).
