---
# Unless explicitly stated otherwise all files in this repository are licensed
# under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/)
# Copyright 2026-Present Datadog, Inc.
title: "Load shedding"
description: "Why PG Proxy can reject new work under load, and how that affects connections and transactions."
draft: true
---

When a database slows down, clients can send work faster than it finishes. Connections stay busy, other queries wait for a connection, and retries can add even more work. A connection pool limits how many connections reach PostgreSQL, but waiting clients still consume resources in the proxy.

Load shedding rejects some new work so the system has room to finish the work it has already accepted. Clients see an error sooner instead of waiting for capacity that may not become available within their deadline.

## Where PG Proxy checks for overload

PG Proxy provides two places for an application to apply a load-shedding policy:

| Check | What it protects | What a rejection does |
| --- | --- | --- |
| New client connection | Capacity to handle client connections in the proxy. | Rejects and closes the new connection. |
| New backend work | Capacity to accept more work for a backend. | Returns an error to the client before processing that work. |

The connection check covers the lifetime of an accepted connection. The backend check covers an attempt to acquire and use a pooled PostgreSQL connection, including the time spent waiting for one.

These checks are separate from the configured session and pool limits. A session limit caps the number of connected clients. A pool limit caps the number of backend connections. A load-shedding policy can reject new work before either limit is reached, depending on the signals it uses.

## Client capability

Query enforcement is opt-in per client connection. A client that can safely handle
query-level SQLSTATE `53000` rejections advertises:

```text
-c pg_proxy.query_load_shedding=true
```

Put this inside the PostgreSQL startup `options` parameter, not a standalone
startup parameter. The built-in server consumes it without forwarding it to
PostgreSQL. When constructing sessions directly, propagate the parsed option to
`session.Config.QueryLoadShedding`.

This option uses the same boolean parsing rules as the other session options.
It defaults to `false`. See [Parsing and invalid values](../../reference/configuration/client-connections/#parsing-and-invalid-values)
for duplicate-key and malformed-value behavior. The capability is fixed for the
lifetime of the connection.

A capable client is a candidate for the backend policy; the capability does not
enable that policy. Unadvertised clients bypass query admission, even when the
backend policy rejects work. Workload priority does not imply capability and
cannot opt a client in. Connection admission still applies independently to all
clients, including capable clients.

Deploy the capability-aware proxy before enabling client advertisement. Older
proxies ignore this unknown option, but retain their previous query-admission
behavior: advertising is not a way to disable an older proxy's enforcement.
Qualify each public client interface's error recovery separately; sharing a
PostgreSQL driver is not sufficient. No retry behavior is enabled by this option.

`pg_proxy.messages.load_shedding_rejected` counts rejected attempts. Capability
adoption and policy-specific metrics are left to the embedding application and
its admission policy.

## Transactions already in progress

The backend check runs before new work starts. Once a session acquires a backend connection, it keeps its admission slot until that connection is released. PG Proxy does not make another load-shedding decision for every statement in the transaction.

For example, if a client has already started a transaction when the policy begins rejecting work, its next statement and `COMMIT` can continue on the same connection. A different client starting new work can be rejected. This avoids abandoning an active transaction just because the proxy became busy.

That also means load shedding does not reclaim connections held by long-running or idle transactions. [Timeouts](../../reference/configuration/timeouts/) and [transaction pooling](../pooling/) still matter.

## Workload priority

Clients can supply `pg_proxy.priority` through their connection's startup options. Priorities range from `0` (highest) to `5` (lowest), with `3` as the default. PG Proxy passes that priority to the backend load-shedding policy.

A policy could use this to reject background work before interactive requests. The priority value alone does not reserve capacity, change PostgreSQL query scheduling, or guarantee that a request will be accepted. Its effect depends on the policy supplied by the application.

## What clients receive

A backend load-shedding rejection returns a PostgreSQL error with SQLSTATE `53000` (`insufficient_resources`). For a simple query, the proxy then reports that it is ready for another command. For the extended query protocol, it discards messages until the client sends `Sync`, then reports that it is ready again. The rejection itself does not require closing the client connection.

Retrying immediately can keep the system overloaded. Applications should bound their retries, leave time between attempts, and respect the request's deadline. A load-shedding rejection happens before the rejected work runs; a connection failure during an active transaction has different retry implications. See [Graceful shutdown](../graceful-shutdown/) for that distinction.

## Where the policy comes from

Session caps and load-shedding policy are separate controls; setting a session cap does not enable load shedding.

The library supplies the checks and protocol behavior. The embedding application supplies the policy that decides when to admit or reject work. It might use resource usage, the time spent waiting for backend connections, or a concurrency budget.

Without a listener load shedder, the listener skips that check. The built-in backend runtime also defaults to accepting work when no load shedder is supplied. Setting a client priority does not enable an overload policy by itself.

See [Go library configuration](../../reference/configuration/library/) for the application integration points and [Set connection and session limits](../../how-to/set-pool-limits/) for the separate capacity limits.
