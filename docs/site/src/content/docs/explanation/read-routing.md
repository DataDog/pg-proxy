---
title: "Read routing"
description: "How logical databases, read clusters, and replica pools select a destination."
---

Read routing lets eligible work use read backends while other work uses the logical database's main cluster. It can reduce load on the primary, but replica reads can be stale. PG Proxy does not guarantee read-after-write consistency across transactions routed to replicas.

## Names and destinations

Routing starts with the database name in the client's connection string:

| Configuration | Role |
| --- | --- |
| `logical_databases` | Maps the client-facing name to a main cluster and optional read clusters. |
| `clusters` | Maps each cluster name to a primary backend and optional replica backends. |
| `databases` | Supplies the physical PostgreSQL database, address, and pool settings. |

A cluster's `primary` is the default backend for that configured group. A cluster dedicated to reads can point its `primary` at a physical replica.

See the [configuration schema](https://github.com/DataDog/pg-proxy/blob/main/pkg/config/README.md) for the file format. The OSS binary currently accepts one primary per cluster and rejects replicas and read clusters; embedding applications can supply them.

## When routing is enabled

Configured `read_clusters` enable read routing for the logical database. Otherwise, a client can request `pg_proxy.dynamic_routing=true`, provided the backend supports replica routing. The portable example binary has one backend and no replica pools.

A false client option does not override configured `read_clusters`.

## Classifying work

The SQL classifier identifies work eligible for read routing. These examples describe current classification behavior:

| SQL | Eligible for read routing |
| --- | --- |
| `SELECT * FROM items` | Yes |
| `SHOW statement_timeout` | Yes |
| `BEGIN READ ONLY` | Yes |
| `SELECT count(*) FROM items` | Yes |
| `SELECT * FROM items FOR UPDATE` | No |
| `SELECT * INTO copy_of_items FROM items` | No |
| `BEGIN` | No |
| `INSERT`, `UPDATE`, `DELETE`, or `EXPLAIN` | No |

A string containing multiple statements must have every statement classified as read-only. The classifier also examines common-table expressions and checks function calls against the list below. Classification is a routing heuristic, not a substitute for PostgreSQL permissions or read-only enforcement.

Routing happens when the session needs a backend connection. Once acquired, that connection stays attached for the transaction; the proxy does not move each statement of an active transaction between backends.

### Known read-only functions

The classifier recognizes these function names for read routing:

| Group | Recognized names |
| --- | --- |
| Time zones | `timezone`, `pg_catalog.timezone` |
| Numeric aggregates | `count`, `sum`, `avg`, `min`, `max` |
| String and array aggregates | `string_agg`, `array_agg` |
| JSON | `json_agg`, `jsonb_agg`, `json_build_object`, `jsonb_build_object` |
| Boolean aggregates | `bool_and`, `bool_or` |

`AT TIME ZONE` is also recognized because PostgreSQL's parser represents it as a call to `pg_catalog.timezone`.

Names must match the list, including any schema qualifier. For example, `SELECT count(*) FROM items` is eligible, but `SELECT pg_catalog.count(*) FROM items` is not. The list is fixed in the classifier; it is not a YAML setting, and the proxy does not look up whether a function is read-only in PostgreSQL.

The current check examines the first direct function call in the `SELECT` list. If that name is unrecognized, the statement is not eligible for read routing. It does not inspect every function in nested expressions, function arguments, or other clauses, so this list is not a guarantee that a query cannot write data.

## Two routing decisions

**Choosing a read cluster.** With `read_clusters`, eligible work selects a cluster according to its weight. Weights of `1` and `3` give approximately one quarter and three quarters of selections over time. A zero weight excludes that cluster from weighted selection. When all weights are zero, selection falls back to the logical database's main backend.

**Choosing a pool within a cluster.** The current backend runtime sends roughly 90% of eligible replica-routed acquisitions to replica pools and retains 10% on that cluster's primary pool. Replica selection uses round robin. If replica acquisition fails, fallback to the primary is rate limited and can itself fail.

The 90/10 policy describes this second decision. It does not mean every configuration sends exactly 90% of all queries to physical replicas.

## Consistency and observation

Work requiring fresh reads should use a destination that routes to the primary. A recent write does not currently force subsequent transactions back to the primary: the backend's time-based read-after-write check is disabled.

Use [pool and waiting metrics](../../reference/observability/metrics/) alongside query classification and routing measurements to distinguish a routing choice from an overloaded destination.
