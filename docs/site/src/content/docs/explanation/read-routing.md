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

See the [configuration schema](https://github.com/DataDog/pg-proxy/blob/main/pkg/config/README.md) for the file format. For example, this `pg-proxy` configuration sends eligible reads for `app` to the `main` cluster, which has one replica:

```yaml
databases:
  - {name: main-primary, host: 10.0.0.10, dbname: app, username: app, password: example}
  - {name: main-replica, host: 10.0.0.11, dbname: app, username: app, password: example}
clusters:
  main: {primary: main-primary, replicas: [main-replica]}
logical_databases:
  app:
    cluster: main
    read_clusters:
      main: {weight: 1}
```

`pg-proxy` validates that each replica is a configured database other than the cluster primary, and that each read cluster is a configured cluster. It logs a warning at startup when a replica, or the primary of a read cluster, uses a different `dbname` or username than the main primary. That setup can be intentional, but it can also send reads to the wrong database or role. It also warns when `read_clusters` cannot send any read away from the main primary, or when all weights are `0`.

Replica databases default to a `max_conn_lifetime` of 15 minutes with 5 minutes of `max_conn_lifetime_jitter`, matching the library's replica pools. The defaults apply only when `max_conn_lifetime` is unset; an explicit `max_conn_lifetime_jitter` is kept.

Its replicas are static: each replica must accept a connection at startup, and there are no replica health checks. A failed replica checkout falls back to the cluster primary, as described below. File changes require a restart. Embedding applications can supply discovered replicas and health checks instead.

Each cluster opens its own pool for each database it uses. A database used by two clusters, for example as a replica of one and the primary of another, can therefore receive up to the sum of both pools' `max_conns`. Keep that total below the server's `max_connections`. Pool metrics cover primary pools only; replica pools do not export pool gauges.

## When routing is enabled

Configured `read_clusters` enable read routing for the logical database. Otherwise, a client can request `pg_proxy.dynamic_routing=true`, provided the backend supports replica routing: its cluster must have at least one replica.

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
| `START TRANSACTION READ ONLY` | No |
| `INSERT`, `UPDATE`, `DELETE`, or `EXPLAIN` | No |

The route is chosen when a transaction starts, from its first statement. A transaction that opens with plain `BEGIN`, as most ORMs do, runs entirely on the main cluster, even if every statement in it is a read. Open read-only transactions with `BEGIN READ ONLY` to make them eligible.

A string containing multiple statements must have every statement classified as read-only. The classifier also examines common-table expressions and checks function calls against the list below. Classification is a routing heuristic, not a substitute for PostgreSQL permissions or read-only enforcement.

Routing happens when the session needs a backend connection. Once acquired, that connection stays attached for the transaction; the proxy does not move each statement of an active transaction between backends.

The proxy classifies the SQL text in `Query` messages and in `Parse` messages for unnamed statements. It does not classify a `Parse` for a named statement, or any `Parse` when the client sets `pg_proxy.auto_cached_prepared_statements=true`, and a `Bind` carries no SQL. Those statements use the main backend. Clients that prepare named statements, such as pgx in its default mode, therefore send their reads to the main backend. For reads that should be routed, use the simple protocol or unnamed statements without automatic statement caching.

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

Any `read_clusters` entry turns on read routing, including an entry with weight `0`. With routing on, the main backend still sends eligible reads to its own replicas, as described next. To keep all reads on a primary, remove `read_clusters` and do not configure replicas for that cluster. `pg-proxy` logs a warning when all weights are zero.

**Choosing a pool within a cluster.** The current backend runtime sends roughly 90% of eligible replica-routed acquisitions to replica pools and retains 10% on that cluster's primary pool. Replica selection uses round robin. If no replica connection is available within 50 ms, because the replica pool is busy or the replica is down, the proxy uses the primary instead. These fallbacks are limited to 100 per second for each cluster (burst 100). Over that limit, the client gets the error `primary database fallback rate limit exceeded for cluster:<name>`. The `pg_proxy.replica_acquire_fallback` metric counts fallbacks, tagged `outcome:allowed` or `outcome:rate_limited`.

The 90/10 policy describes this second decision. It does not mean every configuration sends exactly 90% of all queries to physical replicas.

## Consistency and observation

Work requiring fresh reads should use a destination that routes to the primary. A recent write does not currently force subsequent transactions back to the primary: the backend's time-based read-after-write check is disabled.

Use [pool and waiting metrics](../../reference/observability/metrics/) alongside query classification and routing measurements to distinguish a routing choice from an overloaded destination.
