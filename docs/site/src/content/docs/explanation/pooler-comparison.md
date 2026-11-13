---
title: "Compare poolers"
description: "How PG Proxy compares with PgBouncer, Pgpool-II, Odyssey, and PgDog."
---

PG Proxy is useful when an application needs to compose PostgreSQL pooling with its own authentication, routing configuration, admission policy, and telemetry. PgBouncer, Pgpool-II, Odyssey, and PgDog offer standalone poolers with established configuration and administration interfaces.

This comparison describes the current PG Proxy checkout and upstream documentation reviewed on **October 14, 2026**. PG Proxy's portable library is still being extracted; the local example is not a production server. The tables compare documented capabilities, not measured performance.

## At a glance

Start with the operating model you need. These are starting points for evaluation, not a ranking.

<table class="pooler-table pooler-table-overview" aria-label="Poolers at a glance">
  <thead>
    <tr><th scope="col">Pooler</th><th scope="col">When to evaluate it</th><th scope="col">Pooling</th></tr>
  </thead>
  <tbody>
    <tr>
      <th scope="row">PG Proxy</th>
      <td>Embed pooling in a Go application.</td>
      <td>Transaction</td>
    </tr>
    <tr>
      <th scope="row">PgBouncer</th>
      <td>Run a standalone connection pooler.</td>
      <td>Session, transaction, statement</td>
    </tr>
    <tr>
      <th scope="row">Pgpool-II</th>
      <td>Combine read balancing and failover.</td>
      <td>Connections reused between sessions</td>
    </tr>
    <tr>
      <th scope="row">Odyssey</th>
      <td>Set pool policies per database and user.</td>
      <td>Session, transaction</td>
    </tr>
    <tr>
      <th scope="row">PgDog</th>
      <td>Combine pooling, read balancing, and sharding.</td>
      <td>Session, transaction</td>
    </tr>
  </tbody>
</table>

Pooling documentation: [PgBouncer](https://www.pgbouncer.org/features.html), [Pgpool-II](https://www.pgpool.net/docs/latest/en/html/runtime-config-connection-pooling.html), [Odyssey](https://pg-odyssey.tech/features/pooling.html), [PgDog](https://docs.pgdog.dev/features/connection-pooler/).

PG Proxy's main distinction is its [embedding model](../../reference/configuration/library/). Its API is evolving, and the application owns production setup. See [Features](../../features/) for its current capabilities.

## Capability comparison

Each table covers one area. Follow the source links for configuration details and limitations.

### Integration

<table class="pooler-table" aria-label="Integration">
  <thead>
    <tr><th scope="col">Pooler</th><th scope="col">Integration</th></tr>
  </thead>
  <tbody>
    <tr>
      <th scope="row">PG Proxy</th>
      <td>Embeddable Go library; application supplies operating policies.</td>
    </tr>
    <tr>
      <th scope="row">PgBouncer</th>
      <td>Standalone pooler configured through INI. <a href="https://www.pgbouncer.org/config.html">Configuration</a>.</td>
    </tr>
    <tr>
      <th scope="row">Pgpool-II</th>
      <td>Standalone middleware for pooling, read balancing, and high availability. <a href="https://www.pgpool.net/docs/latest/en/html/intro-whatis.html">Overview</a>.</td>
    </tr>
    <tr>
      <th scope="row">Odyssey</th>
      <td>Standalone pooler with worker threads and database/user routing rules. <a href="https://pg-odyssey.tech/">Project overview</a>.</td>
    </tr>
    <tr>
      <th scope="row">PgDog</th>
      <td>Standalone Rust pooler and router using <code>pgdog.toml</code> and <code>users.toml</code>. <a href="https://github.com/pgdogdev/pgdog#features">Configuration</a>.</td>
    </tr>
  </tbody>
</table>

### Prepared statements

<table class="pooler-table" aria-label="Prepared statements in transaction pooling">
  <thead>
    <tr><th scope="col">Pooler</th><th scope="col">Prepared statements in transaction pooling</th></tr>
  </thead>
  <tbody>
    <tr>
      <th scope="row">PG Proxy</th>
      <td>Protocol-level named statements; optional caching of unnamed statements.</td>
    </tr>
    <tr>
      <th scope="row">PgBouncer</th>
      <td>Protocol-level named statements with nonzero <code>max_prepared_statements</code>. <a href="https://www.pgbouncer.org/config.html#max_prepared_statements">Setting</a>.</td>
    </tr>
    <tr>
      <th scope="row">Pgpool-II</th>
      <td>Not applicable: backend reuse happens between sessions, rather than transactions. <a href="https://www.pgpool.net/docs/latest/en/html/runtime-config-connection-pooling.html">Pooling</a>.</td>
    </tr>
    <tr>
      <th scope="row">Odyssey</th>
      <td>Supported with <code>pool_reserve_prepared_statement</code>. <a href="https://pg-odyssey.tech/features/pooling.html#prepared-statements-support">Pooling documentation</a>.</td>
    </tr>
    <tr>
      <th scope="row">PgDog</th>
      <td>Named-statement cache and optional SQL-level rewriting; unnamed statements pass through uncached. <a href="https://docs.pgdog.dev/features/connection-pooler/prepared-statements/">Statements</a>.</td>
    </tr>
  </tbody>
</table>

### Routing

<table class="pooler-table" aria-label="Routing">
  <thead>
    <tr><th scope="col">Pooler</th><th scope="col">Routing</th></tr>
  </thead>
  <tbody>
    <tr>
      <th scope="row">PG Proxy</th>
      <td>SQL classification, weighted read clusters, and replica pool selection.</td>
    </tr>
    <tr>
      <th scope="row">PgBouncer</th>
      <td>Database-to-host mappings; read/write destination selection belongs to the client or surrounding infrastructure. <a href="https://www.pgbouncer.org/faq.html">FAQ</a>.</td>
    </tr>
    <tr>
      <th scope="row">Pgpool-II</th>
      <td>Read/write routing and replica balancing, with session-level or statement-level read destination selection. <a href="https://www.pgpool.net/docs/latest/en/html/read-query-load-balancing.html">Routing</a>.</td>
    </tr>
    <tr>
      <th scope="row">Odyssey</th>
      <td>Database/user rules and primary/standby selection through <code>target_session_attrs</code>. <a href="https://pg-odyssey.tech/features/tsa.html">Routing</a>.</td>
    </tr>
    <tr>
      <th scope="row">PgDog</th>
      <td>Read/write splitting, replica balancing, and sharding with cross-shard query handling. <a href="https://docs.pgdog.dev/">Overview</a>.</td>
    </tr>
  </tbody>
</table>

### Observability

<table class="pooler-table" aria-label="Observability">
  <thead>
    <tr><th scope="col">Pooler</th><th scope="col">Observability</th></tr>
  </thead>
  <tbody>
    <tr>
      <th scope="row">PG Proxy</th>
      <td>Logging and metrics interfaces, plus query spans through a tracer configured by the application.</td>
    </tr>
    <tr>
      <th scope="row">PgBouncer</th>
      <td>Administrative <code>SHOW</code> commands, pool statistics, and logs. <a href="https://www.pgbouncer.org/usage.html">Administration</a>.</td>
    </tr>
    <tr>
      <th scope="row">Pgpool-II</th>
      <td>SQL commands expose nodes, pools, processes, health checks, and backend query statistics. <a href="https://www.pgpool.net/docs/latest/en/html/sql-commands.html">Administration</a>.</td>
    </tr>
    <tr>
      <th scope="row">Odyssey</th>
      <td>Connection identifiers in logs, an administrative console, and Prometheus exporter support. <a href="https://pg-odyssey.tech/">Overview</a>, <a href="https://pg-odyssey.tech/features/console.html">console</a>, <a href="https://pg-odyssey.tech/features/prometheus-metrics.html">metrics</a>.</td>
    </tr>
    <tr>
      <th scope="row">PgDog</th>
      <td>Administrative database, OpenMetrics endpoint, and OpenTelemetry metrics export. <a href="https://github.com/pgdogdev/pgdog#monitoring">Monitoring</a>, <a href="https://docs.pgdog.dev/features/metrics/">metrics</a>.</td>
    </tr>
  </tbody>
</table>

Pgpool-II also provides Watchdog for availability of Pgpool-II itself. [Overview](https://www.pgpool.net/docs/latest/en/html/intro-whatis.html).

For PgDog, check its supported SQL and sharding restrictions against your workload. For PG Proxy, see [Prepared statements](../prepared-statements/), [Read routing](../read-routing/), and [Observability](../../reference/observability/).

## Compatibility matters more than a checkbox

Prepared-statement support generally refers to PostgreSQL protocol messages. It does not imply arbitrary SQL `PREPARE` commands or all session-scoped state work across pooled transactions. PgBouncer documents this distinction in its [feature map](https://www.pgbouncer.org/features.html); PG Proxy's [compatibility reference](../../reference/compatibility/) describes its own boundaries.

Pgpool-II reuses backend connections after client sessions end. Its `statement_level_load_balance` setting chooses a read destination per query; it does not provide transaction or statement pooling across clients. Account for this distinction when sizing backend connections. [Connection pooling](https://www.pgpool.net/docs/latest/en/html/runtime-config-connection-pooling.html), [read balancing](https://www.pgpool.net/docs/latest/en/html/read-query-load-balancing.html).

PgDog additionally offers SQL `PREPARE` rewriting with `prepared_statements = "full"`. Its documentation says unnamed statements are passed through without caching, and SQL `EXECUTE` for sharded prepared statements is not supported. These differ from PG Proxy’s optional unnamed-statement caching. [PgDog prepared statements](https://docs.pgdog.dev/features/connection-pooler/prepared-statements/).

Replica routing also changes consistency. A pooler's ability to send reads to replicas does not establish read-after-write guarantees. PG Proxy's [routing explanation](../read-routing/) documents its limitations.

## Performance comparison

There are no comparable benchmarks in these docs yet. Threading model or feature count alone does not establish which pooler is faster.

For a meaningful comparison, keep PostgreSQL, hardware, client drivers, TLS, pool sizes, query mix, and concurrency consistent. Measure throughput, tail latency, CPU, memory, and behavior when pools are full or backends fail. Include the prepared-statement and transaction patterns your application actually uses.
