# Pool extraction review guide

This package owns PostgreSQL pool lifecycle, instance selection, and health
state. The application supplies configuration, credentials, discovery and health
policy, retries, and telemetry callbacks.

## Where the code moved

Paths in the first column are relative to `domains/orgstore/apps/pg-proxy`.
Paths in the second column are relative to this package.

| Original code | Portable code | What stays in the app |
| --- | --- | --- |
| `pg-instances/postgres.go` | `instance.go`, `pgx.go` | Connection-string construction and logging/metrics callbacks |
| `pools/direct_postgres_pool.go` | `direct_runtime.go`, `direct_state.go` | Retry policy, telemetry, and probe connection strings |
| `pools/cloud_postgres_pool.go` | `cloud.go` | Configured server list and cloud health checks |
| `pools/pgk_pool.go` | `pgk.go`, `lookup.go` | Shared health-worker subscription, retries, and telemetry |
| `pools/pool_health_check_worker.go` | `health_worker.go`, `health_state.go` | Process singleton, DNS/Patroni calls, refresh/retention settings, and telemetry |
| `pools/replica_pool.go` | `replica.go`, `replica_hooks.go` | Shared health snapshot and pgx configuration/telemetry callbacks |
| `pools/database_connections_pool.go` | `composed.go` | Pool choice, database metadata, and temporary inventory identifiers |
| `pg-instances/pgk.go` | `patroni.go` | Role/path selection, HTTP client, retries, and telemetry |

## Review order and behavior

1. `Instance` owns one pgx pool. `DirectState` tracks instances and selects
   healthy addresses; `DirectRuntime` acquires, resets, and closes their pools.
2. `CloudPool` refreshes a fixed server list. `PGKPool` refreshes the retained
   addresses supplied by discovery. Both use `DirectRuntime`.
3. `HealthWorker` shares a snapshot and parallel probes among subscribers to
   the same host. The first subscription refreshes inline; later subscriptions
   reuse it. `HealthState` owns DNS-miss retention. `PGKPool` does not count those
   misses again: it removes an absent instance unless it belongs to the last known
   healthy set. That set survives a period when no instance is healthy.
4. `ReplicaPool` owns one shared pgx pool and a health snapshot. A newly seen
   address triggers pool replacement; the previous pool closes asynchronously.
5. `PGXPool`, `FallbackPool`, and `DirectOnlyPool` preserve the existing checkout,
   fallback, statistics, and close behavior behind the common pool interface.

App constructors still perform the initial health refresh before returning and
start periodic work afterward. PGK setup latency is reported for periodic
attempts, including retries, but not for initial construction. Cloud setup
latency includes the initial refresh, as before.

The original PGK discovery/cleanup scenarios live in `pgk_setup_test.go`, with
their original expected health and inventory results. Tests of app-owned policy,
telemetry, probe connection strings, and the process singleton remain in the
app. Shared-worker subscription, probing, and cleanup tests move to
`health_worker_test.go`. Both pool test targets retain race detection.

## Stack boundaries

- [#99773](https://github.com/ddoghq/dd-source/pull/99773) moves these pool runtimes.
- [#99774](https://github.com/ddoghq/dd-source/pull/99774) composes the listener and
  session registry in `pkg/server`.
- [#99776](https://github.com/ddoghq/dd-source/pull/99776) moves Warden and removes
  the temporary app inventory identifiers and server registry adapter.

Sheaf compatibility stays in the app until the Warden step. The later PRs use
the pool contracts through the app backend factory; they do not require a
second replica watcher or another PGK cleanup mode.

## SQL snapshot timing

The optional `HealthWorkerHooks.SQLPotentialCompared` callback compares completed
SQL and Patroni snapshots for a replacement primary. It reports signed speedup,
without downtime durations or percentages, using the completed-snapshot mismatch
aggregate. See [SQL snapshot timing](../../docs/sql-potential-timing.md) for the
snapshot boundary, lifecycle rules, and how to compare it with hybrid timing.
