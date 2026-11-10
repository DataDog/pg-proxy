# Pool runtimes

This package owns PostgreSQL pool lifecycle, instance selection, and health
state. The application supplies configuration, credentials, discovery and health
policy, retries, and telemetry callbacks.

## Components

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

Constructors perform the initial health refresh before returning and start
periodic work afterward. Discovered-pool setup latency is reported for periodic
attempts, including retries, but not for initial construction. Cloud setup
latency includes the initial refresh.

Discovery and cleanup scenarios live in `pgk_setup_test.go`. Shared-worker
subscription, probing, and cleanup tests live in `health_worker_test.go`.

## SQL snapshot timing

The optional `HealthWorkerHooks.SQLPotentialCompared` callback compares completed
SQL and Patroni snapshots for a replacement primary. It reports signed speedup,
without downtime durations or percentages, using the completed-snapshot mismatch
aggregate. See [SQL snapshot timing](../../docs/sql-potential-timing.md) for the
snapshot boundary, lifecycle rules, and how to compare it with hybrid timing.
