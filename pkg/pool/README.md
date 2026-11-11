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

## SQL shutdown banlist

The hybrid policy remembers an IP after a PostgreSQL `57P02` crash-shutdown
response, or `57P03` containing `the database system is shutting down`.
Timeouts, connection refusal, authentication failures, startup responses, and
`57P01` administrative connection termination do not create a ban.

Banned IPs remain discovery candidates and continue to be probed. Their results
do not force SQL fallback, and cached Patroni fallback cannot select them. A
successful role result from a poll started after the latest observed shutdown clears it, provided
the current credential evidence contains neither shutdown nor conflicting roles
for that IP. Cached successes and polls already in flight when the ban was
observed cannot clear it. Removing an IP from the retained SQL candidate set also
removes its ban; the map is bounded by that set, rather than growing with every
historical address. There is no time-based expiry.

`SQLHealthWorkerHooks.RoutingBanned` and `RoutingUnbanned` report transitions,
including during shadow evaluation. Banned IPs are excluded from repeated
`ConclusiveError` callbacks. The raw SQL/Patroni comparison and Patroni-only
shadow routing remain unchanged. This policy does not itself wire SQL decisions
into client routing or filter mismatch metrics.

## SQL snapshot timing

The optional `HealthWorkerHooks.SQLPotentialCompared` callback compares completed
SQL and Patroni snapshots for a replacement primary. It reports signed speedup,
without downtime durations or percentages, using the completed-snapshot mismatch
aggregate. See [SQL snapshot timing](../../docs/sql-potential-timing.md) for the
snapshot boundary, lifecycle rules, and how to compare it with hybrid timing.
