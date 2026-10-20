# Connection pools

This package owns PostgreSQL pool lifecycle, instance selection, and health
state. The embedding application supplies configuration, credentials, discovery,
health policy, retry policy, and telemetry callbacks.

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

The embedding application owns initialization and periodic refresh scheduling.
Pool implementations expose hooks for application-specific logging and metrics.

The integration test in `sql_health_check_pools_integration_test.go` verifies SQL
health probes and connection reuse against PostgreSQL. Run it with Docker:

```sh
go test -tags=integration ./pkg/pool
```
