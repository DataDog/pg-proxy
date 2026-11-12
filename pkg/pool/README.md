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

## Hybrid SQL evidence precedence

Credential results are reconciled per IP. A successful role query takes
precedence over an allowlisted error from another credential on the same IP.
For example, `primary + 57P01` remains primary, and `standby + 57P01` remains
standby, in either credential-result order. This rule expresses evidence
precedence, not which observation is newer. Unresolved evidence on another IP,
conflicting successful roles, or multiple primaries still requires Patroni
fallback.

`TestHybridHealthPrimaryTakesPrecedenceOverAdminShutdownOnSameIP` covers both
orders with Patroni selecting a different IP, ensuring the policy actually uses
the successful SQL role rather than falling back to the cached Patroni snapshot.

System (`58`) and internal (`XX`) SQLSTATE classes are indeterminate. They
identify error categories, not instance-wide unavailability. They require the
same fallback or retained-leader handling as other indeterminate errors unless
another credential resolves that IP's role.

## Patroni recovery after SQL finds no primary

With SQL routing enabled, a conclusive SQL snapshot with no primary allows a
newer Patroni observation with one healthy leader to restore routing immediately.
Uniqueness is checked before excluding a superseded leader: an ambiguous A+C
snapshot cannot initiate recovery or clear A's exclusion just because filtering
A leaves C alone.
The policy follows Patroni updates until a fresh SQL snapshot completes. Routing
comparisons record this publication as `patroni_recovery` at the Patroni observation
time, rather than waiting for the next SQL completion.

SQL outcomes must carry `StartedAt`. The checker captures it before probing;
callers combining credential snapshots must preserve the earliest start time of
all included evidence (including an unknown/zero time). A snapshot that started
at or before the recovery transition cannot overwrite that route. Repeated
Patroni polls for the same leader do not advance this freshness boundary.

Fresh SQL snapshots resume SQL policy, including conclusive no-primary results.

## SQL leader retention while Patroni catches up

When conclusive SQL selects B while Patroni still selects A, the policy records
B as awaiting Patroni agreement and excludes A from subsequent Patroni fallback.
An indeterminate SQL snapshot retains B (`retained_sql`) while B remains a
candidate and has no explicit demotion or conclusive unroutable evidence.
Conflicting successful roles or multiple SQL primaries also prevent retention.
The same-IP successful-role precedence described above still applies.

Repeated Patroni polls reporting A do not establish a new leadership transition.
If Patroni instead advances to a unique C after SQL selected B, without this proxy
observing Patroni confirm B, C can replace retained B through the same recovery
path. Previously superseded SQL leaders remain ineligible for this shortcut;
that history is bounded to current candidate IPs.
Once Patroni agrees with B, normal fallback resumes. A new conclusive SQL leader
replaces the retained leader. If B is disqualified or removed, the policy stops
retaining B but does not restore A solely from Patroni's superseded view. A different
eligible Patroni leader can restore routing through the immediate recovery path;
a fresh conclusive SQL selection can establish A as primary again.

There is no timeout that silently restores the superseded leader. This trades
continued use of the last conclusive SQL leader during uncertainty against
returning to an unconfirmed old leader. Future banlist integration must apply its
eligibility checks to retained SQL routes and immediate Patroni recovery too.

Routing-flag transitions clear retained leaders, superseded-leader exclusions,
and recovery state while preserving the latest Patroni snapshot for fallback.
Re-enabling evaluation therefore starts from current evidence instead of
resurrecting leadership history from before the disabled interval. Repeated
evaluations of the same flag value preserve the current policy state.

## SQL snapshot timing

The optional `HealthWorkerHooks.SQLPotentialCompared` callback compares completed
SQL and Patroni snapshots for a replacement primary. It reports signed speedup,
without downtime durations or percentages, using the completed-snapshot mismatch
aggregate. See [SQL snapshot timing](../../docs/sql-potential-timing.md) for the
snapshot boundary, lifecycle rules, and how to compare it with hybrid timing.
