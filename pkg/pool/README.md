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

## Stack boundaries

- [#99773](https://github.com/ddoghq/dd-source/pull/99773) moves these pool runtimes.
- [#99774](https://github.com/ddoghq/dd-source/pull/99774) composes the listener and
  session registry in `pkg/server`.
- [#99776](https://github.com/ddoghq/dd-source/pull/99776) moves Warden and removes
  the temporary app inventory identifiers and server registry adapter.

Sheaf compatibility stays in the app until the Warden step. The later PRs use
the pool contracts through the app backend factory; they do not require a
second replica watcher or another PGK cleanup mode.
