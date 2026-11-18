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

## SQL health decision flow

SQL checks tell us which IP is primary. Patroni supplies a separate view that we
can use as a backup. A **snapshot** is the saved result of a completed check across
all IPs for one credential. **Fallback** means using Patroni's results instead
of choosing a route from SQL results.

The main path is:

`scheduleSQLRefresh` -> `SQLHealthChecker.Check` -> `completeSQLHealthCheck` ->
`aggregateSQLHealthCheckOutcomes` -> `hybridHealthPolicy.observeSQL`.

Patroni updates arrive separately through `applyPatroniHealth` -> `observePatroni`.
The policy chooses a result, and the worker publishes it for client routing.

### How checks run

SQL checks are normally scheduled **every second** using `SQLHealthCheckInterval`.
The application sets this through `HealthWorkerConfig.SQLInterval`; zero disables
the periodic SQL loop. Each tick starts a new check for each ready credential.
Each credential checks all retained IPs in parallel using
`SELECT pg_is_in_recovery()`. Different credentials can check the same IP at the
same time through separate pools. Subscribers with the same target share checks.

One slow credential does not hold up the others. If its previous check is still
running at the next tick, the worker skips starting another check for that
credential. It keeps that credential's last completed snapshot until the running
check finishes. Each credential still waits for all of its own IP checks and
retries before saving a new snapshot.

When a credential finishes, its new snapshot replaces its old one, including
old successes becoming errors. The worker combines the latest completed
snapshots from the active credentials. They do not all have to finish together.
So the one-second tick schedules work; it does not mean every saved result is
exactly one second old.

These results feed SQL/Patroni comparison metrics and, when SQL routing is enabled,
routing decisions. Availability subscribers can hear about a successful primary
earlier, as soon as that probe finishes. Routing still waits for the full checks
and the decision rules below.

### Combining results

We first combine credentials checking the **same IP**, optimistically: a successful
role query can answer a question that another credential's error could not.
Then we check **all IPs** pessimistically: an unknown role on another IP could hide
a second primary, so one known primary is not enough by itself.

Before combining successful roles, we remove an older observation only when
another successful probe of the **same IP started after the older probe
finished**, including retries. For example, a standby probe at
`12:00:00.000–.010` is replaced by a primary probe at `12:00:01.000–.010`.
The same rule handles a later primary-to-standby change. Arrival order and the
whole credential round's completion time do not establish that order: another
slow IP can delay that round after this IP's probe has finished.

Overlapping probes, missing timestamps, and invalid intervals cannot establish
which role is newer, so their conflicting successful results still require
Patroni fallback. Errors do not replace successful roles. Raw per-credential
snapshots remain available for diagnosis.

| Results for the same IP | How routing treats that IP |
| --- | --- |
| Primary + errors | Primary. The errors do not erase the successful answer. |
| Standby + errors | Standby. The errors do not erase the successful answer. |
| Primary + standby | Use the newer role only if the probe intervals establish its order as described above. Otherwise, `conflicting_roles`: do not choose a SQL route. |
| No successful answer, but an error that proves the IP cannot serve traffic | Unusable. This can resolve another credential's timeout on the same IP. |
| Only errors that do not prove its role or availability | Unknown. A timeout is unknown too. |

A successful answer is not necessarily newer than another credential's error.
These rules describe which saved answer the policy trusts.

After removing provably older successful roles, the aggregate `Health` map marks
an IP healthy if any remaining credential result saw it as primary. The policy
also checks the remaining evidence: **an unresolved primary + standby conflict
does not become a safe SQL route just because that map says true**.

Across IPs, the checks run in this order: conflicting roles, more than one primary,
then unknown results. If none apply, SQL has a definite answer: either one primary
or no primary. For example, all standbys plus one unusable IP means no primary;
all standbys plus one unknown IP needs the uncertainty rules below.

Errors treated as proof that an IP is unusable are listed in
[sql_health_errors.go](sql_health_errors.go):

- Connection refused (`ECONNREFUSED`).
- Admin shutdown (`57P01`), crash shutdown (`57P02`), or cannot connect now (`57P03`).
- Disk full (`53100`) or out of memory (`53200`).

Timeouts, connection resets, authentication failures, and SQLSTATE classes `58`
and `XX` do not prove that the whole instance is unusable. If the driver combines
several errors, every one must be on the unusable list. A refusal combined with
a timeout is still unknown.

There is **no rule to wait for N failed rounds** before falling back. One IP whose
latest results are all unknown is enough to enter the uncertainty path. The
policy first checks whether it can keep the previous SQL leader; otherwise it
uses Patroni, with the old-leader protections below.

Retries happen before the policy sees a result. The library makes one attempt
unless the application supplies `RunWithRetry`. Each adapter picks its own retry
policy. For example, an adapter can allow two attempts total, but not retry
`57P02`, `57P03`, or `28P01`.
Not retrying an error does not make it proof of instance failure: `28P01` only
says those credentials were rejected.

### What leader history remembers

Suppose SQL sees new primary B while Patroni still reports old primary A.
We remember B and prevent a later fallback from sending traffic back to A just
because B's next SQL check times out.

| Variable | Meaning |
| --- | --- |
| `awaitingPatroniLeader` | The primary SQL selected that Patroni has not yet confirmed, such as B. |
| `sqlLeaderSelectedAt` | When SQL selected that leader. Patroni must report agreement after this time. |
| `supersededPatroniLeader` | The old Patroni leader replaced by SQL, such as A. Leave it out of fallback. |
| `supersededSQLLeaders` | Other replaced leaders. For A -> B -> C, remember B too, so losing C does not bring B back. Remove entries when they leave the SQL candidate set. |
| `sqlNoPrimaryAt` | When SQL last found no primary, or filtered fallback left no healthy IP. A later Patroni result can restore a route. |
| `patroniRecoveryActive` / `patroniRecoveryAt` | Whether we are following Patroni after it restored a route, and the time SQL checks must start after to replace that route. |

A replaced leader can become primary again. A new, definite SQL result can select
it again; the old-leader list is not a permanent ban.

### Decision table: SQL finishes

Before calling the policy, the worker checks that the credential is still active,
the poll was not invalidated, and it covered every currently retained IP. A poll
scheduled before a routing-mode change cannot publish a route in the new mode.
Rejected polls leave the route unchanged. If no SQL candidates remain and SQL
owned routing, the worker restores the saved Patroni snapshot instead.

To keep previous SQL leader B, B must still be a candidate. A successful standby
answer for B rules it out. An unusable error also rules it out unless another
credential successfully reports B as primary. Conflicting successful roles or
more than one primary IP prevent keeping B.

For a snapshot that reaches `observeSQL`, use the following order:

| Order and condition | Result | History changes |
| --- | --- | --- |
| 1. SQL routing is off | `shadow`: SQL does not publish a route. | Do not establish SQL leader history. |
| 2. Patroni restored a route at T, but any included SQL snapshot started at or before T (or has no start time) | Keep the current route: `patroni_recovery`. | Keep history unchanged. A late finish does not make an old check new. |
| 3. The snapshot passes that time check | Continue below. | Stop active Patroni recovery. Remove replaced SQL leaders no longer in the candidate set. Reset `sqlNoPrimaryAt`, setting it again if SQL finds no primary. Keep the recovery time so another old completion cannot undo the route later. |
| 4. SQL is uncertain, but we can keep the awaiting SQL leader | `retained_sql`: keep B. | Keep B and the old-leader list. Apply the checks described above before keeping B. |
| 5. SQL is uncertain and we cannot keep that leader | `cached_fallback`: use saved Patroni results, with replaced leaders marked unhealthy. There may be no healthy leader left. | Add the previous awaiting leader to history if it is still a candidate, then clear awaiting. Keep older exclusions. If those exclusions leave no healthy IP, set `sqlNoPrimaryAt` to allow later Patroni recovery. |
| 6. SQL definitely selects B | `conclusive_sql`: use B. | If Patroni uniquely agrees on B, clear awaiting and the old-leader lists. Otherwise set awaiting B and its selection time, remember leaders it replaced, and exclude Patroni's different unique leader. SQL can select a previously excluded IP again. |
| 7. SQL definitely finds no primary | `conclusive_sql`: publish no healthy leader. | Clear awaiting, remembering the previous leader if it is still a candidate. Keep older exclusions and set `sqlNoPrimaryAt`. |

`StartedAt` is captured before probes start. When combining credential snapshots,
use the earliest included start time, including an unknown/zero time. One new
credential result must not make another credential's old answer pass row 2.

### Decision table: Patroni reports a result

Patroni keeps polling and saving its own results even while SQL supplies the route.
These updates can also restore routing without waiting for SQL to finish:

| Order and condition | Result | History changes |
| --- | --- | --- |
| 1. Result is older than the policy's saved Patroni result | Ignore it. | Keep the policy cache and history unchanged. |
| 2. Accept the new result | Save it. When SQL routing is off, Patroni supplies routing. | If Patroni uniquely confirms awaiting B after `sqlLeaderSelectedAt`, clear awaiting and both old-leader lists. |
| 3. Prepare the Patroni route | Mark replaced leaders unhealthy. | Keep the lists. Check whether Patroni reported one leader **before** filtering: A+C must not become proof of one leader just because A is excluded. |
| 4. Patroni reports one allowed leader after `sqlNoPrimaryAt`, or reports a newer, different leader C while we await B | `patroni_recovery`: use it immediately. C must not be one of the replaced leaders. | Start recovery and its time barrier. Remember the awaiting leader before clearing it. Keep older exclusions; Patroni alone has not confirmed the old SQL history. |
| 5. Recovery is already active | Follow later filtered Patroni results, including losing the leader, until a newer-started SQL snapshot arrives. | Move the time barrier when Patroni's leader or one-leader status changes. Repeated reports of the same state do not move it. |
| 6. None of the above | Keep SQL's route if SQL still owns routing. During fallback, continue publishing filtered Patroni updates. | An unchanged old A does not clear A's exclusion or replace SQL's B. |

Changing routing mode clears this leader history and routing timing, but keeps the
Patroni cache. Disabling SQL routing immediately restores raw Patroni results,
even if filtered fallback had already stopped SQL ownership. The worker latches a
disable for that `PoolHealth`; these reset rules do not mean it can dynamically
turn SQL routing back on for that same instance.

### Examples and limits

- **Keep B during a timeout:** SQL selected B ahead of Patroni A. B's next check
  times out. Keep B while it remains a candidate and no result rules it out.
  This keeps service available, but does not prove B is still primary.
- **No safe fallback:** B then reports standby while A's SQL check times out.
  Stop keeping B. Patroni's old A is still excluded, so there may be no healthy
  route. Losing B does not prove A became primary again.
- **Two quick switchovers:** SQL selected B and then C before Patroni confirmed B.
  Remember both old A and replaced B. If C later fails, do not return to cached
  Patroni B. A later allowed unique Patroni leader D can restore service.
- **Old SQL finishes after recovery:** Patroni restored B at T. A SQL check that
  started before T finishes afterward. Ignore it for routing, even if another
  credential's check started after T.

Keeping a previous SQL leader favors availability during unknown results. Keeping
old leaders excluded can instead leave no route; this avoids returning to a
leader we already replaced. There is no general age limit on kept SQL leaders or
cached credential results. The start-time check protects a **Patroni recovery**;
it does not run against every ordinary Patroni leader change. Repeated polls
rejected for missing IP coverage also keep the old route rather than automatically
falling back. Probe timeouts do not cover these cases. Banlist integration must
also check the retained-SQL and Patroni-recovery paths.

See [hybrid_health_test.go](hybrid_health_test.go) for policy examples and
[health_worker_test.go](health_worker_test.go) for scheduling and publication tests.

## SQL snapshot timing

The optional `HealthWorkerHooks.SQLPotentialCompared` callback compares completed
SQL and Patroni snapshots for a replacement primary. It reports signed speedup,
without downtime durations or percentages, using the completed-snapshot mismatch
aggregate. See [SQL snapshot timing](../../docs/sql-potential-timing.md) for the
snapshot boundary, lifecycle rules, and how to compare it with hybrid timing.
