# SQL observation potential versus hybrid routing timing

Keep the two measurements. `HealthWorkerHooks.SQLPotentialCompared` measures an
optimistic observed bound; the hybrid timing callbacks proposed in PR9 measure
when the conservative policy can actually make its hypothetical selection.
Neither changes Patroni-owned routing or measures end-to-end client recovery.

| Measurement | SQL event | What it tells you |
| --- | --- | --- |
| SQL potential (this callback) | First successful primary result for an IP, from any credential, after retries | Earliest observed opportunity without waiting for other probes or resolving conflicting evidence |
| Conclusive SQL detection (PR9) | Complete aggregated evidence supports the replacement leader | Cost of waiting for enough evidence |
| Hybrid selection (PR9) | Policy selects the replacement via SQL or cached Patroni | Improvement the proposed routing policy can achieve |

A successful probe can finish while another IP is timing out, or while another
credential still reports the old primary. The potential callback deliberately
accepts that successful evidence immediately. It does not prove there is only
one primary, or authorize routing to it. Only primary pool subscriptions produce
these comparisons; replica health is not a leader signal.

## Per-event speedup

The worker records individual post-retry SQL results and completed Patroni
snapshots independently. For the same replacement IP:

```text
potential speedup = PatroniObservedAt - SQLObservedAt
```

Positive means SQL was first; negative means Patroni was first. Repeated success
for an address does not move its timestamp. Completed results from all current
credentials contribute independently: a failed credential does not erase another
credential's success on the same IP. There is no cache TTL and missed scheduler
ticks are not probe results; a blocked credential retains its last evidence.

The first nonempty observation from each source establishes that source's
baseline, not a failover sample. An unmatched observation is discarded when its
source stops reporting that IP, preventing a later promotion from using its old
timestamp. An interrupted observation may acquire a fresh timestamp when it
returns. A disable, retirement, or DNS eviction that removes recorded evidence
resets timing baselines and pending pairs rather than measuring across that gap.
Late results from a disabled lifecycle or an evicted IP are ignored.

## Paired observed downtime

After SQL has established a primary baseline, the first completed result that
leaves **no successful primary evidence across the current credentials/IPs**
opens an observed availability gap. This includes errors such as timeouts; it
is not proof that PostgreSQL had no primary. Repeated failures preserve the
start. The first successful replacement-primary probe closes the gap.

```text
t0: latest SQL evidence has no successful primary
t1: first successful SQL probe of replacement B
t2: completed Patroni snapshot reports B healthy

SQLDowntime     = t1 - t0
PatroniDowntime = t2 - t0
Speedup         = t2 - t1
Reduction (%)   = 100 * (PatroniDowntime - SQLDowntime) / PatroniDowntime
```

For `t0=1s`, `t1=2s`, `t2=5s`, the durations are 1s and 4s, the speedup is 3s,
and the reduction is 75%. If Patroni observes B at 2s and SQL at 4s, the paired
durations are 3s and 1s: speedup is -2s and reduction is -200%. Values are never
clamped to zero.

`HasDowntimeComparison` is true only for a replacement leader with a matching
stored gap, nonnegative SQL duration, and strictly positive Patroni duration.
Startup, same-leader recovery, and a failover without an observed SQL gap do
not produce downtime percentages. They do not invent a denominator.

Both durations use the SQL-defined start. `PatroniDowntime` is not an independent
measurement of a Patroni outage. This gap differs from PR9's **conclusive**
no-leader interval and from the existing missed-tick availability SLI. Keep
those series separate.

## Comparing and aggregating

Keep the signed per-event distribution, event count, and paired durations in
addition to maxima. A maximum answers how much improvement was observed in the
best sampled event; it cannot demonstrate absence of regressions. Poll cadence,
retries, and cached credential evidence all affect the observed bound.

For the same failover and the **same Patroni observation**, the remaining gap is:

```text
potential speedup - hybrid improvement = hybrid selection time - earliest SQL success
```

Join by host, replacement IP, and the Patroni event timestamp, not merely IP:
addresses can be promoted repeatedly. This callback exposes both observation
timestamps. PR9's consumer must also retain that event identity before producing
a paired gap metric. Do not subtract maxima or percentiles of unmatched series,
or compare downtime percentages with different starts. To compare downtime,
recompute the hybrid duration from the same `DowntimeStartedAt` and event.

For an overall duration-weighted potential reduction, use the same eligible
events for both sums:

```text
100 * (sum(PatroniDowntime) - sum(SQLDowntime)) / sum(PatroniDowntime)
```

The portable library emits optional callbacks only. The embedding application
owns metric names, units, distributions, event correlation, and dashboard queries.
Leave the hook nil to disable this tracking. Callbacks execute outside the timing
lock and must return promptly because they run on the observation path.
