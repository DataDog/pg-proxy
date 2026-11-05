# SQL snapshot timing versus hybrid routing timing

`HealthWorkerHooks.SQLPotentialCompared` reports signed speedup between completed
SQL and Patroni snapshots. Keep this alongside the hybrid timing callbacks in PR9:
the snapshot signal describes when SQL reports a replacement primary, while the
hybrid signal describes when the conservative policy can select it.

| Measurement | SQL event | Purpose |
| --- | --- | --- |
| SQL snapshot speedup (this callback) | A completed comparison snapshot reports the replacement IP as primary | Compare SQL observation timing with Patroni |
| Conclusive SQL detection (PR9) | Complete aggregated evidence satisfies the hybrid policy | Measure when there is sufficient evidence for the policy |
| Hybrid selection (PR9) | Policy selects the replacement via SQL or cached Patroni | Measure the proposed routing policy's improvement |

## Snapshot boundary

Each credential must finish its full multi-IP poll before contributing new
evidence or a timing observation. The worker uses the same aggregate as
`SQLPatroniCompared`: this completed poll plus the other credentials' latest
completed polls. Credentials remain independently scheduled; this does not wait
for every credential to finish a synchronized round.

A successful individual probe can still publish early availability, but it does
not update timing. `SQLObservedAt` is the newest completion timestamp among all
credential snapshots included in the aggregate. Each timestamp includes waiting
for that credential's slower probes. If an older completion is accepted after a
newer one, the aggregate cannot be timestamped earlier than the evidence it uses.
Patroni is timestamped immediately after all its probes complete, before completion
callbacks, health cleanup, or waiting for the timing lock.
Individual credential timestamps remain intact for diagnostics; timing is never
backdated to the first successful primary response.

Completed polls replace their credential's entire previous evidence, including
errors and empty results. Successful primary/standby conflicts on an IP exclude
that IP from the aggregate. Unknown results do not erase another credential's
successful role. Missed scheduler ticks do not replace completed snapshots, and
there is no cache TTL. These comparison rules are not the full hybrid routing
policy: uncertainty on another IP does not automatically reject a primary here.
Only primary pool subscriptions produce timing comparisons.

Timing and snapshot acceptance share the same lock and lifecycle validation.
Rejected completions cannot update timing. Disable or retirement resets timing
continuity. If no completed SQL evidence remains, reference timing pauses until a
valid SQL completion establishes a fresh baseline. Patroni observations captured
during the gap cannot be accepted later after resuming. Repeated disabled ticks
do not reset an enabled sibling's timing.

DNS eviction removes only the evicted addresses from pending observations and
baselines, preserving timing for retained replacements; evicted results remain
excluded from the aggregate. Eviction and timing invalidation hold the same lock,
so Patroni cannot match a pre-eviction timestamp before the next SQL completion.
Per-IP lifecycle cutoffs survive eviction and advance on rediscovery: a delayed
Patroni callback captured before eviction or during absence cannot recreate that
IP's pending observation after rediscovery. SQL timing filters each credential's
results against the same per-IP cutoff before resolving roles, using that
credential's completion time. A newer sibling completion cannot make old SQL
evidence fresh. Raw mismatch snapshots remain unchanged. Evidence for an evicted IP also
remains ineligible while the IP is absent. These checks run
under the same lock and do not invalidate timing for other retained IPs. Patroni
callbacks older than the latest accepted reference snapshot are ignored, so they
cannot interrupt fresh pending evidence after rediscovery.
Retention is tracked from the first discovery, including while initial SQL polls
are unfinished. DNS lookup stays outside the timing lock. Callbacks run after
releasing the lock.

## Signed speedup

For the same replacement IP:

```text
speedup = PatroniObservedAt - SQLObservedAt
```

If SQL completes at 2s and Patroni observes the replacement at 5s, speedup is 3s.
If Patroni observes it at 2s and SQL completes at 4s, speedup is -2s. Negative
values are preserved to expose regressions.

The first eligible nonempty snapshot from each source establishes a baseline.
Matching follows the existing hybrid timing policy: repeated snapshots do not
move a pending timestamp, empty observations do not advance the baseline, and
unmatched timestamps remain available when a source moves to another leader.
Each IP is paired independently once both sources have a matching observation.
Explicit disable, retirement, and DNS lifecycle invalidation still clear affected
evidence as described above. No additional interruption or agreement cleanup
policy is introduced here.

This callback contains only signed speedup and the IP/timestamps needed to
identify the event. It defines no outage start and emits no downtime durations
or reduction percentages. PR9 retains its own downtime measurements.

## Comparing with PR9

Snapshot timing and `hybrid_health.go` keep their own leader selection, baselines,
and pending observations. They share only `leaderTimingPair`, which records the
two timestamps and calculates their signed difference. Hybrid detection still
requires exactly one newly observed primary; hybrid routing requires one healthy
primary. Snapshot timing compares each newly observed primary independently.
Hybrid routing and timing behavior remain unchanged.

For the same failover and the **same Patroni observation**:

```text
snapshot speedup - hybrid improvement = hybrid selection time - SQL snapshot time
```

For example, 3s snapshot speedup and 2s hybrid improvement leave a 1s delay between
SQL's snapshot and the hybrid selection. An absolute duration is easier to
interpret than a percentage when the denominator is small, zero, or negative.

Join by host, replacement IP, and Patroni event timestamp, not merely IP: an
address can be promoted repeatedly. This callback exposes both observation
timestamps. PR9's consumer must also retain that event identity before producing
a paired gap metric. Do not subtract maxima or percentiles from unmatched series.

Keep the signed distribution and sample count as well as maxima. A maximum alone
can hide slower events. Poll cadence, retries, aggregation, and cached evidence
affect these observations; they do not establish end-to-end client recovery.

The portable library emits optional callbacks only. The embedding application
owns metric names, units, event correlation, and dashboard queries. Leave the
hook nil to disable tracking. Callbacks must return promptly and may run
concurrently for different hosts.
