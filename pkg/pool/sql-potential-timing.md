# SQL snapshot timing versus hybrid routing timing

`HealthWorkerHooks.SQLPotentialCompared` reports signed speedup between completed
SQL and Patroni snapshots. Keep this alongside the hybrid timing callbacks:
the snapshot signal describes when SQL reports a replacement primary, while the
hybrid signal describes when the conservative policy can select it.

| Measurement | SQL event | Purpose |
| --- | --- | --- |
| SQL snapshot speedup (this callback) | A completed comparison snapshot reports the replacement IP as primary | Compare SQL observation timing with Patroni |
| Conclusive SQL detection | Complete aggregated evidence satisfies the hybrid policy | Measure when there is sufficient evidence for the policy |
| Hybrid selection | Policy selects the replacement via SQL or cached Patroni | Measure the proposed routing policy's improvement |

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
For primary subscriptions with the comparison hook configured, retention is
tracked from the first discovery, including while initial SQL polls are unfinished.
Subscriptions without the hook still discover and probe SQL addresses but retain
no DNS timing history. DNS lookup stays outside the timing lock. Callbacks run after
releasing the lock.

## Signed speedup

For the same replacement IP:

```text
speedup = PatroniObservedAt - SQLObservedAt
```

If SQL completes at 2s and Patroni observes the replacement at 5s, speedup is 3s.
If Patroni observes it at 2s and SQL completes at 4s, speedup is -2s. Negative
values are preserved to expose regressions.

The first eligible nonempty snapshot from each source establishes a baseline
and clears any pending observations for its initial primary IPs. Startup evidence
must not pair with a later replacement observation for the same IP.
Repeated snapshots, including a primary reappearing after an intervening leader,
do not move a pending timestamp. Empty observations do not advance the baseline,
and unmatched timestamps remain available when a source moves to another leader.
Each IP is paired independently once both sources have a matching observation.
Explicit disable, retirement, and DNS lifecycle invalidation still clear affected
evidence as described above. These snapshot tracking rules do not change the
shared timestamp helper or hybrid routing timing policy.

This callback contains only signed speedup and the IP/timestamps needed to
identify the event. It defines no outage start and emits no downtime durations
or reduction percentages. The hybrid timing callbacks keep their own downtime measurements.
