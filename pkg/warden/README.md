# Warden

This package owns the configuration loop and live inventory. The embedder
supplies configuration, credentials, backend construction, and retry policy.
Lifecycle logs and metrics use the dependencies supplied through
`pgproxy.Bootstrap`.

| Responsibility | Portable owner | Supplied by the embedder |
| --- | --- | --- |
| Initial fetch, version check, apply, periodic refresh | `refresh.go` | Configuration provider, defaults, validation |
| Compare configuration; create, replace, and remove backends | `controller.go`, `reconcile.go` | Config conversion, users, mirroring, experiments |
| Backend creation with retries, state futures | `reconcile.go`, `future.go` | State factory, backoff policy |
| Logical database inventory and session remapping | `warden.go` | Configuration types |
| Session inventory and shutdown waiting | `sessions.go` | Listener authentication and session extensions |
| Concurrent stores | `store.go` | Compatibility inventory using these stores |
| Credential loading | None | Secret-store and local environment loading |

Read `Controller.Accept`, `Reconciler.Accept`, and
`Warden.ReconcileLogicalDatabases` for the main flow. `pkg/server` uses
`Warden.Sessions()` directly.

Configuration updates follow this order:

1. Compute the backend changes against the previous configuration.
2. Publish the database and cluster configuration used by backend creation.
3. Close removed backends; create replacements and close their predecessors
   before installing them.
4. Remap existing sessions, then replace the logical database inventory.
5. Apply mirroring settings and any configured refresh interval.

The controller's caller serializes configuration updates. Backend creation can
retry in the background. The reconciler and session-facing inventory read the
same future, so readiness does not depend on a second publication goroutine.
Removal or replacement cancels the old future. A known in-flight factory
cancellation limitation is documented at `createBackendWithRetries`.

Tests cover lifecycle ordering, retries, stale futures, remapping, session
shutdown, and the concurrent stores. They also pin lifecycle log messages and
metric names, tags, and sample rates.
