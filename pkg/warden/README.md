# Warden extraction review guide

This package moves the configuration loop and live inventory out of
`apps/pg-proxy/ctl`. The app still constructs `ctl.Warden`, which supplies Datadog
configuration, credentials, backend construction, and retry policy. Lifecycle
logs and metrics use the library dependencies supplied through `pgproxy.Bootstrap`.

| Before | Portable owner | Stays in the app |
| --- | --- | --- |
| `ctl.warden.Initialize`, `refreshConfiguration` | `refresh.go`: fetch, version check, apply, periodic refresh | COBS provider, defaults, validation |
| `ctl.warden.acceptImpl`, `getStatesDiff` | `controller.go`, `reconcile.go`: compare configuration, create/replace/remove backends | Config conversion, users, mirroring, experiments |
| `ctl.warden.createStateWithRetries`, state futures | `reconcile.go`, `future.go` | State factory, backoff policy |
| Logical database inventory and session remapping | `warden.go` | Datadog configuration types |
| Session inventory and shutdown waiting | `sessions.go` | Listener authentication and session extensions |
| `ctl/internal` concurrent stores | `store.go` | Compatibility inventory using these stores |
| `resources.Database.FetchSecret` | None | `credentials`: Vault and local environment loading |

Read `ctl.newWarden` for the app boundary, then `Controller.Accept`,
`Reconciler.Accept`, and `Warden.ReconcileLogicalDatabases` for the moved flow.
`pkg/server` now uses `Warden.Sessions()` directly, replacing the temporary
session adapter introduced in #99774. Sheaf compatibility remains in the app.

Configuration updates retain this order:

1. Compute the backend changes against the previous configuration.
2. Publish the database and cluster configuration used by backend creation.
3. Close removed backends; create replacements and close their predecessors
   before installing them.
4. Remap existing sessions, then replace the logical database inventory.
5. Apply app mirroring and any configured refresh interval.

The controller's caller serializes configuration updates. Backend creation can
retry in the background. The reconciler and session-facing inventory read the
same future, so readiness does not depend on a second publication goroutine.
Removal or replacement cancels the old future. The pre-existing in-flight
factory cancellation limitation remains documented at `createBackendWithRetries`.

Portable tests cover lifecycle ordering, retries, stale futures, remapping,
session shutdown, and the original concurrent stores. Library tests also preserve
lifecycle log messages and metric names, tags, and sample rates. App tests retain
config defaults, mirroring, credential loading, and adapter behavior. Both retain
race detection. The library dependency check rejects Datadog dependencies.
