# Configuration and inventory lifecycle

This package manages configuration refresh, backend reconciliation, session
inventory, and shutdown. The embedding application supplies configuration,
credentials, backend construction, and retry policy. Lifecycle logs and metrics
use the dependencies supplied through `pgproxy.Bootstrap`.

Read `Controller.Accept`, `Reconciler.Accept`, and
`Warden.ReconcileLogicalDatabases` for the configuration flow. `Warden.Sessions()`
provides the session registry used by `pkg/server`.

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

Tests cover lifecycle ordering, retries, stale futures, session remapping,
shutdown, concurrent stores, and telemetry hooks.
