# Server extraction review guide

`pkg/server` composes the listener and session runtimes around caller-owned
routing and session inventories.

| Responsibility | Owner |
| --- | --- |
| Socket, TLS, startup packets, admission, connection lifecycle | `pkg/listener` |
| Logical database lookup, startup resolution, read-cluster selection, session construction | `pkg/server.Runtime` |
| Session registration, PID lookup, deletion, shutdown wiring | `pkg/server.Server` |
| Session protocol and backend connections | `pkg/session` |
| Authentication, config conversion, experiments, metadata, metrics adapters, session extensions | App adapters |
| Backend and session inventories | App Warden in this PR; portable Warden in #99776 |

Read `server.go` and `runtime.go`, then `proxy.NewServer`. Callers supply one
`server.Config`; `NewServer` creates its runtime and wires startup resolution,
session construction, and lifecycle callbacks internally. The lower-level
`pkg/listener` remains available for custom composition. The runtime retains the
backend resolved before authentication, selects current read backends when constructing the
session, and preserves checkout wrappers through `Backend.SessionBackend()`.
The mirror budget is shared by all sessions from one runtime.

The caller owns both inventories. Construction binds the socket. `Run`, `Close`,
and context shutdown retain the listener's existing ordering. Startup errors,
read weights (including explicit zero), admission, metrics, and extension behavior
remain unchanged. The app adds environment metadata in place to the runtime's
single private copy of backend parameters, preserving backend precedence
(including empty values) and the proxy version override.

## Warden follow-up (#99776)

The server does not import Warden. After stacking the Warden extraction:

1. Make the portable Warden implement `server.Inventory`. Its `GetBackend` must
   return `server.Backend` exactly: Go does not allow a different interface return
   type even when the methods match. `warden.Backend` can alias `server.Backend`;
   backend lifecycle requirements stay on Warden's managed-backend interface.
2. Pass `cfg.Warden.Runtime()` as the runtime inventory and
   `cfg.Warden.Runtime().Sessions()` as the session registry.
3. Remove `wardenInventory`, `serverBackend`, and `wardenSessionRegistry` from
   the app. Keep the app's backend checkout wrapper in `SessionBackend()`.
4. Update the app's mirroring and gauge inventory adapters to use the portable
   Warden. Startup resolution and session construction require no further move.

Tests cover real startup/cancel packets, registry cleanup, shutdown waiting,
startup resolution, read routing, and session extensions. Portable tests travel
with this package and retain race detection. Datadog auth, metadata, and Docker
integration tests remain in the app.

Authentication uses the existing `Config.Authenticate` callback. Verify credentials
and check access to `resolved.LogicalDatabase` before returning a nonempty
`listener.AuthenticationResult.Service`. The listener checks cancellation and
rejects empty identities before admission. Client passthrough requests are
rejected before resolution or authentication. Credential sources and access policy
remain application-owned; no additional authentication wrapper is required.
