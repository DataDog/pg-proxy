# Server

`pkg/server` composes the listener and session runtimes around caller-owned
routing and session inventories.

| Responsibility | Owner |
| --- | --- |
| Socket, TLS, startup packets, admission, connection lifecycle | `pkg/listener` |
| Logical database lookup, startup resolution, read-cluster selection, session construction | `pkg/server.Runtime` |
| Session registration, PID lookup, deletion, shutdown wiring | `pkg/server.Server` |
| Session protocol and backend connections | `pkg/session` |
| Authentication, config conversion, experiments, metadata, metrics adapters, session extensions | Embedder adapters |
| Backend and session inventories | Caller, or `pkg/warden` |

Read `server.go` and `runtime.go`, then `NewServer`. Callers supply one
`server.Config`; `NewServer` creates its runtime and wires startup resolution,
session construction, and lifecycle callbacks internally. The lower-level
`pkg/listener` remains available for custom composition. The runtime retains the
backend resolved before authentication, selects current read backends when constructing the
session, and preserves checkout wrappers through `Backend.SessionBackend()`.
The mirror budget is shared by all sessions from one runtime.

The caller owns both inventories. Construction binds the socket. `Run`, `Close`,
and context shutdown retain the listener's existing ordering. Startup errors,
read weights (including explicit zero), admission, metrics, and extension behavior
remain unchanged. An embedder can add environment metadata in place to the
runtime's single private copy of backend parameters, preserving backend
precedence (including empty values) and the proxy version override.

Tests cover real startup/cancel packets, registry cleanup, shutdown waiting,
startup resolution, read routing, and session extensions.

Authentication uses the existing `Config.Authenticate` callback. Verify credentials
and check access to `resolved.LogicalDatabase` before returning a nonempty
`listener.AuthenticationResult.Service`. The listener checks cancellation and
rejects empty identities before admission. Passthrough policy belongs to the
application's authentication callback. The standalone command rejects it;
embedders can retain their existing role handling after authentication.
Credential sources and access policy remain application-owned.

Set `MaxPendingConnections` to bound concurrent TLS/startup handlers. The listener
closes excess connections before creating a handler and releases each slot after
startup or failure. Zero preserves the unbounded embedding default. Pair this
with `StartupTimeout`; the command uses 100 pending connections and 10 seconds.
