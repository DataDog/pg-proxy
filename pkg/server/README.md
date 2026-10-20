# Server composition

`pkg/server` composes the listener and session runtimes around caller-owned
routing and session inventories.

| Responsibility | Owner |
| --- | --- |
| Socket, TLS, startup packets, admission, connection lifecycle | `pkg/listener` |
| Logical database lookup, startup resolution, read-cluster selection, session construction | `pkg/server.Runtime` |
| Session registration, PID lookup, deletion, shutdown wiring | `pkg/server.Server` |
| Session protocol and backend connections | `pkg/session` |
| Authentication, config conversion, experiments, metadata, metrics adapters, session extensions | App adapters |
| Backend and session inventories | Caller-provided inventory and session registry, including `pkg/warden` |

Read `server.go` and `runtime.go`. Callers supply one
`server.Config`; `NewServer` creates its runtime and wires startup resolution,
session construction, and lifecycle callbacks internally. The lower-level
`pkg/listener` remains available for custom composition. The runtime retains the
backend resolved before authentication, selects current read backends when constructing the
session, and preserves checkout wrappers through `Backend.SessionBackend()`.
The mirror budget is shared by all sessions from one runtime.

The caller owns both inventories. Construction binds the socket. `Run`, `Close`,
and context shutdown retain the listener's existing ordering. Startup errors,
read weights (including explicit zero), admission, metrics, and extension behavior
are defined by the supplied configuration and hooks. The caller can add
application metadata through those hooks.
