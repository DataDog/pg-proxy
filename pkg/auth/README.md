# Client authentication

`Hook` composes verification and authorization using the existing listener hook.
Both callbacks are required. Verification returns a nonempty `Identity.Service`
for session accounting; authorization checks the resolved logical database and
requested proxy username. `DatabaseAccess` supplies an exact, default-deny
allowlist for username/database pairs. Passthrough requests are rejected.

`NewStatic` constructs concurrent SCRAM verification and database authorization
from unique `StaticUser` entries. Supply a parsed or newly derived `scram.Verifier`
and explicit database grants, then compose `static.Verify` and `static.Authorize`
with `Hook`. Unknown usernames perform a dummy exchange and are always rejected.
Their fake salts are stable per username and derived from a provider-owned
random secret. Fake salt lengths and iteration counts match configured users;
unknown usernames are never cached.

Proxy usernames identify clients. Backend pools retain their configured login
credentials and SQL permissions. Multiple proxy users may share a backend pool.
No PostgreSQL roles are created or selected by this package.

Callbacks return failures to the listener and must not send terminal errors or
successful startup messages. The listener authenticates before admission. Only
successful verification and authorization produce an authenticated identity log.
Do not log passwords, tokens, authentication messages, or arbitrary provider errors.

This package does not choose a transport policy. Standalone authentication modes
require TLS; legacy trust remains restricted to loopback. Cancellation keeps its
separate PID/secret exchange, including plaintext cancellation when TLS is required.
