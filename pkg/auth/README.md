# Static client authentication

`NewStatic` copies SCRAM credentials and explicit database grants for concurrent
logins. Call `static.Authenticate(ctx, username, resolvedDatabase, exchange)` from
the listener's existing authentication callback. It verifies the proof and then
checks database access. Return `AuthenticationResult{Service: username}` only on
success. Backend pools keep their configured PostgreSQL accounts.

Unknown usernames receive fake challenges and are always rejected. Fake salts
stay stable per username and match configured salt lengths and iteration counts.
Unknown names are never cached.

Applications own TLS, credential loading, and rotation. Set `RequireTLS`,
`StartupTimeout`, and `AuthenticationMessageLimit` on the server. The `pg-proxy`
binary uses a 10-second startup timeout and a 16 KiB authentication message limit
in every mode, and requires TLS for password authentication. Established
sessions and SQL messages do not inherit startup limits.

The listener rejects empty identities before admission. Applications own
passthrough policy; the `pg-proxy` binary rejects it before credential exchange.
Callbacks return errors to the listener, which sends startup responses and logs
the accepted identity. Never log passwords, tokens, proofs, or provider payloads.

Other mechanisms, such as token or secret-store based authentication, belong in
application callbacks. They use the same listener checks and the library's
protocol and SCRAM exchanges directly.
