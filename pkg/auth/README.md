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
`StartupTimeout`, and `AuthenticationMessageLimit` on the server. The standalone
command uses TLS, 10 seconds, and 16 KiB for password authentication. Established
sessions and SQL messages do not inherit startup limits.

The listener rejects passthrough requests and empty identities before admission.
Callbacks return errors to the listener, which sends startup responses and logs
the accepted identity. Never log passwords, tokens, proofs, or provider payloads.

Datadog JWT and Vault policy stay in dd-source callbacks. They use the same
listener checks and the library's protocol and SCRAM exchanges directly.
