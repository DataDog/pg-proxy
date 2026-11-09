<!--
Unless explicitly stated otherwise all files in this repository are licensed
under the Apache-2.0 License.
This product includes software developed at Datadog (https://www.datadoghq.com/)
Copyright 2026-Present Datadog, Inc.
-->

# Client authentication

The standalone command supports opt-in password/SCRAM authentication. Start with
the complete [YAML](../examples/password-auth.yaml) or
[JSON](../examples/password-auth.json) example. Replace the hosts, credentials,
certificate paths, and every `<...>` placeholder before starting.

Set `proxy.password_auth_enabled: true` and `proxy.tls_required: true`, with a
valid TLS certificate/key pair. JSON calls these `passwordAuthEnabled` and
`tlsRequired`. Authenticated listeners accept explicit IPv4 or IPv6 addresses,
including `0.0.0.0:15432` and `[::]:15432`. Authentication requires TLS even on
loopback. JWT authentication remains a separate, unsupported mode.

Each `users` entry requires a unique `static_credentials.username`, exactly one
`password` or `scram_sha_256`, and an explicit list of logical `databases`.
JSON uses `staticCredentials` and `scramSHA256`. Names match exactly, including
case. Unknown users and databases outside the user's list are denied. Wildcards
have no special meaning. Static mode rejects `email`, `group`, `pg_roles`, and
nonzero `max_sessions` rather than ignoring them.

Proxy users are independent of backend accounts. Alice and Bob may both connect
to `orders`, sharing its configured pool and backend account. Their sessions and
authentication debug logs retain their distinct proxy usernames. SQL permissions and
`current_user` belong to `orders_backend` in the example. Different SQL permissions
require separate configured backend accounts/routes. The proxy does not create
PostgreSQL roles or run `SET ROLE`; role passthrough remains rejected.

## Credentials

Both credential forms use the PostgreSQL SCRAM-SHA-256 exchange. A client supplies
its ordinary password. A stored verifier is a server credential, never a client
password. Plaintext configuration is converted to a random-salt verifier once
at startup. Protect the configuration file because it contains credentials.

Stored verifiers use:

```text
SCRAM-SHA-256$iterations:base64-salt$base64-stored-key:base64-server-key
```

The parser accepts 1–1,000,000 iterations, a 1–1024-byte decoded salt, and two
32-byte keys. New verifiers use 4096 iterations and an 18-byte random salt.
The whole encoded verifier is limited to 4096 bytes.

To generate a verifier without creating a PostgreSQL role, save this helper
as `scram-verifier.go` and run it from a module requiring pg-proxy:

```go
package main

import (
    "fmt"
    "io"
    "os"

    "github.com/DataDog/pg-proxy/pkg/scram"
)

func main() {
    password, err := io.ReadAll(os.Stdin)
    if err != nil { panic("could not read password") }
    verifier, err := scram.NewVerifier(string(password))
    if err != nil { panic("could not derive verifier") }
    fmt.Println(verifier.Encode())
}
```

Supply a protected password file containing the exact password bytes, without
an added newline:

```sh
go run scram-verifier.go < /path/to/password-file
```

Copy its output into `scram_sha_256` in a protected configuration file. The helper
prints sensitive verifier material. Normal `Secret` and `Verifier` formatting
redacts credentials; `Verifier.Encode` explicitly returns the stored value.

## Connecting

Use a server certificate whose name matches the proxy hostname and a trusted CA:

```sh
psql "host=proxy.example.com port=15432 dbname=orders user=alice sslmode=verify-full sslrootcert=/path/to/ca.crt" -W
```

Enter Alice's client password when prompted. Bob uses `user=bob` and his client
password. Neither client needs a backend password. See [client TLS](client-connections.md)
for certificate setup. SCRAM-SHA-256-PLUS/channel binding is not implemented.

## Defaults and failures

With authentication disabled, the listener remains loopback-only and the client
username must match the selected backend username. TLS alone does not permit a
non-loopback trust listener. Supplying users without explicitly enabling password
authentication fails startup. Enabling both password and JWT authentication fails.

Invalid credentials, grants, unsupported user settings, and missing TLS fail
before backend pools or the listener open. Authentication failures receive one
safe terminal error followed by closure, without a session or admission slot.
TLS, startup, and authentication must finish within 10 seconds. The deadline is
removed after startup and does not limit query or session duration. At most 100
connections may negotiate TLS/startup at once; excess connections close immediately.
Established sessions do not consume these startup slots.
Authentication messages are limited to 16 KiB; SQL messages retain their existing
limits. Passwords, stored verifiers, and authentication payloads are excluded from
logs and client errors.

Cancellation still uses the session PID and secret on a separate connection.
Both TLS cancellation and legacy plaintext cancellation work when TLS is required.
No new password exchange is performed for cancellation. Configuration and
credential changes require a restart. The existing global and per-user default
session limits remain 100.

## Integration tests

The standalone test runs against the existing Docker fixture, or an explicit
`PG_PROXY_TEST_DATABASE_URL`. That URL must point to a disposable PostgreSQL
instance with superuser access: the suite creates databases and fixture roles.
GitHub Actions supplies a disposable local PostgreSQL instance and executes the
suite with race detection:

```sh
PG_PROXY_TEST_DATABASE_URL='postgres://postgres:postgres@127.0.0.1:5432/postgres?sslmode=disable' \
  go test -race -run '^TestProxyAgainstPostgres$' -count=1 ./cmd/pg-proxy
```
