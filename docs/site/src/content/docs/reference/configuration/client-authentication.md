---
# Unless explicitly stated otherwise all files in this repository are licensed
# under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/)
# Copyright 2026-Present Datadog, Inc.
title: "Client authentication"
description: "Password and SCRAM-SHA-256 client authentication in the pg-proxy binary, and credential references."
---

The `pg-proxy` binary supports opt-in password/SCRAM authentication. Start with
the complete [YAML](https://github.com/DataDog/pg-proxy/blob/main/examples/password-auth.yaml) or
[JSON](https://github.com/DataDog/pg-proxy/blob/main/examples/password-auth.json) example. Replace the hosts, credential sources,
and certificate paths before starting.

Set `proxy.password_auth_enabled: true` and `proxy.tls_required: true`, with a
valid TLS certificate/key pair. JSON calls these `passwordAuthEnabled` and
`tlsRequired`. Authenticated listeners accept explicit IPv4 or IPv6 addresses,
including `0.0.0.0:15432` and `[::]:15432`. Authentication requires TLS even on
loopback. JWT authentication remains a separate, unsupported mode.

Each `users` entry requires a unique `static_credentials.username` of at most
256 bytes, with no NUL byte, exactly one
`password` or `scram_sha_256`, and an explicit list of logical `databases`.
JSON uses `staticCredentials` and `scramSHA256`. Usernames and database names
match exactly, including case. Unknown users and databases outside the user's
list are denied. Wildcards have no special meaning. Static mode rejects `email`,
`group`, and `pg_roles` rather than ignoring them.
Optional `max_sessions` sets a per-user admission cap.

Each credential can be inline, or come from a file or an environment variable;
see [credential references](#credential-references).

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
at startup. Protect the configuration file if it contains inline credentials.

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
    "strings"

    "github.com/DataDog/pg-proxy/pkg/scram"
)

func main() {
    data, err := io.ReadAll(os.Stdin)
    if err != nil { panic("could not read password") }
    // Remove one trailing newline, the same rule as password_file.
    password, ok := strings.CutSuffix(string(data), "\r\n")
    if !ok { password = strings.TrimSuffix(password, "\n") }
    if password == "" { panic("password is empty") }
    verifier, err := scram.NewVerifier(password)
    if err != nil { panic("could not derive verifier") }
    fmt.Println(verifier.Encode())
}
```

Supply a protected password file. The helper removes one trailing newline
(`\n` or `\r\n`), like `password_file` does, so the same file gives the same
password to both:

```sh
go run scram-verifier.go < /path/to/password-file
```

Copy its output into `scram_sha_256` in a protected configuration file, or
write it to a protected file and point `scram_sha_256_file` at that file. The
helper prints sensitive verifier material. Normal `Secret` and `Verifier` formatting
redacts credentials; `Verifier.Encode` explicitly returns the stored value.

### Credential references

The `pg-proxy` binary can read a credential from a file or an environment
variable instead of the configuration file. This lets you keep the
configuration file free of secrets, for example in version control, and supply
the values from Kubernetes Secrets, systemd credentials, or similar.

| Inline | File | Environment variable |
|--------|------|----------------------|
| `databases[].password` | `password_file` | `password_env` |
| `static_credentials.password` | `password_file` | `password_env` |
| `static_credentials.scram_sha_256` | `scram_sha_256_file` | `scram_sha_256_env` |

JSON uses `passwordFile`, `passwordEnv`, `scramSHA256File`, and `scramSHA256Env`.

```yaml
databases:
  - name: orders-primary
    username: orders_backend
    password_file: /run/secrets/orders_backend_password
users:
  - static_credentials:
      username: bob
      scram_sha_256_env: BOB_SCRAM_VERIFIER
    databases:
      - orders
```

Rules:

- Set at most one of the inline, file, and environment forms for a credential.
- The command reads each reference once, at startup. If a file is missing,
  unreadable, or empty, or a variable is unset or empty, startup fails. Change
  the file or variable and restart the proxy to rotate a credential.
- The command removes one trailing newline (`\n` or `\r\n`) from file contents,
  because mounted secrets often end with one. It keeps all other bytes. A
  password that ends with a newline must therefore have one more newline in the
  file. The verifier helper above uses the same rule. Environment values are
  used as-is.
- Relative `*_file` paths are resolved from the proxy's working directory, not
  from the directory of the configuration file. Use absolute paths.
- Errors name the setting, not the path, the variable, or the value. Resolved
  values are redacted like inline ones.

The verifier helper above prints a trailing newline, so you can redirect its
output directly to a `scram_sha_256_file`.

This is a feature of the `pg-proxy` binary only. `pkg/config` and its
`ReadJSON`/`ReadYAML` readers accept inline credentials only; embedding
applications resolve their own references. See
[adapting configuration](../adapting-configuration/).

## Connecting

Use a server certificate whose name matches the proxy hostname and a trusted CA:

```sh
psql "host=proxy.example.com port=15432 dbname=orders user=alice sslmode=verify-full sslrootcert=/path/to/ca.crt" -W
```

Enter Alice's client password when prompted. Bob uses `user=bob` and his client
password. Neither client needs a backend password. See [client TLS](../client-tls/)
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
credential changes require a restart.

The global session cap defaults to 100,000 (`proxy.max_sessions`); the per-user
default is 10,000 (`proxy.default_max_user_sessions`). Set `users[].max_sessions`
to override one user's limit across all allowed logical databases. Omitted or
zero values use the applicable default; negative values fail at startup. JSON
uses `maxSessions` and `defaultMaxUserSessions`. These caps are independent of
backend pool sizes and the 100 pending-startup slots.
