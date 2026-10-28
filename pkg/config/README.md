# Configuration

Start with [`Config` in config.go](config.go). That file contains the complete
portable schema and its JSON/YAML field names. Database connection and pool
settings are embedded Go structs but stay flat in configuration files.
Readers validate credential sources only. Defaults, routing validation, and
runtime wiring remain with the caller.

The six sections are `databases`, `clusters`, `logical_databases`, `proxy`,
`users`, and `mirroring`. YAML uses snake_case; JSON uses the camelCase names in
the struct tags. This package defines the model. The
[OSS binary](../../README.md#oss-binary) reads a limited subset
for direct primary backends; embedding applications can support other sections.

## Credentials

A database accepts a username/password directly:

```yaml
databases:
- name: orders-primary
  host: localhost
  port: 5432
  dbname: orders
  username: orders_service
  password: replace-with-database-password
```

Or omit both fields and use `credentials: {name: ORDERS, revision: v1}`. The
caller resolves that reference. Mixing inline credentials and references is
rejected. An inline username may have an omitted or empty password.

Users retain the existing `static_credentials` block:

```yaml
users:
- static_credentials:
    username: alice
    password: replace-with-client-password
  databases: [orders]
  pg_roles: [reader]
- static_credentials:
    username: bob
    scram_sha_256: 'SCRAM-SHA-256$4096:<base64-salt>$<base64-stored-key>:<base64-server-key>'
  databases: [orders]
```

The verifier above shows the format, not a usable value. Supply a PostgreSQL
SCRAM-SHA-256 stored verifier with a positive iteration count, base64 salt, and
two 32-byte keys. JSON calls the field `scramSHA256`.
A user may supply a password, verifier, or reference (`name`/`revision` in
`static_credentials`), never more than one.

The OSS binary uses `-config` for direct backend credentials, connection
and pool settings, primary/logical routes, and loopback listener settings.
It rejects configured users, credential references, listener TLS, mirroring, and
other unsupported options with explicit startup errors. TODO: wire client SCRAM
and listener TLS support. The library schema remains broader than this command.

[`pool.PGXConfig`](../pool/config.go) consumes a `Database` and returns a pgx pool
configuration without opening connections. It supports direct backends; resolve
credential references in your application and clear the reference before passing
inline credentials. Application-specific runtime construction remains caller-owned.

Passwords and verifiers use `Secret`: input retains the value, while JSON, YAML,
and ordinary Go formatting redact it. Convert with `string(secret)` only where
the secret is consumed.
Marshaled config is diagnostic output, not a lossless secret backup.

## Extending the schema

Follow the [configuration extension guide](../../docs/extending-configuration.md)
to change a nested field for your application, add settings, or contribute a
shared library field. The guide follows an application that replaces database
credential references with environment-variable names and converts them for the
library.
