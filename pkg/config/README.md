# Configuration

Start with [`Config` in config.go](config.go). That file contains the complete
portable schema and its JSON/YAML field names. `ConfigSections` defines the
top-level fields once; `Config` supplies the standalone section types. Applications
can supply their own section types without duplicating those fields or tags.
Shared settings stay flat in configuration files.
Readers validate credential sources only. Defaults, routing validation, and
runtime wiring remain with the caller.

The six sections are `databases`, `clusters`, `logical_databases`, `proxy`,
`users`, and `mirroring`. YAML uses snake_case; JSON uses the camelCase names in
the struct tags. Serialization includes empty sections, except that nil mirroring
is omitted. Readers still accept documents with missing sections. This package defines the model. The
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

A username may have an omitted or empty password. Secret-store references belong
to the application: embed `DatabaseSettings`, add your own credential fields,
and resolve them into `Username`/`Password` before building a pool.

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
A user may supply either a password or a verifier, never both.

The OSS binary uses `-config` for direct backend credentials, connection
and pool settings, primary/logical routes, and loopback listener settings.
It rejects configured users, listener TLS, mirroring, and
other unsupported options with explicit startup errors. TODO: wire client SCRAM
and listener TLS support. The library schema remains broader than this command.

[`pool.PGXConfig`](../pool/config.go) consumes a `Database` and returns a pgx pool
configuration without opening connections. It supports direct backends and takes
resolved credentials. Application-specific runtime construction remains caller-owned.

Passwords and verifiers use `Secret`: input retains the value, while JSON, YAML,
and ordinary Go formatting redact it. Convert with `string(secret)` only where
the secret is consumed.
Marshaled config is diagnostic output, not a lossless secret backup.

## Datadog application boundary

The deployed app does **not** decode files into this `Config`. Its persisted
schema is `PGProxyConfig` in `domains/orgstore/libs/config/go/config.go`, which
composes the credential-free library settings with internal provider fields.
The [app config guide (Datadog internal)](https://github.com/ddoghq/dd-source/blob/main/domains/orgstore/apps/pg-proxy/config/README.md) lists its
additional fields and credential policy.

App database/user types expose Vault references, not passwords or verifiers.
The app providers and Atlas write paths explicitly apply the internal no-inline-
secrets policy. Shared JSON decoding is unchanged. Vault resolution stays in the
app; resolved credentials are passed to connection code in memory. No COBS/protobuf
version or schema change is needed for these library options.

## Extending the schema

Follow the [configuration extension guide](../../docs/extending-configuration.md)
to change a nested field for your application, add settings, or contribute a
shared library field. The guide follows an application that supplies database
passwords through environment variables and converts them for the library.
