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
- static_credentials:
    username: bob
    scram_sha_256: 'SCRAM-SHA-256$4096:<base64-salt>$<base64-stored-key>:<base64-server-key>'
  databases: [orders]
```

The verifier above shows the format, not a usable value. Supply a PostgreSQL
SCRAM-SHA-256 stored verifier with 1–1,000,000 iterations, a 1–1024-byte base64-decoded salt, and
two 32-byte keys. JSON calls the field `scramSHA256`.
A user may supply either a password or a verifier, never both.

The OSS binary uses `-config` for direct backend credentials, connection
and pool settings, primary/logical routes, and loopback listener settings,
including optional or required client TLS through `tls_cert_file`, `tls_key_file`,
and `tls_required`. See [client TLS](../../docs/client-connections.md#standalone-tls).
Set `password_auth_enabled: true` (JSON: `passwordAuthEnabled`) with required TLS
to enable static users and non-loopback listeners. Each user needs an explicit
logical-database list. Proxy usernames are independent of backend accounts.
See [client authentication](../../docs/client-authentication.md).
Set `admin_address` (JSON: `adminAddress`) to serve HTTP `/livez` and `/readyz`.
It rejects JWT, mirroring, and other unsupported options with explicit startup errors.
The library schema remains broader than this command. The command also reads
its own `*_file` and `*_env` credential references and resolves them into these
types before validation; see
[credential references](../../docs/client-authentication.md#credential-references).
`ReadJSON` and `ReadYAML` do not accept those fields.

[`pool.PGXConfig`](../pool/config.go) consumes a `Database` and returns a pgx pool
configuration without opening connections. It supports direct backends and takes
resolved credentials. Application-specific runtime construction remains caller-owned.

Passwords and verifiers use `Secret`: input retains the value, while JSON, YAML,
and ordinary Go formatting redact it. Convert with `string(secret)` only where
the secret is consumed.
Marshaled config is diagnostic output, not a lossless secret backup.

## Embedding applications

An embedding application does not have to decode files into this `Config`. It
can keep its own persisted schema that composes these credential-free library
settings with its own provider fields, such as references into a secret store.
Resolve those references in the application and pass the resulting credentials
to connection code in memory. Shared JSON decoding is unchanged.

## Extending the schema

Follow the [configuration extension guide](../../docs/extending-configuration.md)
to change a nested field for your application, add settings, or contribute a
shared library field. The guide follows an application that supplies database
passwords through environment variables and converts them for the library.
