---
# Unless explicitly stated otherwise all files in this repository are licensed
# under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/)
# Copyright 2026-Present Datadog, Inc.
title: "Adapting configuration"
description: "Change or replace parts of the PG Proxy configuration schema in a Go application that embeds the library."
---

You can change an existing field; you do not have to add another section. If you
just want a different value, set it on the decoded config before building the
proxy. For example, `cfg.Databases[0].MaxConns = 20` changes that database's pool
limit. Changing the struct after creating a pool does not reconfigure the pool.

Changing a field's type or its meaning in a config file takes a little more
work. The library's [`Config`](https://github.com/DataDog/pg-proxy/blob/main/pkg/config/config.go) has concrete Go types,
so your application needs its own type for the part that differs, then a small
conversion into the library's config. The rest can use the library types as-is.

## A different way to supply database credentials

Suppose your deployment puts database passwords in environment variables. You
want each database to name the variable it uses in an app-owned `credentials`
section:

```yaml
databases:
  - name: orders-primary
    host: localhost
    port: 5432
    dbname: orders
    username: orders_service
    credentials:
      password_env: ORDERS_DATABASE_PASSWORD
```

The library accepts a password directly. Your application instead reads
`credentials.password_env` and looks up the password before building a pool.
The connection and pool fields still mean the same thing.

In your application, define the database type you want to read. In these snippets,
`pgconfig` is this library's [`pkg/config`](https://github.com/DataDog/pg-proxy/tree/main/pkg/config) package:

```go
type Database struct {
    pgconfig.DatabaseSettings `yaml:",inline" mapstructure:",squash"`
    Username            string      `json:"username" yaml:"username"`
    Credentials         Credentials `json:"credentials" yaml:"credentials"`
}

type Credentials struct {
    PasswordEnv string `json:"password_env" yaml:"password_env"`
}
```

`DatabaseSettings` holds the shared fields without credentials. Embedding it
keeps your application on the same field definitions as the library.

- **Included:** the connection fields, `max_conns`, `connection_options`, and the
  discovery fields (`postgres_host`, `route_through_envoy`, `load_balanced_servers`).
- **Not included:** the pool tuning fields (`min_conns`, `min_idle_conns`,
  `max_conn_lifetime`, `max_conn_lifetime_jitter`), `application_name`, and
  `runtime_params`. These are on `pgconfig.Database`.
- **To accept them:** embed `pgconfig.PoolConfig` with a `yaml:",inline"` tag
  (without the tag, a strict YAML reader rejects the fields), and add your own `ApplicationName`
  and `RuntimeParams` fields if you need them. The `LibraryConfig` method below
  copies `DatabaseSettings` and sets the username and resolved password. If you
  add these fields, also set `PoolConfig: database.PoolConfig`, `ApplicationName`,
  and `RuntimeParams` in its `pgconfig.Database` literal, or they are silently
  dropped.

Your application still needs to reject settings it does not support.

The containing config must also use your database type. `ConfigSections` lets you
choose each section's type without copying the top-level fields or their tags:

```go
type AppConfig pgconfig.ConfigSections[
    pgconfig.User, Database, pgconfig.Cluster,
    pgconfig.LogicalDatabase, pgconfig.Proxy, pgconfig.Mirroring,
]
```

This defines a new type with the shared fields, so `cfg.Databases` and
`AppConfig{Databases: ...}` still work directly. Your type can have its own methods;
it does not inherit `Config.ValidateCredentials`. Here, only the
database type differs. The other sections keep their library types.

Embedding `pgconfig.Database` would also accept an inline `password`, which this
application does not want. Embed `DatabaseSettings` to share the common fields
while defining your own credential input.

For other field replacements, Go's JSON decoder lets an outer field hide an
embedded one, but YAML's inline handling rejects duplicate keys. This also
applies to embedding `pgconfig.Config` and redeclaring `Databases`. Using
`ConfigSections` selects the field type directly and avoids that ambiguity.
Embedding still works for adding fields whose names do not overlap.

`DatabaseSettings`, `UserSettings`, and `ProxySettings` separate shared fields from credential and certificate providers,
so you can embed those without inheriting password fields. `LogicalDatabaseSettings`
lets you substitute the read-cluster type when you need different routing settings.

Once the file is decoded, resolve each password and build the library value.
This method uses the standard `os` and `errors` packages:

```go
func (c AppConfig) LibraryConfig() (pgconfig.Config, error) {
    result := pgconfig.Config{
        Users: c.Users, Clusters: c.Clusters,
        LogicalDatabases: c.LogicalDatabases,
        Proxy: c.Proxy, Mirroring: c.Mirroring,
        Databases: make([]pgconfig.Database, len(c.Databases)),
    }
    for i, database := range c.Databases {
        password, ok := os.LookupEnv(database.Credentials.PasswordEnv)
        if !ok {
            return pgconfig.Config{}, errors.New("database password environment variable is not set")
        }
        result.Databases[i] = pgconfig.Database{
            DatabaseSettings: database.DatabaseSettings,
            Username: database.Username,
            Password: pgconfig.Secret(password),
        }
    }
    if err := result.ValidateCredentials(); err != nil {
        return pgconfig.Config{}, err
    }
    return result, nil
}
```

The resulting databases can go straight to
[`pool.PGXConfig`](https://github.com/DataDog/pg-proxy/blob/main/pkg/pool/config.go). The pool code receives a username and
password; it does not need to know where the password came from. This conversion
reuses the maps and slices in the unchanged sections, so treat the input as
immutable once you hand the result to the proxy.

## Reading your format

Decode into `AppConfig` with `pgconfig.ReadJSONAs[AppConfig]` or
`pgconfig.ReadYAMLAs[AppConfig]`. They apply the same checks as the library's
`ReadJSON` and `ReadYAML`: unknown fields are rejected, anything after the first
document is rejected, and decode errors report positions and field paths but
not field values, which could contain a misplaced password. `ReadJSON` and `ReadYAML` themselves read the
library type, so they would reject `password_env`. After decoding, call
`LibraryConfig()` to resolve credentials and validate the library value.

Credential validation does not check routes or listener options. Your application
still needs to check the settings it uses and report an error for anything it
cannot support. The unchanged `users` section in this example retains the
library's password and SCRAM verifier support; the environment-variable rule
applies only to database passwords.

Pass the converted structs directly to the runtime. `Secret` redacts passwords
when config is serialized, so a JSON or YAML round trip would lose the password
you just resolved. Convert again when you reload the file or refresh credentials;
changing an environment variable alone does not update an existing pool.

This custom format belongs to your application. The
[`pg-proxy` binary](../../../how-to/local-development/) uses the same pattern for its own
[credential references](../client-authentication/#credential-references):
it reads `password_env` and similar fields next to `password`, not in a
`credentials` section, so it will not read the format in this example. See
`cmd/pg-proxy/credentials.go` for a complete reader.

## When the library itself should change

If the new field or behavior would be useful to other proxy applications, add it
to the relevant struct in [`config.go`](https://github.com/DataDog/pg-proxy/blob/main/pkg/config/config.go) and connect it
to the code that uses it. Keep existing names and defaults so old files keep
working. Decide what omitted and zero values mean; `ReadCluster.Weight` uses a
pointer because those cases have different meanings.

For an application-only format like the one above, the library needs no change.
Test that JSON and YAML produce the same library config, that old or misspelled
credential fields are rejected, and that missing credentials fail without
printing their values. If you change the library format, also cover old files
with the field omitted. Its strict readers reject unknown fields, so deploy a
reader that understands a new field before adding that field to config files.
