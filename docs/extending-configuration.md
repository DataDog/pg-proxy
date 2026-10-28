# Adapting configuration to your application

You can change an existing field; you do not have to add another section. If you
just want a different value, set it on the decoded config before building the
proxy. For example, `cfg.Databases[0].MaxConns = 20` changes that database's pool
limit. Changing the struct after creating a pool does not reconfigure the pool.

Changing a field's type or its meaning in a config file takes a little more
work. The library's [`Config`](../pkg/config/config.go) has concrete Go types,
so your application needs its own type for the part that differs, then a small
conversion into the library's config. The rest can use the library types as-is.

## A different way to supply database credentials

Suppose your deployment puts database passwords in environment variables. You
want each database to name the variable it uses, in the existing `credentials`
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

The library normally expects `credentials` to contain `name` and `revision`.
Here, you are replacing that field's format with `password_env`. The connection
and pool fields still mean the same thing.

In your application, define the database type you want to read. In these snippets,
`pgconfig` is this library's [`pkg/config`](../pkg/config) package:

```go
type Database struct {
    Name                string `json:"name" yaml:"name"`
    pgconfig.Connection `yaml:",inline"`
    pgconfig.PoolConfig `yaml:",inline"`
    Username            string      `json:"username" yaml:"username"`
    Credentials         Credentials `json:"credentials" yaml:"credentials"`
}

type Credentials struct {
    PasswordEnv string `json:"password_env" yaml:"password_env"`
}
```

This example supports direct database connections. It deliberately lists the
fields it accepts, so a strict reader will reject inline `password` fields and
settings such as `load_balanced_servers`. If your application needs those fields,
include them and carry them through the conversion too.

The containing config must also use your database type. Keep the other sections
unchanged by referring to their library types:

```go
type AppConfig struct {
    Databases        []Database                           `json:"databases" yaml:"databases"`
    Users            []pgconfig.User                      `json:"users" yaml:"users"`
    Clusters         map[string]pgconfig.Cluster          `json:"clusters" yaml:"clusters"`
    LogicalDatabases map[string]pgconfig.LogicalDatabase  `json:"logicalDatabases" yaml:"logical_databases"`
    Proxy            pgconfig.Proxy                      `json:"proxy" yaml:"proxy"`
    Mirroring        *pgconfig.Mirroring                  `json:"mirroring" yaml:"mirroring"`
}
```

Why not embed `pgconfig.Database` and declare another `Credentials` field? Go's
JSON decoder lets the outer field hide the embedded one, but YAML's inline
handling rejects duplicate keys in the type. Embedding `pgconfig.Config` and
redeclaring `Databases` has the same problem. Listing the containing fields
explicitly makes both formats behave the same way. Embedding still works for
adding fields whose names do not overlap, or for reusing `Connection` and
`PoolConfig` as above. If you want all common database fields, embed
`pgconfig.DatabaseSettings` instead. It includes discovery settings but has no
username, password, or credential-reference fields, so your application can
supply its own credential type without a JSON/YAML field conflict.

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
            DatabaseSettings: pgconfig.DatabaseSettings{
                Name: database.Name, Connection: database.Connection,
                PoolConfig: database.PoolConfig,
            },
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
[`pool.PGXConfig`](../pkg/pool/config.go). The pool code receives a username and
password; it does not need to know where the password came from. This conversion
reuses the maps and slices in the unchanged sections, so treat the input as
immutable once you hand the result to the proxy.

## Reading your format

Use your own reader to decode into `AppConfig`. The library's `ReadJSON` and
`ReadYAML` read the library type and will reject `password_env`. Keep the same
checks as the [library readers](../pkg/config/json.go): use
`DisallowUnknownFields()` for JSON or `KnownFields(true)` for YAML, and reject
anything after the first document. Return fixed decode errors, since parser
errors can include input values that contain a misplaced password. After decoding,
call `LibraryConfig()` to resolve credentials and validate the library value.

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
[OSS binary](../README.md#oss-binary) accepts the library's
format and supports direct primary backends. It will not understand
`password_env` without its own reader and conversion.

## When the library itself should change

If the new field or behavior would be useful to other proxy applications, add it
to the relevant struct in [`config.go`](../pkg/config/config.go) and connect it
to the code that uses it. Keep existing names and defaults so old files keep
working. Decide what omitted and zero values mean; `ReadCluster.Weight` uses a
pointer because those cases have different meanings.

For an application-only format like the one above, the library needs no change.
Test that JSON and YAML produce the same library config, that old or misspelled
credential fields are rejected, and that missing credentials fail without
printing their values. If you change the library format, also cover old files
with the field omitted. Its strict readers reject unknown fields, so deploy a
reader that understands a new field before adding that field to config files.
