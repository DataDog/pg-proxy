---
# Unless explicitly stated otherwise all files in this repository are licensed
# under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/)
# Copyright 2026-Present Datadog, Inc.
title: "Client connection options"
description: "PG Proxy client startup options, defaults, and connection examples."
---

Clients connect to PG Proxy using a PostgreSQL connection string. The address points to the proxy, and the database name selects a logical database configured in the proxy.

## Proxy session options

Place these arguments inside the PostgreSQL startup `options` parameter using the form `-c pg_proxy.<name>=<value>`. Separate multiple arguments with spaces. They are not standalone URI query keys.

| Option | Default | Meaning |
| --- | --- | --- |
| `pg_proxy.dynamic_routing` | `false` | Requests dynamic read routing. Effective routing also depends on configured read clusters and backend support. Configured read clusters can enable routing even when the client sends `false`. |
| `pg_proxy.auto_cached_prepared_statements` | `false` | Gives unnamed extended-protocol statements proxy-generated names, so they are cached per backend connection like named statements. Without it, the proxy parses the unnamed statement again in each transaction that uses it. Cached statements are not classified, so extended-protocol reads use the main backend. See [Read routing](../../../explanation/read-routing/#classifying-work). |

These values are read during connection startup. Reconnect to change them. A routing option does not create read replicas; configure them in the proxy configuration file. See [Read routing](../../../explanation/read-routing/).

## Connection examples

The other fields are standard PostgreSQL connection parameters. Point `host` and `port` at the proxy and set `dbname` to a configured logical database. If the startup packet omits the database name, the listener uses `user`.

These examples use the settings from [Local development](../../../how-to/local-development/). See [PostgreSQL connection parameters](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS) for standard fields and client-side behavior.

### Keyword form

```sh
psql "host=127.0.0.1 port=5432 user=postgres dbname=local_db sslmode=disable connect_timeout=5 options='-c pg_proxy.auto_cached_prepared_statements=true'" \
  -At -v ON_ERROR_STOP=1 -c 'select 1'
```

The `-c` inside `options` is part of the startup-option syntax. The final `psql -c 'select 1'` executes SQL after connecting.

### URI form

Percent-encode the spaces and equals signs in the `options` value:

```sh
psql 'postgres://postgres@127.0.0.1:5432/local_db?sslmode=disable&connect_timeout=5&options=-c%20pg_proxy.auto_cached_prepared_statements%3Dtrue' \
  -At -v ON_ERROR_STOP=1 -c 'select 1'
```

## Parsing and invalid values

- Use `true` or `false` for boolean values. Invalid boolean tokens that reach the boolean parser produce a warning and resolve to `false`.
- If a boolean option appears more than once, the last occurrence that the parser matches wins, even when its value is invalid or truncated: `-c pg_proxy.dynamic_routing=true -c pg_proxy.dynamic_routing=maybe` sets the option to `false`. An occurrence the parser does not match, such as one with an empty value, is ignored, so an earlier value stays. Avoid repeating keys.
- Boolean values can contain only ASCII letters, digits, `_`, and `$`. Parsing of a value stops at any other character. `tru-e` is read as `tru`, which is not a valid boolean, so the option is `false` and an `invalid bool` warning is logged. Truncation can also produce a valid value: `true-x` is read as `true`.
- `pg_proxy.priority` is used only by a query-level load shedder (`backend.Config.LoadShedder`) in an application that embeds the library; the connection-level `server.Config.LoadShedder` does not receive it. It is parsed differently: the proxy reads the value up to the next whitespace and uses the last occurrence, even an empty one after a valid one. If that value is not an integer from `0` (highest priority) to `5` (lowest), such as `1-5`, the proxy uses the default, `3`, and logs an `invalid priority` warning. If no occurrence has a value that starts with a letter, digit, `_`, or `$` (for example `-c pg_proxy.priority=-1`, or only an empty value), the option is ignored without that warning, and the default applies.
- Unknown matched `pg_proxy.*` keys are warned about and ignored. Nonmatching option text is not interpreted as a proxy option. Malformed percent-encoding causes startup to fail.

The parser expects the literal `-c pg_proxy.` form with a space after `-c`. Use the examples above instead of relying on other PostgreSQL option spellings.

## Client settings versus backend settings

The client connects to a logical database on the proxy. The `pg-proxy` configuration file chooses the physical PostgreSQL database and pool settings. Set `databases[].max_conns` there, not in the client connection string.

The built-in server composition consumes the user, logical database, and recognized proxy session options. It does not forward every client startup parameter to the backend. For example, sending `application_name` or an arbitrary `-c` setting does not by itself configure the backend session through this composition.

See [Prepared statements](../../../explanation/prepared-statements/) and [Read routing](../../../explanation/read-routing/) for how the corresponding session options affect queries.

See [Configuration file](../../../reference/configuration/configuration-file/) for backend and routing settings, or [Go library configuration](../library/) when constructing the library directly.
