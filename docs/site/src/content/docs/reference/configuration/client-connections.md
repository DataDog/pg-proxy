---
title: "Client connection options"
description: "PG Proxy client startup options, defaults, and connection examples."
---

Clients connect to PG Proxy using a PostgreSQL connection string. The address points to the proxy, and the database name selects a logical database configured in the proxy.

## Proxy session options

Place these arguments inside the PostgreSQL startup `options` parameter using the form `-c pg_proxy.<name>=<value>`. Separate multiple arguments with spaces. They are not standalone URI query keys.

| Option | Default | Meaning |
| --- | --- | --- |
| `pg_proxy.dynamic_routing` | `false` client request | Requests dynamic read routing. Effective routing also depends on configured read clusters and backend support. Configured read clusters can enable routing even when the client sends `false`. |
| `pg_proxy.auto_cached_prepared_statements` | `false` | Enables the session's automatic prepared-statement caching path, including unnamed statements in the extended query protocol. Cached statements are not classified, so extended-protocol reads use the main backend. See [Read routing](../../../explanation/read-routing/#classifying-work). |

These values are read during connection startup. Reconnect to change them. A routing option does not create read replicas; configure them in the proxy configuration file. See [Read routing](../../../explanation/read-routing/).

## Connection examples

The other fields are standard PostgreSQL connection parameters. Point `host` and `port` at the proxy and set `dbname` to a configured logical database. If the startup packet omits the database name, the listener uses `user`.

These examples use the settings from [Local development](../../../how-to/local-development/). See [PostgreSQL connection parameters](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS) for standard fields and client-side behavior.

### Keyword form

```sh
psql "host=127.0.0.1 port=15432 user=postgres dbname=local_db sslmode=disable connect_timeout=5 options='-c pg_proxy.auto_cached_prepared_statements=true'" \
  -At -v ON_ERROR_STOP=1 -c 'select 1'
```

The `-c` inside `options` is part of the startup-option syntax. The final `psql -c 'select 1'` executes SQL after connecting.

### URI form

Percent-encode the spaces and equals signs in the `options` value:

```sh
psql 'postgres://postgres@127.0.0.1:15432/local_db?sslmode=disable&connect_timeout=5&options=-c%20pg_proxy.auto_cached_prepared_statements%3Dtrue' \
  -At -v ON_ERROR_STOP=1 -c 'select 1'
```

## Parsing and invalid values

- Use `true` or `false` for boolean values. Invalid boolean tokens that reach the boolean parser produce a warning and resolve to `false`.
- Repeated recognized assignments generally use the last matched value. Avoid duplicate keys, particularly malformed ones, because unmatched tokens can be ignored by the parser.
- Unknown matched `pg_proxy.*` keys are warned about and ignored. Nonmatching option text is not interpreted as a proxy option. Malformed percent-encoding causes startup to fail.

The parser expects the literal `-c pg_proxy.` form with a space after `-c`. Use the examples above instead of relying on other PostgreSQL option spellings.

## Client settings versus backend settings

The client connects to a logical database on the proxy. The `pg-proxy` configuration file chooses the physical PostgreSQL database and pool settings. Set `databases[].max_conns` there, not in the client connection string.

The built-in server composition consumes the user, logical database, and recognized proxy session options. It does not forward every client startup parameter to the backend. For example, sending `application_name` or an arbitrary `-c` setting does not by itself configure the backend session through this composition.

See [Prepared statements](../../../explanation/prepared-statements/) and [Read routing](../../../explanation/read-routing/) for how the corresponding session options affect queries.

See [`pg-proxy` binary settings](../../../how-to/local-development/#settings) for backend and routing settings, or [Go library configuration](../library/) when constructing the library directly.
