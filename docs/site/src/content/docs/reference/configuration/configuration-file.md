---
# Unless explicitly stated otherwise all files in this repository are licensed
# under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/)
# Copyright 2026-Present Datadog, Inc.
title: "Configuration file"
description: "Every key the pg-proxy binary reads from its YAML or JSON configuration file, with defaults and validation rules."
---

The `pg-proxy` binary reads one configuration file, named with `-config`. The file extension selects the format: `.yaml` or `.yml` for YAML, `.json` for JSON. The binary reads the file once at startup; restart it to apply changes.

YAML keys use `snake_case`. JSON keys use `camelCase`; the JSON name is in each table. Unknown keys fail startup in both formats.

This page covers the binary. Applications that embed the Go library decode the same schema with `pkg/config`; see [Go library configuration](../library/).

## Minimal example

```yaml
databases:
  - name: primary
    host: 127.0.0.1
    port: 15432
    dbname: postgres
    username: postgres
    password: postgres
    sslmode: disable
clusters:
  local:
    primary: primary
logical_databases:
  local_db:
    cluster: local
```

This matches [Local development](../../../how-to/local-development/): PostgreSQL listens on `127.0.0.1:15432`, and the proxy uses its default address, `127.0.0.1:5432`. If your PostgreSQL server uses `127.0.0.1:5432`, set `proxy.listener_address` to a free port.

`databases`, `clusters`, and `logical_databases` are required and must not be empty. `proxy` is optional. `users` is allowed only with password authentication.

## How the sections connect

Clients connect to a **logical database** by name. Each logical database points to a **cluster**. Each cluster names one primary **database** and optional replica databases. Each database entry describes one PostgreSQL server and its backend connection pool.

Every database must be the primary or a replica of at least one cluster.

## `databases`

A list. Each entry opens one backend connection pool for each cluster that uses it.

| YAML key | JSON key | Default | Description |
| --- | --- | --- | --- |
| `name` | `name` | Required | Identifier that clusters refer to. Must be unique. |
| `host` | `host` | Required | PostgreSQL host name or IP address. |
| `port` | `port` | `5432` | PostgreSQL port, `1` to `65535`. |
| `dbname` | `dbName` | Required | Physical database name on the server. |
| `username` | `username` | Required | Backend role. In trust mode, clients must connect with this same user name. |
| `password` | `password` | None | Backend password. Set at most one of `password`, `password_file`, and `password_env`. |
| `password_file` | `passwordFile` | None | Read the backend password from this file at startup. See [credential references](../client-authentication/#credential-references). |
| `password_env` | `passwordEnv` | None | Read the backend password from this environment variable at startup. |
| `sslmode` | `sslMode` | pgx default (`prefer`) | Backend TLS mode, with the same values as libpq: `disable`, `allow`, `prefer`, `require`, `verify-ca`, `verify-full`. |
| `max_conns` | `maxConns` | pgx default: the greater of 4 and the number of CPUs | Maximum connections in this pool. |
| `min_conns` | `minConns` | `0` | Connections the pool keeps open. Must not exceed `max_conns`. |
| `min_idle_conns` | `minIdleConns` | `0` | Idle connections the pool keeps open. Must not exceed `max_conns`. |
| `max_conn_lifetime` | `maxConnLifetime` | pgx default (`1h`); `15m` for replicas | Close a backend connection after this age. |
| `max_conn_lifetime_jitter` | `maxConnLifetimeJitter` | `0`; `5m` for replicas | Random extra lifetime, so connections do not all close at once. |
| `connection_options` | `connectionOptions` | Inherit `PGOPTIONS` | Map of PostgreSQL settings sent as `-c key=value` in the startup `options` parameter, for example `statement_timeout: 30s`. An empty map clears `PGOPTIONS`. |
| `runtime_params` | `runtimeParams` | None | Map of startup parameters sent directly, for example `search_path`. |
| `application_name` | `applicationName` | None | Backend `application_name`. Clients cannot set it through the proxy. |

Negative pool values fail startup.

**Durations.** In YAML, write durations as Go duration strings such as `15m` or `1h30m`. In JSON, `maxConnLifetime` and `maxConnLifetimeJitter` are integers in **nanoseconds**: `900000000000` is 15 minutes. A string such as `"15m"` fails startup in JSON.

**PostgreSQL environment variables.** Backend connections are made with pgx, which reads libpq environment variables such as `PGSSLROOTCERT`, `PGSSLMODE`, and `PGOPTIONS` for settings the file does not set. For example, there is no file key for a backend CA certificate; set `PGSSLROOTCERT` for a private CA with `sslmode: verify-full`.

**Not supported by the binary.** `postgres_host`, `route_through_envoy`, and `load_balanced_servers` are database-discovery settings for embedding applications. The binary fails at startup if they are set.

## `clusters`

A map from cluster name to cluster.

| YAML key | JSON key | Default | Description |
| --- | --- | --- | --- |
| `primary` | `primary` | Required | Name of the primary database entry. |
| `replicas` | `replicas` | None | Names of replica database entries. Each must be a configured database, listed once, and different from `primary`. See [Read routing](../../../explanation/read-routing/). |

Each cluster opens its own pool for each database it uses. A database that two clusters share can use up to `max_conns` connections in each cluster's pool.

## `logical_databases`

A map from the name clients connect to, to its routing.

| YAML key | JSON key | Default | Description |
| --- | --- | --- | --- |
| `cluster` | `cluster` | Required | Cluster that serves this logical database. |
| `read_clusters` | `readClusters` | None | Map from cluster name to `{weight: N}`. `weight` is `0` to `255` and defaults to `1`. Any entry, including weight `0`, turns on read routing. See [Read routing](../../../explanation/read-routing/). |

## `proxy`

| YAML key | JSON key | Default | Description |
| --- | --- | --- | --- |
| `listener_address` | `listenerAddress` | `127.0.0.1:5432` | IP address and port to listen on. Without password authentication, only loopback addresses are accepted. Host names are always rejected. |
| `password_auth_enabled` | `passwordAuthEnabled` | `false` | Turn on password/SCRAM authentication for the users in `users`. Requires `tls_required: true` and a certificate and key. See [Client authentication](../client-authentication/). |
| `tls_cert_file` | `tlsCertFile` | None | PEM server certificate for client TLS. Set together with `tls_key_file`. See [Client TLS](../client-tls/). |
| `tls_key_file` | `tlsKeyFile` | None | Unencrypted PEM private key for `tls_cert_file`. |
| `tls_required` | `tlsRequired` | `false` | Reject client startup without TLS. Requires `tls_cert_file` and `tls_key_file`. |
| `max_sessions` | `maxSessions` | `100000` | Maximum client sessions for the whole process. Zero uses the default. |
| `default_max_user_sessions` | `defaultMaxUserSessions` | `10000` | Maximum client sessions for each client user. Zero uses the default. |
| `shutdown_grace_period` | `shutdownGracePeriod` | `30s` | Time allowed for sessions and pools to close after `SIGINT` or `SIGTERM`. A positive Go duration string in both YAML and JSON. See [Graceful shutdown](../../../explanation/graceful-shutdown/). |
| `postgres_msg_buffer_size` | `postgresMsgBufferSize` | `10240` | Bytes of PostgreSQL replies the proxy buffers before it flushes them to the client. |
| `client_disconnect_timer` | `clientDisconnectTimer` | `false` | How each session checks for a client that disconnects during a backend checkout. `false` starts a goroutine and a ticker for each checkout. `true` keeps one timer for the session and arms it for each checkout. Both check every 100 ms and act the same way on a disconnect. |
| `reply_buffer_pool` | `replyBufferPool` | `false` | `true` lets a reply of 512 bytes or more continue in a buffer from a pool that all sessions share, so that replies of many rows do not allocate a new buffer each time. `false` keeps one buffer of at most 1 KiB for each session, and a larger reply grows its own buffer. |
| `jwt_auth_enabled` | `jwtAuthEnabled` | `false` | Not supported by the binary; `true` fails startup. |

Negative session limits fail startup. Session limits are separate from backend pool sizes; see [Set connection and session limits](../../../how-to/set-pool-limits/).

## `users`

Client users for password authentication. Allowed only when `proxy.password_auth_enabled` is `true`.

| YAML key | JSON key | Default | Description |
| --- | --- | --- | --- |
| `static_credentials.username` | `staticCredentials.username` | Required | Client user name, up to 256 bytes. Must be unique. Matched exactly, including case. |
| `static_credentials.password` | `staticCredentials.password` | None | Client password. The binary converts it to a SCRAM verifier at startup. |
| `static_credentials.password_file` | `staticCredentials.passwordFile` | None | Read the client password from a file. |
| `static_credentials.password_env` | `staticCredentials.passwordEnv` | None | Read the client password from an environment variable. |
| `static_credentials.scram_sha_256` | `staticCredentials.scramSHA256` | None | Stored SCRAM-SHA-256 verifier. See [Credentials](../client-authentication/#credentials). |
| `static_credentials.scram_sha_256_file` | `staticCredentials.scramSHA256File` | None | Read the verifier from a file. |
| `static_credentials.scram_sha_256_env` | `staticCredentials.scramSHA256Env` | None | Read the verifier from an environment variable. |
| `databases` | `databases` | None | Logical databases this user may connect to. Each must be a configured logical database. A user with no databases cannot connect. |
| `max_sessions` | `maxSessions` | `proxy.default_max_user_sessions` | Session cap for this user across all its logical databases. |

Set exactly one password or verifier source for each user. `email`, `group`, and `pg_roles` are not supported by the binary and fail startup.

## `mirroring`

Traffic mirroring is available only to applications that embed the library. The binary fails at startup if the file has a `mirroring` section. See [Traffic mirroring](../../../explanation/traffic-mirroring/).
