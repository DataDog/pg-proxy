---
# Unless explicitly stated otherwise all files in this repository are licensed
# under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/)
# Copyright 2026-Present Datadog, Inc.
title: "Troubleshooting"
description: "Common pg-proxy startup and connection errors, what they mean, and how to fix them."
---

## Where to look

`pg-proxy` logs to stderr. Use `-log-level DEBUG` for more detail.

Clients get short, generic errors on purpose, so they cannot probe user names or configuration. The proxy log has the reason. Each failed connection logs one line with the client address, the `stage` where it failed, and a `reason`:

```text
level=ERROR msg="failed to open connection from 127.0.0.1:36676: logical database is not configured in this pg-proxy" requested_user=postgres requested_database=nope stage=resolve_startup reason=logical_database_not_configured
```

## The proxy does not start

Run `pg-proxy -config proxy.yaml -validate-config` first. It reports every configuration error it can find, one per line, with the field path and the line in the file, and exits with status `1` if there are any. It does not connect to PostgreSQL.

At startup, the same errors are printed after `invalid configuration:` and the process exits with status `1` before it opens any pool or the listener.

| Error | Fix |
| --- | --- |
| `-config is required` | Pass `-config path/to/proxy.yaml`. |
| `could not open configuration file "…": …` | Check the path and file permissions. Relative paths are resolved from the working directory. |
| `configuration file must use .json, .yaml, or .yml` | Rename the file. The extension selects the format. |
| `configuration YAML at line N, column M (path): …` or `configuration JSON at …` | The value at that field has the wrong type, or the key is misspelled. In JSON, `maxConnLifetime` and `maxConnLifetimeJitter` are integers in nanoseconds, not strings like `"15m"`. See [Configuration file](../configuration/configuration-file/). |
| `databases must not be empty` (or `clusters`, `logical_databases`) | All three sections are required. |
| `each database must be used by a cluster` | Remove the unused database entry, or add it to a cluster. |
| `databases[0]: database host is required` (or `dbname`, `username`) | Set all three on every database. |
| `proxy.listener_address must use a loopback IP unless client authentication is enabled` | Use `127.0.0.1` or `[::1]`, or turn on [password authentication](../configuration/client-authentication/). |
| `proxy.listener_address must use a loopback IP and port in trust mode` | Use an IP address, not a host name such as `localhost`. |
| `client authentication requires proxy.tls_required and a TLS certificate/key pair` | Password authentication needs `tls_required: true`, `tls_cert_file`, and `tls_key_file`. See [Client TLS](../configuration/client-tls/). |
| `configured users require proxy.password_auth_enabled` | Set `proxy.password_auth_enabled: true`, or remove `users`. |
| `direct pools do not support postgres_host, route_through_envoy, or load_balanced_servers` | Remove these keys. The binary connects directly to `host`. |
| `mirroring configuration is not implemented…`, `JWT authentication is not implemented…` | These features are available only in the Go library. Remove the `mirroring` section or `jwt_auth_enabled`. |

After the configuration is valid, the proxy opens a connection to every backend. If one cannot be reached, it logs `pg-proxy stopped` with the connection error and exits with status `1`. Check that PostgreSQL is running and that `host`, `port`, `username`, password, and `sslmode` are right.

## A client cannot connect

| Client sees | Log `reason` | Cause and fix |
| --- | --- | --- |
| `FATAL: could not establish connection` | `logical_database_not_configured` | The database name in the connection string is not in `logical_databases`. Clients connect to the logical name, not the backend `dbname`. |
| `FATAL: could not establish connection` | `startup_failure`, at `stage=authenticate` | Without password authentication: the client user is not the backend `username`. Connect as that user. |
| `FATAL: could not establish connection` | `invalid_credentials` | Wrong password, unknown user, or a logical database that is not in the user's `databases` list. |
| `FATAL: TLS is required` | `tls_required` | `tls_required` is on and the client connected without TLS. Use `sslmode=require` or `verify-full`. |
| `FATAL: pg-proxy is at capacity. Please retry.` (SQLSTATE `53300`) | Not logged | A session limit was reached: `proxy.max_sessions`, `proxy.default_max_user_sessions`, or the user's `max_sessions`. See [Set connection and session limits](../../how-to/set-pool-limits/). |
| Connection refused | None | The proxy is not running, or listens on another address. Check `proxy.listener_address` (default `127.0.0.1:5432`). |
| Proxy exits at startup with `address already in use` | None | Another process, often a local PostgreSQL server, already listens on `proxy.listener_address`. The default `127.0.0.1:5432` is the standard PostgreSQL port; set `proxy.listener_address` to a free port. |
| Connection closes after about 10 seconds | Varies | The client did not finish TLS, startup, and authentication within 10 seconds. |

## Queries are slow or wait

- **Waiting for a backend connection.** When all `max_conns` connections in a pool are in use, new transactions wait for one. Raise `max_conns`, or find long transactions with `pg_stat_activity` on the backend. See [Transaction pooling](../../explanation/pooling/).
- **Idle transactions.** A client that opens a transaction and does not finish it holds a backend connection. Set `idle_in_transaction_session_timeout` with `connection_options`; see [Timeouts](../configuration/timeouts/).

## Shutdown takes too long

After `SIGINT` or `SIGTERM`, the proxy waits for open transactions to finish, for up to `proxy.shutdown_grace_period` (default `30s`). If the time runs out, it logs `shutdown timed out waiting for client sessions` and exits with status `1`. See [Graceful shutdown](../../explanation/graceful-shutdown/).

## Startup warning about the mirror budget

Every start logs `mirror budget memory hard limit unavailable; using fallback` when the process has no cgroup memory limit. The binary does not support mirroring, so you can ignore this warning.
