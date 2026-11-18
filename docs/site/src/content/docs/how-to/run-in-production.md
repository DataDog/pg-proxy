---
# Unless explicitly stated otherwise all files in this repository are licensed
# under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/)
# Copyright 2026-Present Datadog, Inc.
title: "Run in production"
description: "Deploy the PG Proxy binary with TLS, password authentication, sizing, telemetry, and safe restarts."
---

This guide takes the `pg-proxy` binary from the [local development](../local-development/) setup to a shared or production environment. It covers client TLS and authentication, backend connections, sizing, telemetry, health checks, and restarts.

If you need features the binary does not configure, such as discovered replicas with health checks, traffic mirroring, or your own authentication, embed the library instead. See [Go library configuration](../../reference/configuration/library/).

## 1. Build the binary

There are no releases yet. Build from a known `main` commit, and record it, with Go 1.26 or newer and CGO enabled:

```sh
git clone https://github.com/DataDog/pg-proxy.git
cd pg-proxy
go build -o bin/pg-proxy ./cmd/pg-proxy
```

Record the commit you built (`git rev-parse HEAD`) so you can report it with any issue.

## 2. Get a TLS certificate

Outside loopback, the binary requires password authentication, and password authentication requires client TLS. Get a certificate whose name matches the hostname clients use, signed by a CA your clients trust.

- Both files must be PEM encoded. The private key must be unencrypted.
- The certificate file can include intermediate certificates after the server certificate.
- TLS 1.2 and 1.3 are supported.
- The binary loads the pair once at startup, so a renewed certificate needs a restart. Plan renewals with the [restart process](#7-restart-and-roll-out-changes) below.

See [client TLS](../../reference/configuration/client-tls/#pg-proxy-binary) for the full set of TLS settings.

## 3. Write the configuration

Start from this example and replace every `<...>` placeholder:

```yaml
databases:
  - name: orders-primary
    host: orders-db.internal.example.com
    port: 5432
    dbname: orders
    username: orders_backend
    password_file: /etc/pg-proxy/orders-backend-password
    sslmode: verify-full
    max_conns: 20
    connection_options:
      statement_timeout: "30s"
clusters:
  orders: {primary: orders-primary}
logical_databases:
  orders: {cluster: orders}
proxy:
  listener_address: "0.0.0.0:15432"
  password_auth_enabled: true
  tls_required: true
  shutdown_grace_period: 30s
  max_sessions: 100000
  default_max_user_sessions: 10000
  tls_cert_file: /etc/pg-proxy/server.crt
  tls_key_file: /etc/pg-proxy/server.key
users:
  - static_credentials:
      username: orders_app
      scram_sha_256: "<SCRAM verifier>"
    databases: [orders]
  - static_credentials:
      username: monitor
      scram_sha_256: "<monitor SCRAM verifier>"
    databases: [orders]
```

**Backends.** Use `sslmode: verify-full` so the proxy checks the backend's certificate. Every client of a logical database shares that database's backend account, so its PostgreSQL permissions apply to all of them. Use separate backend accounts and routes when clients need different permissions. Set server-side limits such as `statement_timeout` in `connection_options`; see [Timeouts](../../reference/configuration/timeouts/).

**Client users.** Each user lists the logical databases it can reach. Unknown users and unlisted databases are denied. Prefer `scram_sha_256` verifiers over plaintext `password` values, so the file never holds a client's actual password. The [client authentication guide](../../reference/configuration/client-authentication/#credentials) shows how to generate a verifier.

**Keep secrets out of the file.** The example reads the backend password from `password_file`. Credentials can also come from environment variables; see [credential references](../../reference/configuration/client-authentication/#credential-references). Make secret files readable only by the user that runs the proxy, for example `chmod 600`. If you keep any credentials inline, protect the configuration file the same way and keep it out of version control.

**Replicas.** To send eligible reads to replicas, list them under `clusters[].replicas` and add `read_clusters` to the logical database. The binary's replicas are static: each must accept a connection at startup, and there are no replica health checks. If a replica checkout fails, the read falls back to the primary. See [Read routing](../../explanation/read-routing/) for an example and its consistency limits.

Complete [YAML](https://github.com/DataDog/pg-proxy/blob/main/examples/password-auth.yaml) and [JSON](https://github.com/DataDog/pg-proxy/blob/main/examples/password-auth.json) examples are in the repository. The [settings table](../../reference/configuration/configuration-file/) lists the other options.

## 4. Size pools and sessions

Four limits are involved:

| Limit | Where it is set | What it caps |
| --- | --- | --- |
| `databases[].max_conns` | Configuration file | Backend connections per pool, per proxy instance |
| Global client sessions | `proxy.max_sessions` (default `100000`) | Open client sessions per proxy instance |
| Per-user client sessions | `proxy.default_max_user_sessions` (default `10000`), overridden by `users[].max_sessions` | Open sessions for one client user across logical databases |
| `max_connections` | PostgreSQL | Total connections the server accepts |

Keep `max_conns` × the number of proxy instances below PostgreSQL's `max_connections`, for each primary and replica server, and leave headroom for administrative and other connections. Size client-session limits for the proxy's memory budget and expected workload. These caps do not open backend connections or change `max_conns`.

A client that sits idle inside an open transaction keeps its backend connection. Set `idle_in_transaction_session_timeout` on the backend so stuck transactions do not hold the pool. See [Set connection and session limits](../set-pool-limits/) and [Transaction pooling](../../explanation/pooling/).

## 5. Start the proxy with telemetry

Choose Datadog or OpenTelemetry:

```sh
# Datadog Agent: traces, DogStatsD metrics, and JSON logs on stderr
DD_SERVICE=pg-proxy DD_ENV=prod DD_AGENT_HOST=127.0.0.1 \
  ./bin/pg-proxy -config /etc/pg-proxy/proxy.yaml -telemetry datadog

# OpenTelemetry: OTLP/HTTP traces, metrics, and logs
OTEL_SERVICE_NAME=pg-proxy OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318 \
  ./bin/pg-proxy -config /etc/pg-proxy/proxy.yaml -telemetry otel
```

Traces include SQL text with literal values. Restrict access to them accordingly, and leave `-trace-parameters` off unless you need bind values. See [Telemetry configuration](../../reference/observability/).

Useful signals to watch, all prefixed with `datadog.pg_proxy.`:

| Metric | Watch for |
| --- | --- |
| `messages.queries` | Traffic volume |
| `messages.latency_us` | Message-handling latency |
| `sessions.connect_wait_us` | Time clients wait for a backend connection; growth means pool pressure |
| `sessions.connections.destroyed.release_non_idle` | Connections discarded because a client left them in an unsafe state |

See [Metrics](../../reference/observability/metrics/) for more.

## 6. Add health checks

The binary has no HTTP health endpoint. Use one of these instead:

- **TCP check** on the listener port. This confirms the proxy is accepting connections.
- **Query check:** connect as the configured `monitor` user and run `select 1`. This also confirms the backend is reachable. Store the password matching its SCRAM verifier in a libpq password file with mode `0600`, and set `PGPASSFILE` to that file for unattended checks.

```sh
PGPASSFILE=/path/to/monitor.pgpass \
  psql "host=proxy.example.com port=15432 dbname=orders user=monitor sslmode=verify-full sslrootcert=/path/to/ca.crt" \
  -w -At -c 'select 1'
```

## 7. Restart and roll out changes

All configuration, credential, and certificate changes need a restart. On `SIGTERM` or `SIGINT`, the proxy stops accepting connections, drains sessions at transaction boundaries, and waits for sessions and pools to close for up to `proxy.shutdown_grace_period` (30 seconds in the example above). If cleanup exceeds that deadline, the process exits with an error and unfinished work is cut off. Omitted or empty values keep the 30-second default.

Set the grace period to cover normal transaction durations. Give your service manager or container a longer stop timeout to allow cleanup and telemetry flushing; for Kubernetes, set `terminationGracePeriodSeconds` above the proxy grace period and allow time for any pre-stop hooks.

To restart without downtime, run at least two instances behind a TCP load balancer and restart them one at a time:

1. Remove the instance from the load balancer.
2. Send `SIGTERM` and wait for the process to exit.
3. Start it with the new configuration and wait for the health check to pass.
4. Add it back, then move on to the next instance.

Clients must reconnect when a connection closes. Most driver pools do this automatically. A closed connection does not tell the client whether an in-flight write committed, so retry writes only when they are safe to repeat. See [Graceful shutdown](../../explanation/graceful-shutdown/).

**Query cancellation and load balancers.** A cancel request only works if it reaches the proxy instance that holds the session. Behind a load balancer, a cancel request opens a new connection and can land on a different instance, where it fails. Use client-IP session affinity if your clients rely on cancellation, for example `pg_cancel_backend` or a driver's query timeout.

## 8. Check compatibility before go-live

Transaction pooling changes the lifetime of session state. Session-level `SET`, session-level advisory locks, `LISTEN`, and temporary tables do not behave as they do on a dedicated connection. Review [Transaction-pooling compatibility](../../reference/compatibility/) and [Limitations](../../reference/limitations/) against your application before switching traffic.
