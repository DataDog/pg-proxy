---
title: "Local development"
description: "Build, run, and test the OSS binary from its repository."
---

The OSS binary and shared library live in [DataDog/pg-proxy](https://github.com/DataDog/pg-proxy). Build and test them from that repository.

You need Go 1.26 or newer, a C compiler with CGO enabled, and `psql` on your PATH. The examples use a disposable PostgreSQL database at `127.0.0.1:5432`, with database, username, and password all set to `postgres`. Port `15432` must be free. Adapt the backend settings to your local database.

## 1. Build the local executable

Build the OSS binary from `main`:

```sh
git clone https://github.com/DataDog/pg-proxy.git
cd pg-proxy
go build -o bin/pg-proxy ./cmd/pg-proxy
```

## 2. Run against a development database

With your disposable PostgreSQL database running, save this as `proxy.yaml` in your `pg-proxy` checkout:

```yaml
databases:
  - name: primary
    host: 127.0.0.1
    port: 5432
    dbname: postgres
    username: postgres
    password: postgres
    sslmode: disable
    max_conns: 10
clusters:
  local:
    primary: primary
logical_databases:
  local_db:
    cluster: local
proxy:
  listener_address: '127.0.0.1:15432'
  postgres_msg_buffer_size: 10240
```

Validate the file before starting the proxy:

```sh
./bin/pg-proxy -config proxy.yaml -validate-config
```

This checks syntax, field types, required settings, routing references, pool bounds,
authentication policy, and local TLS files. It exits with status 0 on success or 1
on a configuration error. Errors include field paths and source locations when
available. Validation does not connect to PostgreSQL, open a listener, or start
telemetry, so database access and port availability still need a runtime check.

Start the OSS binary:

```sh
./bin/pg-proxy \
  -config proxy.yaml -telemetry none
```

Wait for `local proxy ready; clients are trusted`. The executable listens on loopback and trusts the configured username without client TLS. The [settings table](#settings) below lists its options and defaults.

### Connect a client

In another terminal, connect to the proxy's address and logical database:

```sh
psql 'postgres://postgres@127.0.0.1:15432/local_db?sslmode=disable' \
  -At -v ON_ERROR_STOP=1 -c 'select 1'
```

The result should be `1`. The client username must match the backend username. Use a name from `logical_databases` for the client connection (`local_db` here), even if the backend `dbname` names a different physical database.

See [Client connection options](../../reference/configuration/client-connections/) for the `pg_proxy.*` session options and connection examples.

## 3. Format Go changes

Format changed Go files with `gofmt -w`.

## 4. Run the checks

From the `pg-proxy` repository root:

```sh
go build ./...
go vet ./...
go test -race -count=1 ./...
```

These commands build the code and run the unit tests. Docker-backed integration tests need the `integration` build tag and a running Docker daemon; they pull public PostgreSQL images pinned by digest:

```sh
go test -race -tags=integration -count=1 ./...
```

## 5. Stop the local proxy

Close test clients and press `Ctrl+C` in the proxy terminal. SIGINT and SIGTERM stop acceptance and allow up to five seconds for sessions and pools to close. An unfinished transaction can exhaust that deadline and cause a nonzero exit. Startup or runtime errors also cause a nonzero exit. Query cancellation is supported.

For Datadog and OTel configuration, see [Telemetry configuration](../../reference/observability/).

For website changes, follow [Contribute to the documentation](../documentation-contributing/).

## Settings

The OSS binary requires `-config` pointing to a `.yaml`, `.yml`, or `.json` file. File changes require a restart. The file uses the portable [configuration schema](https://github.com/DataDog/pg-proxy/blob/main/pkg/config/README.md); the OSS binary reads the subset described here.

| Setting | Default | Meaning |
| --- | --- | --- |
| `proxy.listener_address` | `127.0.0.1:15432` | Loopback IP address and port. Non-loopback addresses and hostnames are rejected. |
| `proxy.postgres_msg_buffer_size` | `10240` | PostgreSQL message buffer size in bytes. |
| `databases[].max_conns` | pgx default when zero or omitted | Maximum connections in a backend pool; distinct from the client-session cap. |
| `logical_databases` | Required | Maps client database names to clusters; each cluster selects a primary database. |
| `-validate-config` | `false` | Run local configuration checks and exit without starting the proxy. |
| `-telemetry` | Independent `OTEL_*_EXPORTER` selection | `datadog`, `otel`, or `none`; see [Telemetry configuration](../../reference/observability/). |
| `-tracing` | `OTEL_TRACES_EXPORTER` selection | `otel` or `none`; overrides the trace exporter selector and cannot be combined with `-telemetry`. |
| `-trace-parameters` | `false` | Set to `true` to capture raw bind parameter values in traces. SQL text is recorded regardless. |
| `-log-level` | `INFO` | Minimum severity for proxy logs on stderr and through OTLP. Use `DEBUG` for local diagnostics. |

The client-session cap is currently fixed at 100. There are no separate backend URL, listener, logical-database, or session-limit flags. `PG_PROXY_BACKEND_URL` is not read.

### Backend connection settings

Use direct database settings and inline backend credentials in the file. `sslmode` configures backend TLS; pool sizing and lifetime fields configure pgx pools; `connection_options` sets backend startup parameters such as `statement_timeout`. The OSS binary rejects replicas, read clusters, credential references, discovery settings, JWT authentication, and mirroring with explicit startup errors. See [Go library configuration](../../reference/configuration/library/) for embedding capabilities beyond this command.

### Client authentication

By default, the local executable uses local trust without client TLS. It accepts only loopback listen addresses and clients whose username matches the backend username. All queries run as the backend user.

Any local process using that username can access the backend with its configured privileges. Use this default only with disposable local development databases.

Opt-in password/SCRAM authentication requires client TLS and allows non-loopback listeners with explicit per-user database access. See the [client authentication guide](https://github.com/DataDog/pg-proxy/blob/main/docs/client-authentication.md). Set `proxy.tls_cert_file` and `proxy.tls_key_file` to enable client TLS, and `proxy.tls_required: true` to reject plaintext startup; see the [client TLS guide](https://github.com/DataDog/pg-proxy/blob/main/docs/client-connections.md#standalone-tls).
