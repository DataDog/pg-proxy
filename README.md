# pg-proxy

A PostgreSQL wire-protocol proxy with transaction-level connection pooling.

The Go module provides an embeddable library and a development reference command
under `cmd/pg-proxy`. Embedders supply configuration, authentication, discovery,
feature flags, and runtime integrations through Go interfaces.

**Status: experimental.** The API is not stable. The reference command is for
local development and is not a production server.

## Build and test

Requirements:

- Go 1.26 or newer.
- A C compiler with CGO enabled. The SQL parser uses `pg_query_go`.
- Docker for integration tests. Unit tests do not require Docker.

From the repository root:

```sh
go build ./...
go test ./...
go test -race ./...
go vet ./...
go test -tags=integration ./...
```

Integration tests use the public PostgreSQL 14 image pinned by digest in the test
sources and Testcontainers' versioned public Ryuk image. They fail if Docker is
unavailable. Docker tests cover multiple backends and usernames, transaction
pooling, startup cleanup, cancellation, shutdown, and SQL health-check connection
reuse.

The repository uses standard Go tooling and public dependencies. Bazel and a
Datadog account are not required to build supplied source. While the repository
has internal visibility, obtaining its source still requires repository access.

## Continuous integration

[The CI workflow](.github/workflows/ci.yml) runs on pull requests to `main`,
pushes to `main`, and pushes to the initial rehearsal branch. It also supports
manual runs after the workflow reaches the default branch.

| Job | Checks |
| --- | --- |
| Go (minimum) | Exact Go version from `go.mod`; formatting, module tidiness, build, vet, and unit tests |
| Go (stable) | Current stable Go release; build, vet, and unit tests with race detection |
| PostgreSQL integration | Current stable Go release; Docker-backed tests using the pinned public PostgreSQL image |

Jobs use GitHub-hosted Ubuntu 24.04 runners, CGO, public module downloads, and a
read-only repository token. Actions are pinned to commit SHAs. Go automatically
upgrading its toolchain is disabled so the minimum-version check remains exact.
No Datadog credentials are required. Module and build caches use `go.sum`.

## Packages

| Package | Role |
| --- | --- |
| `.` (`pgproxy`) | Process-wide `Bootstrap` / `Dependencies` (Logger, Metrics) |
| `pkg/actions` | Portable frontend-message decision state machine |
| `pkg/backend` | Within-cluster routing, connection checkout, and session-link construction |
| `pkg/config` | Portable database, routing, listener, user, and mirroring configuration |
| `pkg/classifier` | SQL read-routing classification and statement counting |
| `pkg/protocol` | Wire-protocol conversations (`Exchange`, cleartext) |
| `pkg/scram` | SCRAM-SHA-256 authentication |
| `pkg/server` | Startup resolution, read routing, session construction, and listener/inventory composition |
| `pkg/query` | Post-auth message decisions and query tracing |
| `pkg/prepared` | Prepared-statement name mapping for txn pooling |
| `pkg/pool` | Direct, discovered (PGK), static cloud, and replica pool runtimes; instance health, selection, and pgx adapters |
| `pkg/link` | Session↔Postgres link: expectations, prepared-on-conn, release |
| `pkg/limiter` | Portable global and per-service session caps |
| `pkg/listener` | TCP/TLS negotiation, startup, admission, and session lifecycle |
| `pkg/mirror` | Bounded asynchronous traffic-mirroring queue and worker |
| `pkg/session` | Client session protocol engine, routing, draining, and lifecycle |
| `pkg/topology` | Portable logical-database routing and runtime configuration |
| `pkg/warden` | Versioned configuration refresh, backend reconciliation, runtime inventory, and shutdown coordination |

The root package is importable as `github.com/DataDog/pg-proxy`. Packages remain
under `pkg/` to preserve embedding paths. Authentication mechanisms receive
credentials from the embedder; credential storage stays with the application.

## Configuration

Start with [`pkg/config/config.go`](pkg/config/config.go) and the
[config guide](pkg/config/README.md). For new settings, follow the
[configuration extension guide](docs/extending-configuration.md).
The OSS binary consumes a limited subset, described below. Applications can
use `pool.PGXConfig(config.Database)` to build a direct backend pool configuration;
resolve credential references in your application first.

## Client TLS

When `listener.Config.RequireTLS` (or `server.Config.RequireTLS`) is true, a
non-nil `TLSConfig` is required before the listener binds. Normal startup requires
a completed TLS handshake before database resolution, authentication, or session
admission. Plaintext startup receives `FATAL 08006: TLS is required` and closes.
Certificate callbacks (`GetCertificate` and `GetConfigForClient`) are supported;
certificates need not be loaded when the listener is constructed.

`CancelRequest` remains allowed on a separate plaintext connection, even when TLS
is required, for compatibility with existing PostgreSQL clients. Cancellation
still requires the session's PID and secret key. Cancellation over TLS also works.
With `RequireTLS=false`, both optional TLS and plaintext startup remain supported.

The OSS binary continues to use loopback trust without TLS.

## Local reference command

`cmd/pg-proxy` runs the library against configured PostgreSQL backends, with
static routing and transaction pooling. Client connections use **local trust
without TLS**: the command accepts only loopback IP listen addresses and the
selected backend's username. Any local process using that username can access the
backend with its configured privileges. Use a disposable development database.
This is not a production server.

With PostgreSQL running locally, save this configuration as `proxy.yaml`:

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
  local: {primary: primary}
logical_databases:
  local_db: {cluster: local}
proxy:
  listener_address: '127.0.0.1:15432'
  postgres_msg_buffer_size: 10240
```

Build and start the command:

```sh
go build -o bin/pg-proxy ./cmd/pg-proxy
./bin/pg-proxy -config proxy.yaml
```

In another terminal:

```sh
psql 'postgres://postgres@127.0.0.1:15432/local_db?sslmode=disable' -c 'select 1'
```

Add entries to `databases`, `clusters`, and `logical_databases` for more backends.
Each cluster selects one primary database. Each logical database selects a cluster.
Multiple logical databases can select the same cluster.

The command accepts direct connection settings, inline backend credentials, pool
sizing/lifetimes, a loopback listener address, and the message buffer size.
Omitted listener and buffer settings use `127.0.0.1:15432` and 10240 bytes. Zero
pool settings use pgx defaults; negative values and minimum connections above the
maximum are rejected. Backend connection parsing retains pgx environment defaults.
`application_name` in connection options is overridden by a nonempty
`application_name` database field.

File mode rejects users/JWT, listener TLS options, mirroring sections, replicas,
read clusters, credential references, and discovery/Envoy/load-balanced database
options with explicit startup errors. Backend TLS is available through the
database's `sslmode` setting.

`-config` is required and accepts `.yaml`, `.yml`, or `.json` files. All database
and listener settings come from that file. There are no separate backend,
listener, database, or session-limit flags, and `PG_PROXY_BACKEND_URL` is not
read. The session limit remains 100. File changes require a restart.

Role passthrough is rejected; all queries use the backend user. Query
cancellation is supported. SIGINT/SIGTERM stop acceptance and allow up to five
seconds for sessions and pools to close. An unfinished transaction can exhaust
that deadline, causing a nonzero exit.
