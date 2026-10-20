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
unavailable. Docker tests cover proxy connections, transaction pooling,
cancellation, shutdown, and SQL health-check connection reuse.

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

## Local reference command

`cmd/pg-proxy` uses static routing and transaction pooling against one PostgreSQL
backend. Client connections use **local trust without TLS**. The command accepts
only loopback IP listen addresses and the configured backend username. Any local
process using that username can access the backend with its configured privileges.
Use a disposable development database.

With PostgreSQL already running locally:

```sh
go build -o bin/pg-proxy ./cmd/pg-proxy
./bin/pg-proxy \
  -backend-url 'postgres://postgres:postgres@127.0.0.1:5432/postgres?sslmode=disable&pool_max_conns=10'
```

In another terminal:

```sh
psql 'postgres://postgres@127.0.0.1:15432/local_db?sslmode=disable' -c 'select 1'
```

Supply `-backend-url` or `PG_PROXY_BACKEND_URL`; a nonempty flag takes precedence.
Backend TLS and pool settings use pgx connection string options. Other flags are
`-listen` (default `127.0.0.1:15432`), `-database`
(default `local_db`), and `-max-sessions` (default `100`). The logical database
maps to the physical database in the backend URL. Role passthrough is rejected;
all queries use the backend user. Query cancellation is supported. SIGINT/SIGTERM
stop acceptance and allow up to five seconds for sessions and pools to close.
An unfinished transaction can exhaust that deadline, causing a nonzero exit.
