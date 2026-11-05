# pg-proxy

A PostgreSQL wire-protocol proxy with transaction-level connection pooling.

Runtime integrations — configuration, feature flags, authentication, and load
shedding — are supplied by the embedder through Go interfaces, so the proxy can
run standalone or inside a larger system.

**Status: early.** Most of the proxy still lives in Datadog's monorepo
(`domains/orgstore/apps/pg-proxy`) and is being moved here package by package.
What is here is in use, but the API is not yet stable.

## Dependency policy

This library must build and run without any Datadog-internal code, so it stays
extractable into a standalone public repository. Concretely, imports beginning
with `github.com/DataDog/` are rejected unless they are:

| Allowed | Example |
| --- | --- |
| Packages in this library | `…/domains/orgstore/libs/pg-proxy/…` |
| Publicly released Datadog modules | `github.com/DataDog/datadog-go/v5/…` |

Everything else under `github.com/DataDog/` is off limits, including the rest of
the monorepo and private modules such as `go-service-authn`. Third-party
dependencies outside `github.com/DataDog/` are allowed.

While the library lives in the monorepo, a linter enforces this: it reads the
imports of every Go file in the tree — library, test, and generated alike — and
fails `bzl test` on a disallowed one. After extraction the constraint is no
longer needed, since none of the forbidden code is reachable.

Optional integrations using public Datadog SDKs live in `pkg/datadog`.
Implementations requiring private Datadog dependencies stay in the consuming app.

## Layout

Add or remove a row when a `pkg/` directory lands or leaves. Do not rewrite this
as prose — one-line rows keep each extraction PR's doc diff small.

| Package | Role |
| --- | --- |
| `.` (`pgproxy`) | Process-wide `Bootstrap` / `Dependencies` (Logger, Metrics, Tracer) |
| `pkg/datadog` | Native Datadog tracing, DogStatsD metrics, and JSON log setup using public SDKs |
| `pkg/otel` | OpenTelemetry trace, metric, and slog adapters using caller-owned providers |
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

Target as more code moves: public API under `pkg/`, internals under `internal/`,
OSS binary under `cmd/pg-proxy`.

Auth mechanisms here are given the password to verify against and only perform
the exchange — credential storage stays with the embedder.

Anything the monorepo still imports has to stay under `pkg/`: Go only allows
`internal/` to be imported from within this library, so code moves there once its
subsystem no longer has callers in `apps/pg-proxy`.

## Configuration

Start with [`pkg/config/config.go`](pkg/config/config.go) and the
[config guide](pkg/config/README.md). The [app config guide (Datadog internal)](https://github.com/ddoghq/dd-source/blob/main/domains/orgstore/apps/pg-proxy/config/README.md)
lists app-only settings and credential rules. For new settings, follow the
[configuration extension guide](docs/extending-configuration.md).
The OSS binary consumes a limited subset, described below. Applications can
use `pool.PGXConfig(config.Database)` to build a direct backend pool configuration;
resolve application-owned credentials into a username and password first.

## Startup failures

The listener sends one terminal FATAL error for a reportable startup failure,
then closes the connection. Generic resolver and authentication-hook failures
receive `08006: could not establish connection`. `listener.ErrJWTExpired` retains
the fixed `08006: jwt authentication failed: token expired` response for existing
client refresh/retry behavior. Arbitrary hook errors,
authentication payloads, and startup options are excluded from responses and
startup logs.
Server logs include a `connection_id` and unverified `requested_user` and
`requested_database`. Hooks receive a context logger through
`pgproxy.LoggerFromContext`; child loggers add fields without modifying the
caller's context. Identity and resource fields are structured and capped at
256 characters. These fields do not add metric tags.

The listener records the startup stage without formatting arbitrary hook errors.
Known listener failures and `listener.AuthenticationError` constants retain
specific server-side diagnostics while client responses remain generic except
for the fixed JWT-expiry response. Hooks
log safe resource and verified identity details where they are available.
Unknown failures retain `08006` and a generic diagnostic.
Cancellation failures retain the previous missing-session and incorrect-secret
server messages, including the PID. They log `stage=cancel` with
`reason=session_not_found` or `reason=incorrect_secret`. Client responses remain
generic and cancellation secrets are never logged.

Hooks must return failures to the listener and must not write terminal errors,
AuthenticationOk, BackendKeyData, or ReadyForQuery. `listener.ClientNotified`
suppresses another response when a hook explicitly owns the terminal response.
Session-cap rejection retains its fixed `53300` response and metric ownership.
EOF and known client disconnects remain silent. Authentication failures never
receive successful startup messages. Session-factory failure after successful
authentication remains a separate lifecycle issue.

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

Before rollout, check client TLS modes, trusted certificate authorities, server
names, and certificate-provider configuration. Clients that previously connected
in plaintext despite `RequireTLS=true` must enable TLS. The OSS binary
uses loopback trust with optional or required TLS. See the
[client TLS guide](docs/client-connections.md#standalone-tls) for certificate
configuration and verified connections.

## Development

### OSS binary

`cmd/pg-proxy` runs the portable library against configured PostgreSQL backends, with
static routing and transaction pooling. It needs no COBS, Vault, or experiments
service. Client connections use **local trust with optional or required TLS**: the command accepts
only loopback IP listen addresses and the selected backend's username. Any local
process using that username can access the backend with its configured privileges.
Use a disposable development database. This is not a production server.

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

From the repository root, build and start the OSS binary with Go 1.26 or
newer and a C compiler (CGO is required):

```sh
go build -o bin/pg-proxy ./cmd/pg-proxy
./bin/pg-proxy -config proxy.yaml
```

For the telemetry examples below, add the built binary to your PATH:

```sh
export PATH="$PWD/bin:$PATH"
```

In another terminal:

```sh
psql 'postgres://postgres@127.0.0.1:15432/local_db?sslmode=disable' -c 'select 1'
```

Add entries to `databases`, `clusters`, and `logical_databases` for more backends.
Each cluster selects one primary database. Each logical database selects a cluster.
Multiple logical databases can select the same cluster.

The OSS binary accepts direct connection settings, inline backend credentials, pool sizing/lifetimes, a loopback
listener address, and the message buffer size. Omitted listener and buffer settings
use `127.0.0.1:15432` and 10240 bytes. Zero pool settings use pgx defaults; negative
values and minimum connections above the maximum are rejected. Backend connection
parsing retains pgx environment defaults. `application_name` in connection options
is overridden by a nonempty `application_name` database field.

Set `proxy.tls_cert_file` and `proxy.tls_key_file` to enable client TLS. Set
`proxy.tls_required: true` to reject plaintext startup. See the
[client TLS guide](docs/client-connections.md#standalone-tls) for examples.

File mode rejects users/JWT, mirroring sections, replicas, read clusters, and
discovery/Envoy/load-balanced database options with explicit startup errors.
**TODO:** wire client SCRAM support. Backend TLS is available through the
database's `sslmode` setting.

`-config` is required and accepts `.yaml`, `.yml`, or `.json` files. All database
and listener settings come from that file. The OSS binary has no separate backend,
listener, database, or session-limit flags and does not read `PG_PROXY_BACKEND_URL`.
The session limit remains 100. File changes require a restart.

Role passthrough is rejected; all queries use the backend user. Query cancellation is supported. SIGINT/SIGTERM
stop acceptance and allow up to five seconds for sessions and pools to close.
An unfinished transaction can exhaust that deadline, causing a nonzero exit.

The Docker-backed OSS binary test covers multiple backends and usernames, queries,
transactions, pool reuse, startup cleanup, cancellation, and shutdown:

```sh
go test -race -tags=integration -count=1 ./cmd/pg-proxy ./pkg/pool
```

These tests require Docker and access to Datadog's container registry. They use
the existing private PostgreSQL images. The consuming Datadog repository runs
them in CI against its pinned module version; GitHub Actions compiles them here.

### Differential protocol verification

The [protocol harness](docs/protocol-verification.md) compares deterministic raw
PostgreSQL conversations through the proxy and two direct controls. It checks
ordered wire responses, committed effects, and client recovery. Its dedicated
CI job runs with Docker and a pinned public PostgreSQL image; it needs no
Datadog account or private registry.

```sh
go test ./internal/protocoltest
go test -tags=integration -count=1 -timeout=10m -v ./internal/protocoltest/integration
```

### Telemetry

The OSS binary supports native Datadog and OpenTelemetry. With no telemetry flags
or exporter variables, it writes text logs to stderr and exports no telemetry.
Telemetry settings are flags and environment variables, not fields in `proxy.yaml`.
Proxy logs default to INFO in every mode. Add `-log-level DEBUG` to include debug
records on stderr and, when enabled, through OTLP. SDK diagnostics keep their own settings.

The examples below use Bash or Zsh, `pg-proxy` on your PATH, and the database
configuration above. See the [telemetry reference (Datadog internal)](https://github.com/ddoghq/dd-source/blob/main/domains/orgstore/apps/libs/pg-proxy/docs/src/content/docs/reference/observability/index.md)
for all supported selectors, defaults, destination settings, and SDK configuration links.

#### Datadog

Use an Agent that accepts native traces and DogStatsD metrics. Configure the
Agent to collect stderr or container logs if you want log ingestion.
Remove any previous OTel trace selection for this command (also works in Fish):

```bash
env -u OTEL_TRACES_EXPORTER \
  DD_SERVICE=pg-proxy \
  DD_ENV=local \
  DD_VERSION=dev \
  DD_AGENT_HOST=127.0.0.1 \
  pg-proxy \
  -config proxy.yaml \
  -telemetry datadog
```

This mode sends traces to the Agent and metrics through DogStatsD. It writes
JSON logs to stderr. The SDKs read the `DD_*` settings; PG Proxy rejects ambiguous
`OTEL_TRACES_EXPORTER` settings as described in the telemetry reference.

#### OpenTelemetry

Use an OTLP/HTTP receiver with pipelines for traces, metrics, and logs:

```bash
export OTEL_SERVICE_NAME=pg-proxy
export OTEL_RESOURCE_ATTRIBUTES='deployment.environment.name=local'
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318

pg-proxy \
  -config proxy.yaml \
  -telemetry otel
```

`-telemetry otel` enables all three exporters. To select signals individually,
omit `-telemetry` and set `OTEL_TRACES_EXPORTER`, `OTEL_METRICS_EXPORTER`, and
`OTEL_LOGS_EXPORTER` to `otlp` or `none`; all default to `none`.
`-telemetry none` disables export while retaining text logs on stderr.
`-telemetry` and `-tracing` cannot be used together; startup rejects the combination.

For OTel metrics sent to Datadog, follow [Send OTel metrics to Datadog (Datadog internal)](https://github.com/ddoghq/dd-source/blob/main/domains/orgstore/apps/libs/pg-proxy/docs/src/content/docs/how-to/otel-metrics-to-datadog.md).
It uses the [Collector configuration](docs/otel-collector-datadog.yaml), delta
export, and the Collector's interval-inference feature gate to convert counts to
rates. Local validation confirms that 100 events over 10 seconds exports as
10/s. Live coexistence with DogStatsD still needs dashboard and monitor validation.

#### Query data

Traces contain SQL text, including literal values, and error messages. Bind
parameter capture is off by default. Add `-trace-parameters=true` to a command
that enables tracing to include raw values as `db.parameters`. This changes only
trace data; PostgreSQL receives the same parameter values.

For application trace propagation, send W3C context in SQL comments. See
[Tracing (Datadog internal)](https://github.com/ddoghq/dd-source/blob/main/domains/orgstore/apps/libs/pg-proxy/docs/src/content/docs/reference/observability/tracing.md)
for client instrumentation, recorded fields, and prepared-statement limitations.

#### Go library configuration

Install integrations with `pgproxy.Bootstrap` before creating sessions or pools.
Omitted integrations are disabled. The application owns their lifetime and must
flush and stop them after sessions drain.

| Backend | Setup |
| --- | --- |
| OpenTelemetry | Create providers, then use `pkg/otel.NewTracer`, `NewMetrics`, and `NewLogHandler` to populate `pgproxy.Dependencies`. The adapters do not install global OTel providers. |
| Datadog | Call `pkg/datadog.Start`, handle its error, and pass the returned dependencies to `Bootstrap`. After sessions stop, call the returned shutdown function. Supplied clients stay caller-owned. |

See [Go telemetry configuration (Datadog internal)](https://github.com/ddoghq/dd-source/blob/main/domains/orgstore/apps/libs/pg-proxy/docs/src/content/docs/reference/configuration/library.md#telemetry-settings)
for adapter and ownership options. Bind parameter capture uses
`server.RuntimeConfig.EnableTraceParameters` (or `session.Config.EnableTraceParameters`)
and defaults to `false`. The internal Datadog app explicitly enables it and
retains its existing logger, StatsD client, tracer startup, and propagation.

#### Test coverage

The OSS binary's tests send parameterized queries through PostgreSQL and decode
the exported OTLP data. They verify SQL and optional parameter capture, W3C
parent context, query counts, and startup logs. Other checks cover independent
signal selection, cumulative and delta counts, and flushing after cancellation.
Datadog tests decode native trace requests, DogStatsD packets, and JSON logs,
and check that shutdown closes only clients created by the helper.

### Library checks

From the repository root, run the build, static checks, and unit tests:

```sh
go build ./...
go vet ./...
go test -race -skip '^TestProxyAgainstPostgres$' -count=1 ./...
```

The unit command excludes the Docker-backed CLI test. Run the Docker checks
above when registry access is available. Format changed Go files with `gofmt -w`;
no Bazel setup or generated BUILD files are needed in this repository.
