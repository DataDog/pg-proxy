# pg-proxy

A PostgreSQL wire-protocol proxy with transaction-level connection pooling.

Runtime integrations — configuration, feature flags, authentication, and load
shedding — are supplied by the embedder through Go interfaces, so the proxy can
run standalone or inside a larger system.

**Status: early.** What is here is in use, but the API is not yet stable.

## Documentation

Guides, reference, and design explanations live in the documentation site under
[`docs/site`](docs/site). Its pages are Markdown in
[`docs/site/src/content/docs`](docs/site/src/content/docs), starting with the
[introduction](docs/site/src/content/docs/index.md). To build and preview the
site locally, see [`docs/site/README.md`](docs/site/README.md).

## Dependency policy

This library builds and runs with public dependencies only. Imports beginning
with `github.com/DataDog/` are limited to publicly released Datadog modules, such
as `github.com/DataDog/datadog-go/v5/…` and `github.com/DataDog/dd-trace-go/v2/…`.
Other third-party dependencies are allowed.

Optional integrations using public Datadog SDKs live in `pkg/datadog`.
Integrations that need private services or credentials belong in the embedding
application.

## Layout

Add or remove a row when a `pkg/` directory is added or removed. Keep one row per
package.

| Package | Role |
| --- | --- |
| `.` (`pgproxy`) | Process-wide `Bootstrap` / `Dependencies` (Logger, Metrics, Tracer) |
| `pkg/datadog` | Native Datadog tracing, DogStatsD metrics, and JSON log setup using public SDKs |
| `pkg/otel` | OpenTelemetry trace, metric, and slog adapters using caller-owned providers |
| `pkg/actions` | Portable frontend-message decision state machine |
| `pkg/auth` | Static SCRAM users and explicit database grants |
| `pkg/backend` | Within-cluster routing, connection checkout, and session-link construction |
| `pkg/config` | Portable database, routing, listener, user, and mirroring configuration |
| `pkg/classifier` | SQL read-routing classification and statement counting |
| `pkg/protocol` | Wire-protocol conversations (`Exchange`, cleartext) |
| `pkg/scram` | SCRAM-SHA-256 authentication |
| `pkg/server` | Startup resolution, read routing, session construction, and listener/inventory composition |
| `pkg/query` | Post-auth message decisions and query tracing |
| `pkg/prepared` | Prepared-statement name mapping for txn pooling |
| `pkg/pool` | Direct, discovered, static cloud, and replica pool runtimes; instance health, selection, and pgx adapters |
| `pkg/link` | Session↔Postgres link: expectations, prepared-on-conn, release |
| `pkg/limiter` | Portable global and per-service session caps |
| `pkg/listener` | TCP/TLS negotiation, startup, admission, and session lifecycle |
| `pkg/mirror` | Bounded asynchronous traffic-mirroring queue and worker |
| `pkg/session` | Client session protocol engine, routing, draining, and lifecycle |
| `pkg/topology` | Portable logical-database routing and runtime configuration |
| `pkg/warden` | Versioned configuration refresh, backend reconciliation, runtime inventory, and shutdown coordination |

Public API lives under `pkg/` and the OSS binary under `cmd/pg-proxy`. Code that
embedders no longer import may move under `internal/`.

Auth mechanisms here are given the password to verify against and only perform
the exchange — credential storage stays with the embedder.

## Configuration

Start with [`pkg/config/config.go`](pkg/config/config.go) and the
[config guide](pkg/config/README.md). For new settings, follow the
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
supports loopback trust and opt-in [password/SCRAM authentication](docs/client-authentication.md). See the
[client TLS guide](docs/client-connections.md#standalone-tls) for certificate
configuration and verified connections.

## Development

### License headers

CI checks every repository Go file against the header in [.licenserc.yaml](.licenserc.yaml),
including tests and files behind build tags. Vendored dependencies keep their own notices.
Check headers locally with:

```sh
go run github.com/apache/skywalking-eyes/cmd/license-eye@v0.9.0 header check
```

To add missing headers, run the same command with `header fix` instead of
`header check`. Review the resulting diff before committing.

### OSS binary

`cmd/pg-proxy` runs the portable library against configured PostgreSQL backends, with
static routing and transaction pooling. It needs no external configuration,
secrets, or experiment service. Client connections default to loopback trust using the selected backend's
username. Opt-in password/SCRAM authentication requires TLS and allows non-loopback
listeners with explicit per-user database access. See the
[authentication guide](docs/client-authentication.md) for complete YAML/JSON examples.

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

The OSS binary accepts direct connection settings, inline backend credentials, pool sizing/lifetimes, a configured
listener address, and the message buffer size. Omitted listener and buffer settings
use `127.0.0.1:15432` and 10240 bytes. Zero pool settings use pgx defaults; negative
values and minimum connections above the maximum are rejected. Backend connection
parsing retains pgx environment defaults. `application_name` in connection options
is overridden by a nonempty `application_name` database field.

Set `proxy.tls_cert_file` and `proxy.tls_key_file` to enable client TLS. Set
`proxy.tls_required: true` to reject plaintext startup. See the
[client TLS guide](docs/client-connections.md#standalone-tls) for examples.

File mode rejects JWT, mirroring sections, replicas, read clusters, and
discovery/Envoy/load-balanced database options with explicit startup errors.
Backend TLS is available through the
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

Integration tests pull public Docker Hub images pinned by digest in
[`internal/itest/images.go`](internal/itest/images.go). To use a Docker Hub
mirror, set Testcontainers' standard `TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX`.
Tests fail if Docker is unavailable.

The standalone-command suite also accepts an explicit disposable PostgreSQL URL
through `PG_PROXY_TEST_DATABASE_URL`. CI uses the shared container fixture instead.
See the [client integration test instructions](docs/client-authentication.md#integration-tests).

#### Ping protocol behavior

`TestProxyAgainstPostgres/ping_protocol` sends identical raw protocol messages
to PostgreSQL and to the proxy, and requires the same replies from both:

- `ping_idle`: `EmptyQueryResponse`, then `ReadyForQuery(I)`.
- `ping_in_transaction`: the same empty response, then `ReadyForQuery(T)`.
- `ping_in_failed_transaction`: the same empty response, then `ReadyForQuery(E)`;
  a rollback restores `I`.
- `extended_ping` and `named_extended_ping`: Parse/Flush returns `ParseComplete`;
  Bind/Execute/Sync returns `BindComplete`, `EmptyQueryResponse`, `ReadyForQuery(I)`.
- Pending Parse messages and error recovery: replies stay in order, and pings
  discarded before Sync produce no reply of their own.

Every scenario ends with another query to detect stray replies. The test uses a
raw protocol client because a comment passed to `psql`, or driver statement
caching, can hide the exchange being checked. Run just these scenarios with:

```sh
go test -race -tags=integration -count=1 -v ./cmd/pg-proxy \
  -run 'TestProxyAgainstPostgres/ping_protocol'
```

The idle ping's load-shedding bypass is covered by unit tests in `pkg/actions`
and `pkg/session`, which need no Docker.

#### Black-box integration tests

`test/integration` runs the `cmd/pg-proxy` binary as a black box. Each test starts
a proxy process from a YAML configuration and connects to it with pgx, against
a PostgreSQL container shared by the package. The harness lives in `internal/itest`:

- `itest.Main` builds the binary once and starts shared fixtures from `TestMain`.
- `itest.StartPostgres` starts a PostgreSQL container. `Postgres.Database`
  returns a `pkg/config` database entry pointing at it.
- `itest.StartProxy` / `itest.MustStartProxy` write a `pkg/config.Config` to a
  file, start the binary on a free loopback port, and stop it with SIGTERM.
  `MustStartProxy` fails the test if the process does not exit cleanly and logs
  its output when the test fails.

```sh
go test -tags=integration ./test/integration
```

Running with `-race` also builds the proxy binary with `-race`. For custom build
flags, build the binary separately and set `PG_PROXY_ITEST_BINARY` to its absolute
path. A prebuilt binary is used as is, so build it with `-race` yourself if needed.

| Variable | Effect |
| --- | --- |
| `PG_PROXY_TEST_POSTGRES_IMAGE` | PostgreSQL image instead of the pinned default |
| `TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX` | Docker Hub mirror for images without a registry host, including Testcontainers' own. A fully qualified image name bypasses it |
| `PG_PROXY_ITEST_BINARY` | Absolute path to a prebuilt pg-proxy binary instead of building one |

Readiness uses a timed TCP port probe, not startup log parsing. Captured output
is printed only for failure diagnostics.

GitHub Actions runs unit tests on minimum and stable Go and Docker-backed
integration tests with race detection.

### Telemetry

The OSS binary supports native Datadog and OpenTelemetry. With no telemetry flags
or exporter variables, it writes text logs to stderr and exports no telemetry.
Telemetry settings are flags and environment variables, not fields in `proxy.yaml`.
Proxy logs default to INFO in every mode. Add `-log-level DEBUG` to include debug
records on stderr and, when enabled, through OTLP. SDK diagnostics keep their own settings.

The examples below use Bash or Zsh, `pg-proxy` on your PATH, and the database
configuration above. Exporter selection, defaults, and destinations follow the
standard [Datadog](https://docs.datadoghq.com/tracing/trace_collection/library_config/go/)
and [OpenTelemetry](https://opentelemetry.io/docs/languages/sdk-configuration/)
SDK environment variables.

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
`OTEL_TRACES_EXPORTER` settings.

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

To send OTel metrics to Datadog, use the
[Collector configuration](docs/otel-collector-datadog.yaml). It uses delta
export, and the Collector's interval-inference feature gate to convert counts to
rates. Local validation confirms that 100 events over 10 seconds exports as
10/s. Live coexistence with DogStatsD still needs dashboard and monitor validation.

#### Query data

Traces contain SQL text, including literal values, and error messages. Bind
parameter capture is off by default. Add `-trace-parameters=true` to a command
that enables tracing to include raw values as `db.parameters`. This changes only
trace data; PostgreSQL receives the same parameter values.

For application trace propagation, send W3C context (`traceparent`) in SQL
comments. Span fields are recorded in
[`pkg/query/trace_state.go`](pkg/query/trace_state.go).

#### Go library configuration

Install integrations with `pgproxy.Bootstrap` before creating sessions or pools.
Omitted integrations are disabled. The application owns their lifetime and must
flush and stop them after sessions drain.

| Backend | Setup |
| --- | --- |
| OpenTelemetry | Create providers, then use `pkg/otel.NewTracer`, `NewMetrics`, and `NewLogHandler` to populate `pgproxy.Dependencies`. The adapters do not install global OTel providers. |
| Datadog | Call `pkg/datadog.Start`, handle its error, and pass the returned dependencies to `Bootstrap`. After sessions stop, call the returned shutdown function. Supplied clients stay caller-owned. |

Bind parameter capture uses `server.RuntimeConfig.EnableTraceParameters` (or
`session.Config.EnableTraceParameters`) and defaults to `false`. Embedders that
already own a logger, StatsD client, or tracer can pass them through
`pgproxy.Dependencies` instead of using the helpers.

#### Metric names

Metrics are named `datadog.pg_proxy.*`, for example
`datadog.pg_proxy.sessions.connections`. The same names are used with
DogStatsD and OpenTelemetry. Library code emits bare names (`pg_proxy.…`), and
the metrics client installed by `pgproxy.Bootstrap` adds the namespace exactly
once.

Set `pgproxy.Dependencies.MetricNamespace` to replace the `datadog.` prefix:

- Empty selects `datadog.`.
- A missing trailing dot is added, so `"acme"` emits
  `acme.pg_proxy.sessions.connections`.
- It must start with a letter and contain only letters, digits, `_` and `.`.
  `Bootstrap` panics on an invalid value; call `pgproxy.NormalizeMetricNamespace`
  first to get the same check as an error.

A custom `session.Metrics` sink receives bare names. Forward them through
`pgproxy.Metrics()`, or wrap them with `pgproxy.MetricName` when sending to
another client. Do not also set a namespace on the client, such as
`statsd.WithNamespace`, or names are prefixed twice. If you change the
namespace, update the filter in the
[Collector configuration](docs/otel-collector-datadog.yaml) to match.

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
go test -race -count=1 ./...
```

Docker-backed tests require the `integration` build tag. Run the Docker checks
above when needed. Format changed Go files with `gofmt -w`;
no Bazel setup or generated BUILD files are needed in this repository.

## License

PG Proxy is licensed under [Apache-2.0](LICENSE). Third-party licenses and
attributions are listed in [LICENSE-3rdparty.csv](LICENSE-3rdparty.csv).
