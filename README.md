# PG Proxy

A PostgreSQL proxy and transaction-level connection pooler, written in Go.

Every PostgreSQL connection is a backend process with its own memory, so a
server can only hold so many. Applications often open far more connections
than they actively use, and most of them sit idle between queries.

PG Proxy sits between your applications and PostgreSQL. Client sessions share a
smaller pool of backend connections: a session holds a connection only for the
length of a transaction, then returns it to the pool. Datadog's PostgreSQL
Platform team built it and runs it in production.

## Two ways to use it

- **Run the binary.** `cmd/pg-proxy` reads a YAML or JSON config file that maps
  databases, clusters, and listener settings. See the [quick start](#quick-start).
- **Embed the Go library.** Build your own proxy and supply configuration,
  authentication, routing, load shedding, and telemetry through Go interfaces.
  See [Go library configuration](docs/site/src/content/docs/reference/configuration/library.md).

## Features

- [**Transaction pooling.**](docs/site/src/content/docs/explanation/pooling.md) Keep many more client sessions open than backend connections.
- [**Prepared statements.**](docs/site/src/content/docs/explanation/prepared-statements.md) Named prepared statements keep working when transactions run on different backend connections.
- [**Read routing.**](docs/site/src/content/docs/explanation/read-routing.md) Map client-facing database names to clusters, and send eligible reads to replicas.
- [**Session limits and load shedding.**](docs/site/src/content/docs/how-to/set-pool-limits.md) Cap client sessions separately from backend connections.
- [**Traffic mirroring.**](docs/site/src/content/docs/explanation/traffic-mirroring.md) Replay live traffic against a separate database without slowing clients.
- [**Graceful shutdown.**](docs/site/src/content/docs/explanation/graceful-shutdown.md) Drain sessions at transaction boundaries during a planned stop.
- [**TLS and SCRAM authentication.**](docs/site/src/content/docs/explanation/authentication.md) Secure client connections and verify passwords with SCRAM-SHA-256.
- [**Observability.**](docs/site/src/content/docs/reference/observability/index.md) Logs, metrics, and query traces through OpenTelemetry or native Datadog.

## Quick start

You need Go 1.26+, a C compiler (CGO is required), Docker, and `psql`.

Start a disposable PostgreSQL:

```sh
docker run -d --name pg -p 127.0.0.1:5432:5432 -e POSTGRES_PASSWORD=postgres postgres:18
```

Build the proxy:

```sh
git clone https://github.com/DataDog/pg-proxy.git
cd pg-proxy
go build -o bin/pg-proxy ./cmd/pg-proxy
```

Save this as `proxy.yaml`:

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
  listener_address: 127.0.0.1:15432
```

Check the configuration without starting the proxy:

```sh
./bin/pg-proxy -config proxy.yaml -validate-config
```

This runs local startup checks, including credential references and TLS files,
but does not check database connectivity or listener availability. See the
[validation guide](docs/site/src/content/docs/how-to/local-development.md#settings)
for details.

Run it:

```sh
./bin/pg-proxy -config proxy.yaml
```

Connect through the proxy from another terminal:

```sh
psql 'postgres://postgres@127.0.0.1:15432/local_db?sslmode=disable' -c 'select 1'
```

This setup is for local development: the proxy listens on loopback and trusts
the backend username. To deploy it, follow
[Run in production](docs/site/src/content/docs/how-to/run-in-production.md).

## Documentation

The full docs live in [`docs/site`](docs/site/src/content/docs/index.md):

- [Features](docs/site/src/content/docs/features.md) and [limitations](docs/site/src/content/docs/reference/limitations.md)
- [Configuration settings](docs/site/src/content/docs/how-to/local-development.md#settings) and the [config schema](pkg/config/README.md)
- [Telemetry](docs/site/src/content/docs/reference/observability/index.md): OpenTelemetry and Datadog
- [Transaction-pooling compatibility](docs/site/src/content/docs/reference/compatibility.md)
- [Using the Go library](docs/site/src/content/docs/reference/configuration/library.md) and [package overview](docs/site/src/content/docs/reference/packages.md)
- [How PG Proxy compares](docs/site/src/content/docs/explanation/pooler-comparison.md) with PgBouncer, Pgpool-II, Odyssey, and PgDog

## Contributing

We welcome bug reports, fixes, and docs improvements. Please open an issue
before starting non-trivial work. See [CONTRIBUTING.md](CONTRIBUTING.md) for
development setup and the checks to run before a pull request.

To report a security issue, email security@datadoghq.com. Do not open a public issue.

## License

[Apache-2.0](LICENSE). Third-party licenses are listed in
[LICENSE-3rdparty.csv](LICENSE-3rdparty.csv).
