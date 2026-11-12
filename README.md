# PG Proxy

A PostgreSQL proxy and transaction-level connection pooler, written in Go.

PG Proxy sits between your applications and PostgreSQL. Many client sessions
share a smaller pool of backend connections, and each connection goes back to
the pool when its transaction ends. Datadog's PostgreSQL Platform team built it
and runs it in production.

> **Status:** early. The proxy is in use, but the Go API is still changing.

## Features

- **Transaction pooling.** Keep many more client sessions open than backend connections.
- **Prepared statements.** Named prepared statements keep working when transactions run on different backend connections.
- **Routing.** Map client-facing database names to clusters, and send eligible reads to replicas.
- **Observability.** Logs, metrics, and query traces through OpenTelemetry or native Datadog.
- **Embeddable.** Use the Go library to plug in your own configuration, authentication, and load shedding.

## Quick start

You need Go 1.26+, a C compiler (CGO is required), Docker, and `psql`.

Start a disposable PostgreSQL:

```sh
docker run -d --name pg -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres:17
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
  local: {primary: primary}
logical_databases:
  local_db: {cluster: local}
proxy:
  listener_address: 127.0.0.1:15432
```

Run it:

```sh
./bin/pg-proxy -config proxy.yaml
```

Connect through the proxy from another terminal:

```sh
psql 'postgres://postgres@127.0.0.1:15432/local_db?sslmode=disable' -c 'select 1'
```

Clients connect to a logical database (`local_db`), and the proxy routes them to
the cluster behind it. By default the proxy listens only on loopback and trusts
the backend username, so use this setup only for local development. To run it
anywhere else, turn on [TLS and password authentication](docs/client-authentication.md).

## Documentation

The full docs live in [`docs/site`](docs/site/src/content/docs/index.md):

- [Features](docs/site/src/content/docs/features.md) and [limitations](docs/site/src/content/docs/reference/limitations.md)
- [Configuration settings](docs/site/src/content/docs/how-to/local-development.md#settings) and the [config schema](pkg/config/README.md)
- [Telemetry](docs/site/src/content/docs/reference/observability/index.md): OpenTelemetry and Datadog
- [Transaction-pooling compatibility](docs/site/src/content/docs/reference/compatibility.md)
- [Using the Go library](docs/site/src/content/docs/reference/configuration/library.md) and [package overview](docs/site/src/content/docs/reference/packages.md)
- [How PG Proxy compares](docs/site/src/content/docs/explanation/pooler-comparison.md) with PgBouncer, Pgpool-II, Odyssey, and PgDog

## Development

```sh
go build ./...
go vet ./...
go test -race ./...
go test -race -tags=integration ./...   # needs Docker
```

See [Local development](docs/site/src/content/docs/how-to/local-development.md)
for the full workflow.

## Contributing

We welcome bug reports, fixes, and docs improvements. Please open an issue
before starting non-trivial work. See [CONTRIBUTING.md](CONTRIBUTING.md).

To report a security issue, email security@datadoghq.com. Do not open a public issue.

## License

[Apache-2.0](LICENSE). Third-party licenses are listed in
[LICENSE-3rdparty.csv](LICENSE-3rdparty.csv).
