---
# Unless explicitly stated otherwise all files in this repository are licensed
# under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/)
# Copyright 2026-Present Datadog, Inc.
title: "Local development"
description: "Run pg-proxy from source against a local PostgreSQL database."
---

This guide runs `pg-proxy` from source, in front of a disposable local PostgreSQL database.

You need Go 1.26 or newer, a C compiler with CGO enabled (the SQL parser uses `libpg_query`), Docker or another way to run PostgreSQL, and `psql`. Ports `5432` and `15432` must be free.

## 1. Get the source

```sh
git clone https://github.com/DataDog/pg-proxy.git
cd pg-proxy
```

## 2. Start PostgreSQL

You can use a PostgreSQL server you already run: change the backend settings in the next step to match it. If it listens on `127.0.0.1:5432`, also set `proxy.listener_address` to a free port, because the proxy uses `5432` by default.

If you don't have one, start a disposable PostgreSQL 18 server in Docker. It listens on port `15432`, which leaves the standard PostgreSQL port `5432` for the proxy:

```sh title="Start PostgreSQL"
docker run --detach --rm \
  --name pg-proxy-dev \
  --publish 127.0.0.1:15432:5432 \
  --env POSTGRES_PASSWORD=postgres \
  postgres:18
```

| Setting | Value |
| --- | --- |
| Address | `127.0.0.1:15432` |
| Database | `postgres` |
| User | `postgres` |
| Password | `postgres` |

`--rm` deletes the container and its data when it stops.

## 3. Configure and start the proxy

Save this as `proxy.yaml` in your `pg-proxy` checkout:

```yaml
databases:
  - name: primary
    host: 127.0.0.1
    port: 15432
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
```

The file has no `proxy` section, so the proxy listens on its default address, `127.0.0.1:5432`.

Check the file:

```sh
go run ./cmd/pg-proxy -config proxy.yaml -validate-config
```

This reports every configuration error it finds, with the field path and line, then exits: status `0` if the file is valid, `1` if not. It does not connect to PostgreSQL or open the listener, so a valid file can still fail to start if the database is unreachable or the port is in use.

Start `pg-proxy`:

```sh
go run ./cmd/pg-proxy -config proxy.yaml -telemetry none
```

The first run compiles the proxy, which takes a little longer. Wait for the `proxy ready` log line. The proxy listens on `127.0.0.1:5432`.

:::caution
Without password authentication, `pg-proxy` trusts any client on the local machine that connects with the backend user name, and runs its queries as that backend user. Use this mode only with disposable databases. To require passwords, see [Client authentication](../../reference/configuration/client-authentication/).
:::

## 4. Connect a client

In another terminal:

```sh
psql \
  --dbname 'postgres://postgres@127.0.0.1:5432/local_db?sslmode=disable' \
  --no-align \
  --tuples-only \
  --command 'select 1'
```

The result is `1`.

- The database name in the connection string is the **logical database** (`local_db`), not the backend `dbname`.
- In this mode, the client user must be the same as the backend `username` (`postgres`).

If the connection fails, the client usually sees only `could not establish connection`. The proxy log has the reason; see [Troubleshooting](../../reference/troubleshooting/).

## 5. Stop the proxy

Close your clients and press `Ctrl+C` in the proxy terminal. The proxy waits up to `proxy.shutdown_grace_period` (default `30s`) for sessions and pools to close. An open transaction can use up that time; then the proxy exits with status `1`.

Stop PostgreSQL with `docker stop pg-proxy-dev`.

## Next steps

- [Configuration file](../../reference/configuration/configuration-file/): every setting and its default.
- [Flags and environment variables](../../reference/configuration/command-line/).
- [Set connection and session limits](../set-pool-limits/).
- [Run in production](../run-in-production/): TLS, password authentication, sizing, and telemetry.
- [Contribute](../contributing/): formatting, tests, and the checks CI runs.
