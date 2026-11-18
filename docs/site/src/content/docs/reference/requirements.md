---
# Unless explicitly stated otherwise all files in this repository are licensed
# under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/)
# Copyright 2026-Present Datadog, Inc.
title: "Requirements and tested versions"
description: "What you need to build and run pg-proxy, and which PostgreSQL versions are tested."
---

## Build

| Requirement | Version |
| --- | --- |
| Go | 1.26 or newer. CI builds with the minimum version in `go.mod` and the latest stable release. |
| C compiler, CGO enabled | Any compiler Go supports, such as GCC or Clang. The SQL parser uses `libpg_query` through cgo, so `CGO_ENABLED=0` builds fail. |

There are no prebuilt binaries, container images, or releases yet. Build from a known commit:

```sh
go build -o bin/pg-proxy ./cmd/pg-proxy
```

## Operating system

CI builds and tests on Linux (Ubuntu 24.04, x86-64). The binary also builds and runs on macOS and other Unix systems. One feature is Linux-only: detecting a client that disconnects while it waits for a backend connection. On other systems, the proxy notices the disconnect only when it next writes to the client.

## PostgreSQL

| PostgreSQL | Status |
| --- | --- |
| 14 | Tested in CI on every change. |
| 15, 16, 17, 18 | The full integration suite passes. Not yet tested in CI. |
| 13 and older | Not tested. |

PG Proxy speaks PostgreSQL wire protocol version 3.0, which every supported PostgreSQL version uses. Features that depend on session state have limits under transaction pooling; see [Transaction-pooling compatibility](../compatibility/).

To run the integration suite against another version, set `PG_PROXY_TEST_POSTGRES_IMAGE`:

```sh
PG_PROXY_TEST_POSTGRES_IMAGE=postgres:17-bookworm go test -tags=integration -count=1 ./...
```

## Clients

Any PostgreSQL driver that uses protocol version 3.0 can connect, including libpq-based clients such as `psql`, pgx, JDBC, and node-postgres.

| Client feature | Support |
| --- | --- |
| TLS (`sslmode`) | Supported. See [Client TLS](../configuration/client-tls/). |
| SCRAM-SHA-256 password authentication | Supported with `proxy.password_auth_enabled`. |
| SCRAM-SHA-256-PLUS (channel binding) | Not supported. Clients that require it (`channel_binding=require`) cannot connect. |
| MD5, GSSAPI, SSPI, client certificates | Not supported. |
| Named and unnamed prepared statements | Supported across backend connections. See [Prepared statements](../../explanation/prepared-statements/). |
| Query cancellation | Supported. |
