---
# Unless explicitly stated otherwise all files in this repository are licensed
# under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/)
# Copyright 2026-Present Datadog, Inc.
title: "Authentication and TLS"
description: "How client authentication, TLS, and SCRAM channel binding fit together."
---

Authentication decides whether a client may connect. TLS protects traffic on the connection.

## In the `pg-proxy` binary

The binary has two modes:

- **Trust mode** (default). The proxy listens only on loopback and accepts any client that connects with the backend user name. Use it only for local development.
- **Password mode.** With `proxy.password_auth_enabled`, clients authenticate with SCRAM-SHA-256 as users listed in the configuration file. Each user lists the logical databases it may use. TLS is required, and the proxy may listen on any address.

Proxy users are separate from backend roles: every client of a logical database runs its queries as that database's configured backend user. See [Client authentication](../../reference/configuration/client-authentication/) and [Client TLS](../../reference/configuration/client-tls/) to set them up.

## In the Go library

The listener calls the application's `Authenticate` hook during connection startup. The application chooses how to look up credentials and whether the requested user may access the requested database.

The library provides these helpers:

- `scram.Authenticate` runs a `SCRAM-SHA-256` exchange against a password supplied by the application. The client proves knowledge of the password without sending it in that exchange.
- `scram.AuthenticateVerifier` runs the same exchange against a stored SCRAM verifier, so the application does not need the password. `scram.NewVerifier` and `scram.ParseVerifier` create and read verifiers.
- `auth.NewStatic` builds an in-memory user list with SCRAM verifiers and per-database grants. The binary's password mode uses it.
- `protocol.GetCleartextPassword` requests a password from the client. The application must validate it. This exchange sends the password directly and needs a protected connection, such as TLS.

Except for `auth.Static`, these helpers do not store credentials or decide database access. None of them connect to an external credential store. Client authentication is also separate from the credentials used to establish pooled connections to PostgreSQL.

## Transport security

The listener accepts a TLS configuration for client connections. TLS encrypts queries, results, and authentication messages in transit. SCRAM alone does not encrypt the session's query traffic.

The client-to-proxy and proxy-to-PostgreSQL connections are separate connections. Configuring TLS on the listener protects the client-facing connection. For the backend connection, set `sslmode` on each database in the binary's configuration file, or set TLS on the pgx pool configuration in the library.

## SCRAM channel binding (lack of)

Channel binding ties a SCRAM authentication exchange to the TLS connection carrying it. PostgreSQL calls the channel-binding mechanism `SCRAM-SHA-256-PLUS`. See [PostgreSQL's SASL authentication documentation](https://www.postgresql.org/docs/current/sasl-authentication.html) for the protocol details.

PG Proxy's built-in SCRAM helper currently offers only `SCRAM-SHA-256`. A client that requires channel binding cannot authenticate through this helper, even when TLS is enabled. Supporting encrypted connections and supporting channel binding are separate capabilities.

Support varies across poolers. As checked in October 2026, [PgBouncer's SCRAM implementation](https://github.com/pgbouncer/pgbouncer/blob/master/src/scram.c) also rejects clients that require channel binding. This is not a constraint of pooling itself; check the implementation and version when comparing authentication support.

See [Go library configuration](../../reference/configuration/library/) for listener integration and [Limitations](../../reference/limitations/) for other compatibility boundaries.

## Startup diagnostics

A `Hooks.ResolveStartup` function should wrap `listener.ErrLogicalDatabaseNotConfigured` or `listener.ErrBackendNotReady` so these failures log `reason=logical_database_not_configured` or `reason=backend_not_ready`. Client responses remain generic. Hooks should log safe resource and verified identity details where available; arbitrary hook errors and authentication payloads must stay out of client responses and startup logs.
