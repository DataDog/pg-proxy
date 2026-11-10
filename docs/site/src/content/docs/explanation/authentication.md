---
title: "Authentication and TLS"
description: "How client authentication, TLS, and SCRAM channel binding fit together."
---

Authentication decides whether a client may connect. TLS protects traffic on the connection. PG Proxy provides the protocol handling and integration points for both; the application supplies credentials and access policy.

## Client authentication

The listener calls the application's `Authenticate` hook during connection startup. The application chooses how to look up credentials and whether the requested user may access the requested database.

The library provides two helpers:

- `scram.Authenticate` runs a `SCRAM-SHA-256` exchange against a password supplied by the application. The client proves knowledge of the password without sending it in that exchange.
- `protocol.GetCleartextPassword` requests a password from the client. The application must validate it. This exchange sends the password directly and needs a protected connection, such as TLS.

These helpers do not provide a credential store or decide database access policy. Client authentication is also separate from the credentials used to establish pooled connections to PostgreSQL.

## Transport security

The listener accepts a TLS configuration for client connections. TLS encrypts queries, results, and authentication messages in transit. SCRAM alone does not encrypt the session's query traffic.

The client-to-proxy and proxy-to-PostgreSQL connections are separate connections. Configuring TLS on the listener protects the client-facing connection; backend TLS belongs to the backend connection configuration.

## SCRAM channel binding (lack of)

Channel binding ties a SCRAM authentication exchange to the TLS connection carrying it. PostgreSQL calls the channel-binding mechanism `SCRAM-SHA-256-PLUS`. See [PostgreSQL's SASL authentication documentation](https://www.postgresql.org/docs/current/sasl-authentication.html) for the protocol details.

PG Proxy's built-in SCRAM helper currently offers only `SCRAM-SHA-256`. A client that requires channel binding cannot authenticate through this helper, even when TLS is enabled. Supporting encrypted connections and supporting channel binding are separate capabilities.

Support varies across poolers. As checked in October 2026, [PgBouncer's SCRAM implementation](https://github.com/pgbouncer/pgbouncer/blob/master/src/scram.c) also rejects clients that require channel binding. This is not a constraint of pooling itself; check the implementation and version when comparing authentication support.

See [Go library configuration](../../reference/configuration/library/) for listener integration and [Limitations](../../reference/limitations/) for other compatibility boundaries.
