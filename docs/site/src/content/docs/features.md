---
# Unless explicitly stated otherwise all files in this repository are licensed
# under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/)
# Copyright 2026-Present Datadog, Inc.
title: "Features"
description: "What PG Proxy provides and where to learn more."
---

PG Proxy combines PostgreSQL protocol handling, transaction pooling, and query observability in the `pg-proxy` binary and a Go library that applications can embed.

For a comparison with PgBouncer, Pgpool-II, Odyssey, and PgDog, see [Compare poolers](../explanation/pooler-comparison/).

Read [Limitations](../reference/limitations/) for transaction-pooling constraints and current implementation limits.

## Transaction pooling

Share backend PostgreSQL connections across client sessions. A connection stays with a transaction and returns to the pool when it is safe to reuse, allowing applications to keep more client sessions open than backend connections. A reset on release keeps one client's session settings from reaching the next; each database can reset on every release, only when a checkout may have changed session state, or never.

Read [Transaction pooling](../explanation/pooling/) and [Transaction-pooling compatibility](../reference/compatibility/) for the implications for session state.

## Prepared statements across transactions

Use protocol-level named prepared statements even when successive transactions use different backend connections. PG Proxy maps client names to backend names and prepares statements on a connection when needed. The client's unnamed statement is parsed again in each transaction that uses it; optional automatic caching instead caches unnamed statements per backend connection, like named ones.

Read [Prepared statements](../explanation/prepared-statements/) and the [client connection options](../reference/configuration/client-connections/).

## Read routing

Route eligible reads to replica pools or weighted read clusters while keeping other work on the main destination. Routing depends on configured backends and does not guarantee fresh replica reads after a write.

Read [Read routing](../explanation/read-routing/) for selection rules and consistency limits.

## Logs, metrics, and traces

Inspect connection usage, pool waits, message handling, query execution, and result delivery. Applications supply logging and metric clients and configure trace export.

Start with [Observability](../reference/observability/), then use the [metrics](../reference/observability/metrics/) and [tracing](../reference/observability/tracing/) references.

## Configurable databases and pools

Map client-facing database names to backend clusters, set pool limits, and supply PostgreSQL startup settings such as statement timeouts. The `pg-proxy` binary reads a YAML or JSON configuration file; applications embedding the library supply configuration through Go interfaces.

See [Configuration file](../reference/configuration/configuration-file/), [Timeouts](../reference/configuration/timeouts/), and [Go library configuration](../reference/configuration/library/).

## Authentication and TLS integration

The listener supports client TLS and application-supplied authentication. The SCRAM-SHA-256 exchange lets a client prove knowledge of a password without sending the password in that exchange. The application supplies the credential to verify against and decides who may connect; credential storage stays outside the authentication package.

SCRAM authentication and TLS serve different purposes: TLS protects the connection's traffic. The current SCRAM package does not implement `SCRAM-SHA-256-PLUS` channel binding.

See [Authentication and TLS](../explanation/authentication/) for authentication helpers, transport security, and channel binding. The `pg-proxy` binary also supports static password/SCRAM users over TLS.

## Routing for database migrations

Keep the client-facing database name while changing the PostgreSQL cluster behind it. The configuration can also move eligible reads between read clusters by adjusting their weights. This lets applications keep their connection strings while operators change routing during a migration. In the `pg-proxy` binary, a routing change needs a restart; applications that embed the library can apply it at runtime.

PG Proxy handles routing; copying data, keeping databases in sync, and deciding when to switch writes require a separate migration process. Main-cluster changes can interrupt active transactions. See the [configuration schema](https://github.com/DataDog/pg-proxy/blob/main/pkg/config/README.md), [Read routing](../explanation/read-routing/), and [Configuration changes and pool replacement](../reference/limitations/#configuration-changes-and-pool-replacement).

## Session limits and load shedding

Control client-session admission separately from backend pool capacity, with global and per-user caps.

Applications that embed the library can supply a load-shedding policy to reject new connections or backend work under load. The `pg-proxy` binary has no load-shedding policy. Apart from the session caps, it closes a new connection, without an error message, when 100 connections are already in TLS negotiation, startup, or authentication.

See [Set connection and session limits](../how-to/set-pool-limits/), [Client connection options](../reference/configuration/client-connections/), and [Go library configuration](../reference/configuration/library/).

## Traffic mirroring

Available only to applications that embed the library; the `pg-proxy` binary does not support it. Replay copies of forwarded messages on a separate database while clients continue using the source. Per-session queues and a shared byte budget limit queued replay data; each mirrored session also keeps up to 4 MiB of prepared-statement definitions outside those limits. Copies can be dropped when capacity is exhausted; shadow execution does not hold up the client waiting for its results.

Read [Traffic mirroring](../explanation/traffic-mirroring/) for replay behavior, application integration, and limits.

## Graceful shutdown

Drain sessions at safe transaction boundaries during a planned stop. Active work can finish within the application's shutdown deadline, after which clients reconnect through their configured endpoint. Open sessions are not transferred between proxies.

Read [Graceful shutdown](../explanation/graceful-shutdown/) for client behavior and the shutdown deadline.

## PostgreSQL client protocol

Handle simple and extended query messages, transactions, and query cancellation through the PostgreSQL wire protocol. Transaction pooling changes the lifetime of backend session state, so protocol support does not imply every session-based PostgreSQL feature behaves like a dedicated connection.

Check [Transaction-pooling compatibility](../reference/compatibility/) when choosing client behavior.
