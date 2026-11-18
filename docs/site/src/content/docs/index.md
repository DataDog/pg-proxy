---
title: "Introduction"
description: "A PostgreSQL wire-protocol proxy with transaction-level connection pooling."
---

PG Proxy sits between PostgreSQL clients and their databases. The Go library handles the PostgreSQL wire protocol, client sessions, routing, and transaction-level connection pooling.

**Early development.** Configuration and Go APIs may change between releases.

There are two ways to use PG Proxy: run the `pg-proxy` binary, or use the Go library to build your own proxy.

**Run the `pg-proxy` binary:** Configure it with a YAML or JSON file. Start with [Local development](how-to/local-development/), then follow [Run in production](how-to/run-in-production/) to deploy it with TLS and password authentication.

**Build your own application with the Go library:** Supply your own configuration, authentication, routing, and telemetry integrations. See [Go library configuration](reference/configuration/library/) and [Packages](reference/packages/).

## About PG Proxy

We're Datadog's PostgreSQL Platform team. We started building PG Proxy in 2023 after running into limitations with existing tools, including PgBouncer. We needed a pooler and proxy that we could adapt to how we run PostgreSQL at Datadog, so we built our own.

Since then, we've run it in production, fixed it, and learned what works. We think that work is useful outside Datadog too.

## Overview

- [Features](features/): Pooling, prepared statements, read routing, observability, and application integrations.
- [Limitations](reference/limitations/): Transaction-pooling constraints and current implementation limits.
- [Compare poolers](explanation/pooler-comparison/): How PG Proxy compares with other connection poolers such as PgBouncer, Pgpool-II, Odyssey, and PgDog.

## How-to guides

Complete a specific task once you know the basics.

- [Local development](how-to/local-development/): Build, run, and test the `pg-proxy` binary.
- [Run in production](how-to/run-in-production/): Deploy the binary with TLS, authentication, sizing, telemetry, and safe restarts.
- [Send OTel metrics to Datadog](how-to/otel-metrics-to-datadog/): Configure the Collector to export event counters as rates.
- [Contribute to PG Proxy](how-to/contributing/): Report bugs, propose changes, and prepare a pull request.
- [Contribute to the documentation](how-to/documentation-contributing/): Edit, preview, and validate the documentation site.
- [Set connection and session limits](how-to/set-pool-limits/): Configure the connection pool and client-session cap.

## Reference

The technical details.

### Configuration

- [`pg-proxy` binary settings](how-to/local-development/#settings): The configuration file, flags, and defaults.
- [Client connection options](reference/configuration/client-connections/): PG Proxy session options, defaults, and connection examples.
- [Timeouts](reference/configuration/timeouts/): Backend statement limits and transaction overrides.

### Observability

- [Telemetry configuration](reference/observability/): Backend selection, options, defaults, and examples for OpenTelemetry and Datadog.
- [Metrics](reference/observability/metrics/): Metric clients, session overrides, and selected measurements.
- [Tracing](reference/observability/tracing/): Span operations, propagation, and recorded data.

### Compatibility

- [Transaction-pooling compatibility](reference/compatibility/): Session state, prepared statements, and connection reuse.

### Go library

- [Go library configuration](reference/configuration/library/): Process dependencies, listener settings, and routing interfaces.
- [Packages](reference/packages/): Responsibilities of the library's public packages.

## Explanation

Understand the design and the reasons behind it.

- [Authentication and TLS](explanation/authentication/): Understand client authentication, transport security, and channel binding.
- [Transaction pooling](explanation/pooling/): Understand how client sessions share backend connections.
- [Prepared statements](explanation/prepared-statements/): How statement mappings survive backend connection changes.
- [Read routing](explanation/read-routing/): How the proxy selects clusters and replica pools.
- [Traffic mirroring](explanation/traffic-mirroring/): Replay traffic on a separate database and understand what can be dropped.
- [Graceful shutdown](explanation/graceful-shutdown/): How sessions drain and clients reconnect.
