---
title: "Introduction"
description: "How-to guides, reference, and explanation for PG Proxy."
---

PG Proxy sits between PostgreSQL clients and their databases. The Go library handles the PostgreSQL wire protocol, client sessions, routing, and transaction-level connection pooling.

**Early development.** The library is being extracted from Datadog's proxy, and its API is still evolving. These docs describe the library in the current checkout.

There are two ways to use PG Proxy: run the supplied application or use the Go library to build your own.

**Run the PG Proxy application:** Configure it with YAML. Start with the OSS binary configuration in [Local development](how-to/local-development/). The current application is for local development; we plan to ship a production-ready default application with the open-source release.

**Build your own application with the Go library:** Supply your own configuration, authentication, routing, and telemetry integrations. See [Go library configuration](reference/configuration/library/) and [Packages](reference/packages/).

## About PG Proxy

We're Datadog's PostgreSQL Platform team. We started building PG Proxy in 2023 after running into limitations with existing tools, including PgBouncer. We needed a pooler and proxy that we could adapt to how we run PostgreSQL at Datadog, so we built our own.

Since then, we've spent a lot of time running it, fixing it, and learning what works in production. In 2026, we're preparing to open source it. We think that work could be useful outside Datadog too.

## Overview

- [Features](features/): Pooling, prepared statements, read routing, observability, and application integrations.
- [Limitations](reference/limitations/): Transaction-pooling constraints and current implementation limits.
- [Compare poolers](explanation/pooler-comparison/): How PG Proxy compares with other connection poolers such as PgBouncer, Pgpool-II, Odyssey, and PgDog.

## How-to guides

Complete a specific task once you know the basics.

- [Local development](how-to/local-development/): Build, run, and test the OSS binary.
- [Send OTel metrics to Datadog](how-to/otel-metrics-to-datadog/): Configure the Collector to export event counters as rates.
- [Contribute to the documentation](how-to/documentation-contributing/): Edit, preview, and validate the documentation site.
- [Set connection and session limits](how-to/set-pool-limits/): Configure the connection pool and client-session cap.

## Reference

The technical details.

### Configuration

- [OSS binary settings](how-to/local-development/#settings): The supplied application's configuration file, flags, and defaults.
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
