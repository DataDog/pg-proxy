---
# Unless explicitly stated otherwise all files in this repository are licensed
# under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/)
# Copyright 2026-Present Datadog, Inc.
title: "Packages"
description: "The current public package inventory."
---

The public Go API lives under `pkg/`; the `pg-proxy` binary lives under `cmd/pg-proxy`. The API may change between releases.

| Package | Role |
| --- | --- |
| `.` (`pgproxy`) | Process-wide `Bootstrap` / `Dependencies` (Logger, Metrics, Tracer) |
| `pkg/actions` | Portable frontend-message decision state machine |
| `pkg/auth` | Static SCRAM users and explicit database grants |
| `pkg/backend` | Within-cluster routing, connection checkout, and session-link construction |
| `pkg/config` | Portable database, routing, listener, user, and mirroring configuration |
| `pkg/classifier` | SQL read-routing classification and statement counting |
| `pkg/protocol` | Wire-protocol conversations (`Exchange`, cleartext) |
| `pkg/scram` | SCRAM-SHA-256 authentication |
| `pkg/server` | Startup resolution, read routing, session construction, and listener/inventory composition |
| `pkg/datadog` | Native Datadog tracing and setup for traces, DogStatsD metrics, and JSON logs |
| `pkg/otel` | OpenTelemetry trace, metric, and slog adapters |
| `pkg/query` | Post-auth message decisions and query tracing |
| `pkg/bindparams` | Bounded copies of Bind parameters, rendered as typed per-position trace tags |
| `pkg/prepared` | Prepared-statement name mapping for txn pooling |
| `pkg/pool` | Direct, discovered, static cloud, and replica pool runtimes; instance health, selection, and pgx adapters |
| `pkg/link` | Session↔Postgres link: expectations, prepared-on-conn, release |
| `pkg/limiter` | Portable global and per-service session caps |
| `pkg/listener` | TCP/TLS negotiation, startup, admission, and session lifecycle |
| `pkg/mirror` | Bounded asynchronous traffic-mirroring queue and worker |
| `pkg/session` | Client session protocol engine, routing, draining, and lifecycle |
| `pkg/topology` | Portable logical-database routing and runtime configuration |
| `pkg/warden` | Versioned configuration refresh, backend reconciliation, runtime inventory, and shutdown coordination |
