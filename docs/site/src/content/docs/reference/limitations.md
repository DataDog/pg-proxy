---
# Unless explicitly stated otherwise all files in this repository are licensed
# under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/)
# Copyright 2026-Present Datadog, Inc.
title: "Limitations"
description: "Transaction-pooling constraints and current implementation limits of PG Proxy."
---

Some constraints come from transaction pooling; others belong to PG Proxy's current implementation. Each section explains which applies. Check them alongside [Features](../../features/) when deciding whether the proxy fits your application.

## Transaction-pooling constraints

Transaction pooling shares PostgreSQL connections between clients. Session state does not follow a client between transactions, and long or idle transactions hold connections that other clients need. These constraints are shared with other transaction poolers.

PG Proxy also has specific boundaries around prepared statements and connection cleanup. See [Transaction-pooling compatibility](../compatibility/) for supported SQL patterns, client options, and the state that can remain on a reused connection.

## Traces can contain sensitive data

When tracing is enabled, both Datadog and OpenTelemetry spans can contain raw SQL and error messages. The `pg-proxy` binary omits bind parameter values by default; `-trace-parameters=true` enables capture as `db.parameters` on sampled traces. Disabling capture does not remove SQL literals from `db.statement` or redact error messages. PG Proxy does not provide general telemetry redaction. See [Telemetry configuration](../observability/) for binary and library options.

## Client messages have no configured size limit

After authentication, PG Proxy does not limit the body size of client messages. (During authentication, the `pg-proxy` binary limits messages to 16 KiB; see [fixed limits](../configuration/timeouts/#fixed-limits-in-the-binary).) The protocol reader buffers the declared body before decoding it, so a sufficiently large message can exhaust proxy memory and interrupt other clients. The message-buffer setting controls flushing replies to clients; it does not limit incoming messages.

The `datadog.pg_proxy.client_messages.body_length_bytes` metric estimates size from decoded fields after a successful read. Some message decoders accept trailing padding without retaining it in those fields, so the metric can underreport the body that was buffered. It cannot enforce a memory limit or report a message that exhausts memory before decoding finishes. See [Metrics](../observability/metrics/) and [Go library configuration](../configuration/library/).

## Read consistency with replica routing

This applies when reads are routed to PostgreSQL replicas. With asynchronous replication, a replica can briefly lag behind the primary, so a read immediately after a write may not see that write. Traffic sent only to the PostgreSQL primary is unaffected by replica lag.

The scope includes routing enabled by the client's `pg_proxy.dynamic_routing` option and routing enabled by configured `read_clusters`. It depends on whether the chosen destination is a replica, not just whether the client supplied the option. PG Proxy does not currently pin later transactions to the primary after a write. Reads that need to observe that write should use a destination configured to use the primary.

SQL classification guides routing; it does not enforce database permissions or read-only access. Once a transaction has acquired a backend connection, its statements stay on that connection.

See [Read routing](../../explanation/read-routing/) for the selection rules and fallback behavior.

## Configuration changes and pool replacement

This section applies to applications that embed the library and refresh configuration with `pkg/warden`. The `pg-proxy` binary reads its configuration file once at startup; restart it to apply changes, which closes all sessions.

When the configuration reconciler detects a changed cluster definition or a changed database used by that cluster, it recreates the cluster's backend and closes the previous one. This can replace an entire pool even when only one backend setting changed.

The replacement is not warmed to its configured capacity before taking traffic. A successful connection check does not mean all pool connections are ready, so the first requests after a change may spend extra time opening connections. The reconciler also does not guarantee that the old backend remains available if replacement creation fails.

Changing a logical database's main `cluster` has a separate effect: existing sessions are remapped, and a session holding a backend connection can have its active transaction aborted. Changing only the `read_clusters` mapping or weights affects future acquisitions without moving an active transaction. Replacing the underlying backend pools is still subject to the behavior above.

Plan main-cluster switches around a pause in application traffic and completion of active transactions. Watch connection errors and acquisition latency after configuration changes. See the [configuration schema](https://github.com/DataDog/pg-proxy/blob/main/pkg/config/README.md) and [Read routing](../../explanation/read-routing/).

## Mirroring can lose replay traffic

Traffic mirroring can drop copies when queues are full, replay fails, or sessions close. Shadow replies are discarded, with no built-in comparison of source and shadow results. Copying and replay also consume resources, so mirroring does not guarantee unchanged latency or synchronized databases. See [Traffic mirroring](../../explanation/traffic-mirroring/).

## SCRAM channel binding is not implemented

PG Proxy's built-in SCRAM helper offers `SCRAM-SHA-256`, but not `SCRAM-SHA-256-PLUS`. Clients that require channel binding cannot authenticate through this helper. TLS connections are supported separately. See [Authentication and TLS](../../explanation/authentication/) for how these pieces fit together.
