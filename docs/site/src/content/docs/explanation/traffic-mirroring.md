---
# Unless explicitly stated otherwise all files in this repository are licensed
# under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/)
# Copyright 2026-Present Datadog, Inc.
title: "Traffic mirroring"
description: "How PG Proxy replays traffic on a separate database without waiting for its replies."
---

Traffic mirroring copies forwarded PostgreSQL messages to a separate database, called the shadow database. This lets you exercise another database with a real workload, for example while evaluating a PostgreSQL upgrade. Clients continue to receive responses from the source database.

## The replay path

Each mirrored session has a queue and a worker. The proxy forwards a message to the source, then attempts to enqueue a copy. The worker replays queued messages in order, using the source's flush and connection-release boundaries to coordinate its shadow connection.

The worker reads and discards shadow responses. It does not compare result rows with the source or return shadow results to the client. Mirrored writes execute on the shadow, so the shadow must be a separate destination intended for replay.

## Prepared statements on the shadow

The worker borrows a shadow connection for each batch, independently of the source's connection, so a `Bind` can reach a shadow connection that never saw its `Parse`. The worker remembers each named statement the session prepared, including statements confirmed before mirroring started. Before it replays a `Bind` or `Describe`, it parses the statement again on a shadow connection that lacks it. It skips a replayed `Parse` when the shadow connection already has that statement. Without automatic caching, the unnamed statement needs no such step: the `Parse` that re-creates it in each source checkout that uses it is replayed too. With [automatic caching](../prepared-statements/#unnamed-statements), unnamed statements have proxy-generated names and are handled like named ones.

The statement catalog holds up to 4096 statements or 4 MiB of statement definitions for each mirrored session, dropping the earliest recorded first, however often it is used. A single statement larger than 4 MiB is not kept. The catalog is not counted against the queue limits below.

## What happens when the shadow falls behind

Queue capacity is measured in bytes, including an estimate of message overhead. Each session has a limit, and the server runtime shares an additional byte budget across its sessions. Queued messages and messages still buffered for replay count toward those limits.

When a data message would exceed a budget, its copy is dropped. The worker skips the remaining messages in that acquisition batch and resumes after its release boundary. Replay errors can also cause a batch to be skipped. Closing the queue discards its unread tail. A `Parse` lost to a drop, including one later in the same batch, also loses that statement's definition unless the worker already has it. Later batches that use the statement can then fail on shadow connections that lack it. The worker learns it again from the next copied `Parse` of that statement, sent when the client parses it again or when a source connection lacks it. Once every source connection has the statement, that may not happen.

The client path does not wait for shadow execution or for queue capacity. Copying messages, accounting for memory, and running workers still consume resources; asynchronous replay does not guarantee unchanged latency.

## What replay can tell you

Mirroring can expose workload and protocol problems on the shadow, but delivery is best effort. Dropped traffic, different starting data, and independent execution can make the databases diverge. Use separate validation when you need to compare results or verify an upgrade. Mirroring does not maintain a database replica.

Monitor queued-message counts, drops, replay errors, and shadow error responses through the [mirroring metrics](../../reference/observability/metrics/#traffic-mirroring). A count of replayed messages alone does not prove that the source and shadow produced equivalent results.

## Application integration

The application supplies the mirroring policy and a callback to acquire shadow connections through `server.RuntimeConfig.Mirroring`. The policy names the source and shadow databases, sets the per-session queue size, and requires a future expiry time. The `pg-proxy` binary does not support mirroring and fails at startup if the configuration has a `mirroring` section.

See [Go library configuration](../../reference/configuration/library/#traffic-mirroring) for the fields and [Limitations](../../reference/limitations/) for deployment boundaries.
