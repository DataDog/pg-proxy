---
title: "Traffic mirroring"
description: "How PG Proxy replays traffic on a separate database without waiting for its replies."
---

Traffic mirroring copies forwarded PostgreSQL messages to a separate database, called the shadow database. This lets you exercise another database with a real workload, for example while evaluating a PostgreSQL upgrade. Clients continue to receive responses from the source database.

## The replay path

Each mirrored session has a queue and a worker. The proxy forwards a message to the source, then attempts to enqueue a copy. The worker replays queued messages in order, using the source's flush and connection-release boundaries to coordinate its shadow connection.

The worker reads and discards shadow responses. It does not compare result rows with the source or return shadow results to the client. Mirrored writes execute on the shadow, so the shadow must be a separate destination intended for replay.

## What happens when the shadow falls behind

Queue capacity is measured in bytes, including an estimate of message overhead. Each session has a limit, and the server runtime shares an additional byte budget across its sessions. Queued messages and messages still buffered for replay count toward those limits.

When a data message would exceed a budget, its copy is dropped. The worker skips the remaining messages in that acquisition batch and resumes after its release boundary. Replay errors can also cause a batch to be skipped. Closing the queue discards its unread tail.

The client path does not wait for shadow execution or for queue capacity. Copying messages, accounting for memory, and running workers still consume resources; asynchronous replay does not guarantee unchanged latency.

## What replay can tell you

Mirroring can expose workload and protocol problems on the shadow, but delivery is best effort. Dropped traffic, different starting data, and independent execution can make the databases diverge. Use separate validation when you need to compare results or verify an upgrade. Mirroring does not maintain a database replica.

Monitor queued-message counts, drops, and replay errors through the [mirroring metrics](../../reference/observability/metrics/#traffic-mirroring). A count of replayed messages alone does not prove that the source and shadow produced equivalent results.

## Application integration

The application supplies the mirroring policy and a callback to acquire shadow connections through `server.RuntimeConfig.Mirroring`. The policy names the source and shadow databases, sets the per-session queue size, and requires a future expiry time. The local reference binary does not configure mirroring.

See [Go library configuration](../../reference/configuration/library/#traffic-mirroring) for the fields and [Limitations](../../reference/limitations/) for deployment boundaries.
