---
title: "Graceful shutdown"
description: "How sessions drain, when connections close, and what clients must handle."
---

Graceful shutdown gives active transactions an opportunity to finish before the proxy closes their client connections. It reduces disruption during a planned stop, but depends on the application's shutdown deadline and client reconnection behavior.

## Stop accepting, then drain

The application stops accepting new connections and signals existing sessions to drain. A session that has no active backend work can close promptly. A busy session continues processing until it reaches an idle state that is safe to release.

At that point, the proxy sends an administrative-shutdown error and closes the client connection. The client or its connection pool must establish a new connection to an available proxy. Sessions and open transactions are not transferred to another process.

This describes the current PG Proxy library's drain implementation. Requiring reconnection on shutdown is not a rule of transaction pooling or the PostgreSQL protocol; other implementations can support connection handoff during a restart.

## Deadlines still apply

The application controls how long it waits for sessions and pools to close. An unfinished transaction, including a client idle inside a transaction, can prevent draining from completing within that time. Forced termination, network failures, and shutdown deadlines can still interrupt work.

The local reference binary handles `SIGINT` and `SIGTERM`, stops acceptance, and allows up to `proxy.shutdown_grace_period` for cleanup (30 seconds by default). Set a positive duration such as `30s` or `2m` in YAML, or `shutdownGracePeriod` in JSON. If cleanup exceeds that deadline, it reports an error and exits unsuccessfully. This is the example's policy, not a fixed library-wide grace period.

## Client behavior matters

Client pools should discard closed connections and reconnect through the application's configured endpoint. An interrupted connection does not always reveal whether a write committed. Retry decisions must account for the transaction's possible outcome; reconnection alone does not make replaying a write safe.

See [Timeouts](../../reference/configuration/timeouts/), [Transaction pooling](../pooling/), and [Local development](../../how-to/local-development/) for related behavior.
