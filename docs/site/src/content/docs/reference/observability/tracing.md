---
title: "Tracing"
description: "Query spans, propagation, and trace metadata in the current library."
---

PG Proxy records query and protocol spans with either Datadog or OpenTelemetry. See [Telemetry configuration](../) for enabling tracing, choosing an exporter, and setting sampling. For application-owned providers, see [Go telemetry configuration](../../configuration/library/#telemetry-settings).

## Span operations

| Operation | Purpose |
| --- | --- |
| `Proxy forwarding ` | Root operation; the current name includes a trailing space. |
| `Handling messages` | Client-message handling. |
| `Forwarding to postgres` | Sending buffered messages to the backend. |
| `postgres.request` | Client span for a backend exchange, from send through response completion. |
| `Query Execution and proxying` | Query execution and proxy work. |
| `Batch execution and proxying` | Grouped batch execution. |
| `Returning rows to client` | Returning results. |

Both adapters mark incoming root spans as `server`, processing stage spans as `internal`, and `postgres.request` spans as `client`. Native Datadog database spans use type `sql` and `db.system:postgres`; OpenTelemetry database spans use `db.system:postgresql`. The exact query `-- ping` does not start a root trace.

## Propagation

Without incoming context, each query or protocol batch gets a proxy-local root
span and child spans. For OpenTelemetry, leading or trailing sqlcommenter blocks
can supply W3C `traceparent` and URL-encoded `tracestate`:

```sql
SELECT 1 /*traceparent='00-0123456789abcdef0123456789abcdef-0123456789abcdef-01'*/
```

The first valid context wins: a valid leading context takes precedence. An
ordinary leading comment, metadata-only comment, or invalid leading context does
not hide a valid trailing context.

In Go, use the application's active span and the standard propagator to obtain
that context before sending SQL:

```go
carrier := propagation.MapCarrier{}
propagation.TraceContext{}.Inject(ctx, carrier)
// sql is the query text. Avoid adding per-request context to reusable named SQL.
if parent := carrier.Get("traceparent"); parent != "" {
    sql += " /*traceparent='" + url.PathEscape(parent) + "'"
    if state := carrier.Get("tracestate"); state != "" {
        sql += ",tracestate='" + url.PathEscape(state) + "'"
    }
    sql += "*/"
}
```

This fragment uses `net/url` and `go.opentelemetry.io/otel/propagation`; the client
must send SQL through a protocol mode that includes its text for that request.
It follows [sqlcommenter formatting](https://google.github.io/sqlcommenter/spec/).

The client must replace these example IDs with the active application context.
The root then shares the application's trace ID and uses the supplied span as
its remote parent. Sampling and trace state are preserved. Invalid or absent
context starts a local trace. SQL is forwarded unchanged; PostgreSQL does not
automatically become an instrumented participant.

Context is available when Query or Parse contains SQL. Reusing a prepared
statement through Bind/Execute alone cannot provide fresh application context.

The session code warns when named prepared statements contain per-query trace context: changing SQL text for each trace undermines prepared-statement reuse.

## Interpreting durations

`postgres.request` starts after connection acquisition and before the buffered send. It ends after the expected backend responses are consumed, before pool release and the final client flush. It measures a wire-protocol exchange, which may contain multiple statements or only part of an extended-query interaction. Streaming results can include time waiting for the client to consume earlier rows; this is not server-only SQL execution time.

PostgreSQL `ErrorResponse` messages mark both the database span and the root request as errors, including errors followed by a successful `ReadyForQuery`. Transport failures also finish the database span with an error. A client write failure after backend completion does not change the completed database span.

The acquired connection supplies the actual database name, host, and port. Native Datadog spans use `db.instance`, `out.host`, and `out.port`; OpenTelemetry spans use `db.namespace` (also `db.name` for compatibility), `server.address`, and `server.port`. These fields do not contain credentials or connection strings. If a custom link does not supply destination metadata, these tags are omitted.

Custom tracers keep the existing `Tracer` and `Span` interfaces. To emit database spans, their root spans must also implement `pgproxy.DatabaseSpanStarter`. The built-in adapters implement it. Embedders that decorate links should capture destination metadata through the library backend's checkout path; that path saves it before applying the decorator.

`Query Execution and proxying` covers the wait for backend results and proxy work; it is not a pure measurement of PostgreSQL execution time. `Returning rows to client` helps separate result delivery from that work. Large results or a slow-reading client can increase delivery time without a slow SQL plan.

For batches, use `Batch execution and proxying` rather than assuming every query has its own complete span tree. Trace visibility also depends on the application's tracer setup and sampling.

## Recorded data

The root span receives the query text as `db.statement`. The database span receives that statement when known, except for batches where naming only the first statement would be misleading. The trace state tracks up to 15 protocol-message types; `DataRow` and `CopyData` messages are excluded from that list. Additional messages set `messages_truncated`.

For OTel export, SQL and captured parameter strings containing invalid UTF-8 are quoted with byte escapes, such as `\xe9`. Valid UTF-8 stays unchanged. This prevents one query from causing the entire export batch to fail; the SQL and parameters sent to PostgreSQL stay unchanged.

Completed child spans can include `messages`, `rowsReturned/Affected`, and `rowsTotalSize`. The row fields are added when the tracked total size is positive. Non-EOF errors are attached when a span finishes; EOF-only finishes are not marked as errors by this code.

The OSS binary omits bind parameter values by default in both Datadog and OTel
modes. Set `-trace-parameters=true` to capture actual values as `db.parameters`,
without obfuscation. The library also defaults to capture disabled; embedders can
set `server.RuntimeConfig.EnableTraceParameters` (or `session.Config.EnableTraceParameters`
when constructing sessions directly) to `true` to enable it.

Disabling capture skips parameter formatting and trace tagging; it does not change
the values forwarded to PostgreSQL. SQL text in `db.statement`, including literal
values, and error messages are unaffected. This option is not SQL obfuscation or
a general telemetry redaction policy.

See [Telemetry configuration](../) for exporter selection, destinations, and sampling options.
