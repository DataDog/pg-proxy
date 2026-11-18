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
| `Forwarding to postgres` | Forwarding messages to the backend. |
| `Query Execution and proxying` | Query execution and proxy work. |
| `Batch execution and proxying` | Grouped batch execution. |
| `Returning rows to client` | Returning results. |

The OpenTelemetry adapter marks the incoming request span as `SERVER` and its processing child spans as `INTERNAL`. The native Datadog adapter preserves its existing `server` kind for both root and child spans. Both adapters set the database system to `postgres`. The exact query `-- ping` does not start a root trace.

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

`Query Execution and proxying` covers the wait for backend results and proxy work; it is not a pure measurement of PostgreSQL execution time. `Returning rows to client` helps separate result delivery from that work. Large results or a slow-reading client can increase delivery time without a slow SQL plan.

For batches, use `Batch execution and proxying` rather than assuming every query has its own complete span tree. Trace visibility also depends on the application's tracer setup and sampling.

## Recorded data

The root span receives the query text as `db.statement`. The trace state tracks up to 15 protocol-message types; `DataRow` and `CopyData` messages are excluded from that list. Additional messages set `messages_truncated`.

For OTel export, SQL and captured parameter strings containing invalid UTF-8 are quoted with byte escapes, such as `\xe9`. Valid UTF-8 stays unchanged. This prevents one query from causing the entire export batch to fail; the SQL and parameters sent to PostgreSQL stay unchanged.

Completed child spans can include `messages`, `rowsReturned/Affected`, and `rowsTotalSize`. The row fields are added when the tracked total size is positive. Non-EOF errors are attached when a span finishes; EOF-only finishes are not marked as errors by this code.

The `pg-proxy` binary omits bind parameter values by default in both Datadog and OTel
modes. Set `-trace-parameters=true` to capture actual values as `db.parameters`,
without obfuscation. The library also defaults to capture disabled; embedders can
set `server.RuntimeConfig.EnableTraceParameters` (or `session.Config.EnableTraceParameters`
when constructing sessions directly) to `true` to enable it.

Values are recorded only when the root span's head sampling decision keeps the
trace: a Datadog sampling priority above zero, or an OTel span that is recording
and sampled. Traces kept later, for example because they contain errors, omit
`db.parameters`. Tracers that do not implement `pgproxy.SamplingReporter` are
treated as keeping every trace.

Disabling capture skips parameter formatting and trace tagging; it does not change
the values forwarded to PostgreSQL. SQL text in `db.statement`, including literal
values, and error messages are unaffected. This option is not SQL obfuscation or
a general telemetry redaction policy.

See [Telemetry configuration](../) for exporter selection, destinations, and sampling options.
