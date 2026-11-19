---
# Unless explicitly stated otherwise all files in this repository are licensed
# under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/)
# Copyright 2026-Present Datadog, Inc.
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

For OTel export, SQL containing invalid UTF-8 is quoted with byte escapes, such as `\xe9`. Valid UTF-8 stays unchanged. Parameter tags are always valid UTF-8, because invalid bytes in them are already escaped as `\xNN`. This prevents one query from causing the entire export batch to fail; the SQL and parameters sent to PostgreSQL stay unchanged.

Completed child spans can include `messages`, `rowsReturned/Affected`, and `rowsTotalSize`. The row fields are added when the tracked total size is positive. Non-EOF errors are attached when a span finishes; EOF-only finishes are not marked as errors by this code.

The `pg-proxy` binary omits bind parameter values by default in both Datadog and OTel
modes. Set `-trace-parameters=true` to capture actual values, without obfuscation.
The library also defaults to capture disabled; embedders can
set `server.RuntimeConfig.EnableTraceParameters` (or `session.Config.EnableTraceParameters`
when constructing sessions directly) to `true` to enable it.

With capture on, the root span carries one tag per parameter position,
`db.query.parameter.0` through `db.query.parameter.n`, taken from the first Bind of the
trace. `$1` is `db.query.parameter.0`. Each position also gets its type name as
`pgproxy.bind.type.N`, for example `int8`, `timestamptz`, `int4[]` or `unknown`.
The type is the one declared in Parse, else the one PostgreSQL reported for the
statement in a ParameterDescription, else unknown. A type declared as 0 or
`unknown` counts as not declared.

Values are strings, including numbers, so values above 2^53 keep their
precision. Use `pgproxy.bind.type.N` to tell how to read a position.

| Value | Tag value |
| --- | --- |
| NULL | `null` |
| Text format | The first 256 bytes, then `(+N bytes)` for the rest |
| Binary `bool`, `int2`, `int4`, `int8`, `oid`, `float4`, `float8`, `numeric`, `uuid`, `date`, `timestamp`, `timestamptz` | PostgreSQL's text form; timestamps are shown in UTC |
| Binary `text`, `varchar`, `bpchar`, `name`, `json`, `jsonb`, `xml` | As text |
| Binary arrays of those types | `array[len=N]{…}` with the first 10 elements; text elements are double-quoted |
| Any other type, malformed binary value, or format code other than text or binary | `\x` and the hex of the first 128 bytes, then `(+N bytes)` |

The tags total at most about 4 KB and 256 positions. When a limit drops
positions, the root also gets `pgproxy.bind.truncated=true` and
`pgproxy.bind.rendered` with the number of positions emitted. Invalid UTF-8 is
escaped as `\xNN` for both tracers.

Each position adds two attributes, a value and a type. The OpenTelemetry SDK
keeps 128 attributes per span by default, so the `pg-proxy` command raises the
limit to 576 unless `OTEL_SPAN_ATTRIBUTE_COUNT_LIMIT` or
`OTEL_ATTRIBUTE_COUNT_LIMIT` is set. Embedders that build their own tracer
provider should do the same. With a lower limit, the SDK drops the positions
that do not fit and counts them in the span's dropped-attribute count;
`pgproxy.bind.truncated` and `pgproxy.bind.rendered` describe only the proxy's
own limits. They are set before the positions, so the SDK keeps them.

Values are recorded only when the root span's head sampling decision keeps the
trace: a Datadog sampling priority above zero, or an OTel span that is recording
and sampled. Traces kept later, for example because they contain errors, omit
the parameter tags. Tracers that do not implement `pgproxy.SamplingReporter` are
treated as keeping every trace.

Disabling capture skips parameter formatting and trace tagging; it does not change
the values forwarded to PostgreSQL. SQL text in `db.statement`, including literal
values, and error messages are unaffected. This option is not SQL obfuscation or
a general telemetry redaction policy.

See [Telemetry configuration](../) for exporter selection, destinations, and sampling options.
