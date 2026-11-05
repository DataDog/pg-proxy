# Differential protocol verification

The harness compares the same PostgreSQL conversation directly against PostgreSQL
and through PgProxy. It also checks explicit expected responses and committed
state, so matching transcripts alone cannot make an incorrect run pass.

## Run

Run commands from the repository root with Go 1.26 or newer, a C compiler,
and Docker. Missing integration prerequisites fail tests; they never skip them.

```sh
go test ./internal/protocoltest
PGPROXY_TEST_REVISION="$(git rev-parse HEAD)" \
  go test -tags=integration -count=1 -timeout=10m -v ./internal/protocoltest/integration
```

The fixture defaults to public image `postgres:14.20` pinned to digest
`sha256:062b02051873a101837543675b5b4601cba06b1972d22f4b7969adbde5a280e4`.
It needs no Datadog account, private registry, or telemetry service. Repository
access remains separate from these test dependencies. An explicit
`PGPROXY_TEST_POSTGRES_IMAGE` overrides the default; callers own that image's
availability and pinning. Reports record the requested/resolved image, image ID,
and PostgreSQL version. The dedicated GitHub Actions job uses the default image
on GitHub-hosted Ubuntu and uploads failure reports for 14 days. Other Docker
suites retain their existing fixture and CI policies.

Tests use three fresh databases per case: direct reference, second direct
control, and proxy. The proxy uses the real TCP listener, backend, link, and
pgx pool. Local containers bind loopback. With a separate Docker daemon, the
fixture uses its reachable host and published port. Only the fixture's own
container may use a non-loopback endpoint. Authentication uses trust; TLS and
authentication equivalence are outside this milestone.

To use an existing disposable server, set `PGPROXY_TEST_ADMIN_DSN` and
`PGPROXY_TEST_ALLOW_DATABASE_CREATION=1`. The server must expose one loopback TCP
endpoint with `sslmode=disable`, no fallback hosts, trust authentication for test
clients, and an admin role that can create/drop databases.

Select one corpus variant:

```sh
go test -tags=integration -count=1 -timeout=10m -v ./internal/protocoltest/integration \
  -run '^TestDifferential/extended_error_sync/buffer_64_chunk_7$'
```

Set `PGPROXY_TEST_ARTIFACT_DIR` to choose the report directory. Set
`PGPROXY_TEST_KEEP_ARTIFACTS=1` to retain successful cases. Every failed
conversation writes `report.json`, `scenario.json`, and executable `replay.sh`.
Run a saved case from the checkout under test:

```sh
/path/to/pgproxy-protocol-XXXXXX/replay.sh
# Equivalent explicit invocation:
go test -tags=integration -count=1 -timeout=10m -v ./internal/protocoltest/integration \
  -run '^TestDifferential$' -args -protocol-replay /path/to/report.json
```

Replay loads the saved scenario, buffer size, write chunk size, and image. It
rejects a different PostgreSQL version and creates fresh databases. It records
the revision of the checkout under test. Replaying an external-server report
requires the same explicit disposable-server options. Replay never resumes a
partly executed conversation or retries its writes.

## Coverage

The corpus lives in `internal/protocoltest/integration/testdata/`.

| Scenario | Required behavior |
| --- | --- |
| `simple_queries` | Empty and multi-statement queries; ordered rows; NULL versus empty values. |
| `extended_lifecycle` | Parse, statement/portal Describe, Bind, Execute, Close and Sync. |
| `named_reassignment` | Reuse a named statement after observed backend replacement; close and recreate it. |
| `unnamed_replacement` | Replace the unnamed statement and portal inside a transaction. |
| `flush_before_sync` | Receive Parse and execution replies before sending Sync. |
| `multi_pipeline_segments` | Queue three Sync-delimited segments before receiving results. |
| `extended_error_sync` | Roll back an earlier write, discard a later write until Sync, then commit the next segment. |
| `suspended_portal` | Continue a suspended portal across Sync inside an explicit transaction. |
| `binary_values` | Preserve binary int4/bytea, embedded zero bytes, NULL and empty values. |
| `transaction_effects` | Commit, rollback, aborted transaction, and `I`/`T`/`E` transitions. |

Each case ends with a query on the original client connection. Control
connections check declared database effects after the conversation finishes.
Each case runs with proxy buffers of 64/4096 bytes and frontend write chunks of
0/7 bytes (`0` writes the complete batch). `TestBufferBoundaries` separately
generates DataRow frames of size B-1, B and B+1 for each buffer B; for example,
`TestBufferBoundaries/buffer_64_below`. TCP need not preserve write boundaries.
A direct-versus-direct run checks fixture repeatability before judging the proxy.
The corpus and boundary cases are deterministic; no random seed is used.

COPY, cancellation, disconnect/uncertain-commit scenarios, an independent libpq
client, stateful generation and shrinking are not implemented in this milestone.
Session-state persistence across transaction boundaries is not a supported
baseline assumption. Deliberate PgProxy differences require explicit contract
tests and review; current behavior alone does not establish that contract.
Keep the equivalence comparator strict. Do not widen normalization or silently
remove a failing scenario.

## Scenario format

JSON version 1 contains `name`, optional `setup` SQL, `steps`, and optional
`effects`. Unknown and duplicate fields fail decoding. Each step has exactly one
operation: `send`, `flush_transport`, `receive`, or `reassign_backend`.

```json
{
  "version": 1,
  "name": "parse_flush_sync",
  "steps": [
    {"send": [{"type": "Parse", "name": "s", "sql": "SELECT 1"}, {"type": "Flush"}]},
    {"flush_transport": true},
    {"checkpoint": "parsed", "receive": [{"type": "ParseComplete"}]},
    {"send": [{"type": "Sync"}]},
    {"flush_transport": true},
    {"checkpoint": "idle", "receive": [{"type": "ReadyForQuery", "status": "I"}]}
  ]
}
```

`send` queues messages. `flush_transport` writes queued bytes. A protocol `Flush`
asks PostgreSQL to emit pending responses; it has no response of its own and
does not substitute for `Sync`. Receive checkpoints list every expected message
in order. A timeout is a failure, never an implicit end-of-response marker.

Messages support `Query`, `Parse`, `Bind`, `Describe`, `Execute`, `Close`, `Flush`
and `Sync`. Describe/Close use `object: "S"` or `"P"` and `name`. Bind uses
`statement`, `portal`, `parameter_formats`, `parameters` and `result_formats`.
Parameters and expected DataRow `values` contain hexadecimal bytes: `"31"` is
text `1`, `"00000001"` is binary int4 `1`, `null` is SQL NULL, and `""` is a
non-NULL zero-length value. Effect `rows` instead contain plain text or null.

Expectations may assert `code` for ErrorResponse, `tag` for CommandComplete,
`status` for ReadyForQuery, and `values` for DataRow. The differential oracle
compares all decoded fields even when an expectation specifies only the type.

## Oracle and diagnostics

The runner preserves immutable frontend/backend frames and full decoded fields.
Frontend events record the queued client schedule, not proof of backend receipt.
It compares message order, row bytes, metadata, command tags, error fields and
transaction status. Relation OIDs in RowDescription map through catalog-verified
relation names; zero and built-in type OIDs remain exact. Diagnostic field ordering
is canonicalized, while every error and notice field remains significant. It does not discard
notices, reorder rows, ignore error details, or use pgproto3 trace text as an
oracle. Startup credentials and cancellation secrets are redacted; startup
exchange ordering is outside the post-authentication comparison.

Failure artifacts include the scenario, transcripts, effects, configuration,
versions, first mismatch and replay command. Per-conversation fixture setup
failures include the failing stage, cause, and any cleanup error. `PGPROXY_TEST_ARTIFACT_DIR` selects
an output directory; otherwise the harness uses `TEST_UNDECLARED_OUTPUTS_DIR`
or prints a temporary directory. Set `PGPROXY_TEST_REVISION` to record the
source revision when invoking `go test`. Reports bound proxy logs to 1 MiB and
PostgreSQL logs to 2 MiB. Conversations bound frame size, message count, total
transcript bytes, receive/write time, and fixture cleanup time.

To add a case, write deterministic SQL with explicit ordering, list expected
responses, finish with a recovery query, and declare committed effects where
relevant. Keep unsupported behavior visible as a failure. Run the unit and
integration commands above after adding a case. The harness has no AI/LLM
dependency in execution, replay, assertions, or comparison.

The design adapts ideas from [PgCat's paired wire tests](https://github.com/postgresml/pgcat/blob/5b038813eb14f181434ab7b5509e74d9b1fe123b/tests/ruby/protocol_spec.rb),
[pgdog-regress's strict comparison tests](https://github.com/pgdogdev/regress/blob/22032787eeaef9bf97aaa3216735a7369f1f9125/pgdog-regress/src/comparison.rs),
and [libpq's pipeline corpus](https://github.com/postgres/postgres/blob/4848fd6752ad6b88c5a066442df35ba208dfcec5/src/test/modules/libpq_pipeline/libpq_pipeline.c).
Their source is not imported into this harness.
