---
title: "Run pgbench benchmarks"
description: "Measure proxy throughput, latency, CPU, memory, and allocations per query, and compare two builds."
---

The pgbench harness in `test/perf/pgbench` starts PostgreSQL and pg-proxy, runs `pgbench` through the proxy, and writes a results directory. With `-b`, it compares two builds on identical settings.

Each run reports:

- TPS and average latency, from pgbench.
- p50 and p99 client latency, from pgbench per-transaction logs.
- Proxy CPU per query, split into user and system time.
- Proxy RSS at the end of the run, peak RSS, Go live heap, and Go runtime memory.
- Allocated bytes and objects per query.

The harness is not part of CI. Run it on demand.

## Prerequisites

- Go 1.26 or newer.
- `pgbench` on your `PATH`, or `-pgbench /path/to/pgbench`. Any recent PostgreSQL client package provides it.
- Docker, for the default PostgreSQL container. The harness reuses the integration-test container setup in `internal/itest`. To use an existing server instead, pass `-postgres postgres://user:password@host:port/dbname`.

## Measure one build

From the repository root:

```sh
go run -tags integration ./test/perf/pgbench
```

This measures the working tree, including uncommitted changes. Results go to `./pgbench-results/<UTC timestamp>/`. That directory is ignored by Git.

## Compare two builds

Pass a git ref, `.` for the working tree, or a pg-proxy binary path to `-a` and `-b`:

```sh
go run -tags integration ./test/perf/pgbench -a origin/main -b HEAD
```

The harness checks each ref out into a temporary worktree and builds both with the same toolchain and flags. For each client count and repeat, it runs A and B, alternating which runs first. Every run uses a fresh proxy process against the same PostgreSQL server. `summary.md` reports B − A for each repeat, their mean, range, and how many pairs favour B.

Run an A/A comparison (`-a main -b main`) first on a new host. Its spread is the smallest difference that host can resolve.

## Defaults

| Flag | Default | Meaning |
| --- | --- | --- |
| `-script` | `SELECT 1` | pgbench script file |
| `-protocol` | `simple` | `simple`, `extended`, or `prepared` |
| `-clients` | `16` | Comma-separated pgbench client counts, for example `1,16,64` |
| `-pool-size` | `16` | Backend connections (`max_conns`) |
| `-gomaxprocs` | `1` | Proxy `GOMAXPROCS`; empty uses the Go default |
| `-warmup` | `10s` | Unmeasured pgbench run before each measured run |
| `-duration` | `30s` | Measured pgbench run |
| `-repeats` | `3` | Repeats per variant and client count; A/B mode needs at least 3 |
| `-threads` | `4` | Maximum pgbench threads (`-j`) |
| `-proxy-cpus`, `-client-cpus` | unset | Linux only: pin the proxy or pgbench with `taskset -c` |
| `-pprof` | off | Save a CPU profile of each measured run and an allocs profile after it |
| `-proxy-args` | unset | Extra pg-proxy arguments, for example `"-log-level WARN"` |

The harness removes `OTEL_*`, `DD_*`, `PG*`, `GOGC`, `GOMEMLIMIT`, and `GODEBUG` from the proxy environment, so telemetry exporters and shell settings do not change results.

## Output

| Path | Content |
| --- | --- |
| `summary.md` | Host and settings, per-variant medians, paired differences, and every run |
| `results.json` | The same data plus the manifest: host, Go and pgbench versions, PostgreSQL version, commits, binary SHA-256 |
| `runs/c<clients>-r<repeat>-<variant>/` | Proxy config and log, pgbench output, and profiles with `-pprof` |

Inspect profiles with `go tool pprof`, for example `go tool pprof -sample_index=alloc_space runs/c16-r1-A/allocs.pprof`.

## How the harness measures

The harness samples the proxy immediately before and after the measured pgbench run. Per-query values are counter differences divided by the transactions pgbench processed. The window includes connecting and disconnecting the pgbench clients; with 16 clients this is negligible against hundreds of thousands of queries.

Builds from git refs include `test/perf/pgbench/overlay/benchmetrics.go`, added with `go build -overlay` without changing the checked-out source. It serves `getrusage` CPU times, `runtime/metrics` counters, and `net/http/pprof` on a loopback port the harness chooses. It runs only when the harness sets `PG_PROXY_BENCH_ADDR`.

Prebuilt binaries usually lack the overlay. For them, the harness reads CPU from `/proc` on Linux or `ps` on macOS, at 10 ms resolution, and reports allocations, heap, and peak RSS as unavailable.

Peak RSS covers the whole process lifetime, including warmup. Latency percentiles use the nearest rank over every transaction in the run.

## Which numbers compare across hosts

Compare these across hosts with care:

- **Allocations per query.** They depend on the code, Go version, architecture, and workload, but not on CPU speed or load. A/A runs on a laptop agreed within 0.1%.
- **Go heap sizes.** They are usually similar across hosts with the same Go version, pool size, and client count.

Do not compare these across hosts, operating systems, or separate invocations:

- **TPS and latency.** They depend on CPU speed, client placement, and the network path to PostgreSQL.
- **Proxy CPU per query, and its user/system split.** System time depends on the kernel's loopback and event-polling cost. On macOS with PostgreSQL in a Docker VM, system time was about 80% of proxy CPU. Do not compare such numbers with Linux measurements, such as the 71.7 µs per query from earlier reviews.
- **RSS.** It depends on page size and the operating system's memory accounting.

For these metrics, use only paired differences from one invocation on one host.

## Resolve small CPU differences

A laptop cannot resolve a 2 µs/query change. In one A/A run on macOS, proxy CPU ranged from 98 to 126 µs/query across repeats of the same binary. To check acceptance criteria such as "saves at least 2 µs/query":

1. Use an idle Linux host with frequency scaling and turbo disabled.
2. Pin the proxy and pgbench to separate physical cores with `-proxy-cpus` and `-client-cpus`.
3. Keep PostgreSQL off those cores. Run it natively with `taskset`, or in a container restricted with `--cpuset-cpus`, and pass `-postgres`.
4. Run an A/A comparison and note the range of the paired CPU differences.
5. Run the A/B comparison with the same settings and at least as many repeats.
6. Accept the change only if every pair favours B and the mean difference is larger than the A/A range.
