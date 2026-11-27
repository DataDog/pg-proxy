---
# Unless explicitly stated otherwise all files in this repository are licensed
# under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/)
# Copyright 2026-Present Datadog, Inc.
title: "Benchmarks"
description: "PG Proxy throughput and latency under pgbench, and the settings used to measure them."
---

This page reports PG Proxy's throughput and latency for two pgbench workloads. Measure your own workload before you size a deployment.

:::note[Hardware impact]
All runs used one AWS EC2 `c8a.4xlarge` instance. Hardware has a large effect on throughput and latency, so expect different absolute numbers on your hardware. We have measured only this host.
:::

## What PG Proxy is built for

PG Proxy is useful when an application needs to compose PostgreSQL pooling with its own authentication, routing configuration, admission policy, and telemetry. [Compare poolers](../pooler-comparison/) describes how this differs from standalone poolers.

See [Transaction-pooling compatibility](../../reference/compatibility/) and [Settings](#settings).

## Method

<div class="bench-table">

| Setting | Value |
| --- | --- |
| Host | AWS EC2 `c8a.4xlarge`: AMD EPYC 9R45, 16 cores, 32 GiB, Ubuntu 24.04 |
| Topology | pgbench, PG Proxy, and PostgreSQL on the same host |
| PostgreSQL | 17.12 |
| pgbench | 17.12 |
| PG Proxy | Telemetry off, no TLS, no client authentication |
| Pooling | Transaction pooling, `max_conns: 10` |
| Pooler CPUs | 4, set with `GOMAXPROCS` |

</div>

## Primary-key SELECT

`pgbench -S` runs one `SELECT` by primary key on `pgbench_accounts` per transaction.

<div class="bench-table">

<!-- bench:suite-a-4cpu:start -->
| Protocol | Clients | TPS | Latency (mean) |
| --- | ---: | ---: | ---: |
| simple | 1 | 40,448.35 | 0.024 ms |
| simple | 10 | 190,218.65 | 0.052 ms |
| simple | 100 | 196,234.38 | 0.508 ms |
| extended | 1 | 34,918.46 | 0.028 ms |
| extended | 10 | 165,322.68 | 0.060 ms |
| extended | 100 | 165,154.67 | 0.604 ms |
| prepared | 1 | 44,465.61 | 0.022 ms |
| prepared | 10 | 186,758.08 | 0.053 ms |
| prepared | 100 | 179,124.73 | 0.557 ms |
<!-- bench:suite-a-4cpu:end -->

</div>

## TPC-B-like transaction

`pgbench -b tpcb-like`, pgbench's default script, runs three `UPDATE`s, a `SELECT`, and an `INSERT` per transaction. The data is initialized again before each measurement.

<div class="bench-table">

<!-- bench:suite-b-4cpu:start -->
| Protocol | Clients | TPS | Latency (mean) |
| --- | ---: | ---: | ---: |
| simple | 1 | 6,494.84 | 0.154 ms |
| simple | 10 | 29,630.58 | 0.336 ms |
| simple | 100 | 29,606.39 | 3.377 ms |
| extended | 1 | 5,670.03 | 0.176 ms |
| extended | 10 | 25,721.22 | 0.388 ms |
| extended | 100 | 25,219.78 | 3.964 ms |
| prepared | 1 | 7,422.19 | 0.134 ms |
| prepared | 10 | 28,767.11 | 0.347 ms |
| prepared | 100 | 29,163.36 | 3.428 ms |
<!-- bench:suite-b-4cpu:end -->

</div>

## Reading these results

- pgbench, PG Proxy, and PostgreSQL share one host, so the results include no network latency.
- The pool has 10 backend connections. With 100 clients, clients wait for a connection.
- To compare poolers on your own workload, see [Performance comparison](../pooler-comparison/#performance-comparison).

## Settings

These runs disable telemetry and set `reset_mode: dynamic`, `reply_buffer_pool: true`, and `client_disconnect_timer: true` to measure the raw cost of PG Proxy's internals. Each setting comes with performance tradeoffs. Test them against your own workload before you change the defaults.
