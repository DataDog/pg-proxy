---
# Unless explicitly stated otherwise all files in this repository are licensed
# under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/)
# Copyright 2026-Present Datadog, Inc.
title: "Set connection and session limits"
description: "Set backend pool limits and configure global and per-user client-session caps."
---

Use this guide to change a backend pool limit in the OSS binary. It assumes you have a working `proxy.yaml` and can connect through the proxy using the [local development guide](../local-development/).

## 1. Stop the current proxy

Close active client sessions, then press `Ctrl+C` in the proxy terminal. File changes require a restart.

## 2. Set the backend pool limit

In `proxy.yaml`, change `max_conns` on the relevant entry in `databases`. For example:

```yaml
databases:
  - name: primary
    host: 127.0.0.1
    port: 5432
    dbname: postgres
    username: postgres
    password: postgres
    sslmode: disable
    max_conns: 5
```

Keep the rest of your configuration, including `clusters`, `logical_databases`, and `proxy`. Zero or omitted pool settings use pgx defaults; negative values and minimum connections above the maximum are rejected.

## 3. Restart and check connectivity

From your `pg-proxy` checkout, run the previously built executable:

```sh
./bin/pg-proxy \
  -config proxy.yaml -telemetry none
```

Wait for `local proxy ready; clients are trusted`. In another terminal:

```sh
psql 'postgres://postgres@127.0.0.1:15432/local_db?sslmode=disable' \
  -At -v ON_ERROR_STOP=1 -c 'select 1'
```

The result should be `1`. Adapt the client username and logical database if you changed them. This checks connectivity after restarting; it does not test behavior under load.

## Client-session limits

Set client-session limits separately from backend pool sizes:

```yaml
proxy:
  max_sessions: 100000
  default_max_user_sessions: 10000
```

Keep the rest of the configuration. These are also the defaults when either field is omitted or zero. Set `users[].max_sessions` to override the default for one password-authenticated user; omitted or zero inherits `default_max_user_sessions`. Negative limits are rejected. JSON names are `maxSessions` and `defaultMaxUserSessions` under `proxy`, and `maxSessions` under each user.

The global limit applies to the whole proxy process. A user's limit covers all its logical databases. In loopback trust mode, the per-user default applies to the configured backend username. A limit rejects the next established session with SQLSTATE `53300`; it does not disconnect existing sessions. Failed authentication does not consume a session slot, and a disconnect releases it.

Changing `max_conns` does not change these caps. The defaults allow 100,000 total client sessions and 10,000 per user, rather than the previous fixed 100. Choose lower limits when the proxy's memory budget or workload needs them. Restart after editing the file. Embedders can supply their own admission limiter; see [Go library configuration](../../reference/configuration/library/).

See [Transaction pooling](../../explanation/pooling/) for why these limits are separate.
