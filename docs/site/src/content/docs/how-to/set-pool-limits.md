---
title: "Set connection and session limits"
description: "Set backend pool limits and understand the OSS binary's fixed client-session cap."
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

The OSS binary currently fixes its client-session cap at 100. Changing `max_conns` does not change that cap, and there is no session-limit flag. Embedders can supply a configured admission limiter; see [Go library configuration](../../reference/configuration/library/).

See [Transaction pooling](../../explanation/pooling/) for why these limits are separate.
