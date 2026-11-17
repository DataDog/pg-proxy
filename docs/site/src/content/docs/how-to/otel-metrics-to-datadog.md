---
title: "Send OTel metrics to Datadog"
description: "Configure the proxy and Collector to export event counters as Datadog rates."
---

Use this setup when sending PG Proxy's OpenTelemetry metrics to Datadog through a Collector. It converts event counts to per-second rates, matching the format used by DogStatsD. For example, 100 events over 10 seconds becomes a rate of 10/s.

If you use `-telemetry datadog`, the proxy already sends metrics through DogStatsD; this Collector setup is unnecessary.

## Prerequisites

- `pg-proxy` on your PATH and a working [database configuration](../local-development/#2-run-against-a-development-database).
- [OpenTelemetry Collector Contrib 0.163.2](https://github.com/open-telemetry/opentelemetry-collector-releases/releases/tag/v0.163.2), the version validated with this configuration.
- A Datadog API key and your Datadog site.

The examples use Bash or Zsh and run the proxy and Collector on the same machine. The Collector receives **metrics only** on `127.0.0.1:4318`.

## 1. Configure the Collector

Save this as `collector.yaml`. The repository keeps the same file as [`docs/otel-collector-datadog.yaml`](https://github.com/DataDog/pg-proxy/blob/main/docs/otel-collector-datadog.yaml).

```yaml
receivers:
  otlp:
    protocols:
      http:
        endpoint: 127.0.0.1:4318

processors:
  transform/pg_proxy_datadog_rates:
    error_mode: propagate
    metric_statements:
      - context: datapoint
        statements:
          - >-
            set(datapoint.attributes["datadog.metric.as_type"], "rate")
            where instrumentation_scope.name == "pg-proxy"
            and IsMatch(metric.name, "^datadog[.]pg_proxy[.]")
            and metric.type == METRIC_DATA_TYPE_SUM
            and metric.is_monotonic == true
            and metric.aggregation_temporality == AGGREGATION_TEMPORALITY_DELTA
  batch: {}

exporters:
  datadog:
    api:
      key: ${env:DD_API_KEY}
      site: ${env:DD_SITE}

service:
  pipelines:
    metrics:
      receivers:
        - otlp
      processors:
        - transform/pg_proxy_datadog_rates
        - batch
      exporters:
        - datadog
```

The transform applies only to monotonic delta counters in the `pg-proxy` instrumentation scope with the `datadog.pg_proxy.` prefix. It leaves gauges, histograms, and other scopes unchanged. If you embed the library with a different `MetricNamespace`, change the pattern to match, for example `^acme[.]pg_proxy[.]` for `acme.`. If you export to several backends, use this transform only in the Datadog pipeline.

## 2. Start the Collector

In the Collector's terminal, set your API key and site. Replace the API-key placeholder; `datadoghq.com` is the US1 site.

```bash
export DD_API_KEY='<your-datadog-api-key>'
export DD_SITE=datadoghq.com

otelcol-contrib \
  --config collector.yaml \
  --feature-gates=exporter.datadogexporter.InferIntervalForDeltaMetrics
```

The feature gate is required in this version. It lets the exporter divide each count by its reporting interval. Without it, a count can be labeled as a rate without being converted to events per second.

## 3. Start the proxy

In another terminal, select delta export and send only metrics to the Collector:

```bash
export OTEL_METRICS_EXPORTER=otlp
export OTEL_TRACES_EXPORTER=none
export OTEL_LOGS_EXPORTER=none
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318
export OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE=delta
export OTEL_METRIC_EXPORT_INTERVAL=1000

pg-proxy -config proxy.yaml
```

Omit `-telemetry` so these per-signal settings take effect. The one-second export interval is for local feedback; the SDK default is 60 seconds. Add trace and log pipelines to the Collector before enabling those exporters.

## 4. Check the result

Run queries through the proxy and look for `datadog.pg_proxy.messages.queries` in Datadog. The counter should be reported as a rate. SQL text and bind parameters are not part of this metric.

Local validation of this configuration decoded the Datadog export payload: 100 events over 10 seconds produced a `RATE` value of `10` with a 10-second interval. Control metrics retained their existing mapping.

That check does not establish compatibility with every existing dashboard or monitor. Before a rollout that sends the same metric names through both OTLP and DogStatsD, verify their combined values and queries in Datadog, including after process restarts. The proxy's native DogStatsD path is unchanged.
