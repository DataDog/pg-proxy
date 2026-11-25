// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

const experiments = JSON.parse(
  document.querySelector("#measurements").textContent,
);
const views = {
  "span-pooling": {
    title: "Span pooling",
    pull: 110,
    keys: ["pooling"],
    description: "Recycle finished spans in the standalone Datadog tracer.",
    summary:
      "p99 fell in all three pairs, while throughput was flat within run variation.",
    observability:
      "Trace and APM metric export were preserved. An environment override restores the previous allocator behavior.",
    complexity:
      "Low. One tracer startup option plus a test and rollback documentation.",
    verdict:
      "Canary for the p99 signal and measure allocations and GC before changing the default.",
  },
  "cheaper-tags": {
    title: "Cheaper trace tags",
    pull: 111,
    keys: ["tags"],
    description:
      "Reuse fixed span options and metric tags, bypass SQL comment parsing when no comment marker is present, and defer diagnostics on non-recording spans.",
    summary:
      "All three pairs improved slightly. The no-op simple-query test dropped from 27 to 19 allocations per cycle.",
    observability:
      "Recording spans retain their diagnostic fields and metric tags, including spans eligible for individual retention.",
    complexity:
      "Low to medium. The patch updates tag construction and tracing helpers across the Datadog and OpenTelemetry integrations.",
    verdict:
      "A small, low-risk cleanup that leaves the bulk of the telemetry-on gap.",
  },
  "child-spans": {
    title: "Child-span sampling",
    pull: 112,
    keys: ["sampled", "shadow"],
    description:
      "Record per-operation metrics separately. Sampled mode skips children of rejected proxy-local traces, while shadow mode emits both metric sources.",
    summary:
      "Sampled mode improves throughput by 23.1%. Shadow mode cuts throughput by 5.4% while both sets of signals are emitted. The sampled build included a startup guard that did not affect its benchmark configuration.",
    observability:
      "Sampled mode reduces coverage of child-derived trace.* metrics. DogStatsD covers each completed phase with bounded dimensions and different aggregation. Shadow retains the existing spans.",
    complexity:
      "Moderate code change, high rollout effort. Metric parity and monitor migration need validation.",
    verdict:
      "Prioritize a targeted shadow pilot, then measure sampled mode on traffic with representative trace propagation.",
  },
};
const stats = {
  tps: {
    short: "Throughput",
    label: "Transactions/sec",
    precision: 0,
    unit: "TPS",
    lower: false,
  },
  cpu_us_per_txn: {
    short: "CPU / tx",
    label: "Proxy CPU per transaction",
    precision: 1,
    unit: "µs",
    lower: true,
  },
  p99_ms: {
    short: "p99 latency",
    label: "p99 transaction latency",
    precision: 2,
    unit: "ms",
    lower: true,
  },
};
let metric = "tps";
let view = "overview";
const result = document.querySelector("#result");
const fmt = (n, p = 0) =>
  new Intl.NumberFormat("en-US", {
    minimumFractionDigits: p,
    maximumFractionDigits: p,
  }).format(n);
const signed = (n) =>
  `${n < 0 ? "−" : "+"}${fmt(Math.abs(n), Math.abs(n) < 1 ? 2 : 1)}%`;
const median = (runs, field) =>
  runs.map((run) => run[field]).sort((a, b) => a - b)[
    Math.floor(runs.length / 2)
  ];
const delta = (before, after) => (after / before - 1) * 100;
const benefit = (change) =>
  Math.abs(change) < 0.15
    ? ""
    : change < 0 === stats[metric].lower
      ? "positive"
      : "negative";
const pairValues = (pair, field) => pair.map((run) => run[field]);
const sameVariant = (rows, variant) =>
  rows.filter((row) => row.variant === variant);
const twoMedians = (key, field = metric) =>
  ["baseline", "candidate"].map((label) =>
    median(sameVariant(experiments[key].runs, label), field),
  );
const legend = () =>
  '<div class="legend"><span>Baseline</span><span class="candidate">Candidate</span></div>';
const barRow = (label, value, max, type) =>
  `<div class="bar-row"><span class="bar-label">${label}</span><div class="bar-track" aria-hidden="true"><div class="bar ${type}" style="width:${Math.min(100, (value / max) * 100).toFixed(3)}%"></div></div><span class="bar-amount">${fmt(value, stats[metric].precision)}</span></div>`;
function bars(values, max) {
  return (
    barRow("Baseline", values[0], max, "base") +
    barRow("Candidate", values[1], max, "candidate")
  );
}
function overview() {
  const [off, on] = twoMedians("main", "tps");
  const rows = Object.keys(views).flatMap((key) =>
    views[key].keys.map((run) => [key, run]),
  );
  const maximum =
    Math.max(...rows.flatMap(([, key]) => twoMedians(key))) * 1.04;
  return `<section class="section"><h2>Baseline on/off</h2><p>Datadog telemetry at <code>b117b38</code> with distribution batching enabled.</p><div class="summary"><div class="panel"><p>Telemetry off</p><div class="value">${fmt(off)}</div><small>Transactions/sec</small></div><div class="panel"><p>Datadog telemetry on</p><div class="value">${fmt(on)}</div><small>Transactions/sec</small></div><div class="panel"><p>Throughput change</p><div class="value negative">${signed(delta(off, on))}</div><small>Telemetry on relative to off</small></div></div></section>
  <section class="panel"><div class="chart-head"><div><h3>Results by change</h3><p>${stats[metric].label} · median of three runs</p></div>${metricControl()}</div><div class="overview-grid">${rows
    .map(([page, key]) => {
      const values = twoMedians(key),
        change = delta(...values);
      return `<article class="panel overview-row"><div class="overview-title"><h3><a href="#${page}">${experiments[key].name}</a></h3><span class="overview-delta ${benefit(change)}">${signed(change)}</span></div>${bars(values, maximum)}<p>${experiments[key].caption}</p></article>`;
    })
    .join(
      "",
    )}</div></section>${dataTable(["main", "pooling", "tags", "sampled", "shadow"])}`;
}
function metricControl() {
  return `<div class="toggle" role="group" aria-label="Chart metric">${Object.entries(
    stats,
  )
    .map(
      ([key, s]) =>
        `<button type="button" data-metric="${key}" aria-pressed="${key === metric}">${s.short}</button>`,
    )
    .join("")}</div>`;
}
function detail(page) {
  const first = page.keys[0],
    [baseline, candidate] = twoMedians(first, "tps"),
    change = delta(baseline, candidate);
  const heading =
    page.keys.length > 1 ? "Sampled throughput" : "Candidate throughput";
  const panes = page.keys
    .map((pane) => {
      const rows = experiments[pane].runs;
      const maximum =
        Math.max(
          ...page.keys.flatMap((key) =>
            experiments[key].runs.map((run) => run[metric]),
          ),
        ) * 1.05;
      const medians = twoMedians(pane),
        diff = delta(...medians);
      return `<article class="panel"><div class="chart-head"><div><h3>${page.keys.length > 1 ? experiments[pane].name : "Median comparison"}</h3><p>Median ${fmt(medians[0], stats[metric].precision)} to ${fmt(medians[1], stats[metric].precision)} ${stats[metric].unit}</p></div><span class="overview-delta ${benefit(diff)}">${signed(diff)}</span></div>${legend()}
  ${[1, 2, 3]
    .map((round) => {
      const pair = ["baseline", "candidate"].map((variant) =>
        rows.find((row) => row.repeat === round && row.variant === variant),
      );
      const v = pairValues(pair, metric),
        d = delta(...v);
      return `<div class="pair"><div class="pair-title"><span>Pair ${round}</span><span class="${benefit(d)}">${signed(d)}</span></div>${bars(v, maximum)}</div>`;
    })
    .join("")}</article>`;
    })
    .join("");
  return `<section class="section"><div class="headline"><div><h2>${page.title}</h2><p>${page.description}</p></div><a class="pr" href="https://github.com/DataDog/pg-proxy/pull/${page.pull}">PR #${page.pull}</a></div><div class="summary"><div class="panel"><p>Baseline throughput</p><div class="value">${fmt(baseline)}</div><small>Transactions/sec</small></div><div class="panel"><p>${heading}</p><div class="value">${fmt(candidate)}</div><small>Transactions/sec</small></div><div class="panel"><p>Throughput change</p><div class="value ${change > 0.15 ? "positive" : ""}">${signed(change)}</div><small>Median of three paired runs</small></div></div></section>
 <section class="panel"><div class="chart-head"><div><h3>Paired runs</h3><p>${stats[metric].label} · all three pairs</p></div>${metricControl()}</div><div class="chart-panes ${page.keys.length > 1 ? "dual" : ""}">${panes}</div><p class="extra">${page.summary}</p></section>
 <div class="details"><article class="panel"><h3>Observability</h3><p>${page.observability}</p></article><article class="panel"><h3>Complexity</h3><p>${page.complexity}</p></article><article class="panel"><h3>Recommendation</h3><p>${page.verdict}</p></article></div>${dataTable(page.keys)}`;
}
function dataTable(keys) {
  return `<details id="raw-data"><summary>Raw measurements</summary><div class="data-actions"><span>One row per measured run</span><button type="button" data-download="${keys.join(",")}">Download CSV</button></div><div class="table-scroll">${keys.map((key) => `<table><caption>${experiments[key].name}</caption><thead><tr><th scope="col">Pair</th><th scope="col">Variant</th><th scope="col">TPS</th><th scope="col">CPU µs/tx</th><th scope="col">p99 ms</th><th scope="col">UDP packets/tx</th><th scope="col">Traces/tx</th></tr></thead><tbody>${experiments[key].runs.map((r) => `<tr><td>${r.repeat}</td><td>${key === "main" ? r.mode : r.variant}</td><td>${fmt(r.tps, 2)}</td><td>${fmt(r.cpu_us_per_txn, 2)}</td><td>${fmt(r.p99_ms, 3)}</td><td>${fmt(r.udp_datagrams_per_txn, 4)}</td><td>${fmt(r.traces_per_txn, 5)}</td></tr>`).join("")}</tbody></table>`).join("")}</div></details>`;
}
function render(focusMetric) {
  document
    .querySelectorAll(".nav a")
    .forEach((link) =>
      link.hash === `#${view}`
        ? link.setAttribute("aria-current", "page")
        : link.removeAttribute("aria-current"),
    );
  const wasOpen = Boolean(document.querySelector("#raw-data[open]"));
  result.innerHTML = view === "overview" ? overview() : detail(views[view]);
  if (wasOpen && focusMetric) document.querySelector("#raw-data").open = true;
  if (focusMetric)
    result
      .querySelector(`[data-metric="${metric}"]`)
      .focus({ preventScroll: true });
}
function navigate() {
  const fragment = location.hash.slice(1);
  view = Object.hasOwn(views, fragment) ? fragment : "overview";
  render(false);
}
result.addEventListener("click", (event) => {
  const control = event.target.closest("button");
  if (!control) return;
  if (control.dataset.metric) {
    metric = control.dataset.metric;
    render(true);
  }
  if (control.dataset.download) {
    const keys = control.dataset.download.split(","),
      fields = Object.keys(experiments[keys[0]].runs[0]);
    const rows = [
      ["comparison", ...fields],
      ...keys.flatMap((key) =>
        experiments[key].runs.map((r) => [key, ...fields.map((f) => r[f])]),
      ),
    ];
    const url = URL.createObjectURL(
      new Blob([rows.map((row) => row.join(",")).join("\n") + "\n"], {
        type: "text/csv",
      }),
    );
    const a = document.createElement("a");
    a.href = url;
    a.download = `pg-proxy-${view}.csv`;
    a.click();
    setTimeout(() => URL.revokeObjectURL(url), 500);
  }
});
window.addEventListener("hashchange", navigate);
navigate();
