<!--
Unless explicitly stated otherwise all files in this repository are licensed
under the Apache-2.0 License.
This product includes software developed at Datadog (https://www.datadoghq.com/)
Copyright 2026-Present Datadog, Inc.
-->

# Agent instructions

Read [CONTRIBUTING.md](CONTRIBUTING.md) before making changes. Directories may
add their own `AGENTS.md`; follow it in addition to this file.

## This repository is public

Treat everything you write here as published. That includes code, comments,
tests, fixtures, documentation, commit messages, branch names, pull request
titles and descriptions, and review comments.

Do not disclose Datadog's internal infrastructure or systems. Never include:

- Internal service, cluster, database, schema, namespace, or host names.
- Internal datacenters, regions, environments, or Datadog organizations.
- Internal URLs, IP addresses, dashboards, monitors, notebooks, or traces.
- Internal repositories, file paths, packages, or deployment and CI tooling.
- Links to Jira or Linear tickets, Slack, Confluence, incidents, or internal
  documents.
- Production configuration values, capacity figures, or traffic volumes.
- SQL statements or query parameter values taken from internal systems, such
  as production queries, slow-query logs, or captured traffic.
- Customer names, organization IDs, or data.

These are allowed:

- Public Datadog products and documentation, such as the Datadog Agent,
  Database Monitoring, and `docs.datadoghq.com`.
- The GitHub team handles already in `.github/CODEOWNERS`.
- Jira or Linear ticket IDs, such as `PROJ-123`, without a URL.

Use neutral substitutes:

- Hosts and databases: `localhost`, `db.example.com`, `app_db`.
- SQL: minimal synthetic queries against example tables, such as
  `SELECT id, name FROM users WHERE id = $1`, with made-up parameter values.
- Scale: "a deployment with thousands of client connections", not a cluster name
  or a production figure.
- Performance claims: numbers from benchmarks or tests in this repository, or
  from runs that follow the method on the Benchmarks page
  (`docs/site/src/content/docs/explanation/benchmarks.md`).

Internal context, such as an incident, a ticket, or a production trace, may
inform a change. Describe the behavior it revealed in general terms. Do not copy
its identifiers, other than a ticket ID, or excerpts into the repository or a
pull request.

If a change cannot be explained without internal details, stop and ask the user.

Before you commit or open a pull request, check the diff, the commit message,
and the pull request description against this list.
