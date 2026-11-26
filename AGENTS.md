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
- Jira or Linear ticket IDs and links, Confluence pages, Slack threads, incident
  references, or other internal documentation.
- Datadog-specific jargon or labels such as "Feature Flag". Use plain,
  public-facing terms such as "rollout setting" or "configuration option".
- Internal-only functionality, integrations, or behavior that readers cannot
  verify in this repository or in public documentation.
- Production configuration values, capacity figures, or traffic volumes.
- SQL statements or query parameter values taken from internal systems, such
  as production queries, slow-query logs, or captured traffic.
- Customer names, organization IDs, or data.

These are allowed:

- Public Datadog products and documentation, such as the Datadog Agent,
  Database Monitoring, and `docs.datadoghq.com`.
- The GitHub team handles already in `.github/CODEOWNERS`.

Use neutral substitutes:

- Hosts and databases: `localhost`, `db.example.com`, `app_db`.
- SQL: minimal synthetic queries against example tables, such as
  `SELECT id, name FROM users WHERE id = $1`, with made-up parameter values.
- Scale: "a deployment with thousands of client connections", not a cluster name
  or a production figure.
- Performance claims: numbers from benchmarks or tests in this repository.

Internal context, such as an incident, a ticket, or a production trace, may
inform a change. Describe the problem in terms that a public reader can
understand and reproduce. Keep internal identifiers and excerpts out of the
repository and pull requests. Link to public GitHub issues or public
documentation when readers need more context.

If a change cannot be explained without internal details, stop and ask the user.

Before you commit or open a pull request, check the diff, the commit message,
and the pull request description against this list.
