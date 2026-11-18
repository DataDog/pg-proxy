---
# Unless explicitly stated otherwise all files in this repository are licensed
# under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/)
# Copyright 2026-Present Datadog, Inc.
title: "Contribute to PG Proxy"
description: "Report bugs, propose changes, and prepare a pull request."
---

Thank you for contributing to Datadog's PG Proxy. We accept contributions in many forms: bug reports, fixes, features, documentation, tests, and reviews of open issues and pull requests.

## Open an issue first

Please [open an issue](https://github.com/DataDog/pg-proxy/issues/new/choose) for any non-trivial change before you start writing code. This lets us agree on the approach early, so your work is not spent on something that is already in progress or outside the project's scope. Typos, small documentation fixes, and obvious one-line bug fixes can go straight to a pull request.

Include as much information as possible.

### Reporting a bug

To help us pinpoint the issue, please include:

- **PG Proxy version:** the release tag or commit SHA you built from. For a built binary, `go version -m ./pg-proxy` prints the module version.
- **PostgreSQL version** of the backend (`SELECT version();`).
- **Machine:** operating system, architecture, and Go version (`go version`).
- **Client:** the driver or tool and its version (for example pgx, psql, JDBC).
- **Configuration:** the relevant parts of your PG Proxy config, with passwords and other secrets removed.
- **Logs:** the relevant PG Proxy log output, and any client-side error.
- **Reproduction steps:** what you ran, what you expected, and what happened instead. A minimal config plus a short script or SQL sequence is ideal.

### Proposing a feature or a larger change

Describe the problem you want to solve, why it matters, and how you plan to implement it. Mention any changes to configuration, the wire protocol behavior, metrics, or other public APIs. A maintainer will discuss it with you; once we agree on the approach, open a pull request that links to the issue.

## Reporting security issues

Do not report security vulnerabilities in public issues or pull requests. Email security@datadoghq.com instead.

## Development setup

You need:

- Go at the version declared in [go.mod](https://github.com/DataDog/pg-proxy/blob/main/go.mod) or newer. CI tests both that minimum and the latest stable release.
- A C toolchain: CI builds with `CGO_ENABLED=1`.
- Docker, for the integration tests. They start PostgreSQL in a container.

This repository does not use Bazel or generated `BUILD` files; the standard Go tooling is all you need. To build and run the proxy against your own PostgreSQL, follow [Local development](../local-development/).

## Before you open a pull request

Run the same checks CI runs, from the repository root:

```sh
gofmt -l .                     # must print nothing; fix with gofmt -w
go mod tidy -diff              # must print nothing
go build ./...
go vet -tags=integration ./...
go test -race -count=1 ./...
go test -tags=integration -race -count=1 ./...   # requires Docker
go run github.com/apache/skywalking-eyes/cmd/license-eye@v0.9.0 header check
```

If you changed the documentation site, follow [Contribute to the documentation](../documentation-contributing/) and run its static build, which checks links.

The `test/integration` suite runs the standalone binary as a black box against PostgreSQL. It covers transaction pooling, backend reuse, session-state isolation, transaction status, client disconnects, and process startup/shutdown. Assertions use query results, wire responses, connection state, and process exit status.

Detecting a client that disconnects while waiting for a backend connection relies on Linux `POLLRDHUP`, so that test is skipped on other platforms. From macOS, run the full suite in a Linux container that uses the host's Docker:

```sh
docker run --rm -v /var/run/docker.sock:/var/run/docker.sock -v "$PWD":/src:ro -w /src -e TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock golang:1.26-bookworm go test -tags=integration -race ./test/integration
```

## Guidelines

- **Tests:** include tests with every change. Bug fixes should include a test that fails without the fix. Changes that reach PostgreSQL should be covered by an integration test.
- **License headers:** every Go file, including tests, needs the header in [.licenserc.yaml](https://github.com/DataDog/pg-proxy/blob/main/.licenserc.yaml). Run the `license-eye` command above with `header fix` to add missing headers.
- **Dependencies:** PG Proxy uses public dependencies only. Datadog imports are limited to publicly released modules. Optional public Datadog SDK integrations live in `pkg/datadog`; integrations with private services belong in the embedding application. If you add or update a dependency, update [LICENSE-3rdparty.csv](https://github.com/DataDog/pg-proxy/blob/main/LICENSE-3rdparty.csv) and make sure its license is compatible with Apache-2.0.
- **Configuration:** for new settings, follow [Extending PG Proxy configuration](https://github.com/DataDog/pg-proxy/blob/main/docs/extending-configuration.md).
- **Documentation:** update the README or the documentation site when you change behavior, configuration, or metric names.
- **Scope:** keep pull requests focused. Several small pull requests are easier to review than one large one.

## Commits and pull requests

- **Sign your commits.** Datadog requires signed commits from all contributors. See [GitHub's guide to signing commits](https://docs.github.com/en/authentication/managing-commit-signature-verification/signing-commits).
- Write commit messages that explain why the change is needed, not only what changed.
- Fill in the pull request template, link the related issue, and describe how you tested the change.
- Use a draft pull request for work in progress.
- After review starts, please add new commits instead of force-pushing, so reviewers can see what changed.

Maintainers review pull requests as time allows. If yours has had no response in a week or two, feel free to leave a comment on it.

## AI code assistants

You are welcome to use AI code assistants. The guidelines above still apply, and you are responsible for understanding and testing every change you submit. If you use one, say so in the pull request. Maintainers may close contributions that appear to be largely generated without review or tests.

## License

By contributing, you agree that your contributions are licensed under the [Apache-2.0 License](https://github.com/DataDog/pg-proxy/blob/main/LICENSE).
