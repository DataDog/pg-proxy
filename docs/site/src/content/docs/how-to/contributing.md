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

- **PG Proxy version:** the commit SHA you built from (`git rev-parse HEAD` in your checkout).
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

The standard Go tooling is all you need. To build and run the proxy against your own PostgreSQL, follow [Local development](../local-development/).

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

The `test/integration` suite runs the `pg-proxy` binary as a black box against PostgreSQL. It covers transaction pooling, backend reuse, session-state isolation, transaction status, client disconnects, and process startup/shutdown. Assertions use query results, wire responses, connection state, and process exit status. After each wire-protocol case, a new client on the same backend connection checks that the case left no session state behind. The suite runs against PostgreSQL 14 by default; to choose another version, see [Requirements](../../reference/requirements/#postgresql).

Some cases record known proxy bugs. Each passes against PostgreSQL directly and skips its proxy run, or the hygiene check after it, with a `TODO:` reason that states how the proxy fails. To run what they skip and reproduce the bugs, set `PG_PROXY_ITEST_RUN_KNOWN_BUGS=1`:

```sh
PG_PROXY_ITEST_RUN_KNOWN_BUGS=1 go test -tags=integration -count=1 ./test/integration
```

When you fix one of these bugs, remove the case's `proxySkip` or `hygieneSkip` in the same change.

Detecting a client that disconnects while waiting for a backend connection relies on Linux `POLLRDHUP`, so that test is skipped on other platforms. From macOS, run the full suite in a Linux container that uses the host's Docker:

```sh
docker run --rm -v /var/run/docker.sock:/var/run/docker.sock -v "$PWD":/src:ro -w /src -e TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock golang:1.26-bookworm go test -tags=integration -race ./test/integration
```

### Run the PostgreSQL-backed tests against your own database

The `cmd/pg-proxy` test uses the shared `internal/itest` PostgreSQL container fixture.
GitHub Actions runs authentication, TLS, cancellation, and the other integration
tests together with race detection. It uses public Docker Hub images by default:

```sh
go test -tags=integration -race -run '^TestProxyAgainstPostgres$' -count=1 ./cmd/pg-proxy
```

Alternatively, set `PG_PROXY_TEST_DATABASE_URL` to use an external disposable
PostgreSQL instance with superuser access. The suite creates databases and
fixture roles, so never use a shared or production database:

```sh
PG_PROXY_TEST_DATABASE_URL='postgres://postgres:postgres@127.0.0.1:5432/postgres?sslmode=disable' \
  go test -tags=integration -race -run '^TestProxyAgainstPostgres$' -count=1 ./cmd/pg-proxy
```

The `integration` build tag is required in both cases. The normal unit-test
command does not execute these PostgreSQL-backed tests.

### PostgreSQL regression suite

The `test/regress` suite runs PostgreSQL's own regression tests (`pg_regress`, as used by `make installcheck`) through PG Proxy for every supported PostgreSQL major version. No upstream tests or expected outputs are copied into this repository: [versions.json](https://github.com/DataDog/pg-proxy/blob/main/test/regress/versions.json) pins each source release and its checksum, and the [image](https://github.com/DataDog/pg-proxy/blob/main/test/regress/image/Dockerfile) downloads and builds it. CI runs one job per version on every pull request, as part of the required CI gate.

```sh
go test -tags=integration,regress -timeout=60m -v ./test/regress
```

By default only the newest version runs. Set `PG_PROXY_REGRESS_PG_MAJORS=14,17`, or `all`, to select others. The first run of a version builds its image, which takes a few minutes.

`TestInstallcheckDirect` runs the full upstream schedule without the proxy and must pass completely; it checks the setup itself. `TestInstallcheckThroughProxy` runs the schedule through PG Proxy and compares the results with [`baseline/pg<major>.txt`](https://github.com/DataDog/pg-proxy/tree/main/test/regress/baseline). Each entry names a cause, explained at the top of the file:

- `skip`: left out of the schedule because PG Proxy cannot pass it by design, such as large-object functions or `SET ROLE` across transactions.
- `isolate`: left out of the schedule because its failure would break later tests. These are not established as by design and are worth re-checking as PG Proxy improves.
- `fail`: runs and is expected to fail. Its `diff=<lines>` field records how many lines of its output differ from PostgreSQL's expected output.

The run fails when a test fails without a `fail` entry, when a `fail` entry passes, when a scheduled test reports no result, or when a `fail` entry's diff size changes. A larger diff means the test got worse; a smaller one means it improved, and its count should be lowered so a later regression is caught. Update the baseline in the same change that fixes or breaks compatibility. `PG_PROXY_REGRESS_UPDATE=1` rewrites the `fail` entries and their counts from a run and keeps the other entries and their reasons.

| Variable | Effect |
| --- | --- |
| `PG_PROXY_REGRESS_PG_MAJORS` | Comma-separated major versions from `versions.json`, or `all` (default: the newest) |
| `PG_PROXY_REGRESS_UPDATE` | Rewrite the `fail` entries of each baseline and their diff counts from this run |
| `PG_PROXY_REGRESS_ARTIFACTS` | Directory for `pg_regress` output and `regression.diffs` (default: a new temporary directory, logged) |
| `PG_PROXY_REGRESS_BUILD_LOG` | Print the image build log |
| `PG_PROXY_REGRESS_PREBUILT` | `true` uses an image already built under `pg-proxy.localhost/regress:<version>` instead of building it; CI builds it beforehand with a layer cache |

PG Proxy does not forward client startup parameters to backend connections, so the suite sets the time zone, date style, and options that `pg_regress` sends at startup as database defaults instead.

## Guidelines

- **Tests:** include tests with every change. Bug fixes should include a test that fails without the fix. Changes that reach PostgreSQL should be covered by an integration test.
- **License headers:** every Go file, including tests, needs the header in [.licenserc.yaml](https://github.com/DataDog/pg-proxy/blob/main/.licenserc.yaml). Run the `license-eye` command above with `header fix` to add missing headers.
- **Dependencies:** PG Proxy uses public dependencies only. Datadog imports are limited to publicly released modules. Optional public Datadog SDK integrations live in `pkg/datadog`; integrations with private services belong in the embedding application. If you add or update a dependency, update [LICENSE-3rdparty.csv](https://github.com/DataDog/pg-proxy/blob/main/LICENSE-3rdparty.csv) and make sure its license is compatible with Apache-2.0.
- **Configuration:** for new settings, follow [Adapting configuration](../../reference/configuration/adapting-configuration/).
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
