//go:build integration

// Package itest is the integration test harness: PostgreSQL runs in Docker and
// pg-proxy runs as a subprocess of the real cmd/pg-proxy binary, configured with
// a YAML file. Tests connect to the proxy like any PostgreSQL client.
//
// Files are built only with the integration tag. Environment variables:
//
//   - PG_PROXY_ITEST_POSTGRES_IMAGE overrides DefaultPostgresImage.
//   - PG_PROXY_ITEST_BINARY uses a prebuilt pg-proxy binary instead of building one.
//   - PG_PROXY_ITEST_BUILD_FLAGS adds go build flags; -race is added automatically
//     when the tests themselves run with -race.
package itest
