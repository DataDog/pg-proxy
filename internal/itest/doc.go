// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

//go:build integration

// Package itest is the integration test harness: PostgreSQL runs in Docker and
// pg-proxy runs as a subprocess of the real cmd/pg-proxy binary, configured with
// a YAML file. Tests connect to the proxy like any PostgreSQL client.
//
// Files are built only with the integration tag. The proxy binary is built with
// -race when the tests themselves run with -race. For other build flags, build
// the binary separately and set PG_PROXY_ITEST_BINARY; a prebuilt binary is not
// rebuilt with -race.
//
// Environment variables:
//
//   - PG_PROXY_ITEST_INTERNAL_IMAGES=true pulls images from registry.ddbuild.io
//     instead of Docker Hub (see images.go).
//   - PG_PROXY_ITEST_POSTGRES_IMAGE overrides PostgresImage.
//   - PG_PROXY_ITEST_BINARY uses a prebuilt pg-proxy binary, given as an
//     absolute path, instead of building one.
package itest
