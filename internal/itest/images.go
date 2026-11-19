// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

//go:build integration

package itest

import "os"

// Images are public Docker Hub images pinned by digest. To pull them through a
// mirror, set Testcontainers' standard TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX. It
// applies to every image without a registry host, including Testcontainers' own
// images such as Ryuk, and keeps the digest. The harness never sets it.
const defaultPostgresImage = "postgres:14-bookworm@sha256:5a05f61a534e50e679d27fb45953afec7821f2ffc2aa6c738b94515ca0891ac2"

// PostgresImage returns the image StartPostgres runs by default:
// PG_PROXY_TEST_POSTGRES_IMAGE if set, or else the pinned PostgreSQL 14 image.
// A fully qualified override, such as registry.example.com/postgres:17, is not
// affected by a Testcontainers mirror prefix.
func PostgresImage() string {
	if override := os.Getenv("PG_PROXY_TEST_POSTGRES_IMAGE"); override != "" {
		return override
	}
	return defaultPostgresImage
}
