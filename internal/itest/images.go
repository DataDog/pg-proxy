// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

//go:build integration

package itest

import (
	"os"
	"strconv"
)

// Images come from Docker Hub by default, so the tests run anywhere Docker can
// pull public images. Set PG_PROXY_ITEST_INTERNAL_IMAGES=true to use Datadog's
// internal registry instead, as dd-source's tests do. All are pinned by digest.
const (
	internalPostgresImage = "registry.ddbuild.io/images/postgres-official:14-gbi@sha256:326064632521fb19c9897e9e3d31b2c7dc0c1756763deaef53487d2b8b3c4c20"
	publicPostgresImage   = "postgres:14-bookworm@sha256:5a05f61a534e50e679d27fb45953afec7821f2ffc2aa6c738b94515ca0891ac2"
	internalDebianImage   = "registry.ddbuild.io/images/mirror/debian:bookworm-slim@sha256:4724b8cc51e33e398f0e2e15e18d5ec2851ff0c2280647e1310bc1642182655d"
	publicDebianImage     = "debian:bookworm-slim@sha256:99c0b25c46d0214d5a2091092e7908907dcaecd6a962cde0ac700acee222b6f3"
	// internalHubMirror mirrors Docker Hub. Testcontainers pulls its own images,
	// such as Ryuk, through it.
	internalHubMirror = "registry.ddbuild.io/images/mirror"
)

func internalImages() bool {
	internal, _ := strconv.ParseBool(os.Getenv("PG_PROXY_ITEST_INTERNAL_IMAGES"))
	return internal
}

// PostgresImage returns the image StartPostgres runs by default:
// PG_PROXY_ITEST_POSTGRES_IMAGE if set, or else PostgreSQL 14 from the selected
// registry. With internal images, Testcontainers pulls an override without a
// registry host, such as postgres:17, through the internal Docker Hub mirror;
// use a fully qualified name to pull from elsewhere.
func PostgresImage() string {
	if override := os.Getenv("PG_PROXY_ITEST_POSTGRES_IMAGE"); override != "" {
		return override
	}
	if internalImages() {
		return internalPostgresImage
	}
	return publicPostgresImage
}

// DebianImage returns the Debian image from the selected registry.
func DebianImage() string {
	if internalImages() {
		return internalDebianImage
	}
	return publicDebianImage
}

// UseImageRegistry makes Testcontainers pull Docker Hub images, including its
// own such as Ryuk, through the internal mirror when internal images are
// selected. It does nothing for public images. Testcontainers reads its
// configuration once, so this must run before the first container starts; Main
// calls it. An explicit TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX takes precedence.
// The prefix applies to every image without a registry host. Locally built
// images must therefore be tagged with a host containing a dot, such as
// pg-proxy.localhost/; Testcontainers does not treat a bare localhost/ as a
// registry.
func UseImageRegistry() {
	if _, set := os.LookupEnv("TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX"); set || !internalImages() {
		return
	}
	_ = os.Setenv("TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX", internalHubMirror)
}
