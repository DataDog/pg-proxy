// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

//go:build integration

package itest

import (
	"fmt"
	"maps"
	"os"
	"slices"
)

// Images are public Docker Hub images pinned by digest. To pull them through a
// mirror, set Testcontainers' standard TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX. It
// applies to every image without a registry host, including Testcontainers' own
// images such as Ryuk, and keeps the digest. The harness never sets it.
//
// postgresImages maps each PostgreSQL major the integration suite runs on to
// its pinned image. CI runs every major listed here.
var postgresImages = map[string]string{
	"14": "postgres:14-bookworm@sha256:5a05f61a534e50e679d27fb45953afec7821f2ffc2aa6c738b94515ca0891ac2",
	"18": "postgres:18-bookworm@sha256:194e55feb2597b1465dbee81cc9fd8d204566cd2eb42c8d22fd6c931a38781b1",
}

// defaultPostgresMajor is the major PostgresImage selects when neither
// environment variable is set.
const defaultPostgresMajor = "14"

// PostgresImage returns the image StartPostgres runs by default:
// PG_PROXY_TEST_POSTGRES_IMAGE if set, or else the pinned image for the major
// in PG_PROXY_TEST_POSTGRES_MAJOR, or else the pinned PostgreSQL 14 image.
// A fully qualified override, such as registry.example.com/postgres:17, is not
// affected by a Testcontainers mirror prefix. PostgresImage panics if
// PG_PROXY_TEST_POSTGRES_MAJOR names a major without a pinned image;
// StartPostgres returns that error instead.
func PostgresImage() string {
	image, err := postgresImage()
	if err != nil {
		panic(err)
	}
	return image
}

func postgresImage() (string, error) {
	if override := os.Getenv("PG_PROXY_TEST_POSTGRES_IMAGE"); override != "" {
		return override, nil
	}
	major := os.Getenv("PG_PROXY_TEST_POSTGRES_MAJOR")
	if major == "" {
		major = defaultPostgresMajor
	}
	image, ok := postgresImages[major]
	if !ok {
		return "", fmt.Errorf("PG_PROXY_TEST_POSTGRES_MAJOR=%q has no pinned image; pinned majors: %v",
			major, slices.Sorted(maps.Keys(postgresImages)))
	}
	return image, nil
}
