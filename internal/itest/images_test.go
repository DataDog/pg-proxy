// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

//go:build integration

package itest

import (
	"maps"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestImagesArePinnedPublicImages(t *testing.T) {
	unsetenv(t, "PG_PROXY_TEST_POSTGRES_IMAGE")
	unsetenv(t, "PG_PROXY_TEST_POSTGRES_MAJOR")

	require.Equal(t, postgresImages[defaultPostgresMajor], PostgresImage())
	for major, image := range postgresImages {
		name, digest, pinned := strings.Cut(image, "@sha256:")
		require.True(t, pinned, "%s must be pinned by digest", image)
		require.Len(t, digest, 64)
		require.Equal(t, "postgres:"+major+"-bookworm", name, "the image for major %s must be Docker Hub's", major)
	}
}

func TestPostgresImageOverride(t *testing.T) {
	t.Setenv("PG_PROXY_TEST_POSTGRES_IMAGE", "registry.example.com/postgres:17")
	t.Setenv("PG_PROXY_TEST_POSTGRES_MAJOR", "18")
	require.Equal(t, "registry.example.com/postgres:17", PostgresImage())
}

func TestPostgresImageMajor(t *testing.T) {
	unsetenv(t, "PG_PROXY_TEST_POSTGRES_IMAGE")
	t.Setenv("PG_PROXY_TEST_POSTGRES_MAJOR", "18")
	require.Equal(t, postgresImages["18"], PostgresImage())

	t.Setenv("PG_PROXY_TEST_POSTGRES_MAJOR", "13")
	_, err := postgresImage()
	require.ErrorContains(t, err, `PG_PROXY_TEST_POSTGRES_MAJOR="13" has no pinned image`)
	_, err = StartPostgres(t.Context())
	require.ErrorContains(t, err, "has no pinned image")
}

// TestCIRunsEveryPinnedMajor verifies that the integration job in CI runs
// every major with a pinned image, and no other.
func TestCIRunsEveryPinnedMajor(t *testing.T) {
	source, err := os.ReadFile("../../.github/workflows/ci.yml")
	require.NoError(t, err)
	var workflow struct {
		Jobs map[string]struct {
			Strategy struct {
				Matrix struct {
					Major []string `yaml:"major"`
				} `yaml:"matrix"`
			} `yaml:"strategy"`
		} `yaml:"jobs"`
	}
	require.NoError(t, yaml.Unmarshal(source, &workflow))
	require.ElementsMatch(t, slices.Collect(maps.Keys(postgresImages)), workflow.Jobs["integration"].Strategy.Matrix.Major)
}

// unsetenv unsets key for the duration of t and restores it afterwards.
func unsetenv(t *testing.T, key string) {
	t.Helper()
	t.Setenv(key, "")
	require.NoError(t, os.Unsetenv(key))
}

func TestPrebuiltBinaryMustBeAbsolute(t *testing.T) {
	for _, path := range []string{"pg-proxy", "bin/pg-proxy", "./bin/pg-proxy"} {
		t.Run(path, func(t *testing.T) {
			t.Setenv("PG_PROXY_ITEST_BINARY", path)
			_, err := Binary()
			require.ErrorContains(t, err, "absolute path")
		})
	}
	t.Setenv("PG_PROXY_ITEST_BINARY", "/opt/pg-proxy/bin/pg-proxy")
	binary, err := Binary()
	require.NoError(t, err)
	require.Equal(t, "/opt/pg-proxy/bin/pg-proxy", binary)
}
