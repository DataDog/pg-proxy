// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

//go:build integration

package itest

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// unsetEnv removes name for the test and restores its previous value afterward.
func unsetEnv(t *testing.T, name string) {
	t.Helper()
	t.Setenv(name, "")
	require.NoError(t, os.Unsetenv(name))
}

func TestImagesDefaultToPublicRegistry(t *testing.T) {
	unsetEnv(t, "PG_PROXY_ITEST_INTERNAL_IMAGES")
	unsetEnv(t, "PG_PROXY_ITEST_POSTGRES_IMAGE")
	unsetEnv(t, "TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX")

	require.Equal(t, publicPostgresImage, PostgresImage())
	require.Equal(t, publicDebianImage, DebianImage())
	UseImageRegistry()
	_, prefixed := os.LookupEnv("TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX")
	require.False(t, prefixed, "public images must not be pulled through the internal mirror")
}

func TestInternalImagesAreOptIn(t *testing.T) {
	t.Setenv("PG_PROXY_ITEST_INTERNAL_IMAGES", "true")
	unsetEnv(t, "PG_PROXY_ITEST_POSTGRES_IMAGE")
	unsetEnv(t, "TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX")

	require.Equal(t, internalPostgresImage, PostgresImage())
	require.Equal(t, internalDebianImage, DebianImage())
	UseImageRegistry()
	require.Equal(t, internalHubMirror, os.Getenv("TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX"))
}

func TestExplicitHubPrefixTakesPrecedence(t *testing.T) {
	t.Setenv("PG_PROXY_ITEST_INTERNAL_IMAGES", "true")
	t.Setenv("TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX", "mirror.example.com")

	UseImageRegistry()
	require.Equal(t, "mirror.example.com", os.Getenv("TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX"))
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
