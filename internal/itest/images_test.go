// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

//go:build integration

package itest

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestImagesArePinnedPublicImages(t *testing.T) {
	t.Setenv("PG_PROXY_TEST_POSTGRES_IMAGE", "")
	require.NoError(t, os.Unsetenv("PG_PROXY_TEST_POSTGRES_IMAGE"))

	for _, image := range []string{PostgresImage(), DebianImage()} {
		name, digest, pinned := strings.Cut(image, "@sha256:")
		require.True(t, pinned, "%s must be pinned by digest", image)
		require.Len(t, digest, 64)
		require.NotContains(t, strings.SplitN(name, "/", 2)[0], ".", "%s must be a Docker Hub image", image)
	}
}

func TestPostgresImageOverride(t *testing.T) {
	t.Setenv("PG_PROXY_TEST_POSTGRES_IMAGE", "registry.example.com/postgres:17")
	require.Equal(t, "registry.example.com/postgres:17", PostgresImage())
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
