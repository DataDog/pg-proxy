// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package main

import (
	"context"
	"errors"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// defaultPostgresTestImage is the public image the Docker fixture starts. Set
// PG_PROXY_TEST_POSTGRES_IMAGE to use another image or registry mirror.
const defaultPostgresTestImage = "postgres:14"

func postgresTestImage() string {
	if image := os.Getenv("PG_PROXY_TEST_POSTGRES_IMAGE"); image != "" {
		return image
	}
	return defaultPostgresTestImage
}

// An explicit disposable PostgreSQL URL lets CI execute the suite without
// starting a container. Otherwise the Docker fixture starts postgresTestImage.
func postgresTestConnectionString(t *testing.T, ctx context.Context) func(context.Context, ...string) (string, error) {
	t.Helper()
	if value, supplied := os.LookupEnv("PG_PROXY_TEST_DATABASE_URL"); supplied {
		base, err := url.Parse(value)
		require.NoError(t, err, "invalid integration database URL")
		require.True(t, base.Scheme == "postgres" || base.Scheme == "postgresql", "integration database must use a PostgreSQL URL")
		return func(_ context.Context, options ...string) (string, error) {
			copy := *base
			query := copy.Query()
			for _, option := range options {
				key, value, ok := strings.Cut(option, "=")
				if !ok {
					return "", errors.New("invalid integration connection option")
				}
				query.Set(key, value)
			}
			copy.RawQuery = query.Encode()
			return copy.String(), nil
		}
	}
	testcontainers.SkipIfProviderIsNotHealthy(t)
	pg, err := postgres.Run(ctx, postgresTestImage(),
		postgres.WithDatabase("postgres"), postgres.WithUsername("postgres"), postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2)))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pg.Terminate(context.Background())) })
	return pg.ConnectionString
}
