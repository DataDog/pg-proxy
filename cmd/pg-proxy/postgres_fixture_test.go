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

// An explicit disposable PostgreSQL URL lets CI execute the suite without access
// to the internal container registry. The existing Docker fixture stays intact.
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
	t.Setenv("TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX", "registry.ddbuild.io/images/mirror")
	pg, err := postgres.Run(ctx, "registry.ddbuild.io/images/postgres-official:14-gbi",
		postgres.WithDatabase("postgres"), postgres.WithUsername("postgres"), postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2)))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pg.Terminate(context.Background())) })
	return pg.ConnectionString
}
