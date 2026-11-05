// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

//go:build integration

package main

import (
	"context"
	"errors"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/DataDog/pg-proxy/internal/itest"
)

// postgresTestConnectionString uses the shared container fixture by default.
// An explicit URL supports running this suite against an external disposable
// PostgreSQL instance. The tests create roles and databases, so it must not
// point at a shared or production database.
func postgresTestConnectionString(t *testing.T, ctx context.Context) func(context.Context, ...string) (string, error) {
	t.Helper()
	value, supplied := os.LookupEnv("PG_PROXY_TEST_DATABASE_URL")
	if !supplied {
		itest.UseImageRegistry()
		pg, err := itest.StartPostgres(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, pg.Terminate(context.Background())) })
		value = pg.URL("postgres")
	}
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
