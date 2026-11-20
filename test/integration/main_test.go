// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

//go:build integration

// Package integration runs black-box tests against the pg-proxy binary with a
// PostgreSQL backend in Docker. See internal/itest for the harness.
package integration

import (
	"context"
	"os"
	"testing"

	"github.com/DataDog/pg-proxy/internal/itest"
	pgconfig "github.com/DataDog/pg-proxy/pkg/config"
)

// Logical databases served by startProxy. Both route to the backend's postgres
// database. dbDefault uses the default pool size; dbPooled has one backend
// connection, which makes connection reuse and transaction pinning observable.
const (
	dbDefault = "testdb"
	dbPooled  = "testdb_pooled"
)

// runKnownBugs runs the proxy runs and hygiene checks that proxySkip and
// hygieneSkip would skip, to reproduce the bugs they name. Set
// PG_PROXY_ITEST_RUN_KNOWN_BUGS=1 to enable it.
var runKnownBugs = os.Getenv("PG_PROXY_ITEST_RUN_KNOWN_BUGS") != ""

// backend is the PostgreSQL container shared by every test in the package.
var backend *itest.Postgres

// backendMajor is backend's PostgreSQL major version, such as 14.
var backendMajor int

func TestMain(m *testing.M) {
	os.Exit(itest.Main(func() int { return m.Run() }, func(ctx context.Context) (func(), error) {
		pg, err := itest.StartPostgres(ctx)
		if err != nil {
			return nil, err
		}
		backend = pg
		teardown := func() { _ = pg.Terminate(context.Background()) }
		conn, err := pg.Connect(ctx, "postgres")
		if err != nil {
			return teardown, err
		}
		defer conn.Close(context.Background())
		var version int
		if err := conn.QueryRow(ctx, "SELECT current_setting('server_version_num')::int").Scan(&version); err != nil {
			return teardown, err
		}
		backendMajor = version / 10000
		return teardown, nil
	}))
}

// pooledApplicationName is the application_name of the proxy's backend
// connection for dbPooled, which identifies it in pg_stat_activity.
const pooledApplicationName = "itest-pooled"

// pooledPrimary names dbPooled's database entry, for tests that change its settings.
const pooledPrimary = "pooled-primary"

// startProxy starts a proxy for t serving dbDefault and dbPooled.
func startProxy(t *testing.T) *itest.Proxy {
	t.Helper()
	return itest.MustStartProxy(t, proxyConfig())
}

// proxyConfig returns the configuration startProxy uses, for tests that change it.
func proxyConfig() pgconfig.Config {
	pooled := backend.Database(pooledPrimary, "postgres")
	pooled.MaxConns = 1
	pooled.ApplicationName = pooledApplicationName
	return pgconfig.Config{
		Databases: []pgconfig.Database{backend.Database("default-primary", "postgres"), pooled},
		Clusters: map[string]pgconfig.Cluster{
			"default": {Primary: "default-primary"},
			"pooled":  {Primary: pooledPrimary},
		},
		LogicalDatabases: map[string]pgconfig.LogicalDatabase{
			dbDefault: {Cluster: "default"},
			dbPooled:  {Cluster: "pooled"},
		},
	}
}
