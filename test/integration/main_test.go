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

// backend is the PostgreSQL container shared by every test in the package.
var backend *itest.Postgres

func TestMain(m *testing.M) {
	os.Exit(itest.Main(func() int { return m.Run() }, func(ctx context.Context) (func(), error) {
		pg, err := itest.StartPostgres(ctx)
		if err != nil {
			return nil, err
		}
		backend = pg
		return func() { _ = pg.Terminate(context.Background()) }, nil
	}))
}

// pooledApplicationName is the application_name of the proxy's backend
// connection for dbPooled, which identifies it in pg_stat_activity.
const pooledApplicationName = "itest-pooled"

// startProxy starts a proxy for t serving dbDefault and dbPooled.
func startProxy(t *testing.T) *itest.Proxy {
	t.Helper()
	return itest.MustStartProxy(t, proxyConfig())
}

// proxyConfig returns the configuration startProxy uses, for tests that change it.
func proxyConfig() pgconfig.Config {
	pooled := backend.Database("pooled-primary", "postgres")
	pooled.MaxConns = 1
	pooled.ApplicationName = pooledApplicationName
	return pgconfig.Config{
		Databases: []pgconfig.Database{backend.Database("default-primary", "postgres"), pooled},
		Clusters: map[string]pgconfig.Cluster{
			"default": {Primary: "default-primary"},
			"pooled":  {Primary: "pooled-primary"},
		},
		LogicalDatabases: map[string]pgconfig.LogicalDatabase{
			dbDefault: {Cluster: "default"},
			dbPooled:  {Cluster: "pooled"},
		},
	}
}
