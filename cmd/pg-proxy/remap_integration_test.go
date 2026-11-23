// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

//go:build integration

package main

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	pgconfig "github.com/DataDog/pg-proxy/pkg/config"
	"github.com/DataDog/pg-proxy/pkg/limiter"
	"github.com/DataDog/pg-proxy/pkg/server"
	"github.com/DataDog/pg-proxy/pkg/topology"
	"github.com/DataDog/pg-proxy/pkg/warden"
)

// remapLogicalDB is the logical database whose routing verifyLiveRemaps changes.
const remapLogicalDB = "app_db"

// wardenInventory serves a Warden's logical databases and backends to the server,
// so the Warden's remaps reach the sessions the server creates.
type wardenInventory struct{ runtime *warden.Warden }

func (i wardenInventory) GetLogicalDatabase(name string) (topology.LogicalDatabase, bool) {
	return i.runtime.GetLogicalDatabase(warden.Identifier(name))
}

func (i wardenInventory) GetBackend(name string) (server.Backend, bool) {
	backend, ok := i.runtime.GetBackend(warden.Identifier(name))
	return backend, ok
}

// verifyLiveRemaps changes a logical database's primary and read clusters while a
// client stays connected, the way a configuration refresh does through the Warden,
// and checks that the client's next queries follow each change. Clusters a and b
// are separate databases on one server, each with a marker row naming it, so the
// test can see where a query ran.
func verifyLiveRemaps(t *testing.T, adminURL string, admin *pgx.Conn) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	t.Cleanup(cancel)
	settings, err := pgxpool.ParseConfig(adminURL)
	require.NoError(t, err)
	cfg := config{databases: map[string]*pgxpool.Config{}, clusters: map[string]pgconfig.Cluster{}}
	runtime := warden.New()
	for _, node := range []string{"a", "b"} {
		dbname := "remap_" + node
		_, err := admin.Exec(ctx, "DROP DATABASE IF EXISTS "+dbname)
		require.NoError(t, err)
		_, err = admin.Exec(ctx, "CREATE DATABASE "+dbname)
		require.NoError(t, err)
		database := settings.Copy()
		database.ConnConfig.Database = dbname
		conn, err := pgx.ConnectConfig(ctx, database.ConnConfig)
		require.NoError(t, err)
		_, err = conn.Exec(ctx, "CREATE TABLE marker (node text); INSERT INTO marker VALUES ('"+node+"'); CREATE TABLE writes (id int)")
		require.NoError(t, err)
		require.NoError(t, conn.Close(ctx))
		cfg.databases[dbname] = database
		cfg.clusters[node] = pgconfig.Cluster{Primary: dbname}
		opened, err := openBackend(ctx, cfg, node)
		require.NoError(t, err)
		managed := &clusterBackend{opened}
		t.Cleanup(managed.Close)
		runtime.RegisterBackend(warden.Identifier(node), managed)
	}
	remap := func(logical topology.LogicalDatabase) {
		runtime.ReconcileLogicalDatabases(map[warden.Identifier]topology.LogicalDatabase{remapLogicalDB: logical})
	}
	remap(topology.LogicalDatabase{Cluster: "a"})

	proxy, err := server.NewServer(ctx, server.Config{
		ListenAddress:              "127.0.0.1:0",
		AuthenticationMessageLimit: 16 << 10,
		StartupTimeout:             10 * time.Second,
		MaxPendingConnections:      100,
		Admission:                  limiter.New(limiter.Params{GlobalLimit: 100, DefaultServiceMax: 100}),
		Authenticate:               localUser(map[string]string{remapLogicalDB: settings.ConnConfig.User}),
		Sessions:                   runtime.Sessions(),
		Runtime: server.RuntimeConfig{
			Inventory: wardenInventory{runtime},
			Settings:  topology.RuntimeConfig{PostgresMessageBufferSize: topology.DefaultPostgresMessageBufferSize},
		},
	})
	require.NoError(t, err)
	running := make(chan error, 1)
	go func() { running <- proxy.Run() }()
	t.Cleanup(func() {
		// Cancelling the context closes the listener. Calling proxy.Close as well
		// would race that close and can fail with net.ErrClosed. Run returns once
		// the sessions finish, so the backends close their pools after it.
		cancel()
		select {
		case err := <-running:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Error("proxy did not stop")
		}
	})

	clientConfig, err := pgx.ParseConfig("postgres://" + settings.ConnConfig.User + "@" + proxy.Addr().String() + "/" + remapLogicalDB + "?sslmode=disable")
	require.NoError(t, err)
	// The proxy reports an aborted transaction with a FATAL error but keeps the
	// session open. Keep the client open too, so the test sees the session recover.
	clientConfig.OnPgError = func(*pgconn.PgConn, *pgconn.PgError) bool { return true }
	client, err := pgx.ConnectConfig(ctx, clientConfig)
	require.NoError(t, err)
	defer client.Close(context.Background())

	// The proxy classifies the SQL text of Query messages, so use simple queries.
	exec := func(sql string) error {
		_, err := client.Exec(ctx, sql, pgx.QueryExecModeSimpleProtocol)
		return err
	}
	node := func() string {
		t.Helper()
		var name string
		require.NoError(t, client.QueryRow(ctx, "SELECT node FROM marker", pgx.QueryExecModeSimpleProtocol).Scan(&name))
		return name
	}
	writes := func(node string) int {
		t.Helper()
		conn, err := pgx.ConnectConfig(ctx, cfg.databases["remap_"+node].ConnConfig)
		require.NoError(t, err)
		defer conn.Close(context.Background())
		var count int
		require.NoError(t, conn.QueryRow(ctx, "SELECT count(*) FROM writes").Scan(&count))
		return count
	}
	// The listener registers a session with the Warden after startup, so run a
	// query before the first remap.
	require.Equal(t, "a", node())

	remap(topology.LogicalDatabase{Cluster: "b"})
	require.Equal(t, "b", node(), "an idle session should switch clusters at its next query")

	// A primary remap during a transaction ends the transaction on the old cluster,
	// reports SQLSTATE 40000, and keeps the session.
	require.NoError(t, exec("BEGIN"))
	require.NoError(t, exec("INSERT INTO writes VALUES (1)"))
	remap(topology.LogicalDatabase{Cluster: "a"})
	var pgErr *pgconn.PgError
	require.ErrorAs(t, exec("SELECT 1"), &pgErr)
	require.Equal(t, "40000", pgErr.Code)
	// No transaction is open anymore, so this COMMIT reaches a and commits nothing.
	require.NoError(t, exec("COMMIT"))
	require.Equal(t, "a", node(), "the session should continue on the new cluster")
	require.Zero(t, writes("b"), "the aborted transaction should not commit")

	// A read-cluster remap during a transaction leaves the transaction alone, and
	// reads after it go to the new read cluster.
	require.NoError(t, exec("BEGIN"))
	require.Equal(t, "a", node())
	remap(topology.LogicalDatabase{Cluster: "a", ReadClusters: topology.ReadClusters{"b": {Weight: 1}}})
	require.Equal(t, "a", node(), "a read-cluster remap should not abort the transaction")
	require.NoError(t, exec("INSERT INTO writes VALUES (2)"))
	require.NoError(t, exec("COMMIT"))
	require.Equal(t, 1, writes("a"), "the transaction should commit on the primary")
	for range 10 {
		require.Equal(t, "b", node(), "reads should go to the new read cluster")
	}
	require.NoError(t, exec("INSERT INTO writes VALUES (3)"))
	require.Equal(t, 2, writes("a"), "writes should stay on the primary")
	require.Zero(t, writes("b"))

	remap(topology.LogicalDatabase{Cluster: "a"})
	for range 10 {
		require.Equal(t, "a", node(), "reads should return to the primary once read clusters are removed")
	}
}
