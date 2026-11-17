// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/DataDog/pg-proxy/internal/itest"
	pgconfig "github.com/DataDog/pg-proxy/pkg/config"
)

const (
	// readRoutingDB is created on the primary and the replica container. Each
	// copy holds a marker row with the name of its node.
	readRoutingDB = "read_routing"
	// dbReplicaRouting uses a cluster with one replica pool.
	dbReplicaRouting = "rr_replica"
	// dbReadCluster sends reads to a separate cluster whose primary is the replica.
	dbReadCluster = "rr_read_cluster"
)

// TestReadRouting checks that the binary routes eligible reads to configured
// replicas and read clusters, and keeps writes and transactions on the primary.
// The replica is a separate container, not a streaming replica: each node
// answers with its own marker, so the test can see where a query ran.
func TestReadRouting(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	replica, err := itest.StartPostgres(ctx)
	require.NoError(t, err, "start replica container")
	t.Cleanup(func() { _ = replica.Terminate(context.Background()) })
	for node, pg := range map[string]*itest.Postgres{"primary": backend, "replica": replica} {
		require.NoError(t, pg.Exec(ctx, "DROP DATABASE IF EXISTS "+readRoutingDB, "CREATE DATABASE "+readRoutingDB))
		conn, err := pg.Connect(ctx, readRoutingDB)
		require.NoError(t, err)
		_, err = conn.Exec(ctx, fmt.Sprintf(
			"CREATE TABLE marker (node text); INSERT INTO marker VALUES ('%s'); CREATE TABLE writes (id int)", node))
		require.NoError(t, err)
		require.NoError(t, conn.Close(ctx))
	}

	proxy := itest.MustStartProxy(t, pgconfig.Config{
		Databases: []pgconfig.Database{
			backend.Database("rr-primary", readRoutingDB),
			replica.Database("rr-replica", readRoutingDB),
		},
		Clusters: map[string]pgconfig.Cluster{
			"rr":        {Primary: "rr-primary", Replicas: []string{"rr-replica"}},
			"rr-main":   {Primary: "rr-primary"},
			"rr-reader": {Primary: "rr-replica"},
		},
		LogicalDatabases: map[string]pgconfig.LogicalDatabase{
			// A read cluster entry enables read routing for the logical database.
			dbReplicaRouting: {Cluster: "rr", ReadClusters: map[string]*pgconfig.ReadCluster{"rr": {}}},
			dbReadCluster:    {Cluster: "rr-main", ReadClusters: map[string]*pgconfig.ReadCluster{"rr-reader": {}}},
		},
	})

	const queries = 40
	for _, tc := range []struct {
		database string
		// allReplica is true when every read must reach the replica. The replica
		// pool keeps about 10% of reads on the primary, so the replica cluster
		// case only needs some reads on the replica.
		allReplica bool
	}{
		{database: dbReplicaRouting},
		{database: dbReadCluster, allReplica: true},
	} {
		t.Run(tc.database, func(t *testing.T) {
			conn, err := proxy.Connect(ctx, tc.database, itest.PostgresUser)
			require.NoError(t, err)
			defer conn.Close(context.Background())

			// The proxy classifies SQL text in Query and Parse messages. pgx's
			// default mode reuses a cached statement with Bind only, which the
			// proxy sends to the main cluster, so use modes that send the SQL.
			for _, mode := range []pgx.QueryExecMode{pgx.QueryExecModeSimpleProtocol, pgx.QueryExecModeExec} {
				nodes := map[string]int{}
				for range queries {
					var node string
					require.NoError(t, conn.QueryRow(ctx, "SELECT node FROM marker", mode).Scan(&node))
					nodes[node]++
				}
				if tc.allReplica {
					require.Equal(t, map[string]int{"replica": queries}, nodes, "%v reads should go to the read cluster", mode)
				} else {
					require.Positive(t, nodes["replica"], "some %v reads should go to the replica: %v", mode, nodes)
				}
			}

			for range queries {
				tx, err := conn.Begin(ctx)
				require.NoError(t, err)
				var node string
				require.NoError(t, tx.QueryRow(ctx, "SELECT node FROM marker", pgx.QueryExecModeSimpleProtocol).Scan(&node))
				require.Equal(t, "primary", node, "reads in a read-write transaction should stay on the primary")
				require.NoError(t, tx.Commit(ctx))
			}

			_, err = conn.Exec(ctx, "TRUNCATE writes")
			require.NoError(t, err)
			for i := range queries {
				_, err := conn.Exec(ctx, "INSERT INTO writes VALUES ($1)", i)
				require.NoError(t, err)
			}
			require.Equal(t, queries, countWrites(ctx, t, backend), "writes should go to the primary")
			require.Zero(t, countWrites(ctx, t, replica), "writes should not go to the replica")
		})
	}
}

func countWrites(ctx context.Context, t *testing.T, pg *itest.Postgres) int {
	t.Helper()
	conn, err := pg.Connect(ctx, readRoutingDB)
	require.NoError(t, err)
	defer conn.Close(context.Background())
	var count int
	require.NoError(t, conn.QueryRow(ctx, "SELECT count(*) FROM writes").Scan(&count))
	return count
}
