// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/DataDog/pg-proxy/internal/itest"
)

// TestSelectOne is the end-to-end smoke test through the default pool.
func TestSelectOne(t *testing.T) {
	forEachExecMode(t, startProxy(t).URL(dbDefault, itest.PostgresUser), testSelectOne)
}

func testSelectOne(t *testing.T, connStr string) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err, "connect")
	defer conn.Close(context.Background())

	var n int
	require.NoError(t, conn.QueryRow(ctx, "SELECT 1").Scan(&n))
	require.Equal(t, 1, n)
}
