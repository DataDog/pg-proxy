//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/DataDog/pg-proxy/internal/itest"
)

// TestSelectOne is the end-to-end smoke test through the default pool.
func TestSelectOne(t *testing.T) {
	proxy := startProxy(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	conn, err := proxy.Connect(ctx, dbDefault, itest.PostgresUser)
	require.NoError(t, err, "connect")
	defer conn.Close(context.Background())

	var n int
	require.NoError(t, conn.QueryRow(ctx, "SELECT 1").Scan(&n))
	require.Equal(t, 1, n)
}
