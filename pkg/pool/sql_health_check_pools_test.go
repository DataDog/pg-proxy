package pool

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/puddle/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testSQLHealthCheckConnString = "host=127.0.0.1 port=1 user=probeuser password=probepass dbname=probedb"
const testSQLHealthCheckIP = "127.0.0.1"
const testOrgstoreCluster = "test-cluster"

func newTestSQLHealthCheckPools(t *testing.T) *SQLHealthCheckPools {
	t.Helper()

	pools := NewSQLHealthCheckPools(context.Background(), SQLHealthCheckPoolsConfig{
		ApplicationName: testOrgstoreCluster + "-" + DefaultSQLHealthCheckApplicationName,
	})
	t.Cleanup(pools.Close)
	return pools
}

func TestSQLHealthCheckPoolsApplySettings(t *testing.T) {
	pools := newTestSQLHealthCheckPools(t)
	pool, err := pools.GetOrAdd(testSQLHealthCheckIP, testSQLHealthCheckConnString+" pool_max_conns=12 pool_min_conns=7")
	require.NoError(t, err)

	cfg := pool.Config()
	assert.EqualValues(t, sqlHealthCheckMaxConns, cfg.MaxConns)
	assert.EqualValues(t, sqlHealthCheckMinConns, cfg.MinConns)
	assert.Equal(t, SQLHealthCheckConnectTimeout, cfg.ConnConfig.ConnectTimeout)
	assert.Equal(t, testOrgstoreCluster+"-"+DefaultSQLHealthCheckApplicationName, cfg.ConnConfig.RuntimeParams["application_name"])
	assert.Equal(t, strconv.FormatInt(SQLHealthCheckStatementTimeout.Milliseconds(), 10), cfg.ConnConfig.RuntimeParams["statement_timeout"])
}

func TestSQLHealthCheckPoolsUseDefaultApplicationName(t *testing.T) {
	pools := NewSQLHealthCheckPools(context.Background(), SQLHealthCheckPoolsConfig{})
	t.Cleanup(pools.Close)
	pool, err := pools.GetOrAdd(testSQLHealthCheckIP, testSQLHealthCheckConnString)
	require.NoError(t, err)
	assert.Equal(t, DefaultSQLHealthCheckApplicationName, pool.Config().ConnConfig.RuntimeParams["application_name"])
}

func TestSQLHealthCheckPoolsKeyByIPAndConnectionString(t *testing.T) {
	pools := newTestSQLHealthCheckPools(t)

	first, err := pools.GetOrAdd(testSQLHealthCheckIP, testSQLHealthCheckConnString)
	require.NoError(t, err)
	same, err := pools.GetOrAdd(testSQLHealthCheckIP, testSQLHealthCheckConnString)
	require.NoError(t, err)
	differentConnString, err := pools.GetOrAdd(testSQLHealthCheckIP, "host=127.0.0.1 port=2 user=other password=other dbname=other")
	require.NoError(t, err)
	differentIP, err := pools.GetOrAdd("127.0.0.2", testSQLHealthCheckConnString)
	require.NoError(t, err)

	assert.Same(t, first, same)
	assert.NotSame(t, first, differentConnString)
	assert.NotSame(t, first, differentIP)
	assert.Equal(t, 3, pools.Len())
}

func TestSQLHealthCheckPoolsShareConcurrentCallers(t *testing.T) {
	pools := newTestSQLHealthCheckPools(t)

	const callers = 16
	results := make(chan *pgxpool.Pool, callers)
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pool, err := pools.GetOrAdd(testSQLHealthCheckIP, testSQLHealthCheckConnString)
			if err != nil {
				errs <- err
				return
			}
			results <- pool
		}()
	}
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
	var first *pgxpool.Pool
	for pool := range results {
		if first == nil {
			first = pool
			continue
		}
		assert.Same(t, first, pool)
	}
	assert.NotNil(t, first)
	assert.Equal(t, 1, pools.Len())
}

func TestSQLHealthCheckPoolsRejectInvalidConnectionString(t *testing.T) {
	pools := newTestSQLHealthCheckPools(t)
	_, err := pools.GetOrAdd(testSQLHealthCheckIP, "host=127.0.0.1 pool_max_conns=not-a-number")

	require.Error(t, err)
	assert.Zero(t, pools.Len())
}

func TestSQLHealthCheckPoolsCloseAndReuse(t *testing.T) {
	pools := newTestSQLHealthCheckPools(t)

	first, err := pools.GetOrAdd(testSQLHealthCheckIP, testSQLHealthCheckConnString)
	require.NoError(t, err)
	pools.Close()
	assert.Zero(t, pools.Len())

	_, err = first.Acquire(context.Background())
	require.ErrorIs(t, err, puddle.ErrClosedPool)

	second, err := pools.GetOrAdd(testSQLHealthCheckIP, testSQLHealthCheckConnString)
	require.NoError(t, err)
	assert.NotSame(t, first, second)
	assert.Equal(t, 1, pools.Len())
}

func TestSQLHealthCheckPoolsRemoveIP(t *testing.T) {
	pools := newTestSQLHealthCheckPools(t)

	first, err := pools.GetOrAdd(testSQLHealthCheckIP, testSQLHealthCheckConnString)
	require.NoError(t, err)
	second, err := pools.GetOrAdd(testSQLHealthCheckIP, "host=127.0.0.1 port=2 user=other password=other dbname=other")
	require.NoError(t, err)
	_, err = pools.GetOrAdd("127.0.0.2", testSQLHealthCheckConnString)
	require.NoError(t, err)

	pools.RemoveIP(testSQLHealthCheckIP)

	assert.Equal(t, 1, pools.Len())
	_, err = first.Acquire(context.Background())
	require.ErrorIs(t, err, puddle.ErrClosedPool)
	_, err = second.Acquire(context.Background())
	require.ErrorIs(t, err, puddle.ErrClosedPool)
}

func TestSQLHealthCheckConfigPreservesConnectionSettings(t *testing.T) {
	source, err := pgxpool.ParseConfig("host=localhost dbname=orders user=service sslmode=require")
	require.NoError(t, err)
	source.ConnConfig.RuntimeParams["options"] = "-c search_path=custom"
	source.ConnConfig.RuntimeParams["application_name"] = "serving"
	source.MaxConns = 20
	probe := SQLHealthCheckConfig(source, SQLHealthCheckPoolsConfig{ApplicationName: "probe"})
	assert.Equal(t, source.ConnConfig.TLSConfig, probe.ConnConfig.TLSConfig)
	assert.Equal(t, source.ConnConfig.Fallbacks, probe.ConnConfig.Fallbacks)
	assert.Equal(t, "-c search_path=custom", probe.ConnConfig.RuntimeParams["options"])
	assert.Equal(t, "probe", probe.ConnConfig.RuntimeParams["application_name"])
	assert.Equal(t, "serving", source.ConnConfig.RuntimeParams["application_name"])
	assert.EqualValues(t, 1, probe.MaxConns)
	assert.EqualValues(t, 20, source.MaxConns)
}
