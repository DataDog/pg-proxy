// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

//go:build integration

package integration

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/DataDog/pg-proxy/internal/itest"
	pgconfig "github.com/DataDog/pg-proxy/pkg/config"
)

// TestTransactionPooling verifies the transaction-pooling contract: client
// sessions share backend connections, and an open transaction pins its backend
// connection until it ends. With max_conns=1 both properties are observable.
func TestTransactionPooling(t *testing.T) {
	connStr := startProxy(t).URL(dbPooled, itest.PostgresUser)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	c1, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err, "client 1 connect")
	defer c1.Close(context.Background())
	c2, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err, "client 2 connect")
	defer c2.Close(context.Background())

	// Autocommit queries from two client sessions land on the same backend connection.
	var pid1, pid2 uint32
	require.NoError(t, c1.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid1))
	require.NoError(t, c2.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid2))
	require.Equal(t, pid1, pid2, "both client sessions should share the single pooled backend connection")

	// Pin the only backend connection to a transaction on client 1.
	tx, err := c1.Begin(ctx)
	require.NoError(t, err, "begin tx on client 1")
	_, err = tx.Exec(ctx, "SELECT 1")
	require.NoError(t, err, "exec inside tx on client 1")

	// Client 2 can only progress once client 1 commits. qcancel lets the timeout
	// path join the goroutine before the deferred c2.Close: *pgx.Conn is not safe
	// for concurrent use.
	qctx, qcancel := context.WithCancel(context.Background())
	defer qcancel()
	var c2PID uint32
	c2Done := make(chan error, 1)
	go func() {
		c2Done <- c2.QueryRow(qctx, "SELECT pg_backend_pid()").Scan(&c2PID)
	}()

	// Load can only keep client 2 blocked longer, so this is not racy: it fires
	// only if client 2 completes before COMMIT, which is the regression.
	select {
	case err := <-c2Done:
		t.Fatalf("client 2 completed while client 1 still held the transaction (pinning broken): err=%v pid=%d", err, c2PID)
	case <-time.After(2 * time.Second):
	}

	require.NoError(t, tx.Commit(ctx), "commit tx on client 1")

	select {
	case err := <-c2Done:
		require.NoError(t, err, "client 2 query after client 1 committed")
		require.Equal(t, pid1, c2PID, "client 2 should reuse the same pooled backend connection")
	case <-time.After(10 * time.Second):
		qcancel()
		<-c2Done
		t.Fatal("client 2 query did not complete after client 1 released the pooled connection")
	}
}

// TestSessionStateIsolation verifies that session GUCs set by one client do not
// leak to the next client on the same backend connection: the proxy resets
// session state when it releases a connection at a transaction boundary.
func TestSessionStateIsolation(t *testing.T) {
	connStr := startProxy(t).URL(dbPooled, itest.PostgresUser)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	const leaked = "leaked_by_client_a"

	// Record the startup search_path (what a reset restores) and the backend PID.
	baseConn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err, "baseline connect")
	var baseline string
	require.NoError(t, baseConn.QueryRow(ctx, "SHOW search_path").Scan(&baseline))
	var basePID uint32
	require.NoError(t, baseConn.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&basePID))
	require.NotEqual(t, leaked, baseline, "baseline search_path must differ from the leaked value for a meaningful assertion")
	require.NoError(t, baseConn.Close(ctx))

	// Client A sets a session GUC in autocommit; the connection is released right after.
	clientA, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err, "client A connect")
	_, err = clientA.Exec(ctx, fmt.Sprintf("SET search_path = '%s'", leaked))
	require.NoError(t, err, "client A SET search_path")
	require.NoError(t, clientA.Close(ctx))

	clientB, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err, "client B connect")
	defer clientB.Close(context.Background())

	var bPID uint32
	require.NoError(t, clientB.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&bPID))
	require.Equal(t, basePID, bPID, "clients should share the single pooled backend connection")

	var bSearchPath string
	require.NoError(t, clientB.QueryRow(ctx, "SHOW search_path").Scan(&bSearchPath))
	require.Equal(t, baseline, bSearchPath, "client A's search_path must not leak to client B")

	// A GUC set inside a transaction is visible within it and reset after release,
	// even though the transaction committed.
	tx, err := clientB.Begin(ctx)
	require.NoError(t, err, "begin tx on client B")
	_, err = tx.Exec(ctx, "SET search_path = 'inside_txn'")
	require.NoError(t, err, "SET search_path inside tx")
	var inTxn string
	require.NoError(t, tx.QueryRow(ctx, "SHOW search_path").Scan(&inTxn))
	require.Equal(t, "inside_txn", inTxn, "GUC set inside a transaction should be visible within it")
	require.NoError(t, tx.Commit(ctx), "commit tx on client B")

	var afterTxn string
	require.NoError(t, clientB.QueryRow(ctx, "SHOW search_path").Scan(&afterTxn))
	require.Equal(t, baseline, afterTxn, "GUC must be reset after the transaction is released back to the pool")
}

// TestSessionIdentityIsolation verifies that a role or session user set by one
// client does not leak to the next client on the same backend connection.
// RESET ALL does not reset role or session_authorization, so the proxy must
// reset them separately when it releases a connection. The reset must succeed
// for a non-superuser backend login and keep a role assigned at connection time.
func TestSessionIdentityIsolation(t *testing.T) {
	const (
		leakRole = "itest_identity_leak"
		// appUser is a non-superuser backend login and a member of the roles here.
		appUser     = "itest_identity_app"
		appPassword = "itest_identity_app"
		// alterRole is assigned to appUser by ALTER ROLE ... SET role.
		alterRole = "itest_identity_alter"
		// startupRole is assigned by a -c role= startup option, which overrides alterRole.
		startupRole = "itest_identity_startup"
	)
	require.NoError(t, backend.Exec(t.Context(),
		"CREATE ROLE "+leakRole+" NOLOGIN",
		"CREATE ROLE "+alterRole+" NOLOGIN",
		"CREATE ROLE "+startupRole+" NOLOGIN",
		"CREATE ROLE "+appUser+" LOGIN NOSUPERUSER PASSWORD '"+appPassword+"' IN ROLE "+leakRole+", "+alterRole+", "+startupRole,
		"ALTER ROLE "+appUser+" SET role = "+alterRole,
	))
	t.Cleanup(func() {
		_ = backend.Exec(context.Background(),
			"DROP ROLE IF EXISTS "+appUser,
			"DROP ROLE IF EXISTS "+leakRole,
			"DROP ROLE IF EXISTS "+alterRole,
			"DROP ROLE IF EXISTS "+startupRole,
		)
	})

	// appLogin returns startProxy's configuration with dbPooled logging in as appUser.
	appLogin := func(options map[string]string) pgconfig.Config {
		cfg := proxyConfig()
		for i := range cfg.Databases {
			if cfg.Databases[i].Name == "pooled-primary" {
				cfg.Databases[i].Username = appUser
				cfg.Databases[i].Password = pgconfig.Secret(appPassword)
				cfg.Databases[i].ConnectionOptions = options
			}
		}
		return cfg
	}

	tests := []struct {
		name   string
		config pgconfig.Config
		// user is the client user; without client authentication it must match the backend login.
		user      string
		statement string
		// wantCurrent and wantSession are the identity before client A and after the reset.
		wantCurrent, wantSession string
	}{
		{
			name:        "superuser SET ROLE",
			config:      proxyConfig(),
			user:        itest.PostgresUser,
			statement:   "SET ROLE " + leakRole,
			wantCurrent: itest.PostgresUser,
			wantSession: itest.PostgresUser,
		},
		{
			name:        "superuser SET SESSION AUTHORIZATION",
			config:      proxyConfig(),
			user:        itest.PostgresUser,
			statement:   "SET SESSION AUTHORIZATION " + leakRole,
			wantCurrent: itest.PostgresUser,
			wantSession: itest.PostgresUser,
		},
		{
			name:        "non-superuser with ALTER ROLE role SET ROLE",
			config:      appLogin(nil),
			user:        appUser,
			statement:   "SET ROLE " + leakRole,
			wantCurrent: alterRole,
			wantSession: appUser,
		},
		{
			name:        "non-superuser with startup role SET ROLE",
			config:      appLogin(map[string]string{"role": startupRole}),
			user:        appUser,
			statement:   "SET ROLE " + leakRole,
			wantCurrent: startupRole,
			wantSession: appUser,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			connStr := itest.MustStartProxy(t, test.config).URL(dbPooled, test.user)
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()

			const identity = "SELECT current_user, session_user, pg_backend_pid()"

			// Record the login identity (what a reset restores) and the backend PID.
			baseConn, err := pgx.Connect(ctx, connStr)
			require.NoError(t, err, "baseline connect")
			var baseCurrent, baseSession string
			var basePID uint32
			require.NoError(t, baseConn.QueryRow(ctx, identity).Scan(&baseCurrent, &baseSession, &basePID))
			require.Equal(t, test.wantCurrent, baseCurrent, "baseline current_user")
			require.Equal(t, test.wantSession, baseSession, "baseline session_user")
			require.NoError(t, baseConn.Close(ctx))

			// Client A changes its identity in autocommit; the connection is released
			// right after. One simple query keeps the check on the same backend link.
			clientA, err := pgx.Connect(ctx, connStr)
			require.NoError(t, err, "client A connect")
			results, err := clientA.PgConn().Exec(ctx, test.statement+"; SELECT current_user").ReadAll()
			require.NoError(t, err, "client A "+test.statement)
			require.Len(t, results, 2)
			require.Equal(t, leakRole, scalarResult(t, results[1]), "client A should run as the role it set")
			require.NoError(t, clientA.Close(ctx))

			clientB, err := pgx.Connect(ctx, connStr)
			require.NoError(t, err, "client B connect")
			defer clientB.Close(context.Background())

			// The same PID also shows the reset succeeded: a failed reset closes the connection.
			var bCurrent, bSession string
			var bPID uint32
			require.NoError(t, clientB.QueryRow(ctx, identity).Scan(&bCurrent, &bSession, &bPID))
			require.Equal(t, basePID, bPID, "clients should share the single pooled backend connection")
			require.Equal(t, baseCurrent, bCurrent, "client A's current_user must not leak to client B")
			require.Equal(t, baseSession, bSession, "client A's session_user must not leak to client B")
		})
	}
}

// scalarResult returns the single text cell of a one-row, one-column result.
func scalarResult(t *testing.T, r *pgconn.Result) string {
	t.Helper()
	require.NoError(t, r.Err, "result error")
	require.Len(t, r.Rows, 1, "expected exactly one row")
	require.Len(t, r.Rows[0], 1, "expected exactly one column")
	return string(r.Rows[0][0])
}

// TestSimpleQueryProtocol exercises the simple query protocol ('Q'), which pgx
// does not use by default. A single 'Q' can carry several statements; the proxy
// must return one result set per statement, in order, without desyncing.
func TestSimpleQueryProtocol(t *testing.T) {
	proxy := startProxy(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	conn, err := proxy.Connect(ctx, dbDefault, itest.PostgresUser)
	require.NoError(t, err, "connect")
	defer conn.Close(context.Background())
	pgConn := conn.PgConn()

	single, err := pgConn.Exec(ctx, "SELECT 1").ReadAll()
	require.NoError(t, err, "single simple query")
	require.Len(t, single, 1)
	require.Equal(t, "1", scalarResult(t, single[0]))

	multi, err := pgConn.Exec(ctx, "SELECT 1; SELECT 2; SELECT 3").ReadAll()
	require.NoError(t, err, "multi-statement simple query")
	require.Len(t, multi, 3, "one result set per statement, in order")
	require.Equal(t, "1", scalarResult(t, multi[0]))
	require.Equal(t, "2", scalarResult(t, multi[1]))
	require.Equal(t, "3", scalarResult(t, multi[2]))

	after, err := pgConn.Exec(ctx, "SELECT 42").ReadAll()
	require.NoError(t, err, "simple query after multi-statement batch")
	require.Len(t, after, 1)
	require.Equal(t, "42", scalarResult(t, after[0]))
}

// TestAbortedTransaction verifies the aborted-transaction state is proxied:
// after an error, commands fail with 25P02 until ROLLBACK, and ROLLBACK leaves
// the pooled connection clean for the same client and the next one.
func TestAbortedTransaction(t *testing.T) {
	connStr := startProxy(t).URL(dbPooled, itest.PostgresUser)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err, "connect")
	defer conn.Close(context.Background())

	var pid uint32
	require.NoError(t, conn.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid))

	_, err = conn.Exec(ctx, "BEGIN")
	require.NoError(t, err, "begin")

	_, err = conn.Exec(ctx, "SELECT 1/0")
	require.Error(t, err, "division by zero should error")
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, "22012", pgErr.Code, "expected division_by_zero (22012)")

	_, err = conn.Exec(ctx, "SELECT 1")
	require.Error(t, err, "query in an aborted transaction should be rejected")
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, "25P02", pgErr.Code, "expected in_failed_sql_transaction (25P02)")

	_, err = conn.Exec(ctx, "ROLLBACK")
	require.NoError(t, err, "rollback should end the aborted transaction")

	var n int
	require.NoError(t, conn.QueryRow(ctx, "SELECT 1").Scan(&n))
	require.Equal(t, 1, n, "session usable again after ROLLBACK")
	var pidAfter uint32
	require.NoError(t, conn.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pidAfter))
	require.Equal(t, pid, pidAfter, "same pooled backend connection throughout")

	conn2, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err, "second client connect")
	defer conn2.Close(context.Background())
	var pid2 uint32
	require.NoError(t, conn2.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid2))
	require.Equal(t, pid, pid2, "second client reuses the same pooled backend connection")
	require.NoError(t, conn2.QueryRow(ctx, "SELECT 1").Scan(&n))
	require.Equal(t, 1, n, "the pooled connection is clean for the next client, not aborted")
}

// TestClientDisconnectMidAbortedTransaction verifies that a client vanishing
// inside an aborted transaction, without ROLLBACK or Terminate, does not strand
// the only backend connection in that state.
func TestClientDisconnectMidAbortedTransaction(t *testing.T) {
	connStr := startProxy(t).URL(dbPooled, itest.PostgresUser)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	connA, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err, "client A connect")

	_, err = connA.Exec(ctx, "BEGIN")
	require.NoError(t, err, "A begin")
	_, err = connA.Exec(ctx, "SELECT 1/0")
	require.Error(t, err, "A: division by zero should error")

	_, err = connA.Exec(ctx, "SELECT 1")
	require.Error(t, err, "A: query in aborted transaction should be rejected")
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, "25P02", pgErr.Code, "A's transaction should be aborted before disconnect")

	// Hard-close the socket; conn.Close() would send a graceful Terminate.
	require.NoError(t, connA.PgConn().Conn().Close(), "hard-close A's socket")

	connB, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err, "client B connect")
	defer connB.Close(context.Background())

	var n int
	require.NoError(t, connB.QueryRow(ctx, "SELECT 1").Scan(&n),
		"B must get a clean connection after A disconnected mid-aborted-transaction")
	require.Equal(t, 1, n)
	require.Equal(t, byte('I'), connB.PgConn().TxStatus(),
		"B's session must be idle (not in a leaked or aborted transaction)")
}

// TestClientDisconnectWhileWaitingForPoolSlot checks that a client which hangs
// up while waiting for the only backend connection leaves the queue. Client 1
// holds the connection; client 2 queues pg_sleep(30) and drops its socket. After
// client 1 commits, client 3 must get the connection promptly: if client 2 had
// kept its place, pg_sleep would hold the connection.
func TestClientDisconnectWhileWaitingForPoolSlot(t *testing.T) {
	if runtime.GOOS != "linux" {
		// See pkg/session/client_monitor.go: without POLLRDHUP a peer FIN is not detected.
		t.Skip("detecting a dropped client before it holds a connection requires Linux POLLRDHUP")
	}
	connStr := startProxy(t).URL(dbPooled, itest.PostgresUser)
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()

	c1, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err, "client 1 connect")
	defer c1.Close(context.Background())
	c2, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err, "client 2 connect")

	var pid uint32
	require.NoError(t, c1.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid))

	tx, err := c1.Begin(ctx)
	require.NoError(t, err, "begin tx on client 1")
	_, err = tx.Exec(ctx, "SELECT 1")
	require.NoError(t, err, "exec inside tx on client 1")

	c2ctx, c2cancel := context.WithCancel(context.Background())
	defer c2cancel()
	c2Done := make(chan error, 1)
	go func() {
		_, err := c2.Exec(c2ctx, "SELECT pg_sleep(30)")
		c2Done <- err
	}()

	select {
	case err := <-c2Done:
		t.Fatalf("client 2 finished while client 1 still held the transaction (not waiting for a connection): %v", err)
	case <-time.After(2 * time.Second):
	}

	// Cut the TCP connection without a Terminate message.
	require.NoError(t, c2.PgConn().Conn().Close(), "hard-close client 2's socket")

	select {
	case <-c2Done:
	case <-time.After(5 * time.Second):
		c2cancel()
		<-c2Done
		t.Fatal("client 2 query did not return after its socket was closed")
	}
	_ = c2.Close(context.Background())

	// The proxy polls for dropped clients every 100ms; allow about 20 polls
	// before freeing the connection so a busy machine still drops client 2 first.
	time.Sleep(2 * time.Second)

	require.NoError(t, tx.Commit(ctx), "commit tx on client 1")

	c3, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err, "client 3 connect")
	defer c3.Close(context.Background())

	c3ctx, c3cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer c3cancel()
	var c3PID uint32
	require.NoError(t, c3.QueryRow(c3ctx, "SELECT pg_backend_pid()").Scan(&c3PID),
		"client 3 did not finish after client 1 committed; the dropped client may have taken the connection")
	require.Equal(t, pid, c3PID, "client 3 should reuse the same pooled Postgres connection")
}

// TestTransactionStatus verifies the ReadyForQuery transaction status reaches
// the client across a transaction: 'I' idle, 'T' in a block, 'E' aborted.
func TestTransactionStatus(t *testing.T) {
	proxy := startProxy(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	conn, err := proxy.Connect(ctx, dbDefault, itest.PostgresUser)
	require.NoError(t, err, "connect")
	defer conn.Close(context.Background())

	require.Equal(t, byte('I'), conn.PgConn().TxStatus(), "fresh session should be idle ('I')")

	_, err = conn.Exec(ctx, "BEGIN")
	require.NoError(t, err, "begin")
	require.Equal(t, byte('T'), conn.PgConn().TxStatus(), "should be in a transaction ('T') after BEGIN")

	_, err = conn.Exec(ctx, "SELECT 1")
	require.NoError(t, err, "select inside transaction")
	require.Equal(t, byte('T'), conn.PgConn().TxStatus(), "still in a transaction ('T') after a successful statement")

	_, err = conn.Exec(ctx, "SELECT 1/0")
	require.Error(t, err, "division by zero should error")
	require.Equal(t, byte('E'), conn.PgConn().TxStatus(), "transaction should be aborted ('E') after an error")

	_, err = conn.Exec(ctx, "ROLLBACK")
	require.NoError(t, err, "rollback")
	require.Equal(t, byte('I'), conn.PgConn().TxStatus(), "should be idle ('I') again after ROLLBACK")
}
