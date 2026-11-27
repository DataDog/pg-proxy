// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

//go:build integration

package resizable

import (
	"context"
	"errors"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/puddle/v2"
	"github.com/stretchr/testify/require"

	"github.com/DataDog/pg-proxy/internal/itest"
)

func TestBackendSocketLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	dsn := os.Getenv("PG_PROXY_TEST_DATABASE_URL")
	if dsn == "" {
		pg, err := itest.StartPostgres(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = pg.Terminate(context.Background()) })
		dsn = pg.URL("postgres")
	}
	pool := func(t *testing.T, setup func(*pgxpool.Config)) *Resizable {
		t.Helper()
		cfg, err := pgxpool.ParseConfig(dsn)
		require.NoError(t, err)
		cfg.MaxConns = 4
		if setup != nil {
			setup(cfg)
		}
		p, err := New(ctx, cfg, 1)
		require.NoError(t, err)
		t.Cleanup(p.Close)
		return p
	}
	for _, mode := range []string{"force_close", "release_veto"} {
		t.Run(mode, func(t *testing.T) {
			entered, permit := make(chan struct{}), make(chan struct{})
			var unblock sync.Once
			var dials atomic.Int32
			p := pool(t, func(c *pgxpool.Config) {
				dial := c.ConnConfig.DialFunc
				c.ConnConfig.DialFunc = func(ctx context.Context, n, a string) (net.Conn, error) {
					conn, err := dial(ctx, n, a)
					if err == nil && dials.Add(1) == 1 {
						return &blockedClose{conn, entered, permit}, nil
					}
					return conn, err
				}
				if mode == "release_veto" {
					c.AfterRelease = func(*pgx.Conn) bool { return false }
				}
			})
			defer unblock.Do(func() { close(permit) })
			first, err := p.Acquire(ctx)
			require.NoError(t, err)
			defer first.Release()
			pid := first.Conn().PgConn().PID()
			result := make(chan error, 1)
			go func() {
				conn, err := p.Acquire(ctx)
				if err == nil {
					if conn.Conn().PgConn().PID() == pid {
						err = errors.New("reused a closed backend")
					}
					conn.Release()
				}
				result <- err
			}()
			waitQueue(t, p.gate, 1)
			if mode == "release_veto" {
				first.Release()
			} else {
				go first.Conn().PgConn().Conn().Close()
			}
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			require.EqualValues(t, 1, p.TotalConnectionsOpen())
			require.EqualValues(t, 1, dials.Load())
			unblock.Do(func() { close(permit) })
			require.NoError(t, <-result)
			require.EqualValues(t, 2, dials.Load())
		})
	}
	t.Run("canceled_constructor_publishes_idle", func(t *testing.T) {
		entered, permit := make(chan struct{}), make(chan struct{})
		var once sync.Once
		p := pool(t, func(c *pgxpool.Config) {
			c.AfterConnect = func(context.Context, *pgx.Conn) error { close(entered); <-permit; return nil }
		})
		defer once.Do(func() { close(permit) })
		short, stop := context.WithCancel(ctx)
		defer stop()
		result := make(chan error, 1)
		go func() {
			conn, err := p.Acquire(short)
			if conn != nil {
				conn.Release()
			}
			result <- err
		}()
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		stop()
		require.ErrorIs(t, <-result, context.Canceled)
		require.EqualValues(t, 1, p.TotalConnectionsOpen())
		go func() {
			conn, err := p.Acquire(ctx)
			if conn != nil {
				conn.Release()
			}
			result <- err
		}()
		waitQueue(t, p.gate, 1)
		once.Do(func() { close(permit) })
		require.NoError(t, <-result)
	})
	t.Run("idle_reuse_during_dial_then_quiet_shrink", func(t *testing.T) {
		entered, permit := make(chan struct{}), make(chan struct{})
		var dials atomic.Int32
		var unblock sync.Once
		p := pool(t, func(c *pgxpool.Config) {
			dial := c.ConnConfig.DialFunc
			c.ConnConfig.DialFunc = func(ctx context.Context, n, a string) (net.Conn, error) {
				if dials.Add(1) == 2 {
					close(entered)
					<-permit
				}
				return dial(ctx, n, a)
			}
		})
		defer unblock.Do(func() { close(permit) })
		require.NoError(t, p.Resize(2))
		first, err := p.Acquire(ctx)
		require.NoError(t, err)
		defer first.Release()
		pid := first.Conn().PgConn().PID()
		done := make(chan error, 1)
		go func() {
			second, err := p.Acquire(ctx)
			if second != nil {
				second.Release()
			}
			done <- err
		}()
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("second dial did not start")
		}
		first.Release()
		short, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		reused, err := p.Acquire(short)
		require.NoError(t, err)
		defer reused.Release()
		require.Equal(t, pid, reused.Conn().PgConn().PID())
		reused.Release()
		unblock.Do(func() { close(permit) })
		require.NoError(t, <-done)
		require.EqualValues(t, 2, p.TotalConnectionsOpen())
		require.NoError(t, p.Resize(1))
		require.Eventually(t, func() bool { return p.TotalConnectionsOpen() == 1 && p.Stat().IdleConns() == 1 }, time.Second, time.Millisecond)
	})
	t.Run("cancel_in_startup_hook_at_limit", func(t *testing.T) {
		var validations atomic.Int32
		p := pool(t, func(c *pgxpool.Config) {
			c.ConnConfig.ValidateConnect = func(ctx context.Context, backend *pgconn.PgConn) error {
				validations.Add(1)
				return backend.CancelRequest(ctx)
			}
		})
		require.NoError(t, p.Ping(ctx))
		require.EqualValues(t, 1, validations.Load())
		require.EqualValues(t, 1, p.TotalConnectionsOpen())
	})
	t.Run("cancel_query_at_limit", func(t *testing.T) {
		p := pool(t, nil)
		conn, err := p.Acquire(ctx)
		require.NoError(t, err)
		defer conn.Release()
		observer, err := pgx.Connect(ctx, dsn)
		require.NoError(t, err)
		defer observer.Close(ctx)
		backend := conn.Conn().PgConn()
		pid := backend.PID()
		done := make(chan error, 1)
		go func() { _, err := conn.Exec(ctx, "select pg_sleep(5)"); done <- err }()
		require.Eventually(t, func() bool {
			var active bool
			return observer.QueryRow(ctx, "select state='active' and wait_event='PgSleep' from pg_stat_activity where pid=$1", pid).Scan(&active) == nil && active
		}, 3*time.Second, time.Millisecond)
		require.NoError(t, backend.CancelRequest(ctx))
		var pgErr *pgconn.PgError
		require.ErrorAs(t, <-done, &pgErr)
		require.Equal(t, "57014", pgErr.Code)
		require.EqualValues(t, 1, p.TotalConnectionsOpen())
	})
	t.Run("close_wakes_before_drain", func(t *testing.T) {
		p := pool(t, nil)
		conn, err := p.Acquire(ctx)
		require.NoError(t, err)
		defer conn.Release()
		done := make(chan error, 3)
		for range cap(done) {
			go func() {
				c, err := p.Acquire(ctx)
				if c != nil {
					c.Release()
				}
				done <- err
			}()
		}
		waitQueue(t, p.gate, cap(done))
		go p.Close()
		for range cap(done) {
			select {
			case err := <-done:
				require.ErrorIs(t, err, puddle.ErrClosedPool)
			case <-time.After(time.Second):
				t.Fatal("waiter blocked on checked-out connection")
			}
		}
	})
	t.Run("mixed_fallback_error", func(t *testing.T) {
		want := errors.New("backend dial failed")
		var p *Resizable
		var attempts atomic.Int32
		p = pool(t, func(c *pgxpool.Config) {
			cfg := c.ConnConfig
			cfg.Fallbacks = []*pgconn.FallbackConfig{{Host: cfg.Host, Port: cfg.Port, TLSConfig: cfg.TLSConfig}}
			dial := cfg.DialFunc
			cfg.DialFunc = func(ctx context.Context, n, a string) (net.Conn, error) {
				if attempts.Add(1) == 2 {
					// The primary fails for a real reason; the fallback then hits capacity.
					if err := p.Resize(1); err != nil {
						return nil, err
					}
					return nil, want
				}
				return dial(ctx, n, a)
			}
		})
		require.NoError(t, p.Resize(2))
		held, err := p.Acquire(ctx)
		require.NoError(t, err)
		defer held.Release()
		short, stop := context.WithTimeout(ctx, time.Second)
		defer stop()
		_, err = p.Acquire(short)
		require.ErrorIs(t, err, want)
		require.ErrorIs(t, err, errVirtualConnectionLimit)
		require.EqualValues(t, 2, attempts.Load())
	})
	t.Run("reject_after_connect_and_reset", func(t *testing.T) {
		want := errors.New("reject backend")
		var attempts atomic.Int32
		p := pool(t, func(c *pgxpool.Config) {
			c.AfterConnect = func(context.Context, *pgx.Conn) error {
				if attempts.Add(1) == 1 {
					return want
				}
				return nil
			}
		})
		_, err := p.Acquire(ctx)
		require.ErrorIs(t, err, want)
		require.Zero(t, p.TotalConnectionsOpen())
		require.NoError(t, p.Ping(ctx))
		p.Reset()
		require.Eventually(t, func() bool { return p.TotalConnectionsOpen() == 0 }, time.Second, time.Millisecond)
		require.NoError(t, p.Ping(ctx))
	})
}
