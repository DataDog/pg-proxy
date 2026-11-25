// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package server

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"

	"github.com/DataDog/pg-proxy/pkg/limiter"
	"github.com/DataDog/pg-proxy/pkg/link"
	"github.com/DataDog/pg-proxy/pkg/listener"
	"github.com/DataDog/pg-proxy/pkg/session"
)

// Exercise startup, runtime propagation, real pgx error handling, and a new
// checkout on the same client connection. The synthetic PostgreSQL peer accepts
// only query messages: no client startup options are forwarded to that peer.
func TestQueryCapabilityStartupAndPGXRecovery(t *testing.T) {
	for _, tc := range []struct {
		name, options string
		capable       bool
	}{
		{"old client", "", false},
		{"unaware", "-c pg_proxy.query_load_shedding=false", false},
		{"invalid boolean", "-c pg_proxy.query_load_shedding=maybe", false},
		{"punctuation truncated", "-c pg_proxy.query_load_shedding=true!", true},
		{"numeric boolean", "-c pg_proxy.query_load_shedding=1", true},
		{"uppercase boolean", "-c pg_proxy.query_load_shedding=TRUE", true},
		{"last empty ignored", "-c pg_proxy.query_load_shedding=true -c pg_proxy.query_load_shedding=", true},
		{"missing value", "-c pg_proxy.query_load_shedding=", false},
		{"aware", "-c pg_proxy.query_load_shedding=true", true},
		{"priority alone", "-c pg_proxy.priority=0", false},
		{"aware with priority", "-c pg_proxy.query_load_shedding=true -c pg_proxy.priority=5", true},
		{"future option ignored", "-c pg_proxy.future_capability=true", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			backend := &queryCapabilityBackend{t: t}
			backend.reject.Store(true)
			runtime, inventory := newRuntimeForTest()
			inventory.backends["primary"].(*testBackend).Backend = backend
			engine, err := listener.New(ctx, listener.Config[ResolvedStartup]{
				ListenAddress: "127.0.0.1:0",
				Admission:     limiter.New(limiter.Params{GlobalLimit: 10, DefaultServiceMax: 10}),
				Hooks:         listener.Hooks[ResolvedStartup]{ResolveStartup: runtime.ResolveStartup, NewSession: runtime.NewSession},
			})
			require.NoError(t, err)
			done := make(chan error, 1)
			go func() { done <- engine.Run() }()
			defer func() { cancel(); _ = engine.Close(); <-done }()

			config, err := pgx.ParseConfig("postgres://app_user@" + engine.Addr().String() + "/testdb?sslmode=disable")
			require.NoError(t, err)
			config.RuntimeParams["options"] = tc.options
			conn, err := pgx.ConnectConfig(ctx, config)
			require.NoError(t, err)
			defer conn.Close(context.Background())
			pid := conn.PgConn().PID()

			_, err = conn.Exec(ctx, "SELECT 1", pgx.QueryExecModeSimpleProtocol)
			if tc.capable {
				var pgErr *pgconn.PgError
				require.ErrorAs(t, err, &pgErr)
				require.Equal(t, "53000", pgErr.Code)
				require.Zero(t, backend.checkouts.Load())
				// pgx must also synchronize an extended-protocol rejection.
				_, err = conn.Exec(ctx, "SELECT 1", pgx.QueryExecModeExec)
				require.ErrorAs(t, err, &pgErr)
				require.Equal(t, "53000", pgErr.Code)
				require.Zero(t, backend.checkouts.Load())
			} else {
				require.NoError(t, err)
				require.EqualValues(t, 1, backend.checkouts.Load())
			}
			// Capability is not an enforcement switch. With policy off both kinds
			// of client can execute, without reconnecting or an aborted protocol.
			backend.reject.Store(false)
			_, err = conn.Exec(ctx, "SELECT 1", pgx.QueryExecModeSimpleProtocol)
			require.NoError(t, err)
			require.Equal(t, pid, conn.PgConn().PID())
			require.Equal(t, byte('I'), conn.PgConn().TxStatus())
		})
	}
}

type queryCapabilityBackend struct {
	t         *testing.T
	reject    atomic.Bool
	checkouts atomic.Int64
}

func (b *queryCapabilityBackend) LoadShedder() session.LoadShedder { return b }
func (b *queryCapabilityBackend) Run(_ context.Context, fn func() error, _ session.Priority) (error, error) {
	if b.reject.Load() {
		return nil, errors.New("overloaded")
	}
	return fn(), nil
}
func (b *queryCapabilityBackend) Acquire(context.Context, session.LinkRequest) (link.Link, error) {
	b.checkouts.Add(1)
	proxy, postgres := net.Pipe()
	b.t.Cleanup(func() { _ = proxy.Close(); _ = postgres.Close() })
	go func() {
		peer := pgproto3.NewBackend(postgres, postgres)
		msg, err := peer.Receive()
		if err != nil {
			return
		}
		if _, ok := msg.(*pgproto3.Query); !ok {
			return
		}
		peer.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
		peer.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		_ = peer.Flush()
	}()
	return link.NewFrontendLink(pgproto3.NewFrontend(proxy, proxy)), nil
}
