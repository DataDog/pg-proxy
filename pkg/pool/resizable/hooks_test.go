// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package resizable

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
	"net"
	"testing"
)

func TestEffectiveDialerChargesBackendOnly(t *testing.T) {
	g := testGate(t, 1, nil)
	cfg := testConfig(t, 4)
	cfg.MinConns = 2
	cfg.MinIdleConns = 3
	dialErr := errors.New("dial failed")
	calls := 0
	cfg.BeforeConnect = func(_ context.Context, c *pgx.ConnConfig) error {
		c.DialFunc = func(context.Context, string, string) (net.Conn, error) {
			calls++
			if calls == 1 {
				return nil, dialErr
			}
			a, b := net.Pipe()
			t.Cleanup(func() { _ = b.Close() })
			return a, nil
		}
		return nil
	}
	installHooks(cfg, g)
	require.EqualValues(t, 2, cfg.MinConns)
	require.EqualValues(t, 3, cfg.MinIdleConns)
	c := cfg.ConnConfig.Copy()
	ctx := t.Context()
	require.NoError(t, cfg.BeforeConnect(ctx, c))
	require.Zero(t, g.getTotalOpenConns())
	establish := c.Tracer.(pgx.ConnectTracer).TraceConnectStart(ctx, pgx.TraceConnectStartData{ConnConfig: c})
	_, err := c.DialFunc(establish, "tcp", "localhost")
	require.ErrorIs(t, err, dialErr)
	require.Zero(t, g.getTotalOpenConns())
	backend, err := c.DialFunc(establish, "tcp", "localhost")
	require.NoError(t, err)
	require.EqualValues(t, 1, g.getTotalOpenConns())
	_, err = c.DialFunc(establish, "tcp", "localhost")
	require.ErrorIs(t, err, errVirtualConnectionLimit)
	require.Equal(t, 2, calls)
	control, err := c.DialFunc(ctx, "tcp", "localhost")
	require.NoError(t, err)
	require.NoError(t, control.Close())
	require.EqualValues(t, 1, g.getTotalOpenConns())
	require.NoError(t, backend.Close())
	require.ErrorIs(t, backend.Close(), net.ErrClosed)
	require.Zero(t, g.getTotalOpenConns())
	require.Zero(t, g.getNumConnsRetiring())
}

type dialChoosingTracer struct {
	queryOnlyTracer
	dial pgconn.DialFunc
}

func (t *dialChoosingTracer) TraceConnectStart(_ context.Context, data pgx.TraceConnectStartData) context.Context {
	data.ConnConfig.DialFunc = t.dial
	return context.Background()
}
func (*dialChoosingTracer) TraceConnectEnd(context.Context, pgx.TraceConnectEndData) {}

func TestConnectTracerCanChooseDialerAndContext(t *testing.T) {
	g := testGate(t, 1, nil)
	cfg := testConfig(t, 1)
	local, peer := net.Pipe()
	defer peer.Close()
	defer local.Close()
	cfg.ConnConfig.Tracer = &dialChoosingTracer{dial: func(context.Context, string, string) (net.Conn, error) { return local, nil }}
	installHooks(cfg, g)
	c := cfg.ConnConfig.Copy()
	require.NoError(t, cfg.BeforeConnect(t.Context(), c))
	a := &admission{Context: t.Context(), gate: g}
	g.grant(a)
	ctx := c.Tracer.(pgx.ConnectTracer).TraceConnectStart(a, pgx.TraceConnectStartData{ConnConfig: c})
	conn, err := c.DialFunc(ctx, "tcp", "unused")
	require.NoError(t, err)
	require.Same(t, local, conn.(*budgetConn).Conn)
	require.Zero(t, g.pending)
	require.EqualValues(t, 1, g.getTotalOpenConns())
	require.NoError(t, conn.Close())
}

type blockedClose struct {
	net.Conn
	entered, permit chan struct{}
}

func (c *blockedClose) Close() error { close(c.entered); <-c.permit; return c.Conn.Close() }
func TestClosingSocketStillConsumesBudget(t *testing.T) {
	g := testGate(t, 2, nil)
	c := testSocket(t, g)
	survivor := testSocket(t, g)
	b := &blockedClose{c.Conn, make(chan struct{}), make(chan struct{})}
	c.Conn = b
	done := make(chan error, 1)
	go func() { done <- c.Close() }()
	<-b.entered
	require.NoError(t, g.resize(1))
	require.False(t, survivor.retire(false))
	require.ErrorIs(t, g.reserve(nil), errVirtualConnectionLimit)
	require.EqualValues(t, 2, g.getTotalOpenConns())
	close(b.permit)
	require.NoError(t, <-done)
	require.EqualValues(t, 1, g.getTotalOpenConns())
	require.Zero(t, g.getNumConnsRetiring())
}

func TestBeforeConnectRejectionPreserved(t *testing.T) {
	g := testGate(t, 1, nil)
	cfg := testConfig(t, 2)
	want := errors.New("rejected")
	cfg.BeforeConnect = func(context.Context, *pgx.ConnConfig) error { return want }
	installHooks(cfg, g)
	require.ErrorIs(t, cfg.BeforeConnect(t.Context(), cfg.ConnConfig.Copy()), want)
	require.Zero(t, g.getTotalOpenConns())
}
