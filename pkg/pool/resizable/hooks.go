// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package resizable

import (
	"context"
	"errors"
	"net"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var errVirtualConnectionLimit = errors.New("virtual connection limit reached")

// budgetConn represents one reserved backend dial, then the exact net.Conn
// returned by that dial. Its retirement state is protected by the gate mutex.
type budgetConn struct {
	net.Conn
	gate             *resizableGate
	backend          *pgconn.PgConn
	closeMu          sync.Mutex
	retiring, closed bool
}

func (c *budgetConn) retire(force bool) bool {
	g := c.gate
	g.mu.Lock()
	defer g.mu.Unlock()
	return c.retireLocked(force)
}

func (c *budgetConn) retireLocked(force bool) bool {
	if c == nil || c.closed || c.retiring {
		return true
	}
	g := c.gate
	if !force && g.totalOpenConns-g.numConnsRetiring <= g.limit {
		g.hint()
		return false
	}
	c.retiring = true
	g.numConnsRetiring++
	return true
}
func (c *budgetConn) refund() {
	g := c.gate
	g.mu.Lock()
	defer g.mu.Unlock()
	c.closed = true
	delete(g.sockets, c.backend)
	g.totalOpenConns--
	if c.retiring {
		g.numConnsRetiring--
	}
	g.hint()
}
func (c *budgetConn) Close() error {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	if c.closed {
		return net.ErrClosed
	}
	c.retire(true)
	err := c.Conn.Close()
	if err == nil || errors.Is(err, net.ErrClosed) {
		c.refund()
	}
	return err
}

func installHooks(config *pgxpool.Config, gate *resizableGate) {
	before := config.BeforeConnect
	config.BeforeConnect = func(ctx context.Context, cfg *pgx.ConnConfig) error {
		if before != nil {
			if err := before(ctx, cfg); err != nil {
				return err
			}
		}
		cfg.Tracer = newConnectionTracer(cfg.Tracer, gate)
		return nil
	}
	afterRelease := config.AfterRelease
	config.AfterRelease = func(c *pgx.Conn) bool {
		keep := afterRelease == nil || afterRelease(c)
		return !gate.retireBackend(c, !keep)
	}
	beforeClose := config.BeforeClose
	config.BeforeClose = func(c *pgx.Conn) {
		gate.retireBackend(c, true)
		if beforeClose != nil {
			beforeClose(c)
		}
	}
}

// pgx passes TraceConnectStart a private config copy. Wrap the final dialer,
// after both BeforeConnect and the caller's tracer have seen that copy.
func (gate *resizableGate) wrapDials(cfg *pgx.ConnConfig, attempt *connectAttempt) {
	dial := cfg.DialFunc
	cfg.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
		if ctx.Value(connectKey{}) != attempt || attempt.established.Load() {
			return dial(ctx, network, addr)
		}
		grant, _ := ctx.Value(admissionKey{}).(*admission)
		if err := gate.reserve(grant); err != nil {
			return nil, err
		}
		c := &budgetConn{gate: gate}
		conn, err := dial(ctx, network, addr)
		if err != nil {
			c.refund()
			return nil, err
		}
		c.Conn = conn
		attempt.socket = c
		return c, nil
	}
	// A user hook may itself send a CancelRequest using the supplied context.
	if validate := cfg.ValidateConnect; validate != nil {
		cfg.ValidateConnect = func(ctx context.Context, c *pgconn.PgConn) error {
			return validate(context.WithValue(ctx, connectKey{}, (*connectAttempt)(nil)), c)
		}
	}
	after := cfg.Config.AfterConnect
	cfg.Config.AfterConnect = func(ctx context.Context, c *pgconn.PgConn) error {
		if ctx.Value(connectKey{}) == attempt {
			attempt.established.Store(true)
			gate.register(c, attempt.socket)
		}
		if after != nil {
			return after(ctx, c)
		}
		return nil
	}
}

func (g *resizableGate) register(backend *pgconn.PgConn, socket *budgetConn) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !socket.closed {
		socket.backend = backend
		g.sockets[backend] = socket
	}
}

func (g *resizableGate) retireBackend(c *pgx.Conn, force bool) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.sockets[c.PgConn()].retireLocked(force)
}
