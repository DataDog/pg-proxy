package resizable

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"

	pgproxy "github.com/DataDog/pg-proxy"
)

const (
	poolResizeMetric = "dd.orgstore.pg_proxy.connections.pool_resize"
	poolLimitMetric  = "dd.orgstore.pg_proxy.connections.pool_limit"
)

// Resizable wraps one pgxpool.Pool and enforces a dynamic virtual limit on its
// total physical connections without changing the configured MaxConns ceiling.
type Resizable struct {
	pgx  *pgxpool.Pool
	gate *resizableGate

	// resizeMu keeps the gate update and any required pool reset in one resize operation.
	resizeMu sync.Mutex
}

// New constructs a Resizable without modifying config. The physical ceiling
// remains config.MaxConns, while limit controls current capacity.
//
// Resizable assumes config.MinConns and config.MinIdleConns remain zero.
// pgxpool maintains non-zero minimums through Puddle's CreateResource path,
// which can add a connection to the idle pool after Reset without checking
// whether its construction began before the reset.
func New(ctx context.Context, config *pgxpool.Config, limit int32) (*Resizable, error) {
	if config == nil {
		return nil, errors.New("pgxpool config must not be nil")
	}
	if limit > config.MaxConns {
		return nil, fmt.Errorf("connection limit %d exceeds physical ceiling %d", limit, config.MaxConns)
	}
	gate, err := newResizableGate(limit)
	if err != nil {
		return nil, err
	}
	pgxConfig := config.Copy()
	installHooks(pgxConfig, gate)
	pgx, err := pgxpool.NewWithConfig(ctx, pgxConfig)
	if err != nil {
		return nil, err
	}
	return &Resizable{pgx: pgx, gate: gate}, nil
}

func (r *Resizable) Acquire(ctx context.Context) (*pgxpool.Conn, error) {
	return acquireWithGate(ctx, r.gate, r.pgx.Acquire)
}

func acquireWithGate(
	ctx context.Context,
	gate *resizableGate,
	acquire func(context.Context) (*pgxpool.Conn, error),
) (*pgxpool.Conn, error) {
	for {
		signal := gate.getAvailabilitySignal()
		conn, err := acquire(ctx)
		if !errors.Is(err, errVirtualConnectionLimit) {
			return conn, err
		}
		if err := gate.wait(ctx, signal); err != nil {
			return nil, err
		}
	}
}

// Resize changes the virtual connection limit. Downsizing below the number of
// open connections resets pgx so idle connections close immediately and
// acquired connections close on return.
func (r *Resizable) Resize(limit int32) error {
	r.resizeMu.Lock()
	defer r.resizeMu.Unlock()
	previousLimit := r.Limit()
	physicalCeiling := r.GetPhysicalCeiling()
	if limit > physicalCeiling {
		err := fmt.Errorf("connection limit %d exceeds physical ceiling %d", limit, physicalCeiling)
		reportResizeFailure("physical_ceiling", previousLimit, limit, physicalCeiling, err)
		return err
	}
	if err := r.gate.resize(limit); err != nil {
		reportResizeFailure("invalid_limit", previousLimit, limit, physicalCeiling, err)
		return err
	}
	totalOpen := r.TotalConnectionsOpen()
	resetRequired := limit < totalOpen
	_ = pgproxy.Metrics().Count(poolResizeMetric, 1, []string{"outcome:success"}, 1)
	_ = pgproxy.Metrics().Gauge(poolLimitMetric, float64(limit), nil, 1)
	pgproxy.Logger().Debug("resizable connection pool limit changed",
		"old_limit", previousLimit,
		"new_limit", limit,
		"physical_ceiling", physicalCeiling,
		"total_open_connections", totalOpen,
		"reset_required", resetRequired,
	)
	if resetRequired {
		// Reset retires the current pool generation so replacement connections
		// obey the lower virtual limit. Idle connections close immediately, while
		// acquired connections close when returned.
		// TODO(OSQUERY-615): Consider retiring only excess connections instead
		// of resetting every physical connection.
		pgproxy.Logger().Warn("resetting resizable connection pool after downsize",
			"new_limit", limit,
			"total_open_connections", totalOpen,
		)
		r.pgx.Reset()
	}
	return nil
}

func reportResizeFailure(reason string, previousLimit, limit, physicalCeiling int32, err error) {
	pgproxy.Logger().Error("resizable connection pool resize failed",
		"reason", reason,
		"old_limit", previousLimit,
		"new_limit", limit,
		"physical_ceiling", physicalCeiling,
		"error", err,
	)
	_ = pgproxy.Metrics().Count(poolResizeMetric, 1, []string{"outcome:error", "reason:" + reason}, 1)
}

func (r *Resizable) Limit() int32 { return r.gate.getLimit() }

func (r *Resizable) GetPhysicalCeiling() int32 { return r.pgx.Config().MaxConns }

// TotalConnectionsOpen includes connections being constructed or cleaned up in
// addition to the idle and acquired connections visible in pgxpool statistics.
func (r *Resizable) TotalConnectionsOpen() int32 { return r.gate.getTotalOpenConns() }

func (r *Resizable) Config() *pgxpool.Config { return r.pgx.Config() }

func (r *Resizable) Stat() *pgxpool.Stat { return r.pgx.Stat() }

type pingConnection interface {
	Ping(context.Context) error
	Release()
}

func (r *Resizable) Ping(ctx context.Context) error {
	return pingWithAcquire(ctx, func(ctx context.Context) (pingConnection, error) {
		return r.Acquire(ctx)
	})
}

func pingWithAcquire(ctx context.Context, acquire func(context.Context) (pingConnection, error)) error {
	conn, err := acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	return conn.Ping(ctx)
}

// Reset closes every physical connection while leaving the pool available.
// It is serialized with Resize so recovery resets cannot race limit changes.
func (r *Resizable) Reset() {
	r.resizeMu.Lock()
	defer r.resizeMu.Unlock()
	r.pgx.Reset()
}

func (r *Resizable) Close() { r.pgx.Close() }
