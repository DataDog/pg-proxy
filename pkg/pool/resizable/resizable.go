package resizable

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	pgproxy "github.com/DataDog/pg-proxy"
)

const (
	poolResizeMetric        = "dd.orgstore.pg_proxy.connections.pool_resize"
	poolLimitMetric         = "dd.orgstore.pg_proxy.connections.pool_limit"
	retirementRetryInterval = 50 * time.Millisecond
)

// Resizable wraps one pgxpool.Pool and enforces a dynamic virtual limit on its
// total physical connections without changing the configured MaxConns ceiling.
type Resizable struct {
	pgx  *pgxpool.Pool
	gate *resizableGate

	// resizeMu serializes limit changes and recovery resets.
	resizeMu      sync.Mutex
	reconcileWake chan struct{}
	reconcileStop chan struct{}
	reconcileDone chan struct{}
	closeOnce     sync.Once
}

// New constructs a Resizable without modifying config. The physical ceiling
// remains config.MaxConns, while limit controls current capacity.
//
// pgxpool defaults config.MinConns and config.MinIdleConns to zero, and
// Resizable requires them to remain zero.
// Non-zero minimums could repeatedly recreate connections retired by the
// virtual limit; background minimum maintenance is not supported.
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
	r := &Resizable{
		pgx:           pgx,
		gate:          gate,
		reconcileWake: make(chan struct{}, 1),
		reconcileStop: make(chan struct{}),
		reconcileDone: make(chan struct{}),
	}
	go r.reconcileConnections(r.releaseIdleConnections)
	return r, nil
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

// Resize accepts a new virtual connection limit without waiting for physical
// closures. A worker retries idle retirement until enough connections are
// closing, including connections published idle after the initial sweep.
// Acquired connections are evaluated for retirement when returned.
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
	connRetirementRequired := limit < totalOpen
	_ = pgproxy.Metrics().Count(poolResizeMetric, 1, []string{"outcome:success"}, 1)
	_ = pgproxy.Metrics().Gauge(poolLimitMetric, float64(limit), nil, 1)
	pgproxy.Logger().Debug("resizable connection pool limit changed",
		"old_limit", previousLimit,
		"new_limit", limit,
		"physical_ceiling", physicalCeiling,
		"total_open_connections", totalOpen,
		"retirement_required", connRetirementRequired,
	)
	r.requestReconciliation()
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

// requestReconciliation coalesces resizes; the worker always reads the latest limit.
func (r *Resizable) requestReconciliation() {
	select {
	case r.reconcileWake <- struct{}{}:
	default:
	}
}

// reconcileConnections sleeps between resize requests when no further
// retirements are needed. Retrying while excess remains covers both the gap
// after AfterRelease and canceled constructors that publish directly to idle.
func (r *Resizable) reconcileConnections(sweep func()) {
	defer close(r.reconcileDone)
	for {
		select {
		case <-r.reconcileStop:
			return
		case <-r.reconcileWake:
		}
		ticker := time.NewTicker(retirementRetryInterval)
		for r.gate.needsRetirement() {
			select {
			case <-r.reconcileStop:
				ticker.Stop()
				return
			default:
			}
			sweep()
			select {
			case <-r.reconcileStop:
				ticker.Stop()
				return
			case <-r.reconcileWake:
			case <-ticker.C:
			}
		}
		ticker.Stop()
	}
}

func (r *Resizable) releaseIdleConnections() {
	// AfterRelease owns retirement reservations, including concurrent client
	// releases. The sweep must not reserve or close connections independently.
	for _, conn := range r.pgx.AcquireAllIdle(context.Background()) {
		conn.Release()
	}
}

func (r *Resizable) Close() {
	r.closeOnce.Do(func() {
		close(r.reconcileStop)
		<-r.reconcileDone
		r.pgx.Close()
	})
}
