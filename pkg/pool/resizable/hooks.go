package resizable

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// errVirtualConnectionLimit is internal to Resizable. Acquire catches it and
// waits instead of returning it to the caller.
var errVirtualConnectionLimit = errors.New("virtual connection limit reached")

// installHooks composes physical-capacity accounting with all lifecycle
// hooks already configured by the caller.
func installHooks(config *pgxpool.Config, gate *resizableGate) {
	beforeConnect := config.BeforeConnect
	config.BeforeConnect = func(ctx context.Context, connConfig *pgx.ConnConfig) error {
		if !gate.tryReserve() {
			return errVirtualConnectionLimit
		}
		if beforeConnect != nil {
			if err := beforeConnect(ctx, connConfig); err != nil {
				gate.release()
				return err
			}
		}
		connConfig.Tracer = newConnectionTracer(connConfig.Tracer, gate.release)
		return nil
	}

	afterConnect := config.AfterConnect
	config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		if afterConnect == nil {
			return nil
		}
		if err := afterConnect(ctx, conn); err != nil {
			// pgxpool closes a connection rejected by AfterConnect without adding
			// it to the pool, so BeforeClose cannot release its reserved gate slot.
			releaseAfterCleanup(gate, conn.PgConn().CleanupDone())
			return err
		}
		return nil
	}

	afterRelease := config.AfterRelease
	config.AfterRelease = func(conn *pgx.Conn) bool {
		keep := afterRelease == nil || afterRelease(conn)
		if keep {
			// pgxpool returns the connection after this hook. This notification is
			// therefore only a hint; Acquire also retries on a bounded interval.
			gate.notifyIdleConnAvailability()
		}
		return keep
	}

	beforeClose := config.BeforeClose
	config.BeforeClose = func(conn *pgx.Conn) {
		defer releaseAfterCleanup(gate, conn.PgConn().CleanupDone())
		if beforeClose != nil {
			beforeClose(conn)
		}
	}
}

// releaseAfterCleanup retains the physical slot until pgx confirms that the
// underlying network resources have finished closing.
func releaseAfterCleanup(gate *resizableGate, cleanupDone <-chan struct{}) {
	go func() {
		<-cleanupDone
		gate.release()
	}()
}
