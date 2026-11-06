// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

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

const retirementMarkerKey = "pg-proxy.resizable.retiring"

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
				gate.connConstructionFailed()
				return err
			}
		}
		connConfig.Tracer = newConnectionTracer(connConfig.Tracer, gate.connConstructionFailed)
		return nil
	}

	afterConnect := config.AfterConnect
	config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		if afterConnect == nil {
			return nil
		}
		if err := afterConnect(ctx, conn); err != nil {
			// pgxpool closes a connection rejected by AfterConnect without adding
			// it to the pool, so BeforeClose cannot record when it finishes closing.
			recordConnClosedAfterCleanup(gate, conn.PgConn().CleanupDone(), false)
			return err
		}
		return nil
	}

	afterRelease := config.AfterRelease
	config.AfterRelease = func(conn *pgx.Conn) bool {
		keepConn := afterRelease == nil || afterRelease(conn)
		if !keepConn {
			if conn != nil {
				gate.trackClosingConnection()
				markForRetirement(conn)
			}
			return false
		}
		if gate.tryRetireExcessConnection() {
			markForRetirement(conn)
			return false
		}
		// pgxpool returns the connection after this hook. This notification is
		// therefore only a hint; Acquire also retries on a bounded interval.
		gate.notifyIdleConnAvailability()
		return true
	}

	beforeClose := config.BeforeClose
	config.BeforeClose = func(conn *pgx.Conn) {
		defer recordConnClosedAfterCleanup(gate, conn.PgConn().CleanupDone(), isMarkedForRetirement(conn))
		if beforeClose != nil {
			beforeClose(conn)
		}
	}
}

// markForRetirement records that AfterRelease selected this connection to
// close, so BeforeClose can finish the corresponding gate accounting.
func markForRetirement(conn *pgx.Conn) {
	if conn != nil {
		conn.PgConn().CustomData()[retirementMarkerKey] = struct{}{}
	}
}

func isMarkedForRetirement(conn *pgx.Conn) bool {
	_, marked := conn.PgConn().CustomData()[retirementMarkerKey]
	return marked
}

// recordConnClosedAfterCleanup counts pending closures toward retirement while
// retaining physical capacity until the underlying network resources are closed.
func recordConnClosedAfterCleanup(gate *resizableGate, cleanupDone <-chan struct{}, retiring bool) {
	if !retiring {
		gate.trackClosingConnection()
	}
	go func() {
		<-cleanupDone
		gate.retiringConnClosed()
	}()
}
