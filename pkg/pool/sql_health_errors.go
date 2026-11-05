package pool

import (
	"errors"
	"syscall"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
)

// isConclusiveSQLUnroutableError is intentionally an allowlist. New or ambiguous errors fall back
// to Patroni unless SQL has already determined that same IP's role through another credential.
func isConclusiveSQLUnroutableError(err error) bool {
	if errors.Is(err, syscall.ECONNREFUSED) {
		return true
	}

	var postgresErr *pgconn.PgError
	if !errors.As(err, &postgresErr) {
		return false
	}
	switch postgresErr.Code {
	case pgerrcode.AdminShutdown,
		pgerrcode.CrashShutdown,
		pgerrcode.CannotConnectNow,
		pgerrcode.DiskFull,
		pgerrcode.OutOfMemory:
		return true
	default:
		return pgerrcode.IsSystemError(postgresErr.Code) || pgerrcode.IsInternalError(postgresErr.Code)
	}
}
