// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package pool

import (
	"syscall"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
)

// isConclusiveSQLUnroutableError is intentionally an allowlist. New or ambiguous errors fall back
// to Patroni unless SQL has already determined that same IP's role through another credential.
func isConclusiveSQLUnroutableError(err error) bool {
	// pgx joins failures from multiple connection attempts. A matching error anywhere in that
	// tree is insufficient: an earlier refusal must not hide a later timeout or auth failure.
	// Walk ordinary wrappers and require every branch of a combined error to be conclusive.
	switch wrapped := err.(type) { //nolint:errorlint // the manual walk above peels wrappers; leaves are typed directly
	case interface{ Unwrap() []error }:
		causes := wrapped.Unwrap()
		if len(causes) == 0 {
			return false
		}
		for _, cause := range causes {
			if !isConclusiveSQLUnroutableError(cause) {
				return false
			}
		}
		return true
	case interface{ Unwrap() error }:
		return isConclusiveSQLUnroutableError(wrapped.Unwrap())
	}

	if errno, ok := err.(syscall.Errno); ok { //nolint:errorlint // manual walk above already unwrapped; err is a leaf
		return errno == syscall.ECONNREFUSED
	}
	postgresErr, ok := err.(*pgconn.PgError) //nolint:errorlint // manual walk above already unwrapped; err is a leaf
	if !ok || postgresErr == nil {
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
		// System (58) and internal (XX) classes do not establish instance-wide unavailability.
		return false
	}
}
