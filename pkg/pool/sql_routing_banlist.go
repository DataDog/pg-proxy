// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package pool

import (
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	SQLRoutingBanReasonPostgresShutdown   = "postgres_shutdown"
	SQLRoutingUnbanReasonConclusiveResult = "conclusive_result"
	SQLRoutingUnbanReasonDNSRemoved       = "dns_removed"
)

// SQLRoutingBanChange reports one IP entering or leaving the SQL routing banlist.
type SQLRoutingBanChange struct {
	IP     string
	Banned bool // New state: true on entry, false on recovery or candidate removal.
	Reason string
}

// sqlRoutingBanlist stores each retained IP's latest shutdown observation time.
// The owner must serialize access. Updates return only this call's transitions;
// they do not choose routes or retain an event history.
type sqlRoutingBanlist map[string]time.Time

func (b sqlRoutingBanlist) update(
	candidateIPs []string,
	outcome SQLHealthCheckOutcome,
	fresh SQLHealthResultsSnapshot,
) []SQLRoutingBanChange {
	candidates := make(map[string]struct{}, len(candidateIPs))
	for _, ip := range candidateIPs {
		candidates[ip] = struct{}{}
	}
	successfulResults := make(map[string]struct{})
	shutdownResults := make(map[string]struct{})
	for _, result := range fresh.Results {
		if result.Err == nil {
			successfulResults[result.IP] = struct{}{}
		}
	}
	// Conflicting successful roles cannot establish recovery.
	_, conflictingIPs := aggregateSQLComparisonRoles(outcome.Results)
	for _, ip := range conflictingIPs {
		delete(successfulResults, ip)
	}
	for _, result := range outcome.Results {
		if isPostgresShutdownError(result.Err) {
			shutdownResults[result.IP] = struct{}{}
		}
	}

	var changes []SQLRoutingBanChange
	for ip := range b {
		if _, retained := candidates[ip]; !retained {
			delete(b, ip)
			changes = append(changes, SQLRoutingBanChange{IP: ip, Reason: SQLRoutingUnbanReasonDNSRemoved})
		}
	}
	for ip := range shutdownResults {
		if _, retained := candidates[ip]; !retained {
			continue
		}
		if _, alreadyBanned := b[ip]; !alreadyBanned {
			b[ip] = fresh.ObservedAt
			changes = append(changes, SQLRoutingBanChange{IP: ip, Banned: true, Reason: SQLRoutingBanReasonPostgresShutdown})
		}
	}
	// Repeated fresh shutdown evidence moves the recovery boundary without emitting another
	// transition. Cached shutdown results must not keep moving that boundary on unrelated polls.
	for _, result := range fresh.Results {
		if isPostgresShutdownError(result.Err) {
			if bannedAt, banned := b[result.IP]; banned && fresh.ObservedAt.After(bannedAt) {
				b[result.IP] = fresh.ObservedAt
			}
		}
	}
	for ip := range successfulResults {
		// Credential checks for one host run concurrently, so a success and shutdown result in the
		// same round have no reliable ordering. Keep the IP banned until a later clean round proves
		// that it can conclusively report a PostgreSQL role again.
		if _, shuttingDown := shutdownResults[ip]; shuttingDown {
			continue
		}
		if bannedAt, wasBanned := b[ip]; wasBanned && fresh.ScheduledAt.After(bannedAt) {
			delete(b, ip)
			changes = append(changes, SQLRoutingBanChange{IP: ip, Reason: SQLRoutingUnbanReasonConclusiveResult})
		}
	}
	slices.SortFunc(changes, func(a, b SQLRoutingBanChange) int { return strings.Compare(a.IP, b.IP) })
	return changes
}

func isPostgresShutdownError(err error) bool {
	// A connection failure may join several PostgreSQL errors. Any qualifying
	// shutdown cause must win, regardless of its position in the error tree.
	switch wrapped := err.(type) {
	case interface{ Unwrap() []error }:
		for _, cause := range wrapped.Unwrap() {
			if isPostgresShutdownError(cause) {
				return true
			}
		}
		return false
	case interface{ Unwrap() error }:
		return isPostgresShutdownError(wrapped.Unwrap())
	}

	var postgresErr *pgconn.PgError
	if !errors.As(err, &postgresErr) || postgresErr == nil {
		return false
	}
	switch postgresErr.Code {
	case pgerrcode.AdminShutdown, sqlHealthCheckCrashShutdown:
		return true
	case sqlHealthCheckCannotConnectNow:
		return strings.Contains(postgresErr.Message, "the database system is shutting down")
	default:
		return false
	}
}
