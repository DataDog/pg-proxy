// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package pool

import (
	"context"
	"errors"
	"fmt"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// observeRoutingBanlist supplies one newly completed poll as its own aggregate.
func observeRoutingBanlist(bans sqlRoutingBanlist, ips []string, outcome SQLHealthCheckOutcome, at time.Time) []SQLRoutingBanChange {
	return bans.update(ips, outcome, SQLHealthResultsSnapshot{
		ScheduledAt: at, ObservedAt: at, Results: outcome.Results,
	})
}

func TestSQLRoutingBanlistClearsWhenCandidatesDisappear(t *testing.T) {
	const (
		oldPrimary = "10.0.0.1"
		standby    = "10.0.0.2"
	)
	bans := make(sqlRoutingBanlist)
	observeRoutingBanlist(bans, []string{oldPrimary, standby}, SQLHealthCheckOutcome{
		Results: []SQLHealthCheckResult{
			{IP: oldPrimary, Err: &pgconn.PgError{Code: sqlHealthCheckCrashShutdown}},
			{IP: standby, InRecovery: true},
		},
	}, time.Now())

	dropped := observeRoutingBanlist(bans, []string{standby}, SQLHealthCheckOutcome{
		Results: []SQLHealthCheckResult{{IP: standby, InRecovery: true}},
	}, time.Now())
	assert.Equal(t, []SQLRoutingBanChange{{
		IP:     oldPrimary,
		Reason: SQLRoutingUnbanReasonDNSRemoved,
	}}, dropped)
}

func TestIsPostgresShutdownError(t *testing.T) {
	admin := &pgconn.PgError{Code: pgerrcode.AdminShutdown}
	crash := &pgconn.PgError{Code: sqlHealthCheckCrashShutdown}
	shutdown := &pgconn.PgError{Code: sqlHealthCheckCannotConnectNow, Message: "the database system is shutting down"}
	startup := &pgconn.PgError{Code: sqlHealthCheckCannotConnectNow, Message: "the database system is starting up"}
	tests := map[string]struct {
		err  error
		want bool
	}{
		"crash shutdown": {
			err:  &pgconn.PgError{Code: sqlHealthCheckCrashShutdown},
			want: true,
		},
		"cannot connect while shutting down": {
			err: &pgconn.PgError{
				Code:    sqlHealthCheckCannotConnectNow,
				Message: "the database system is shutting down",
			},
			want: true,
		},
		"cannot connect while starting up": {
			err: &pgconn.PgError{
				Code:    sqlHealthCheckCannotConnectNow,
				Message: "the database system is starting up",
			},
		},
		"admin termination":         {err: admin, want: true},
		"wrapped admin termination": {err: fmt.Errorf("probe: %w", admin), want: true},
		"generic cannot connect":    {err: &pgconn.PgError{Code: sqlHealthCheckCannotConnectNow}},
		"authentication failure":    {err: &pgconn.PgError{Code: "28P01"}},
		"connection refused":        {err: syscall.ECONNREFUSED},
		"wrapped crash":             {err: fmt.Errorf("probe: %w", &pgconn.PgError{Code: sqlHealthCheckCrashShutdown}), want: true},
		"non-Postgres error":        {err: context.DeadlineExceeded},
		"admin before crash":        {err: errors.Join(admin, crash), want: true},
		"crash before admin":        {err: errors.Join(crash, admin), want: true},
		"startup before shutdown":   {err: errors.Join(startup, shutdown), want: true},
		"shutdown before startup":   {err: errors.Join(shutdown, startup), want: true},
		"nested wrapped shutdown": {
			err:  fmt.Errorf("connect: %w", errors.Join(admin, fmt.Errorf("attempt: %w", errors.Join(startup, crash)))),
			want: true,
		},
		"joined without shutdown":  {err: errors.Join(startup, context.DeadlineExceeded)},
		"nil error":                {},
		"typed nil Postgres error": {err: (*pgconn.PgError)(nil)},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, test.want, isPostgresShutdownError(test.err))
		})
	}
}

func TestSQLRoutingBanlistShutdownWinsOverSuccess(t *testing.T) {
	const ip = "10.0.0.1"
	for _, standby := range []bool{false, true} {
		t.Run(fmt.Sprintf("standby_%t", standby), func(t *testing.T) {
			bans := make(sqlRoutingBanlist)
			at := time.Now()
			shutdown := SQLHealthCheckResult{IP: ip, Err: &pgconn.PgError{Code: sqlHealthCheckCrashShutdown}}
			outcome := aggregateSQLHealthCheckResults([]SQLHealthCheckResult{shutdown, {IP: ip, InRecovery: standby}}).Outcome
			changes := bans.update([]string{ip}, outcome, SQLHealthResultsSnapshot{
				ScheduledAt: at, ObservedAt: at, Results: []SQLHealthCheckResult{shutdown},
			})
			require.Contains(t, bans, ip)
			assert.Equal(t, []SQLRoutingBanChange{{IP: ip, Banned: true, Reason: SQLRoutingBanReasonPostgresShutdown}}, changes)
		})
	}
}

func TestSQLRoutingBanlistRequiresFreshUnambiguousRecovery(t *testing.T) {
	const ip = "10.0.0.1"
	for _, tc := range []struct {
		name             string
		fresh            SQLHealthCheckResult
		startedBeforeBan bool
		conflicting      bool
		ordered          bool
		wantRecovered    bool
	}{
		{name: "cached success does not clear ban", fresh: SQLHealthCheckResult{IP: ip, Err: context.DeadlineExceeded}},
		{name: "in-flight success does not clear ban", fresh: SQLHealthCheckResult{IP: ip}, startedBeforeBan: true},
		{name: "conflicting roles do not clear ban", fresh: SQLHealthCheckResult{IP: ip}, conflicting: true},
		{name: "newer primary supersedes cached standby and clears ban", fresh: SQLHealthCheckResult{IP: ip}, conflicting: true, ordered: true, wantRecovered: true},
		{name: "fresh primary clears ban", fresh: SQLHealthCheckResult{IP: ip}, wantRecovered: true},
		{name: "fresh standby clears ban", fresh: SQLHealthCheckResult{IP: ip, InRecovery: true}, wantRecovered: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			at := time.Now()
			bans := make(sqlRoutingBanlist)
			observeRoutingBanlist(bans, []string{ip}, SQLHealthCheckOutcome{Results: []SQLHealthCheckResult{
				{IP: ip, Err: &pgconn.PgError{Code: sqlHealthCheckCrashShutdown}},
			}}, at)
			startedAt := at.Add(time.Second)
			if tc.startedBeforeBan {
				startedAt = at.Add(-time.Second)
			}
			cachedSuccess := SQLHealthCheckResult{IP: ip, InRecovery: tc.fresh.InRecovery}
			if tc.conflicting {
				cachedSuccess.InRecovery = !tc.fresh.InRecovery
			}
			if tc.ordered {
				cachedSuccess.StartedAt, cachedSuccess.CompletedAt = at, at.Add(time.Millisecond)
				tc.fresh.StartedAt, tc.fresh.CompletedAt = startedAt, startedAt.Add(time.Millisecond)
			}
			outcome := aggregateSQLHealthCheckResults([]SQLHealthCheckResult{cachedSuccess, tc.fresh}).Outcome
			changes := bans.update([]string{ip}, outcome, SQLHealthResultsSnapshot{
				ScheduledAt: startedAt, ObservedAt: at.Add(2 * time.Second), Results: []SQLHealthCheckResult{tc.fresh},
			})
			if tc.wantRecovered {
				require.Len(t, changes, 1)
				assert.Empty(t, bans)
			} else {
				assert.Empty(t, changes)
				assert.Contains(t, bans, ip)
			}
		})
	}
}

func TestSQLRoutingBanlistRepeatedShutdownAdvancesRecoveryBoundary(t *testing.T) {
	const ip = "10.0.0.1"
	at := time.Now()
	bans := make(sqlRoutingBanlist)
	shutdown := SQLHealthCheckOutcome{Results: []SQLHealthCheckResult{{IP: ip, Err: &pgconn.PgError{Code: sqlHealthCheckCrashShutdown}}}}
	observeRoutingBanlist(bans, []string{ip}, shutdown, at)
	repeated := observeRoutingBanlist(bans, []string{ip}, shutdown, at.Add(2*time.Second))
	assert.Empty(t, repeated)
	success := SQLHealthCheckOutcome{Results: []SQLHealthCheckResult{{IP: ip}}, PrimaryIPs: []string{ip}}
	late := bans.update([]string{ip}, success, SQLHealthResultsSnapshot{
		ScheduledAt: at.Add(time.Second), ObservedAt: at.Add(3 * time.Second), Results: success.Results,
	})
	assert.Empty(t, late)
	assert.Contains(t, bans, ip)
	recovered := bans.update([]string{ip}, success, SQLHealthResultsSnapshot{
		ScheduledAt: at.Add(3 * time.Second), ObservedAt: at.Add(4 * time.Second), Results: success.Results,
	})
	require.Len(t, recovered, 1)
	assert.NotContains(t, bans, ip)
}

func TestSQLRoutingBanlistChangesDoNotAccumulate(t *testing.T) {
	bans := make(sqlRoutingBanlist)
	at := time.Now()
	for i := range 1000 {
		ip := fmt.Sprintf("instance-%d", i)
		changes := observeRoutingBanlist(bans, []string{ip}, SQLHealthCheckOutcome{Results: []SQLHealthCheckResult{
			{IP: ip, Err: &pgconn.PgError{Code: sqlHealthCheckCrashShutdown}},
		}}, at.Add(time.Duration(i)*time.Second))
		// Churn replaces the candidate each round: only its ban persists, while the
		// current update reports at most one removal and one addition.
		require.Len(t, bans, 1)
		require.Contains(t, bans, ip)
		wantChanges := 2
		if i == 0 {
			wantChanges = 1
		}
		require.Len(t, changes, wantChanges)
	}
	removed := observeRoutingBanlist(bans, nil, SQLHealthCheckOutcome{}, at.Add(1001*time.Second))
	require.Empty(t, bans)
	require.Len(t, removed, 1)
	assert.False(t, removed[0].Banned)
	next := observeRoutingBanlist(bans, nil, SQLHealthCheckOutcome{}, at.Add(1002*time.Second))
	assert.Empty(t, next, "previous unban events are not retained")
}
