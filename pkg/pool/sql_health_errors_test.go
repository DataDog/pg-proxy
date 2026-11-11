// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package pool

import (
	"context"
	"errors"
	"fmt"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConclusiveSQLUnroutableErrorRequiresEveryJoinedCause(t *testing.T) {
	shutdown := &pgconn.PgError{Code: "57P01"}
	password := &pgconn.PgError{Code: "28P01"}
	tests := map[string]struct {
		err  error
		want bool
	}{
		"nil":                    {nil, false},
		"wrapped refusal":        {fmt.Errorf("connect: %w", &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}), true},
		"all conclusive":         {errors.Join(syscall.ECONNREFUSED, shutdown), true},
		"nested all conclusive":  {fmt.Errorf("connect: %w", errors.Join(shutdown, errors.Join(syscall.ECONNREFUSED, &pgconn.PgError{Code: "XX000"}))), true},
		"refusal then timeout":   {errors.Join(syscall.ECONNREFUSED, context.DeadlineExceeded), false},
		"timeout then refusal":   {errors.Join(context.DeadlineExceeded, syscall.ECONNREFUSED), false},
		"refusal then password":  {errors.Join(syscall.ECONNREFUSED, password), false},
		"password then refusal":  {errors.Join(password, syscall.ECONNREFUSED), false},
		"shutdown then password": {errors.Join(shutdown, password), false},
		"password then shutdown": {errors.Join(password, shutdown), false},
		"nested mixed":           {fmt.Errorf("connect: %w", errors.Join(shutdown, fmt.Errorf("retry: %w", errors.Join(syscall.ECONNREFUSED, context.DeadlineExceeded)))), false},
		"multiple percent-w":     {fmt.Errorf("first: %w; second: %w", syscall.ECONNREFUSED, password), false},
		"unknown cause":          {errors.Join(syscall.ECONNREFUSED, errors.New("unknown connection failure")), false},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) { assert.Equal(t, tc.want, isConclusiveSQLUnroutableError(tc.err)) })
	}
}

func TestHybridHealthClassifiesDriverCombinedConnectionErrors(t *testing.T) {
	for _, tc := range []struct {
		name              string
		last              error
		fallback, timeout string
	}{
		{"refusal then timeout", context.DeadlineExceeded, SQLHealthFallbackTimeout, SQLHealthTimeoutContext},
		{"refusal then password", &pgconn.PgError{Code: "28P01"}, SQLHealthFallbackIndeterminate, SQLHealthTimeoutNone},
		{"all refusals", syscall.ECONNREFUSED, "", ""},
		{"refusal then shutdown", &pgconn.PgError{Code: "57P01"}, "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Exercise pgx's real ConnectError / per-attempt / joined wrappers without network I/O.
			cfg, err := pgconn.ParseConfig("host=127.0.0.1,127.0.0.2 port=5432 user=probe dbname=probe sslmode=disable")
			require.NoError(t, err)
			attempts := 0
			cfg.DialFunc = func(context.Context, string, string) (net.Conn, error) {
				attempts++
				if attempts == 1 {
					return nil, syscall.ECONNREFUSED
				}
				return nil, tc.last
			}
			_, err = pgconn.ConnectConfig(context.Background(), cfg)
			require.Error(t, err)
			require.Equal(t, 2, attempts)
			var connectErr *pgconn.ConnectError
			require.ErrorAs(t, err, &connectErr)
			require.ErrorIs(t, err, syscall.ECONNREFUSED, "earlier refusal stays in the combined error")

			const primaryIP, uncertainIP = "10.0.0.1", "10.0.0.2"
			aggregate := aggregateSQLHealthCheckResults([]SQLHealthCheckResult{
				{IP: primaryIP}, {IP: uncertainIP, Err: err},
			})
			patroni := map[string]bool{primaryIP: false, uncertainIP: true}
			policy := newHybridHealthPolicy(true)
			policy.observePatroni(patroni, time.Now())
			decision := policy.observeSQL([]string{primaryIP, uncertainIP}, aggregate.Outcome, time.Now())
			assert.Equal(t, tc.fallback, decision.fallbackReason)
			assert.Equal(t, tc.timeout, decision.timeoutType)
			if tc.fallback != "" {
				assert.Equal(t, SQLRoutingResultCachedFallback, decision.routingResult)
				assert.Equal(t, patroni, decision.health)
				assert.NotContains(t, aggregate.Health, uncertainIP, "mixed causes cannot label an unknown IP unhealthy")
			} else {
				assert.Equal(t, SQLRoutingResultConclusive, decision.routingResult)
				assert.Equal(t, map[string]bool{primaryIP: true, uncertainIP: false}, decision.health)
			}

			var iterations []SQLIteration
			var fallbacks []SQLFallback
			var conclusiveErrors []error
			worker := &HealthWorker{sqlHooks: SQLHealthWorkerHooks{
				Iteration:       func(v SQLIteration, _ []string) { iterations = append(iterations, v) },
				CachedFallback:  func(v SQLFallback, _ []string) { fallbacks = append(fallbacks, v) },
				ConclusiveError: func(v SQLHealthCheckResult, _ []string) { conclusiveErrors = append(conclusiveErrors, v.Err) },
			}}
			worker.reportHybridDecision(&PoolHealth{}, decision, true)
			worker.reportConclusiveErrors([]completedSQLHealthCheck{{target: &sqlHealthTarget{}, outcome: aggregate.Outcome}}, decision)
			require.Len(t, iterations, 1)
			assert.Equal(t, decision.routingResult, iterations[0].RoutingResult)
			if tc.fallback != "" {
				require.Len(t, fallbacks, 1)
				assert.Equal(t, tc.fallback, fallbacks[0].Reason)
				assert.Empty(t, conclusiveErrors)
			} else {
				assert.Empty(t, fallbacks)
				require.Len(t, conclusiveErrors, 1)
			}

			// A successful role from another credential still resolves this same IP.
			resolved := aggregateSQLHealthCheckResults([]SQLHealthCheckResult{
				{IP: primaryIP}, {IP: uncertainIP, Err: err}, {IP: uncertainIP, InRecovery: true},
			})
			decision = policy.observeSQL([]string{primaryIP, uncertainIP}, resolved.Outcome, time.Now())
			assert.Equal(t, SQLRoutingResultConclusive, decision.routingResult)
			assert.Equal(t, map[string]bool{primaryIP: true, uncertainIP: false}, decision.health)
			conclusiveErrors = nil
			worker.reportConclusiveErrors([]completedSQLHealthCheck{{target: &sqlHealthTarget{}, outcome: resolved.Outcome}}, decision)
			if tc.fallback != "" {
				assert.Empty(t, conclusiveErrors, "resolving the role does not make a mixed error conclusive")
			} else {
				require.Len(t, conclusiveErrors, 1)
			}
		})
	}
}
