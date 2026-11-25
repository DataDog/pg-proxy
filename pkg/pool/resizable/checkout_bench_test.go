// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package resizable

import (
	"context"
	"errors"
	"net"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PG_PROXY_TEST_DATABASE_URL points at a disposable Postgres. No SQL runs in
// the timed loop. Latency measures Acquire; ns/op also includes hold and release.
func BenchmarkCheckout(b *testing.B) {
	dsn := os.Getenv("PG_PROXY_TEST_DATABASE_URL")
	if dsn == "" {
		b.Skip("set PG_PROXY_TEST_DATABASE_URL")
	}
	for _, tc := range []struct {
		name    string
		limit   int32
		workers int
		hold    time.Duration
	}{
		{"serial_headroom", 32, 1, 0},
		{"serial_limit_one", 1, 1, 0},
		{"during_slow_dial", 2, 1, 0},
		{"contended", 8, 64, 0},
		{"contended_hold_1ms", 8, 64, time.Millisecond},
	} {
		b.Run(tc.name, func(b *testing.B) {
			cfg, err := pgxpool.ParseConfig(dsn)
			if err != nil {
				b.Fatal(err)
			}
			cfg.MaxConns = 2 * tc.limit
			var slow *stalledDial
			if tc.name == "during_slow_dial" {
				slow = stallSecondDial(cfg)
			}
			p, err := New(b.Context(), cfg, tc.limit)
			if err != nil {
				b.Fatal(err)
			}
			defer p.Close()
			if slow != nil {
				defer close(slow.resume)
			}
			warm := int(tc.limit)
			if tc.name == "serial_headroom" {
				warm = 16
			} else if slow != nil {
				warm = 1
			}
			var held []*pgxpool.Conn
			for range warm {
				conn, err := p.Acquire(b.Context())
				if err != nil {
					b.Fatal(err)
				}
				held = append(held, conn)
				defer conn.Release()
			}
			if slow != nil {
				go func() {
					conn, err := p.Acquire(b.Context())
					if err == nil {
						conn.Release()
					}
				}()
				select {
				case <-slow.entered:
				case <-time.After(time.Second):
					b.Fatal("second dial did not start")
				}
			}
			for _, conn := range held {
				conn.Release()
			}
			readyBy := time.Now().Add(2 * time.Second)
			for p.Stat().IdleConns() != int32(warm) {
				if time.Now().After(readyBy) {
					b.Fatal("warm connections did not become idle")
				}
				time.Sleep(time.Microsecond)
			}
			latencies := make([]time.Duration, b.N)
			var timeouts atomic.Int64
			var wg sync.WaitGroup
			b.ReportAllocs()
			b.ResetTimer()
			for w := range min(tc.workers, b.N) {
				wg.Go(func() {
					for i := w; i < b.N; i += tc.workers {
						ctx := b.Context()
						var cancel context.CancelFunc
						if tc.workers > 1 || slow != nil {
							ctx, cancel = context.WithTimeout(ctx, 50*time.Millisecond)
						}
						start := time.Now()
						c, err := p.Acquire(ctx)
						latencies[i] = time.Since(start)
						if cancel != nil {
							cancel()
						}
						if err != nil {
							if errors.Is(err, context.DeadlineExceeded) {
								timeouts.Add(1)
								continue
							}
							b.Error(err)
							return
						}
						if tc.hold != 0 {
							time.Sleep(tc.hold)
						}
						c.Release()
					}
				})
			}
			wg.Wait()
			b.StopTimer()
			slices.Sort(latencies)
			b.ReportMetric(float64(latencies[(len(latencies)-1)*99/100].Nanoseconds())/1e3, "p99-us")
			b.ReportMetric(float64(timeouts.Load())*100/float64(b.N), "timeout-%")
		})
	}
}

// The second constructor stays inside DialFunc for the whole timed loop. The
// first connection must remain available for repeated checkout and release.
type stalledDial struct{ entered, resume chan struct{} }

func stallSecondDial(cfg *pgxpool.Config) *stalledDial {
	s := &stalledDial{make(chan struct{}), make(chan struct{})}
	dial := cfg.ConnConfig.DialFunc
	var attempts atomic.Int32
	cfg.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		if attempts.Add(1) == 2 {
			close(s.entered)
			<-s.resume
		}
		return dial(ctx, network, address)
	}
	return s
}
