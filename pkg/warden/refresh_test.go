// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package warden

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRefresherAppliesOnlyNewerVersions(t *testing.T) {
	telemetry := captureTelemetry(t)
	value := "first"
	version := uint64(1)
	var applied []string
	refresher := NewRefresher(RefresherConfig[string]{
		Interval: time.Hour,
		Source: func(context.Context) (string, uint64, error) {
			return value, version, nil
		},
		Apply: func(cfg string) error {
			applied = append(applied, cfg)
			return nil
		},
	})

	require.NoError(t, refresher.Refresh())
	value = "same-version"
	require.NoError(t, refresher.Refresh())
	version = 2
	value = "second"
	require.NoError(t, refresher.Refresh())

	assert.Equal(t, []string{"first", "second"}, applied)
	assert.Contains(t, telemetry.logs(), logEvent{slog.LevelDebug, "skipping configuration update based on source change counters: source_revision=1 latest_applied_source_revision=1"})
	assert.NotContains(t, telemetry.logs(), logEvent{slog.LevelInfo, "skipping configuration update based on source change counters: source_revision=1 latest_applied_source_revision=1"})
}

func TestRefresherWrapsSourceAndApplyErrors(t *testing.T) {
	sourceError := errors.New("source")
	refresher := NewRefresher(RefresherConfig[string]{
		Interval: time.Hour,
		Source: func(context.Context) (string, uint64, error) {
			return "", 0, sourceError
		},
	})
	assert.EqualError(t, refresher.Refresh(), "failed to fetch configuration: source")

	refresher = NewRefresher(RefresherConfig[string]{
		Interval: time.Hour,
		Source: func(context.Context) (string, uint64, error) {
			return "config", 1, nil
		},
		Apply: func(string) error { return errors.New("apply") },
	})
	assert.EqualError(t, refresher.Refresh(), "failed to accept configuration: apply")
}

func TestRefresherSerializesConcurrentApplications(t *testing.T) {
	for _, sameVersion := range []bool{true, false} {
		name := "increasing versions"
		if sameVersion {
			name = "same version"
		}
		t.Run(name, func(t *testing.T) {
			const count = 32
			versions := make(chan uint64, count)
			ready := make(chan struct{}, count)
			start := make(chan struct{})
			applied := make(chan uint64, count)
			results := make(chan error, count)
			for version := uint64(1); version <= count; version++ {
				if sameVersion {
					versions <- 1
				} else {
					versions <- version
				}
			}
			refresher := NewRefresher(RefresherConfig[uint64]{
				Interval: time.Hour,
				Source: func(context.Context) (uint64, uint64, error) {
					version := <-versions
					ready <- struct{}{}
					<-start
					return version, version, nil
				},
				Apply: func(version uint64) error {
					applied <- version
					// Give competing refreshes time to try applying while this
					// callback is still running.
					time.Sleep(time.Millisecond)
					return nil
				},
			})
			t.Cleanup(refresher.ticker.Stop)
			for range count {
				go func() { results <- refresher.Refresh() }()
			}
			for range count {
				<-ready
			}
			close(start)
			for range count {
				assert.NoError(t, <-results)
			}
			close(applied)
			var latest uint64
			for version := range applied {
				assert.Greater(t, version, latest, "applied versions must strictly increase")
				latest = version
			}
			want := uint64(count)
			if sameVersion {
				want = 1
			}
			assert.Equal(t, want, latest)
			assert.Equal(t, want, refresher.latestAppliedSourceRevision)
			assert.True(t, refresher.hasAppliedSourceRevision)
		})
	}
}

func TestRefresherStopsOnContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	refresher := NewRefresher(RefresherConfig[string]{Context: ctx, Interval: time.Hour})
	done := make(chan struct{})
	go func() {
		refresher.run()
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("configuration refresh loop did not stop after cancellation")
	}
}

func TestRefresherRetriesRejectedVersionIncludingZero(t *testing.T) {
	for _, version := range []uint64{0, 1} {
		attempts := 0
		refresher := NewRefresher(RefresherConfig[string]{
			Interval: time.Hour,
			Source:   func(context.Context) (string, uint64, error) { return "config", version, nil },
			Apply: func(string) error {
				attempts++
				if attempts == 1 {
					return errors.New("rejected")
				}
				return nil
			},
		})
		require.EqualError(t, refresher.Refresh(), "failed to accept configuration: rejected")
		require.NoError(t, refresher.Refresh())
		require.NoError(t, refresher.Refresh())
		assert.Equal(t, 2, attempts, "version %d must be retried until accepted, then skipped", version)
	}
}
