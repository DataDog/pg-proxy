// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package health

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestSnapshotStatus(t *testing.T) {
	state := NewState([]string{"b", "a"})
	snapshot := state.Snapshot()
	require.Equal(t, []string{"a", "b"}, []string{snapshot.Clusters[0].Cluster, snapshot.Clusters[1].Cluster})
	requireStatus(t, state, false, StatusNotAccepting, false, StatusNotAccepting)

	state.SetAccepting(true)
	requireStatus(t, state, true, StatusOK, true, StatusOK)

	state.record(ClusterStatus{Cluster: "b", Stage: StageNotPrimary})
	requireStatus(t, state, true, StatusOK, false, StatusBackendUnreachable)

	state.record(ClusterStatus{Cluster: "b", OK: true})
	state.StartDrain()
	requireStatus(t, state, true, StatusOK, false, StatusDraining)
}

func requireStatus(t *testing.T, state *State, live bool, liveStatus string, ready bool, readyStatus string) {
	t.Helper()
	snapshot := state.Snapshot()
	gotLive, gotLiveStatus := snapshot.Live()
	gotReady, gotReadyStatus := snapshot.Ready()
	require.Equal(t, []any{live, liveStatus, ready, readyStatus}, []any{gotLive, gotLiveStatus, gotReady, gotReadyStatus})
}

func TestNilStateIgnoresUpdates(t *testing.T) {
	var state *State
	state.SetAccepting(true)
	state.StartDrain()
	require.Equal(t, Snapshot{}, state.Snapshot())
}

func TestCheckerRecordsUnreachableBackend(t *testing.T) {
	// A closed port refuses connections at once.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())
	settings, err := pgx.ParseConfig("postgres://postgres:secret-password@" + address + "/postgres?sslmode=disable")
	require.NoError(t, err)

	state := NewState([]string{"orders"})
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		Checker{State: state, Targets: map[string]*pgx.ConnConfig{"orders": settings}, Interval: 10 * time.Millisecond, Timeout: time.Second}.Run(ctx)
	}()
	require.Eventually(t, func() bool {
		cluster := state.Snapshot().Clusters[0]
		return !cluster.OK && cluster.Stage == StageConnect && !cluster.CheckedAt.IsZero()
	}, 5*time.Second, 10*time.Millisecond)
	cancel()
	<-done
	// The caller's settings must not change.
	require.NotEqual(t, ApplicationName, settings.RuntimeParams["application_name"])
}
