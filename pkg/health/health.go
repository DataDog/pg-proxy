// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package health

import (
	"context"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	pgproxy "github.com/DataDog/pg-proxy"
)

// Stage tells where a backend check failed. It is empty when the check passed.
type Stage string

const (
	StageConnect    Stage = "connect"
	StageQuery      Stage = "query"
	StageNotPrimary Stage = "not_primary"
)

// ApplicationName marks check connections in pg_stat_activity.
const ApplicationName = "pg-proxy-health"

// Status words shared by every surface.
const (
	StatusOK                 = "ok"
	StatusNotAccepting       = "not accepting"
	StatusDraining           = "draining"
	StatusBackendUnreachable = "backend unreachable"
)

// ClusterStatus is the last check result for one cluster primary.
type ClusterStatus struct {
	Cluster   string
	OK        bool
	Stage     Stage
	CheckedAt time.Time
	Latency   time.Duration
}

// Snapshot is a consistent copy of State. Clusters are sorted by name.
type Snapshot struct {
	Accepting bool
	Draining  bool
	Clusters  []ClusterStatus
}

// Live reports whether the client listener is accepting connections.
func (s Snapshot) Live() (bool, string) {
	if !s.Accepting {
		return false, StatusNotAccepting
	}
	return true, StatusOK
}

// Ready reports whether the proxy should receive new connections.
func (s Snapshot) Ready() (bool, string) {
	switch {
	case s.Draining:
		return false, StatusDraining
	case !s.Accepting:
		return false, StatusNotAccepting
	}
	for _, cluster := range s.Clusters {
		if !cluster.OK {
			return false, StatusBackendUnreachable
		}
	}
	return true, StatusOK
}

// State holds health signals. A nil *State ignores updates, so callers
// without a health surface need no checks.
type State struct {
	mu        sync.RWMutex
	accepting bool
	draining  bool
	clusters  map[string]ClusterStatus
}

// NewState starts every cluster as passing. Callers create it after startup
// has connected to each backend once.
func NewState(clusters []string) *State {
	s := &State{clusters: make(map[string]ClusterStatus, len(clusters))}
	for _, name := range clusters {
		s.clusters[name] = ClusterStatus{Cluster: name, OK: true}
	}
	return s
}

func (s *State) SetAccepting(accepting bool) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.accepting = accepting
}

// StartDrain marks the proxy not ready. It cannot be undone.
func (s *State) StartDrain() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.draining = true
}

func (s *State) Snapshot() Snapshot {
	if s == nil {
		return Snapshot{}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	snapshot := Snapshot{Accepting: s.accepting, Draining: s.draining, Clusters: make([]ClusterStatus, 0, len(s.clusters))}
	for _, name := range slices.Sorted(maps.Keys(s.clusters)) {
		snapshot.Clusters = append(snapshot.Clusters, s.clusters[name])
	}
	return snapshot
}

// record stores a result and returns the previous one.
func (s *State) record(status ClusterStatus) ClusterStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	previous := s.clusters[status.Cluster]
	s.clusters[status.Cluster] = status
	return previous
}

// Checker checks each cluster primary on an interval.
type Checker struct {
	State    *State
	Targets  map[string]*pgx.ConnConfig
	Interval time.Duration
	Timeout  time.Duration
}

// Run checks every target each interval until ctx ends. It logs only state
// changes, with the cluster name and stage.
func (c Checker) Run(ctx context.Context) {
	ticker := time.NewTicker(c.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		for _, name := range slices.Sorted(maps.Keys(c.Targets)) {
			status := Check(ctx, c.Targets[name], c.Timeout)
			if ctx.Err() != nil {
				return
			}
			status.Cluster = name
			previous := c.State.record(status)
			switch {
			case previous.OK && !status.OK:
				pgproxy.Logger().Warn("backend health check failed", "cluster", name, "stage", string(status.Stage))
			case !previous.OK && status.OK:
				pgproxy.Logger().Info("backend health check recovered", "cluster", name)
			}
		}
	}
}

// Check connects once with its own connection, confirms the server is a
// writable primary, and closes the connection.
func Check(ctx context.Context, settings *pgx.ConnConfig, timeout time.Duration) ClusterStatus {
	start := time.Now()
	result := func(stage Stage) ClusterStatus {
		return ClusterStatus{OK: stage == "", Stage: stage, CheckedAt: start, Latency: time.Since(start)}
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	settings = settings.Copy()
	if settings.RuntimeParams == nil {
		settings.RuntimeParams = map[string]string{}
	}
	settings.RuntimeParams["application_name"] = ApplicationName
	conn, err := pgx.ConnectConfig(ctx, settings)
	if err != nil {
		return result(StageConnect)
	}
	defer conn.Close(context.Background())
	var inRecovery bool
	if err := conn.QueryRow(ctx, "select pg_is_in_recovery()").Scan(&inRecovery); err != nil {
		return result(StageQuery)
	}
	if inRecovery {
		return result(StageNotPrimary)
	}
	return result("")
}
