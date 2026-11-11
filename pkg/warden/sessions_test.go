// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package warden

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/DataDog/pg-proxy/pkg/link"
	"github.com/DataDog/pg-proxy/pkg/session"
	"github.com/DataDog/pg-proxy/pkg/topology"
)

type testBackend struct{ id string }

func (testBackend) Acquire(context.Context, session.LinkRequest) (link.Link, error) {
	return nil, nil
}
func (testBackend) Close()                            {}
func (testBackend) LoadShedder() session.LoadShedder  { return nil }
func (b testBackend) SessionBackend() session.Backend { return b }
func (b testBackend) ParameterStatuses() map[string]string {
	return map[string]string{"key": b.id}
}
func (testBackend) CanPerformDynamicRouting(bool) bool { return true }

type testSession struct {
	backend    session.Backend
	readStates []session.ReadStateConfig
}

func (s *testSession) Identifier() string                   { return "42" }
func (s *testSession) LogicalDatabaseID() string            { return "app" }
func (s *testSession) Run() error                           { return nil }
func (s *testSession) RemapBackend(backend session.Backend) { s.backend = backend }
func (s *testSession) RemapReadStates(reads []session.ReadStateConfig) {
	s.readStates = reads
}
func (s *testSession) Cancel() error     { return nil }
func (s *testSession) SecretKey() []byte { return nil }

func TestSessionsLifecycle(t *testing.T) {
	sessions := NewSessions()
	sessions.Register("42", nil)

	handle, ok := sessions.Get("42")
	require.True(t, ok)
	assert.Nil(t, handle)

	closed := make(chan struct{})
	go func() {
		sessions.Wait()
		close(closed)
	}()
	select {
	case <-closed:
		t.Fatal("Wait returned before the session was deleted")
	case <-time.After(10 * time.Millisecond):
	}

	sessions.Delete("42")
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Wait did not return after the session was deleted")
	}
}

func TestWardenRuntimeInventory(t *testing.T) {
	runtime := New()
	logical := topology.LogicalDatabase{Cluster: "primary"}
	runtime.RegisterLogicalDatabase("app", logical)
	runtime.RegisterBackend("primary", testBackend{id: "primary"})

	actualLogical, ok := runtime.GetLogicalDatabase("app")
	require.True(t, ok)
	assert.Equal(t, logical, actualLogical)
	backend, ok := runtime.GetBackend("primary")
	require.True(t, ok)
	assert.Equal(t, map[string]string{"key": "primary"}, backend.ParameterStatuses())

	runtime.DeleteLogicalDatabase("app")
	runtime.DeleteBackend("primary")
	_, ok = runtime.GetLogicalDatabase("app")
	assert.False(t, ok)
	_, ok = runtime.GetBackend("primary")
	assert.False(t, ok)
}

func TestWardenRegisterLogicalDatabaseAfterClearingInventory(t *testing.T) {
	for _, acceptSnapshot := range []bool{false, true} {
		name := "direct reconciliation"
		if acceptSnapshot {
			name = "empty snapshot"
		}
		t.Run(name, func(t *testing.T) {
			runtime := New()
			runtime.RegisterLogicalDatabase("old", topology.LogicalDatabase{Cluster: "primary"})
			if acceptSnapshot {
				reconciler := NewReconciler(ReconcilerConfig[string]{Runtime: runtime})
				require.NoError(t, reconciler.Accept(Snapshot[string]{}))
			} else {
				runtime.ReconcileLogicalDatabases(nil)
			}
			_, found := runtime.GetLogicalDatabase("old")
			require.False(t, found)

			database := topology.LogicalDatabase{Cluster: "primary"}
			runtime.RegisterLogicalDatabase("new", database)
			actual, found := runtime.GetLogicalDatabase("new")
			require.True(t, found)
			assert.Equal(t, database, actual)
		})
	}
}

func TestWardenReconcileLogicalDatabaseRouting(t *testing.T) {
	runtime := New()
	runtime.RegisterBackend("primary", testBackend{id: "primary"})
	runtime.RegisterBackend("target", testBackend{id: "target"})
	runtime.ReconcileLogicalDatabases(map[Identifier]topology.LogicalDatabase{
		"app": {Cluster: "primary"},
	})
	active := &testSession{}
	runtime.Sessions().Register(active.Identifier(), active)

	runtime.ReconcileLogicalDatabases(map[Identifier]topology.LogicalDatabase{
		"app": {
			Cluster: "target",
			ReadClusters: topology.ReadClusters{
				"primary": {Weight: 70},
				"target":  {Weight: 30},
			},
		},
	})

	assert.Equal(t, testBackend{id: "target"}, active.backend)
	require.Len(t, active.readStates, 2)
	weights := map[string]uint8{}
	for _, readState := range active.readStates {
		weights[readState.Backend.(testBackend).id] = readState.Weight
	}
	assert.Equal(t, map[string]uint8{"primary": 70, "target": 30}, weights)
	runtime.Sessions().Delete(active.Identifier())
}

func TestWardenDoesNotRemapUnchangedReadClusters(t *testing.T) {
	runtime := New()
	runtime.RegisterBackend("primary", testBackend{id: "primary"})
	current := map[Identifier]topology.LogicalDatabase{
		"app": {
			Cluster: "primary",
			ReadClusters: topology.ReadClusters{
				"primary": {Weight: 100},
			},
		},
	}
	runtime.ReconcileLogicalDatabases(current)
	active := &testSession{}
	runtime.Sessions().Register(active.Identifier(), active)

	runtime.ReconcileLogicalDatabases(map[Identifier]topology.LogicalDatabase{
		"app": {
			Cluster: "primary",
			ReadClusters: topology.ReadClusters{
				"primary": {Weight: 100},
			},
		},
	})

	assert.Nil(t, active.readStates)
	runtime.Sessions().Delete(active.Identifier())
}
