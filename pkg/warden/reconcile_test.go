// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package warden

import (
	"context"
	"errors"
	"iter"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/DataDog/pg-proxy/pkg/topology"
)

type testManagedBackend struct {
	Backend
	id      string
	closed  atomic.Bool
	onClose func()
}

func (b *testManagedBackend) Close() {
	if b.onClose != nil {
		b.onClose()
	}
	b.closed.Store(true)
}

func TestReconcilerCreatesReplacesAndRemovesBackends(t *testing.T) {
	runtime := New()
	created := make(map[string][]*testManagedBackend)
	reconciler := NewReconciler(ReconcilerConfig[string]{
		Runtime: runtime,
		Factory: func(_ context.Context, name, primary string, replicas []string) (ManagedBackend, error) {
			backend := &testManagedBackend{}
			created[name] = append(created[name], backend)
			assert.NotEmpty(t, primary)
			return backend, nil
		},
	})

	initial := Snapshot[string]{
		Databases: map[Identifier]string{"primary": "host-a"},
		Clusters:  map[Identifier]Cluster{"cluster": {Primary: "primary"}},
		LogicalDatabases: map[Identifier]topology.LogicalDatabase{
			"logical": {Cluster: "cluster"},
		},
	}
	require.NoError(t, reconciler.Accept(initial))
	_, found := runtime.GetBackend("cluster")
	assert.True(t, found)

	updated := cloneSnapshot(initial)
	updated.Databases["primary"] = "host-b"
	require.NoError(t, reconciler.Accept(updated))
	assert.Len(t, created["cluster"], 2)
	assert.True(t, created["cluster"][0].closed.Load())

	removed := Snapshot[string]{
		Databases:        map[Identifier]string{},
		Clusters:         map[Identifier]Cluster{},
		LogicalDatabases: map[Identifier]topology.LogicalDatabase{},
	}
	require.NoError(t, reconciler.Accept(removed))
	_, found = runtime.GetBackend("cluster")
	assert.False(t, found)
	assert.True(t, created["cluster"][1].closed.Load())
}

func TestReconcilerDatabaseUpdateHandlerAvoidsBackendReplacement(t *testing.T) {
	created := 0
	var update DatabaseUpdate[string]
	reconciler := NewReconciler(ReconcilerConfig[string]{
		Factory: func(context.Context, string, string, []string) (ManagedBackend, error) {
			created++
			return &testManagedBackend{}, nil
		},
		DatabaseUpdate: func(candidate DatabaseUpdate[string]) (bool, error) {
			update = candidate
			return true, nil
		},
	})
	initial := Snapshot[string]{
		Databases: map[Identifier]string{"primary": "host-a"},
		Clusters:  map[Identifier]Cluster{"cluster": {Primary: "primary"}},
	}
	require.NoError(t, reconciler.Accept(initial))

	next := cloneSnapshot(initial)
	next.Databases["primary"] = "host-b"
	require.NoError(t, reconciler.Accept(next))

	assert.Equal(t, 1, created)
	assert.Equal(t, Identifier("primary"), update.Name)
	assert.Equal(t, "host-a", update.Current)
	assert.Equal(t, "host-b", update.Next)
	assert.Equal(t, []Identifier{"cluster"}, update.Clusters)
	assert.False(t, update.ReceivedAt.IsZero())
	actual, found := reconciler.GetDatabase("primary")
	require.True(t, found)
	assert.Equal(t, "host-b", actual)
}

func TestReconcilerDatabaseUpdateHandlerErrorStopsReconciliation(t *testing.T) {
	sentinel := errors.New("update failed")
	reconciler := NewReconciler(ReconcilerConfig[string]{
		Factory: func(context.Context, string, string, []string) (ManagedBackend, error) {
			return &testManagedBackend{}, nil
		},
		DatabaseUpdate: func(DatabaseUpdate[string]) (bool, error) {
			return false, sentinel
		},
	})
	initial := Snapshot[string]{
		Databases: map[Identifier]string{"primary": "host-a"},
		Clusters:  map[Identifier]Cluster{"cluster": {Primary: "primary"}},
	}
	require.NoError(t, reconciler.Accept(initial))

	next := cloneSnapshot(initial)
	next.Databases["primary"] = "host-b"
	err := reconciler.Accept(next)
	require.ErrorIs(t, err, sentinel)
	actual, found := reconciler.GetDatabase("primary")
	require.True(t, found)
	assert.Equal(t, "host-a", actual)
}

func TestReconcilerReportsBackendReadiness(t *testing.T) {
	events := make(chan BackendReady, 2)
	reconciler := NewReconciler(ReconcilerConfig[string]{
		Factory: func(context.Context, string, string, []string) (ManagedBackend, error) {
			return &testManagedBackend{}, nil
		},
		BackendReady: func(event BackendReady) { events <- event },
	})
	initial := Snapshot[string]{
		Databases: map[Identifier]string{"primary": "host-a"},
		Clusters:  map[Identifier]Cluster{"cluster": {Primary: "primary"}},
	}
	require.NoError(t, reconciler.Accept(initial))
	first := <-events
	assert.Equal(t, Identifier("cluster"), first.Name)
	assert.True(t, first.Initial)
	assert.False(t, first.ReadyAt.Before(first.ReceivedAt))

	next := cloneSnapshot(initial)
	next.Databases["primary"] = "host-b"
	require.NoError(t, reconciler.Accept(next))
	second := <-events
	assert.Equal(t, Identifier("cluster"), second.Name)
	assert.False(t, second.Initial)
	assert.False(t, second.ReadyAt.Before(second.ReceivedAt))
}

func TestReconcilerRegistersRetriedBackend(t *testing.T) {
	var attempts atomic.Int32
	reconciler := NewReconciler(ReconcilerConfig[string]{
		Factory: func(context.Context, string, string, []string) (ManagedBackend, error) {
			if attempts.Add(1) == 1 {
				return nil, errors.New("not ready")
			}
			return &testManagedBackend{}, nil
		},
		Retry: func(_ context.Context, computation func() (ManagedBackend, error)) (ManagedBackend, error) {
			return computation()
		},
	})

	require.NoError(t, reconciler.Accept(Snapshot[string]{
		Databases:        map[Identifier]string{"primary": "host"},
		Clusters:         map[Identifier]Cluster{"cluster": {Primary: "primary"}},
		LogicalDatabases: map[Identifier]topology.LogicalDatabase{},
	}))

	assert.Eventually(t, func() bool {
		_, found := reconciler.Runtime().GetBackend("cluster")
		return found
	}, time.Second, time.Millisecond)
	assert.Equal(t, int32(2), attempts.Load())
}

func TestReconcilerRetryUsesLatestDatabaseInventory(t *testing.T) {
	retryWaiting := make(chan struct{}, 1)
	retry := make(chan struct{})
	reconciler := NewReconciler(ReconcilerConfig[string]{
		Factory: func(context.Context, string, string, []string) (ManagedBackend, error) {
			return &testManagedBackend{}, nil
		},
		Retry: func(ctx context.Context, computation func() (ManagedBackend, error)) (ManagedBackend, error) {
			for {
				backend, err := computation()
				if err == nil {
					return backend, nil
				}
				retryWaiting <- struct{}{}
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-retry:
				}
			}
		},
	})
	initial := Snapshot[string]{
		Databases:        map[Identifier]string{},
		Clusters:         map[Identifier]Cluster{"cluster": {Primary: "primary"}},
		LogicalDatabases: map[Identifier]topology.LogicalDatabase{},
	}
	require.NoError(t, reconciler.Accept(initial))
	<-retryWaiting

	updated := cloneSnapshot(initial)
	updated.Databases["primary"] = "host"
	require.NoError(t, reconciler.Accept(updated))
	close(retry)

	assert.Eventually(t, func() bool {
		_, found := reconciler.Runtime().GetBackend("cluster")
		return found
	}, time.Second, time.Millisecond)
}

func TestReconcilerDoesNotRegisterStaleFuture(t *testing.T) {
	var oldAttempts atomic.Int32
	var newAttempts atomic.Int32
	oldAttemptStarted := make(chan struct{})
	releaseOldAttempt := make(chan struct{})
	newAttemptStarted := make(chan struct{})
	releaseNewAttempt := make(chan struct{})
	reconciler := NewReconciler(ReconcilerConfig[string]{
		Factory: func(_ context.Context, _ string, primary string, _ []string) (ManagedBackend, error) {
			if primary == "old" {
				if oldAttempts.Add(1) == 1 {
					return nil, errors.New("retry old")
				}
				close(oldAttemptStarted)
				<-releaseOldAttempt
				return &testManagedBackend{id: primary}, nil
			}
			if newAttempts.Add(1) == 1 {
				return nil, errors.New("retry new")
			}
			select {
			case <-newAttemptStarted:
			default:
				close(newAttemptStarted)
			}
			<-releaseNewAttempt
			return &testManagedBackend{id: primary}, nil
		},
		Retry: func(_ context.Context, computation func() (ManagedBackend, error)) (ManagedBackend, error) {
			return computation()
		},
	})
	initial := Snapshot[string]{
		Databases:        map[Identifier]string{"primary": "old"},
		Clusters:         map[Identifier]Cluster{"cluster": {Primary: "primary"}},
		LogicalDatabases: map[Identifier]topology.LogicalDatabase{},
	}
	require.NoError(t, reconciler.Accept(initial))
	<-oldAttemptStarted

	updated := cloneSnapshot(initial)
	updated.Databases["primary"] = "new"
	require.NoError(t, reconciler.Accept(updated))
	<-newAttemptStarted
	close(releaseOldAttempt)

	assert.Never(t, func() bool {
		backend, found := reconciler.Runtime().GetBackend("cluster")
		if !found {
			return false
		}
		return backend.(*testManagedBackend).id == "old"
	}, 100*time.Millisecond, time.Millisecond)
	_, found := reconciler.Runtime().GetBackend("cluster")
	assert.False(t, found, "stale backend was published while its replacement was pending")

	close(releaseNewAttempt)
	assert.Eventually(t, func() bool {
		backend, found := reconciler.Runtime().GetBackend("cluster")
		return found && backend.(*testManagedBackend).id == "new"
	}, time.Second, time.Millisecond)
}

func TestReconcilerDiffIncludesClustersUsingUpdatedDatabase(t *testing.T) {
	reconciler := NewReconciler(ReconcilerConfig[string]{
		Factory: func(context.Context, string, string, []string) (ManagedBackend, error) {
			return &testManagedBackend{}, nil
		},
	})
	initial := Snapshot[string]{
		Databases: map[Identifier]string{"shared": "host-a"},
		Clusters: map[Identifier]Cluster{
			"one": {Primary: "shared"},
			"two": {Primary: "shared"},
		},
		LogicalDatabases: map[Identifier]topology.LogicalDatabase{},
	}
	require.NoError(t, reconciler.Accept(initial))

	updated := cloneSnapshot(initial)
	updated.Databases["shared"] = "host-b"
	created, removed := reconciler.Diff(updated)
	assert.ElementsMatch(t, []Identifier{"one", "two"}, created)
	assert.Empty(t, removed)
}

// Readiness must not depend on a second goroutine copying a completed future
// into the runtime inventory. Session startup and remapping read that inventory.
func TestReconcilerReadyBackendIsImmediatelyVisible(t *testing.T) {
	r := NewReconciler(ReconcilerConfig[string]{})
	pending := &future[ManagedBackend]{done: make(chan struct{})}
	r.registerBackend("cluster", pending)
	_, found := r.Runtime().GetBackend("cluster")
	require.False(t, found)

	// Hold the reconciler backend lock to prevent any asynchronous publication.
	r.backends.mapLock.Lock()
	defer r.backends.mapLock.Unlock()
	backend := &testManagedBackend{}
	pending.settle(backend)
	actual, found := r.Runtime().GetBackend("cluster")
	require.True(t, found)
	assert.Same(t, backend, actual)
}

func TestReconcilerPublishesConfigurationBeforeCreatingBackends(t *testing.T) {
	var r *Reconciler[string]
	r = NewReconciler(ReconcilerConfig[string]{
		Factory: func(_ context.Context, name, primary string, _ []string) (ManagedBackend, error) {
			database, found := r.GetDatabase("primary")
			assert.True(t, found)
			assert.Equal(t, primary, database)
			cluster, found := r.GetCluster(Identifier(name))
			assert.True(t, found)
			assert.Equal(t, "primary", cluster.Primary)
			return &testManagedBackend{}, nil
		},
	})
	for _, host := range []string{"old", "new"} {
		require.NoError(t, r.Accept(Snapshot[string]{
			Databases: map[Identifier]string{"primary": host},
			Clusters:  map[Identifier]Cluster{"cluster": {Primary: "primary"}},
		}))
	}
}

func TestReconcilerClosesBackendBeforeReplacingOrRemovingIt(t *testing.T) {
	for _, replace := range []bool{true, false} {
		t.Run(map[bool]string{true: "replace", false: "remove"}[replace], func(t *testing.T) {
			r := NewReconciler(ReconcilerConfig[string]{})
			previous := &testManagedBackend{}
			previous.onClose = func() {
				actual, found := r.GetManagedBackend("cluster")
				assert.True(t, found)
				assert.Same(t, previous, actual)
				visible, found := r.Runtime().GetBackend("cluster")
				assert.True(t, found)
				assert.Same(t, previous, visible)
			}
			r.registerBackend("cluster", newSettledFuture[ManagedBackend](previous))
			if replace {
				next := &testManagedBackend{}
				r.registerBackend("cluster", newSettledFuture[ManagedBackend](next))
				actual, found := r.Runtime().GetBackend("cluster")
				assert.True(t, found)
				assert.Same(t, next, actual)
			} else {
				r.deregisterBackend("cluster")
				_, found := r.Runtime().GetBackend("cluster")
				assert.False(t, found)
			}
			assert.True(t, previous.closed.Load())
		})
	}
}

func TestInventoryIterationBlocksOnlyItsOwnCollection(t *testing.T) {
	t.Run("logical databases", func(t *testing.T) {
		runtime := New()
		runtime.RegisterLogicalDatabase("logical", topology.LogicalDatabase{Cluster: "cluster"})
		assertLockedIteration(t, runtime.LogicalDatabases(), func() {
			runtime.RegisterLogicalDatabase("next", topology.LogicalDatabase{Cluster: "cluster"})
		}, func() {
			runtime.RegisterBackend("cluster", &testManagedBackend{})
		})
	})
	t.Run("databases", func(t *testing.T) {
		r := NewReconciler(ReconcilerConfig[string]{})
		r.databases.Put("primary", "host")
		assertLockedIteration(t, r.Databases(), func() {
			r.databases.Put("next", "host")
		}, func() {
			r.clusters.Put("cluster", Cluster{Primary: "primary"})
		})
	})
	t.Run("clusters", func(t *testing.T) {
		r := NewReconciler(ReconcilerConfig[string]{})
		r.clusters.Put("cluster", Cluster{Primary: "primary"})
		assertLockedIteration(t, r.Clusters(), func() {
			r.clusters.Put("next", Cluster{Primary: "primary"})
		}, func() {
			r.databases.Put("primary", "host")
		})
	})
}

func assertLockedIteration[V any](t *testing.T, values iter.Seq2[Identifier, V], write, otherWrite func()) {
	t.Helper()
	started := make(chan struct{})
	finished := make(chan struct{})
	visited := false
	for range values {
		visited = true
		go func() {
			close(started)
			write()
			close(finished)
		}()
		<-started
		otherFinished := make(chan struct{})
		go func() { otherWrite(); close(otherFinished) }()
		select {
		case <-otherFinished:
		case <-time.After(time.Second):
			t.Error("iteration blocked a different collection")
		}
		select {
		case <-finished:
			t.Error("write completed before iteration released the read lock")
		case <-time.After(25 * time.Millisecond):
		}
		break // Early exit must release the read lock too.
	}
	require.True(t, visited)
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("write stayed blocked after iteration ended")
	}
}

// The first update succeeds and the second fails, regardless of map iteration order.
func TestReconcilerRetainsSuccessfulUpdatesAfterError(t *testing.T) {
	for _, restore := range []bool{true, false} {
		t.Run(map[bool]string{true: "restore original", false: "retry rejected"}[restore], func(t *testing.T) {
			sentinel := errors.New("second update failed")
			live := map[Identifier]string{"one": "old", "two": "old"}
			calls := 0
			var succeeded Identifier
			r := NewReconciler(ReconcilerConfig[string]{
				Factory: func(context.Context, string, string, []string) (ManagedBackend, error) {
					return &testManagedBackend{}, nil
				},
				DatabaseUpdate: func(update DatabaseUpdate[string]) (bool, error) {
					calls++
					assert.Equal(t, live[update.Name], update.Current)
					if calls == 2 {
						return false, sentinel
					}
					live[update.Name] = update.Next
					succeeded = update.Name
					return true, nil
				},
			})
			initial := Snapshot[string]{
				Databases: map[Identifier]string{"one": "old", "two": "old"},
				Clusters:  map[Identifier]Cluster{"one": {Primary: "one"}, "two": {Primary: "two"}},
			}
			require.NoError(t, r.Accept(initial))
			next := cloneSnapshot(initial)
			next.Databases["one"], next.Databases["two"] = "new", "new"
			require.ErrorIs(t, r.Accept(next), sentinel)
			actual, found := r.GetDatabase(succeeded)
			require.True(t, found)
			assert.Equal(t, "new", actual)
			target := next
			if restore {
				target = initial
			}
			require.NoError(t, r.Accept(target))
			assert.Equal(t, 3, calls, "only the database that differs from live state should be updated")
			assert.Equal(t, target.Databases, live)
		})
	}
}

func TestReconcilerReadinessFinishesBeforeRetirement(t *testing.T) {
	for _, replace := range []bool{true, false} {
		t.Run(map[bool]string{true: "replace", false: "remove"}[replace], func(t *testing.T) {
			entered := make(chan struct{})
			release := make(chan struct{})
			finished := make(chan struct{})
			previous := &testManagedBackend{}
			var r *Reconciler[string]
			r = NewReconciler(ReconcilerConfig[string]{
				BackendReady: func(event BackendReady) {
					// Inventory reads remain safe inside the callback.
					actual, found := r.GetManagedBackend(event.Name)
					assert.True(t, found)
					assert.Same(t, previous, actual)
					close(entered)
					<-release
					assert.False(t, previous.closed.Load(), "backend retired during readiness delivery")
					close(finished)
				},
			})
			pending := &future[ManagedBackend]{done: make(chan struct{})}
			r.registerBackend("cluster", pending)
			r.observeBackendReady("cluster", pending, true, time.Now())
			pending.settle(previous)
			<-entered

			started := make(chan struct{})
			retired := make(chan struct{})
			go func() {
				close(started)
				if replace {
					r.registerBackend("cluster", newSettledFuture[ManagedBackend](&testManagedBackend{}))
				} else {
					r.deregisterBackend("cluster")
				}
				close(retired)
			}()
			<-started
			select {
			case <-retired:
				t.Error("retirement completed before readiness callback returned")
			case <-time.After(25 * time.Millisecond):
			}
			close(release)
			<-finished
			<-retired
			assert.True(t, previous.closed.Load())
		})
	}
}

func TestReconcilerSkipsReadinessAfterRetirement(t *testing.T) {
	for _, replace := range []bool{true, false} {
		t.Run(map[bool]string{true: "replace", false: "remove"}[replace], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var events []BackendReady
				r := NewReconciler(ReconcilerConfig[string]{
					BackendReady: func(event BackendReady) { events = append(events, event) },
				})
				pending := &future[ManagedBackend]{done: make(chan struct{})}
				r.registerBackend("cluster", pending)
				r.observeBackendReady("cluster", pending, true, time.Now())
				synctest.Wait()
				if replace {
					replacement := newSettledFuture[ManagedBackend](&testManagedBackend{})
					r.registerBackend("cluster", replacement)
					r.observeBackendReady("cluster", replacement, false, time.Now())
				} else {
					r.deregisterBackend("cluster")
				}
				pending.settle(&testManagedBackend{})
				synctest.Wait()
				if replace {
					require.Len(t, events, 1)
					assert.False(t, events[0].Initial)
				} else {
					assert.Empty(t, events)
				}
			})
		})
	}
}

func TestReconcilerDatabaseUpdateCoversCurrentReferences(t *testing.T) {
	for _, change := range []string{"remove", "change primary", "remove replica"} {
		for _, fail := range []bool{false, true} {
			t.Run(change+"/"+map[bool]string{false: "accepted", true: "rejected then restored"}[fail], func(t *testing.T) {
				sentinel := errors.New("unrelated database update failed")
				initial := Snapshot[string]{
					Databases: map[Identifier]string{"shared": "old", "other": "other"},
					Clusters: map[Identifier]Cluster{
						"a":     {Primary: "shared"},
						"b":     {Primary: "shared"},
						"other": {Primary: "other"},
					},
				}
				if change == "remove replica" {
					initial.Clusters["a"] = Cluster{Primary: "other", Replicas: []string{"shared"}}
				}
				backends := make(map[string]*testManagedBackend)
				sharedUpdates := 0
				r := NewReconciler(ReconcilerConfig[string]{
					Factory: func(_ context.Context, name, primary string, replicas []string) (ManagedBackend, error) {
						value := primary
						if name == "a" && len(replicas) > 0 {
							value = replicas[0]
						}
						backend := &testManagedBackend{id: value}
						backends[name] = backend
						return backend, nil
					},
					DatabaseUpdate: func(update DatabaseUpdate[string]) (bool, error) {
						if update.Name == "other" {
							return false, sentinel
						}
						sharedUpdates++
						for _, name := range update.Clusters {
							backends[string(name)].id = update.Next
						}
						return true, nil
					},
				})
				require.NoError(t, r.Accept(initial))
				oldA, oldB := backends["a"], backends["b"]
				next := cloneSnapshot(initial)
				next.Databases["shared"] = "new"
				switch change {
				case "remove":
					delete(next.Clusters, "a")
				case "change primary", "remove replica":
					next.Clusters["a"] = Cluster{Primary: "other"}
				}
				if fail {
					next.Databases["other"] = "changed"
					require.ErrorIs(t, r.Accept(next), sentinel)
					// Rejection must not record an update that left a live user behind.
					assert.Equal(t, 0, sharedUpdates)
					current, _ := r.GetDatabase("shared")
					assert.Equal(t, "old", current)
					assert.Equal(t, "old", oldA.id)
					assert.Equal(t, "old", oldB.id)
					assert.False(t, oldA.closed.Load())

					restored := cloneSnapshot(initial)
					restored.Databases["shared"] = "new"
					require.NoError(t, r.Accept(restored))
					assert.Equal(t, 1, sharedUpdates)
					assert.Equal(t, "new", oldA.id)
					assert.Equal(t, "new", oldB.id)
				} else {
					require.NoError(t, r.Accept(next))
					assert.Equal(t, 0, sharedUpdates, "in-place update must cover all current references")
					assert.True(t, oldA.closed.Load())
					assert.True(t, oldB.closed.Load())
					assert.Equal(t, "new", backends["b"].id)
				}
			})
		}
	}
}

func TestReconcilerDatabaseUpdateRestartsUnavailableBackend(t *testing.T) {
	for _, exhausted := range []bool{true, false} {
		t.Run(map[bool]string{true: "exhausted retries", false: "pending retry"}[exhausted], func(t *testing.T) {
			var failedAttempts atomic.Int32
			retryStarted := make(chan struct{})
			retryFinished := make(chan struct{})
			handlerCalls := 0
			r := NewReconciler(ReconcilerConfig[string]{
				Factory: func(_ context.Context, name, primary string, _ []string) (ManagedBackend, error) {
					if name == "unavailable" && primary == "old" {
						failedAttempts.Add(1)
						return nil, errors.New("database unavailable")
					}
					return &testManagedBackend{id: primary}, nil
				},
				Retry: func(ctx context.Context, computation func() (ManagedBackend, error)) (ManagedBackend, error) {
					defer close(retryFinished)
					close(retryStarted)
					if exhausted {
						return computation()
					}
					<-ctx.Done()
					return nil, ctx.Err()
				},
				DatabaseUpdate: func(DatabaseUpdate[string]) (bool, error) {
					handlerCalls++
					return true, nil // Even an optimistic handler cannot suppress recovery.
				},
			})
			initial := Snapshot[string]{
				Databases: map[Identifier]string{"shared": "old"},
				Clusters: map[Identifier]Cluster{
					"ready":       {Primary: "shared"},
					"unavailable": {Primary: "shared"},
				},
			}
			require.NoError(t, r.Accept(initial))
			<-retryStarted
			pending, found := r.backends.Get("unavailable")
			require.True(t, found)
			if exhausted {
				<-pending.Done()
				assert.Equal(t, int32(2), failedAttempts.Load())
			}
			_, ready := r.GetManagedBackend("unavailable")
			require.False(t, ready)
			oldReady, ready := r.GetManagedBackend("ready")
			require.True(t, ready)

			next := cloneSnapshot(initial)
			next.Databases["shared"] = "new"
			require.NoError(t, r.Accept(next))
			assert.Zero(t, handlerCalls)
			for _, name := range []Identifier{"ready", "unavailable"} {
				backend, ready := r.GetManagedBackend(name)
				require.True(t, ready, "%s must recover through normal recreation", name)
				assert.Equal(t, "new", backend.(*testManagedBackend).id)
			}
			assert.True(t, oldReady.(*testManagedBackend).closed.Load())
			<-retryFinished
		})
	}
}
