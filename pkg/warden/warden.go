package warden

import (
	"fmt"
	"iter"
	"maps"

	pgproxy "github.com/DataDog/pg-proxy"
	"github.com/DataDog/pg-proxy/pkg/session"
	"github.com/DataDog/pg-proxy/pkg/topology"
)

// Backend is a managed cluster backend available to listener and session code.
type Backend interface {
	session.Backend
	SessionBackend() session.Backend
	ParameterStatuses() map[string]string
	CanPerformDynamicRouting(bool) bool
}

// Warden is the portable runtime inventory for a proxy server.
type Warden struct {
	logicalDatabases *ConcurrentMap[Identifier, topology.LogicalDatabase]
	backends         *ConcurrentMap[Identifier, *future[ManagedBackend]]
	sessions         *Sessions
}

// New creates an empty runtime Warden.
func New() *Warden {
	return &Warden{
		logicalDatabases: NewConcurrentMap(make(map[Identifier]topology.LogicalDatabase)),
		backends:         NewConcurrentMap(make(map[Identifier]*future[ManagedBackend])),
		sessions:         NewSessions(),
	}
}

// Sessions returns the active session inventory.
func (w *Warden) Sessions() *Sessions {
	return w.sessions
}

// RegisterLogicalDatabase adds or replaces a logical database.
func (w *Warden) RegisterLogicalDatabase(name Identifier, database topology.LogicalDatabase) {
	w.logicalDatabases.Put(name, database)
}

// DeleteLogicalDatabase removes a logical database.
func (w *Warden) DeleteLogicalDatabase(name Identifier) {
	w.logicalDatabases.Delete(name)
}

// GetLogicalDatabase returns a logical database by name. Treat nested values as read-only.
func (w *Warden) GetLogicalDatabase(name Identifier) (topology.LogicalDatabase, bool) {
	return w.logicalDatabases.Get(name)
}

// LogicalDatabases holds the collection's read lock throughout iteration.
// Returned values share nested maps and pointers; callers must treat them as read-only.
func (w *Warden) LogicalDatabases() iter.Seq2[Identifier, topology.LogicalDatabase] {
	return w.logicalDatabases.All()
}

// RegisterBackend adds or replaces an already-created cluster backend.
func (w *Warden) RegisterBackend(name Identifier, backend ManagedBackend) {
	w.registerPendingBackend(name, newSettledFuture(backend))
}

func (w *Warden) registerPendingBackend(name Identifier, pending *future[ManagedBackend]) {
	w.backends.Put(name, pending)
}

// DeleteBackend removes a cluster backend.
func (w *Warden) DeleteBackend(name Identifier) {
	w.backends.Delete(name)
}

// GetBackend returns a cluster backend by name.
func (w *Warden) GetBackend(name Identifier) (Backend, bool) {
	pending, ok := w.backends.Get(name)
	if !ok {
		return nil, false
	}
	// Completed futures stay in the inventory, so retries are visible immediately.
	backend, err := pending.Get()
	return backend, err == nil
}

// ReconcileLogicalDatabases replaces the logical database inventory and
// notifies active sessions when their backend routing changes.
func (w *Warden) ReconcileLogicalDatabases(updated map[Identifier]topology.LogicalDatabase) {
	current := maps.Collect(w.logicalDatabases.All())

	for name, next := range updated {
		previous, exists := current[name]
		if !exists {
			continue
		}
		if previous.Cluster != next.Cluster {
			previous.Cluster = next.Cluster
			w.RegisterLogicalDatabase(name, previous)
			for pid, active := range w.sessions.All() {
				if active.LogicalDatabaseID() != string(name) {
					continue
				}
				backend, ok := w.GetBackend(Identifier(next.Cluster))
				if !ok {
					pgproxy.Logger().Error(fmt.Sprintf("could not find an entry in the states map for the given cluster: %q", next.Cluster))
					continue
				}
				pgproxy.Logger().Info(fmt.Sprintf("remapping state for session #%s to cluster %q", pid, next.Cluster))
				active.RemapBackend(backend.SessionBackend())
			}
			pgproxy.Logger().Info(fmt.Sprintf("logical database %q has been remapped to point to cluster %q", name, next.Cluster))
		}
		if topology.ReadClustersChanged(previous.ReadClusters, next.ReadClusters) {
			readStates := w.resolveReadStates(next.ReadClusters)
			for _, active := range w.sessions.All() {
				if active.LogicalDatabaseID() == string(name) {
					active.RemapReadStates(readStates)
				}
			}
			pgproxy.Logger().Info(fmt.Sprintf("logical database %q read clusters have been updated", name))
		}
	}

	// TODO: Define how active sessions should react when their logical database
	// is removed. For extraction parity, removed entries disappear from inventory
	// without changing already-running sessions.
	// Keep the map writable even when nil input clears the inventory.
	logicalDatabases := make(map[Identifier]topology.LogicalDatabase, len(updated))
	maps.Copy(logicalDatabases, updated)
	w.logicalDatabases.Set(logicalDatabases)
	type namedLogicalDatabase struct {
		topology.LogicalDatabase
		Name string
	}
	namedLogicalDatabases := make(map[Identifier]namedLogicalDatabase, len(updated))
	for name, database := range w.LogicalDatabases() {
		namedLogicalDatabases[name] = namedLogicalDatabase{LogicalDatabase: database, Name: string(name)}
	}
	logConfigs("logical databases", namedLogicalDatabases)
}

func (w *Warden) resolveReadStates(reads topology.ReadClusters) []session.ReadStateConfig {
	readStates := make([]session.ReadStateConfig, 0, len(reads))
	for clusterName, settings := range reads {
		backend, ok := w.GetBackend(Identifier(clusterName))
		if !ok {
			pgproxy.Logger().Error(fmt.Sprintf("could not find an entry in the states map for the given cluster: %q", clusterName))
			continue
		}
		readStates = append(readStates, session.ReadStateConfig{
			Backend: backend.SessionBackend(),
			Weight:  topology.Weight(settings),
		})
	}
	return readStates
}
