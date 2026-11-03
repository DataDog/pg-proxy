package warden

import (
	"context"
	"fmt"
	"iter"
	"maps"
	"reflect"
	"slices"

	pgproxy "github.com/DataDog/pg-proxy"
	"github.com/DataDog/pg-proxy/pkg/topology"
)

// Cluster describes the databases composing a managed backend.
type Cluster struct {
	Primary  string
	Replicas []string
}

// Snapshot is the portable configuration consumed by the reconciliation
// engine. D remains generic so applications can keep credential and deployment
// fields at their adapter boundary.
type Snapshot[D any] struct {
	Databases        map[Identifier]D
	Clusters         map[Identifier]Cluster
	LogicalDatabases map[Identifier]topology.LogicalDatabase
}

// ManagedBackend is a runtime backend whose resources can be released when its
// owning cluster is removed or replaced.
type ManagedBackend interface {
	Backend
	Close()
}

// Retry runs a backend factory according to application-provided retry policy.
type Retry[T any] func(context.Context, func() (T, error)) (T, error)

// ReconcilerConfig configures backend lifecycle reconciliation.
type ReconcilerConfig[D any] struct {
	Context context.Context
	Runtime *Warden
	Factory func(context.Context, string, D, []D) (ManagedBackend, error)
	Retry   Retry[ManagedBackend]
}

// Reconciler owns portable backend creation, replacement, removal, retries,
// and logical-database remapping.
type Reconciler[D any] struct {
	ctx     context.Context
	runtime *Warden
	factory func(context.Context, string, D, []D) (ManagedBackend, error)
	retry   Retry[ManagedBackend]

	databases *ConcurrentMap[Identifier, D]
	clusters  *ConcurrentMap[Identifier, Cluster]
	backends  *ConcurrentMap[Identifier, *future[ManagedBackend]]
}

// NewReconciler creates an empty backend reconciler.
func NewReconciler[D any](cfg ReconcilerConfig[D]) *Reconciler[D] {
	ctx := cfg.Context
	if ctx == nil {
		ctx = context.Background()
	}
	runtime := cfg.Runtime
	if runtime == nil {
		runtime = New()
	}
	return &Reconciler[D]{
		ctx:       ctx,
		runtime:   runtime,
		factory:   cfg.Factory,
		retry:     cfg.Retry,
		backends:  NewConcurrentMap(make(map[Identifier]*future[ManagedBackend])),
		databases: NewConcurrentMap(make(map[Identifier]D)),
		clusters:  NewConcurrentMap(make(map[Identifier]Cluster)),
	}
}

// Runtime returns the reconciler's runtime inventory.
func (r *Reconciler[D]) Runtime() *Warden {
	return r.runtime
}

// Accept reconciles the current runtime with the supplied snapshot.
// Callers must serialize Accept calls, as with the original configuration loop.
func (r *Reconciler[D]) Accept(next Snapshot[D]) error {
	if err := r.reconcileClusters(next); err != nil {
		return err
	}
	r.runtime.ReconcileLogicalDatabases(next.LogicalDatabases)
	return nil
}

// reconcileClusters reconciles each cluster's backend runtime with the supplied configuration.
// Callers must serialize it with full configuration acceptance.
func (r *Reconciler[D]) reconcileClusters(next Snapshot[D]) error {
	logConfigs("databases", next.Databases)
	// Retain the named resource shape in configuration logs.
	type namedCluster struct {
		Primary  string   `json:"primary"`
		Replicas []string `json:"replicas"`
		Name     string
	}
	clusters := make(map[Identifier]namedCluster, len(next.Clusters))
	for name, cluster := range next.Clusters {
		clusters[name] = namedCluster{Primary: cluster.Primary, Replicas: cluster.Replicas, Name: string(name)}
	}
	logConfigs("clusters", clusters)

	toCreate, toRemove := r.Diff(next)
	// Publish configuration before creating backends, as the app inventory did.
	snapshot := cloneSnapshot(next)
	r.databases.Set(snapshot.Databases)
	r.clusters.Set(snapshot.Clusters)
	if len(toCreate) > 0 {
		pgproxy.Logger().Info(fmt.Sprintf("States to create: %v", toCreate))
	}
	if len(toRemove) > 0 {
		pgproxy.Logger().Info(fmt.Sprintf("States to remove: %v", toRemove))
	}

	for _, name := range toRemove {
		r.deregisterBackend(name)
	}
	for _, name := range toCreate {
		r.registerBackend(name, r.createBackendWithRetries(name))
	}

	return nil
}

// Diff returns the backend names that must be created and removed.
//
// A cluster must also be recreated when one of its databases changes, even if
// the cluster definition itself is unchanged. For example:
//
//	clusters:              clusters:
//	  cluster1:              cluster1:
//	    Primary: db1           Primary: db1
//
//	databases:        ->   databases:
//	  db1:                   db1:
//	    host: host1            host: host2
//
// cluster1 still points to db1, but its connection pool must be rebuilt for the
// new host.
func (r *Reconciler[D]) Diff(next Snapshot[D]) (toCreate, toRemove []Identifier) {
	currentSnapshot := Snapshot[D]{Databases: maps.Collect(r.databases.All()), Clusters: maps.Collect(r.clusters.All())}
	create := make(map[Identifier]struct{})
	remove := make(map[Identifier]struct{})

	for name, current := range currentSnapshot.Clusters {
		updated, ok := next.Clusters[name]
		switch {
		case !ok:
			remove[name] = struct{}{}
		case !reflect.DeepEqual(current, updated):
			create[name] = struct{}{}
		}
	}
	for name := range next.Clusters {
		if _, ok := currentSnapshot.Clusters[name]; !ok {
			create[name] = struct{}{}
		}
	}

	for databaseName, current := range currentSnapshot.Databases {
		updated, ok := next.Databases[databaseName]
		if !ok || reflect.DeepEqual(current, updated) {
			continue
		}
		for clusterName, cluster := range next.Clusters {
			if cluster.Primary == string(databaseName) || slices.Contains(cluster.Replicas, string(databaseName)) {
				create[clusterName] = struct{}{}
			}
		}
	}

	toCreate = mapKeys(create)
	toRemove = mapKeys(remove)
	return toCreate, toRemove
}

// Databases holds the collection's read lock throughout iteration.
// Returned values are shallow copies and must be treated as read-only.
func (r *Reconciler[D]) Databases() iter.Seq2[Identifier, D] {
	return r.databases.All()
}

// GetDatabase returns a configured database by name. Treat nested values as read-only.
func (r *Reconciler[D]) GetDatabase(name Identifier) (D, bool) {
	return r.databases.Get(name)
}

// Clusters holds the collection's read lock throughout iteration.
// Returned values share their replica slices and must be treated as read-only.
func (r *Reconciler[D]) Clusters() iter.Seq2[Identifier, Cluster] {
	return r.clusters.All()
}

// GetCluster returns a configured cluster by name. Treat its replica slice as read-only.
func (r *Reconciler[D]) GetCluster(name Identifier) (Cluster, bool) {
	return r.clusters.Get(name)
}

// GetManagedBackend returns a ready backend by cluster name.
func (r *Reconciler[D]) GetManagedBackend(name Identifier) (ManagedBackend, bool) {
	pending, ok := r.backends.Get(name)
	if !ok || !pending.IsReady() {
		return nil, false
	}
	backend, err := pending.Get()
	return backend, err == nil
}

func (r *Reconciler[D]) createBackendWithRetries(name Identifier) *future[ManagedBackend] {
	backend, err := r.createBackend(name)
	if err == nil {
		return newSettledFuture(backend)
	}
	pgproxy.Logger().Error(fmt.Sprintf("failed to create state for cluster %q (will retry in the background): %v", name, err))

	// TODO(OSQQUERY-447): tie each created Backend's lifetime to its owning future.
	//
	// The factory currently receives the long-lived Reconciler context. On the
	// background-retry path, this can leak a fully built Backend: if a config
	// refresh removes or replaces the cluster while the factory call is in flight,
	// reconciliation finds a not-yet-ready future, cancels the future's internal
	// retry context, and removes it from the inventory. Retry implementations may only
	// observe cancellation between attempts, while an in-flight factory call rooted
	// at r.ctx can still complete and settle a Backend that nothing will ever Close.
	//
	// In the Datadog adapter, an orphaned Backend leaks its pgxpool (including live
	// Postgres connections and the pool health-check goroutine), its ExportStats
	// goroutine, and the per-cluster FLASh controller goroutine. The pool and stats
	// leaks predate FLASh; the per-cluster load shedder adds another goroutine to
	// the same path.
	//
	// This requires a first-attempt failure followed by cluster removal during a
	// retry, so we accept the narrow race here instead of expanding this extraction
	// into Backend lifetime plumbing. Fix options are to pass an attempt-scoped
	// context to the factory so future cancellation reaches Backend construction,
	// and/or Close the Backend when creation completes after cancellation.
	return newRetriableFuture(context.Background(), r.retry, func() (ManagedBackend, error) {
		return r.createBackend(name)
	}, func(err error) {
		pgproxy.Logger().Error(fmt.Sprintf("Future computation failed after exhausting retries: %+v", err))
	})
}

func (r *Reconciler[D]) createBackend(name Identifier) (ManagedBackend, error) {
	cluster, ok := r.clusters.Get(name)
	if !ok {
		return nil, fmt.Errorf("Cluster %s not found", name)
	}
	primary, ok := r.databases.Get(Identifier(cluster.Primary))
	if !ok {
		return nil, fmt.Errorf("Primary database %s for cluster %s not found", cluster.Primary, name)
	}
	replicas := make([]D, 0, len(cluster.Replicas))
	for _, replicaName := range cluster.Replicas {
		replica, ok := r.databases.Get(Identifier(replicaName))
		if !ok {
			return nil, fmt.Errorf("Replica database %s for cluster %s not found", replicaName, name)
		}
		replicas = append(replicas, replica)
	}
	pgproxy.Logger().Info(fmt.Sprintf("creating new proxy state for cluster %q", name))
	return r.factory(r.ctx, string(name), primary, replicas)
}

func (r *Reconciler[D]) registerBackend(name Identifier, pending *future[ManagedBackend]) {
	previous, _ := r.backends.Get(name)
	// Preserve close-before-replace ordering. Both inventories reference the same
	// future, so a completed retry is immediately visible to session lookups.
	closePendingBackend(previous)
	r.backends.Put(name, pending)
	r.runtime.registerPendingBackend(name, pending)
}

func (r *Reconciler[D]) deregisterBackend(name Identifier) {
	pending, _ := r.backends.Get(name)
	closePendingBackend(pending)
	r.backends.Delete(name)
	r.runtime.DeleteBackend(name)
}

func closePendingBackend(pending *future[ManagedBackend]) {
	if pending == nil {
		return
	}
	if pending.IsReady() {
		if backend, err := pending.Get(); err == nil {
			backend.Close()
		}
	}
	pending.Cancel()
}

func cloneSnapshot[D any](snapshot Snapshot[D]) Snapshot[D] {
	cloned := Snapshot[D]{
		Databases:        make(map[Identifier]D, len(snapshot.Databases)),
		Clusters:         make(map[Identifier]Cluster, len(snapshot.Clusters)),
		LogicalDatabases: make(map[Identifier]topology.LogicalDatabase, len(snapshot.LogicalDatabases)),
	}
	for name, database := range snapshot.Databases {
		cloned.Databases[name] = database
	}
	for name, cluster := range snapshot.Clusters {
		cluster.Replicas = slices.Clone(cluster.Replicas)
		cloned.Clusters[name] = cluster
	}
	for name, database := range snapshot.LogicalDatabases {
		cloned.LogicalDatabases[name] = database
	}
	return cloned
}

func mapKeys(values map[Identifier]struct{}) []Identifier {
	keys := make([]Identifier, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	return keys
}
