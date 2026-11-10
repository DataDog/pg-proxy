// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package warden

import (
	"iter"
	"sync"
)

// TODO: Revisit whether these stores remain necessary once all compatibility
// inventory is owned directly by the portable Warden.

// ConcurrentMap follows a common lock-guarded map pattern, adding an iterator
// and an initializing constructor.

// ConcurrentMap is a generic, thread-safe implementation of a map[K]V.
// It exposes methods to interact with the wrapped value that ensure it is
// protected from concurrent reads and writes.
// This is somewhat analogous to a sync.Map but with type-safety and stronger guarantees,
// so you should usually default to this unless you have stringent requirements AND
// know very well what you are doing -- see the sync.Map docs for further details.
type ConcurrentMap[K comparable, V any] struct {
	innerMap map[K]V
	mapLock  sync.RWMutex
}

// NewConcurrentMap initializes a ConcurrentMap with the provided map.
// The lock is initially open/unlocked.
func NewConcurrentMap[K comparable, V any](values map[K]V) *ConcurrentMap[K, V] {
	return &ConcurrentMap[K, V]{innerMap: values}
}

// All returns an iterator over all key-value pairs in the map.
// Values are shallow copies: callers must treat nested maps, slices, and pointers as read-only.
// This iterator is guaranteed to expose a consistent view of the map at a given point in time,
// meaning writes will be blocked for the duration of the iteration.
// This is generally fine for the inventory use case since writes are relatively infrequent,
// but be aware of this and try to consume iterators quickly.
func (m *ConcurrentMap[K, V]) All() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		m.mapLock.RLock()
		defer m.mapLock.RUnlock()
		for key, value := range m.innerMap {
			if !yield(key, value) {
				return
			}
		}
	}
}

// Put takes a key-value pair and inserts them in the map.
// This operation has the same semantics as a regular map assignment,
// e.g. for how it handles overwrites on existing keys.
func (m *ConcurrentMap[K, V]) Put(key K, value V) {
	m.mapLock.Lock()
	defer m.mapLock.Unlock()
	m.innerMap[key] = value
}

// Get takes a key and returns the associated value in the map, as well as whether it was found.
// The value is a shallow copy: callers must treat nested maps, slices, and pointers as read-only.
func (m *ConcurrentMap[K, V]) Get(key K) (V, bool) {
	m.mapLock.RLock()
	defer m.mapLock.RUnlock()
	value, ok := m.innerMap[key]
	return value, ok
}

// Set takes a new map[K]V and replaces the existing one with it.
func (m *ConcurrentMap[K, V]) Set(values map[K]V) {
	m.mapLock.Lock()
	defer m.mapLock.Unlock()
	m.innerMap = values
}

// Delete takes a key and attempts to delete the associated entry from the map.
// This operation has the same semantics as the built-in delete function.
func (m *ConcurrentMap[K, V]) Delete(key K) {
	m.mapLock.Lock()
	defer m.mapLock.Unlock()
	delete(m.innerMap, key)
}

// Length returns the number of entries in the map.
func (m *ConcurrentMap[K, V]) Length() int {
	m.mapLock.RLock()
	defer m.mapLock.RUnlock()
	return len(m.innerMap)
}

// ConcurrentValue follows a common lock-guarded value pattern, adding a
// method to allow borrowing.

// ConcurrentValue is a generic, thread-safe implementation of a single value.
// It exposes methods to interact with the wrapped value that ensure it is
// protected from concurrent reads and writes.
// This is somewhat analogous to an atomic.AtomicValue but with type-safety stronger guarantees,
// so you should usually default to this unless you have stringent requirements AND
// know very well what you are doing -- see the atomic package docs for further details.
type ConcurrentValue[V any] struct {
	value V
	lock  sync.RWMutex
}

// NewConcurrentValue initializes a ConcurrentValue with the provided value.
// The lock is initially open/unlocked.
func NewConcurrentValue[V any](value V) *ConcurrentValue[V] {
	return &ConcurrentValue[V]{value: value}
}

// Borrow acquires the read-lock on the ConcurrentValue and returns a release
// function the caller can use when it is done. This allows the consumer to
// prevent writes for an extended period of time if necessary.
func (v *ConcurrentValue[V]) Borrow() func() {
	v.lock.RLock()
	return v.lock.RUnlock
}

// Get returns the wrapped value, whether it has been set or whether it is the zero value of V.
func (v *ConcurrentValue[V]) Get() V {
	v.lock.RLock()
	defer v.lock.RUnlock()
	return v.value
}

// Set takes a new value and replaces the existing one with it.
func (v *ConcurrentValue[V]) Set(value V) {
	v.lock.Lock()
	defer v.lock.Unlock()
	v.value = value
}
