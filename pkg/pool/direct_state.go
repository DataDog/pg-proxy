// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package pool

import (
	"errors"
	"fmt"
	"slices"
	"sync"
)

// ErrNoHealthyInstances reports that a direct pool has no usable server.
var ErrNoHealthyInstances = errors.New("no healthy pools available")

// DirectState owns instance inventory and round-robin healthy selection.
type DirectState struct {
	mu         sync.RWMutex
	instances  map[string]PostgresInstance
	healthy    []string
	roundRobin *RoundRobinCounter
}

// NewDirectState creates an empty direct-pool state.
func NewDirectState() *DirectState {
	return &DirectState{
		instances:  make(map[string]PostgresInstance),
		roundRobin: NewRoundRobinCounterWithStart(0),
	}
}

func (s *DirectState) Store(address string, instance PostgresInstance) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.instances[address] = instance
}

func (s *DirectState) Load(address string) (PostgresInstance, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	instance, ok := s.instances[address]
	return instance, ok
}

func (s *DirectState) Delete(address string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.instances, address)
}

// Range visits a snapshot of all instances.
func (s *DirectState) Range(yield func(string, PostgresInstance) bool) {
	s.mu.RLock()
	snapshot := make(map[string]PostgresInstance, len(s.instances))
	for address, instance := range s.instances {
		snapshot[address] = instance
	}
	s.mu.RUnlock()
	for address, instance := range snapshot {
		if !yield(address, instance) {
			return
		}
	}
}

// CheckAllHealth runs instance health checks in parallel.
func (s *DirectState) CheckAllHealth() {
	var wg sync.WaitGroup
	s.Range(func(_ string, instance PostgresInstance) bool {
		wg.Add(1)
		go func() {
			defer wg.Done()
			instance.HealthCheck()
		}()
		return true
	})
	wg.Wait()
}

// RefreshHealthy rebuilds and returns the sorted healthy-address snapshot.
func (s *DirectState) RefreshHealthy() []string {
	healthy := make([]string, 0)
	s.Range(func(address string, instance PostgresInstance) bool {
		if instance.IsHealthy() {
			healthy = append(healthy, address)
		}
		return true
	})
	slices.Sort(healthy)
	s.mu.Lock()
	s.healthy = healthy
	s.mu.Unlock()
	return slices.Clone(healthy)
}

// HealthyAddresses returns the current healthy snapshot.
func (s *DirectState) HealthyAddresses() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return slices.Clone(s.healthy)
}

// SetHealthyAddresses replaces the healthy snapshot.
func (s *DirectState) SetHealthyAddresses(addresses []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.healthy = slices.Clone(addresses)
}

// NextHealthyAddress returns the next healthy server.
func (s *DirectState) NextHealthyAddress(databaseHost string) (string, int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.healthy) == 0 {
		return "", InvalidIndex, fmt.Errorf("%w for db %s", ErrNoHealthyInstances, databaseHost)
	}
	index := s.roundRobin.Next(len(s.healthy))
	if index == InvalidIndex {
		return "", InvalidIndex, fmt.Errorf("round robin counter returned -1 for db %s, no healthy pools available", databaseHost)
	}
	return s.healthy[index], index, nil
}

// CloseAll closes every instance in parallel.
func (s *DirectState) CloseAll() {
	var wg sync.WaitGroup
	s.Range(func(_ string, instance PostgresInstance) bool {
		wg.Add(1)
		go func() {
			defer wg.Done()
			instance.Close()
		}()
		return true
	})
	wg.Wait()
}

// ResetAll resets every instance in parallel.
func (s *DirectState) ResetAll() {
	var wg sync.WaitGroup
	s.Range(func(_ string, instance PostgresInstance) bool {
		wg.Add(1)
		go func() {
			defer wg.Done()
			instance.Reset()
		}()
		return true
	})
	wg.Wait()
}
