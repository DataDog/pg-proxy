// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package pool

import (
	"maps"
	"slices"
	"sync"
	"time"
)

// HealthState tracks resolved instances and their latest health.
type HealthState struct {
	mu sync.RWMutex
	// resolved is the persistent tracked set. Newly discovered addresses are
	// added immediately and retained through transient discovery failures.
	resolved map[string]struct{}
	health   map[string]bool
	// lastKnownHealthy is updated only when at least one address is healthy.
	// Retaining the previous set when all addresses become unhealthy prevents
	// the last viable leader from being evicted during a simultaneous DNS and
	// health-check disruption.
	lastKnownHealthy map[string]struct{}
	// lookupAbsences counts consecutive refreshes where a tracked address was
	// absent from discovery.
	lookupAbsences map[string]int
	// healthSnapshotTimestamp orders the current health snapshot independently from completion order.
	healthSnapshotTimestamp time.Time
	// writableRetentionTimestamp independently orders updates to lastKnownHealthy.
	writableRetentionTimestamp time.Time
}

// NewHealthState creates an empty health snapshot.
func NewHealthState() *HealthState {
	return &HealthState{
		resolved:         make(map[string]struct{}),
		health:           make(map[string]bool),
		lastKnownHealthy: make(map[string]struct{}),
		lookupAbsences:   make(map[string]int),
	}
}

// Track adds newly resolved IPs and returns all retained addresses.
func (s *HealthState) Track(ips []string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ip := range ips {
		s.resolved[ip] = struct{}{}
	}
	return slices.Collect(maps.Keys(s.resolved))
}

// Update applies health and DNS results, evicting stale non-healthy addresses.
func (s *HealthState) Update(
	health map[string]bool,
	systemIDs map[string]string,
	resolved []string,
	absenceThreshold int,
	onEvict func(string),
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	resolvedSet := make(map[string]struct{}, len(resolved))
	for _, ip := range resolved {
		resolvedSet[ip] = struct{}{}
	}
	foreign := foreignAddresses(systemIDs, resolvedSet)
	for ip := range foreign {
		health[ip] = false
	}
	s.recordHealthLocked(health)
	s.updateResolvedLocked(resolvedSet, foreign, absenceThreshold, onEvict)
}

func (s *HealthState) recordHealthLocked(health map[string]bool) {
	healthyNow := healthyAddresses(health)
	if len(healthyNow) > 0 {
		s.lastKnownHealthy = healthyNow
	}
	s.health = maps.Clone(health)
}

func healthyAddresses(health map[string]bool) map[string]struct{} {
	healthy := make(map[string]struct{})
	for ip, ok := range health {
		if ok {
			healthy[ip] = struct{}{}
		}
	}
	return healthy
}

func (s *HealthState) updateResolvedLocked(
	resolvedSet map[string]struct{},
	foreign map[string]struct{},
	absenceThreshold int,
	onEvict func(string),
) {
	for ip := range s.resolved {
		if _, ok := resolvedSet[ip]; ok {
			s.lookupAbsences[ip] = 0
		} else {
			s.lookupAbsences[ip]++
		}
		_, lastKnownHealthy := s.lastKnownHealthy[ip]
		_, isForeign := foreign[ip]
		protected := lastKnownHealthy && !isForeign
		if s.lookupAbsences[ip] >= absenceThreshold && !protected {
			if onEvict != nil {
				onEvict(ip)
			}
			delete(s.resolved, ip)
			delete(s.lookupAbsences, ip)
			delete(s.health, ip)
		}
	}
}

// RefreshResolved tracks SQL discovery independently from Patroni and returns the addresses kept
// through the DNS absence threshold. Addresses in the last-known-healthy set remain protected until
// a later health snapshot identifies a replacement.
func (s *HealthState) RefreshResolved(
	resolved []string,
	absenceThreshold int,
	onEvict func(string),
) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	resolvedSet := make(map[string]struct{}, len(resolved))
	for _, ip := range resolved {
		s.resolved[ip] = struct{}{}
		resolvedSet[ip] = struct{}{}
	}
	s.updateResolvedLocked(resolvedSet, nil, absenceThreshold, onEvict)
	return slices.Collect(maps.Keys(s.resolved))
}

// RecordHealthAt records the latest health snapshot and independently orders healthy retention
// candidates. A newer failed credential check must not prevent an older successful check from
// advancing lastKnownHealthy, while a newer successful check still supersedes an older success.
func (s *HealthState) RecordHealthAt(round time.Time, health map[string]bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !round.Before(s.healthSnapshotTimestamp) {
		s.healthSnapshotTimestamp = round
		s.health = maps.Clone(health)
	}

	healthyNow := healthyAddresses(health)
	if len(healthyNow) == 0 || round.Before(s.writableRetentionTimestamp) {
		return
	}
	s.writableRetentionTimestamp = round
	s.lastKnownHealthy = healthyNow
	for ip := range healthyNow {
		s.resolved[ip] = struct{}{}
	}
}

// foreignAddresses returns the addresses that DNS no longer returns and that
// report a database_system_identifier no address in DNS reports. Kubernetes can
// reuse a departed pod's IP for a pod of another cluster, which still passes the
// Patroni role check. A terminating leader keeps its cluster's identifier, so it
// is not foreign. Nothing is foreign when DNS vouches for no identifier.
func foreignAddresses(systemIDs map[string]string, resolved map[string]struct{}) map[string]struct{} {
	vouched := make(map[string]struct{})
	for ip := range resolved {
		if id := systemIDs[ip]; id != "" {
			vouched[id] = struct{}{}
		}
	}
	foreign := make(map[string]struct{})
	if len(vouched) == 0 {
		return foreign
	}
	for ip, id := range systemIDs {
		if _, inDNS := resolved[ip]; inDNS || id == "" {
			continue
		}
		if _, ok := vouched[id]; !ok {
			foreign[ip] = struct{}{}
		}
	}
	return foreign
}

// IsHealthy returns the latest health for an address.
func (s *HealthState) IsHealthy(ip string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.health[ip]
}

// HealthStateSnapshot contains health and retained addresses from the same state.
// Both fields are independent copies that callers may modify.
type HealthStateSnapshot struct {
	Health   map[string]bool
	Resolved []string
}

// Snapshot copies health and retained addresses under one lock.
func (s *HealthState) Snapshot() HealthStateSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return HealthStateSnapshot{
		Health:   maps.Clone(s.health),
		Resolved: slices.Collect(maps.Keys(s.resolved)),
	}
}

// SetHealth replaces the health snapshot without changing DNS tracking.
func (s *HealthState) SetHealth(health map[string]bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.health = maps.Clone(health)
}

// ReplaceRoutingSnapshot replaces the client-visible address and health snapshots with one
// complete routing decision. The caller supplies addresses retained by the active discovery
// policy, so no additional DNS absence policy is applied here.
func (s *HealthState) ReplaceRoutingSnapshot(
	health map[string]bool,
	resolved []string,
) {
	s.mu.Lock()
	defer s.mu.Unlock()

	nextResolved := make(map[string]struct{}, len(resolved))
	for _, ip := range resolved {
		nextResolved[ip] = struct{}{}
	}
	s.resolved = nextResolved
	s.lookupAbsences = make(map[string]int, len(nextResolved))
	s.recordHealthLocked(health)
}
