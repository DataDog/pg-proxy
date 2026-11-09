// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package pool

import (
	"context"
	"errors"
	"maps"
	"net"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	SQLRoutingResultShadow          = "shadow"
	SQLRoutingResultConclusive      = "conclusive_sql"
	SQLRoutingResultCachedFallback  = "cached_fallback"
	SQLRoutingResultPatroniRecovery = "patroni_recovery"
	SQLRoutingResultRetainedSQL     = "retained_sql"

	SQLHealthTimeoutNone      = "none"
	SQLHealthTimeoutContext   = "context_timeout"
	SQLHealthTimeoutNetwork   = "network_timeout"
	SQLHealthTimeoutStatement = "statement_timeout"
	SQLHealthTimeoutMixed     = "mixed"

	SQLHealthFallbackTimeout           = "timeout"
	SQLHealthFallbackIndeterminate     = "indeterminate_error"
	SQLHealthFallbackMultiplePrimaries = "multiple_primaries"
	SQLHealthFallbackConflictingRoles  = "conflicting_roles"
)

// LeaderDetectionComparison compares independent SQL and Patroni observations of a new leader.
// A positive Speedup means SQL observed the leader first.
type LeaderDetectionComparison struct {
	NewLeaderIP       string
	Speedup           time.Duration
	SQLRoutingEnabled bool
}

// LeaderRoutingComparison compares when hybrid and Patroni-only routing publish a new leader.
// A positive Improvement means hybrid routing published first.
type LeaderRoutingComparison struct {
	NewLeaderIP  string
	Improvement  time.Duration
	HybridSource string
}

type hybridRoutingDecision struct {
	routingResult string
	health        map[string]bool
	publish       bool

	fallbackReason string
	timeoutType    string
	cachedAge      time.Duration
	hasCachedAge   bool

	leaderDetection   *LeaderDetectionComparison
	routingComparison *LeaderRoutingComparison
}

type leaderObservation struct {
	primaryIPs map[string]struct{}
}

type leaderDetectionPair struct {
	sqlAt     time.Time
	patroniAt time.Time
}

type leaderRoutingPair struct {
	patroniAt    time.Time
	hybridAt     time.Time
	hybridSource string
}

// hybridHealthPolicy owns the state needed to choose between a complete SQL snapshot and the
// latest Patroni snapshot. It deliberately does not schedule checks or mutate PoolHealth; the
// health worker supplies observations and applies the returned decisions.
type hybridHealthPolicy struct {
	mu             sync.Mutex
	routingEnabled bool

	patroniHealth    map[string]bool
	patroniUpdatedAt time.Time

	// A conclusive SQL snapshot with no primary allows Patroni to restore routing
	// immediately. Once restored, follow Patroni until a fresh SQL snapshot arrives.
	sqlNoPrimaryAt        time.Time
	patroniRecoveryActive bool
	patroniRecoveryAt     time.Time

	// Preserve SQL's selected leader through uncertainty until Patroni agrees
	// or a newer Patroni observation identifies a different, non-superseded leader.
	// Keep the superseded leader excluded even if the selected leader becomes
	// unusable; losing B does not establish that the old Patroni leader A is valid.
	awaitingPatroniLeader   string
	supersededPatroniLeader string
	sqlLeaderSelectedAt     time.Time
	// Intermediate SQL leaders superseded before Patroni caught up (A -> B -> C).
	// Bound this history to current candidates; delayed Patroni B must not replace C.
	supersededSQLLeaders map[string]struct{}

	patroniLeaderObservation leaderObservation
	sqlLeaderObservation     leaderObservation
	pendingLeaderDetections  map[string]leaderDetectionPair

	patroniRoutingLeaderIP string
	hybridRoutingLeaderIP  string
	pendingRouting         map[string]leaderRoutingPair
}

func newHybridHealthPolicy(routingEnabled bool) *hybridHealthPolicy {
	return &hybridHealthPolicy{
		routingEnabled: routingEnabled,
		patroniHealth:  make(map[string]bool),
	}
}

// observePatroni refreshes the fallback snapshot. Patroni remains authoritative in shadow mode;
// when SQL routing is enabled it can also restore a route after SQL found no primary
// or advance past a SQL leader whose Patroni confirmation was missed.
func (p *hybridHealthPolicy) observePatroni(health map[string]bool, observedAt time.Time) hybridRoutingDecision {
	p.mu.Lock()
	defer p.mu.Unlock()

	if observedAt.Before(p.patroniUpdatedAt) {
		return hybridRoutingDecision{routingResult: SQLRoutingResultShadow}
	}
	previousLeader, previousUnique := singleHealthyIP(p.patroniHealth)
	p.patroniHealth = maps.Clone(health)
	p.patroniUpdatedAt = observedAt
	decision := hybridRoutingDecision{
		routingResult: SQLRoutingResultShadow,
		health:        maps.Clone(health),
		publish:       !p.routingEnabled,
	}
	decision.leaderDetection = p.observeLeaderLocked("patroni", healthyIPs(health), observedAt)
	if p.routingEnabled {
		decision.routingComparison = p.recordRoutingLeaderLocked("patroni", health, observedAt, "")
		leader, unique := singleHealthyIP(health)
		if unique && leader == p.awaitingPatroniLeader && observedAt.After(p.sqlLeaderSelectedAt) {
			p.awaitingPatroniLeader = ""
			p.supersededPatroniLeader = ""
			p.supersededSQLLeaders = nil
		}
		// A -> B -> C may happen without this proxy observing Patroni confirm B.
		// Protecting B from stale A must not block a newer unique Patroni leader C.
		_, supersededSQLLeader := p.supersededSQLLeaders[leader]
		patroniAdvancedPastSQLLeader := p.awaitingPatroniLeader != "" && unique && !supersededSQLLeader &&
			leader != p.awaitingPatroniLeader && leader != p.supersededPatroniLeader &&
			observedAt.After(p.sqlLeaderSelectedAt)
		p.excludeSupersededPatroniLeaderLocked(decision.health)
		// Excluding a superseded leader must not make an ambiguous snapshot
		// eligible to recover routing and clear that exclusion.
		eligibleLeader := unique && decision.health[leader]
		if (!p.sqlNoPrimaryAt.IsZero() && observedAt.After(p.sqlNoPrimaryAt) && eligibleLeader) ||
			patroniAdvancedPastSQLLeader || p.patroniRecoveryActive {
			// Do not wait for another SQL completion to publish a recovered Patroni
			// leader. Continue following Patroni, including loss of that leader,
			// until SQL supplies evidence collected after this routing transition.
			if !p.patroniRecoveryActive || unique != previousUnique || leader != previousLeader {
				p.patroniRecoveryAt = observedAt
			}
			p.patroniRecoveryActive = true
			if eligibleLeader {
				p.awaitingPatroniLeader = ""
				p.supersededPatroniLeader = ""
				p.supersededSQLLeaders = nil
			}
			decision.publish = true
			decision.routingResult = SQLRoutingResultPatroniRecovery
			if comparison := p.recordRoutingLeaderLocked("hybrid", decision.health, observedAt, decision.routingResult); comparison != nil {
				decision.routingComparison = comparison
			}
		}
	}
	return decision
}

// observeSQL evaluates one complete SQL snapshot. SQL routing is conclusive only when every IP has
// a successful role or a narrowly classified unroutable result. Unknown errors, multiple distinct
// primaries, or conflicting deterministic roles require fallback. While Patroni has not caught
// up to a SQL-selected leader, uncertainty can retain that leader but cannot restore the
// superseded Patroni leader.
// candidateIPs includes every address retained for this snapshot, not only addresses returned by
// its latest DNS lookup.
func (p *hybridHealthPolicy) observeSQL(
	candidateIPs []string,
	outcome SQLHealthCheckOutcome,
	observedAt time.Time,
) hybridRoutingDecision {
	p.mu.Lock()
	defer p.mu.Unlock()

	decision := hybridRoutingDecision{
		routingResult: SQLRoutingResultShadow,
	}
	fallbackReason, timeoutType := sqlHealthFallback(outcome)
	if fallbackReason == "" {
		decision.leaderDetection = p.observeLeaderLocked("sql", outcome.PrimaryIPs, observedAt)
	}
	if !p.routingEnabled {
		return decision
	}

	// Completion time cannot prove freshness: a slow check may have observed the
	// old topology before Patroni restored routing. Unknown start times also cannot
	// supersede that recovery. Keep the barrier after accepting a fresh snapshot so
	// another late completion cannot subsequently roll routing back.
	if !p.patroniRecoveryAt.IsZero() && !outcome.StartedAt.After(p.patroniRecoveryAt) {
		decision.routingResult = SQLRoutingResultPatroniRecovery
		return decision
	}
	p.patroniRecoveryActive = false
	for ip := range p.supersededSQLLeaders {
		if !slices.Contains(candidateIPs, ip) {
			delete(p.supersededSQLLeaders, ip)
		}
	}
	p.sqlNoPrimaryAt = time.Time{}
	if fallbackReason == "" && len(outcome.PrimaryIPs) == 0 {
		p.sqlNoPrimaryAt = observedAt
	}
	decision.publish = true
	decision.fallbackReason = fallbackReason
	decision.timeoutType = timeoutType
	if decision.fallbackReason != "" {
		if p.canRetainSQLLeaderLocked(candidateIPs, outcome, fallbackReason) {
			decision.routingResult = SQLRoutingResultRetainedSQL
			decision.health = make(map[string]bool, len(candidateIPs))
			for _, ip := range candidateIPs {
				decision.health[ip] = ip == p.awaitingPatroniLeader
			}
			decision.routingComparison = p.recordRoutingLeaderLocked("hybrid", decision.health, observedAt, decision.routingResult)
			return decision
		}
		p.awaitingPatroniLeader = ""
		decision.routingResult = SQLRoutingResultCachedFallback
		p.supersededSQLLeaders = nil
		decision.health = maps.Clone(p.patroniHealth)
		p.excludeSupersededPatroniLeaderLocked(decision.health)
		if p.supersededPatroniLeader != "" && len(healthyIPs(decision.health)) == 0 {
			// The old leader remains excluded, but a different Patroni leader can
			// restore routing immediately through the case-2 recovery path.
			p.sqlNoPrimaryAt = observedAt
		}
		if !p.patroniUpdatedAt.IsZero() {
			decision.cachedAge = observedAt.Sub(p.patroniUpdatedAt)
			if decision.cachedAge < 0 {
				decision.cachedAge = 0
			}
			decision.hasCachedAge = true
		}
		decision.routingComparison = p.recordRoutingLeaderLocked(
			"hybrid",
			decision.health,
			observedAt,
			SQLRoutingResultCachedFallback,
		)
		return decision
	}

	previousSQLLeader := p.awaitingPatroniLeader
	p.awaitingPatroniLeader = ""
	if len(outcome.PrimaryIPs) == 1 {
		leader := outcome.PrimaryIPs[0]
		patroniLeader, unique := singleHealthyIP(p.patroniHealth)
		if unique && patroniLeader == leader {
			p.supersededPatroniLeader = ""
			p.supersededSQLLeaders = nil
		} else {
			if previousSQLLeader != "" && previousSQLLeader != leader && slices.Contains(candidateIPs, previousSQLLeader) {
				if p.supersededSQLLeaders == nil {
					p.supersededSQLLeaders = make(map[string]struct{})
				}
				p.supersededSQLLeaders[previousSQLLeader] = struct{}{}
			}
			p.awaitingPatroniLeader = leader
			p.sqlLeaderSelectedAt = observedAt
			if unique && p.supersededPatroniLeader == "" {
				p.supersededPatroniLeader = patroniLeader
			}
		}
	} else {
		p.supersededSQLLeaders = nil
	}
	decision.routingResult = SQLRoutingResultConclusive
	decision.health = make(map[string]bool, len(candidateIPs))
	for _, ip := range candidateIPs {
		decision.health[ip] = false
	}
	if len(outcome.PrimaryIPs) == 1 {
		decision.health[outcome.PrimaryIPs[0]] = true
	}
	decision.routingComparison = p.recordRoutingLeaderLocked(
		"hybrid",
		decision.health,
		observedAt,
		SQLRoutingResultConclusive,
	)
	return decision
}

// Retain a previously conclusive SQL leader only through uncertainty. Explicit
// demotion, an unroutable result, removal from the candidate set, or conflicting
// role evidence must not be hidden by the retained route. A successful primary
// result still takes precedence over an allowlisted error for that same IP.
func (p *hybridHealthPolicy) canRetainSQLLeaderLocked(candidateIPs []string, outcome SQLHealthCheckOutcome, fallbackReason string) bool {
	leader := p.awaitingPatroniLeader
	if leader == "" || !slices.Contains(candidateIPs, leader) ||
		fallbackReason == SQLHealthFallbackConflictingRoles || fallbackReason == SQLHealthFallbackMultiplePrimaries {
		return false
	}
	primary, unroutable := false, false
	for _, result := range outcome.Results {
		if result.IP != leader {
			continue
		}
		if result.Err == nil {
			if result.InRecovery {
				return false
			}
			primary = true
		} else if isConclusiveSQLUnroutableError(result.Err) {
			unroutable = true
		}
	}
	return primary || !unroutable
}

func (p *hybridHealthPolicy) excludeSupersededPatroniLeaderLocked(health map[string]bool) {
	if _, present := health[p.supersededPatroniLeader]; present {
		health[p.supersededPatroniLeader] = false
	}
}

func sqlHealthFallback(outcome SQLHealthCheckOutcome) (string, string) {
	type ipEvidence struct {
		deterministicRole    bool
		primary              bool
		conclusiveUnroutable bool
		indeterminate        bool
		timeoutType          string
	}

	evidenceByIP := make(map[string]*ipEvidence)
	conflictingRoles := false
	for _, result := range outcome.Results {
		evidence := evidenceByIP[result.IP]
		if evidence == nil {
			evidence = &ipEvidence{timeoutType: SQLHealthTimeoutNone}
			evidenceByIP[result.IP] = evidence
		}
		if result.Err != nil {
			if isConclusiveSQLUnroutableError(result.Err) {
				evidence.conclusiveUnroutable = true
				continue
			}
			evidence.indeterminate = true
			resultTimeoutType := sqlHealthTimeoutType(result.Err)
			if resultTimeoutType == SQLHealthTimeoutNone {
				continue
			}
			if evidence.timeoutType == SQLHealthTimeoutNone {
				evidence.timeoutType = resultTimeoutType
			} else if evidence.timeoutType != resultTimeoutType {
				evidence.timeoutType = SQLHealthTimeoutMixed
			}
			continue
		}

		primary := !result.InRecovery
		if evidence.deterministicRole && evidence.primary != primary {
			conflictingRoles = true
		} else {
			evidence.deterministicRole = true
			evidence.primary = primary
		}
	}
	if conflictingRoles {
		return SQLHealthFallbackConflictingRoles, SQLHealthTimeoutNone
	}
	if len(outcome.PrimaryIPs) > 1 {
		return SQLHealthFallbackMultiplePrimaries, SQLHealthTimeoutNone
	}

	timeoutType := SQLHealthTimeoutNone
	hasIndeterminateError := false
	for _, evidence := range evidenceByIP {
		// Reconcile credentials per IP before aggregating across IPs. A successful role
		// takes precedence over an allowlisted error on the same IP: primary + 57P01
		// remains primary, and standby + 57P01 remains standby, in either result order.
		// A successful role or an allowlisted error resolves uncertainty from another
		// credential on that IP. Unresolved evidence on another IP still requires
		// Patroni fallback; conflicting successful roles are handled above.
		if evidence.deterministicRole || evidence.conclusiveUnroutable {
			continue
		}
		if !evidence.indeterminate {
			continue
		}
		if evidence.timeoutType == SQLHealthTimeoutNone {
			hasIndeterminateError = true
			continue
		}
		if timeoutType == SQLHealthTimeoutNone {
			timeoutType = evidence.timeoutType
		} else if timeoutType != evidence.timeoutType {
			timeoutType = SQLHealthTimeoutMixed
		}
	}
	if timeoutType != SQLHealthTimeoutNone {
		return SQLHealthFallbackTimeout, timeoutType
	}
	if hasIndeterminateError {
		return SQLHealthFallbackIndeterminate, SQLHealthTimeoutNone
	}
	return "", ""
}

// isConclusiveSQLUnroutableError is intentionally an allowlist. New or ambiguous errors fall back
// to Patroni unless SQL has already determined that same IP's role through another credential.
func isConclusiveSQLUnroutableError(err error) bool {
	if errors.Is(err, syscall.ECONNREFUSED) {
		return true
	}

	var postgresErr *pgconn.PgError
	if !errors.As(err, &postgresErr) {
		return false
	}
	switch postgresErr.Code {
	case pgerrcode.AdminShutdown,
		pgerrcode.CrashShutdown,
		pgerrcode.CannotConnectNow,
		pgerrcode.DiskFull,
		pgerrcode.OutOfMemory:
		return true
	default:
		// System (58) and internal (XX) classes describe a failure category,
		// not whether the whole instance is unavailable. Leave them indeterminate.
		return false
	}
}

func sqlHealthTimeoutType(err error) string {
	if err == nil {
		return SQLHealthTimeoutNone
	}
	var postgresErr *pgconn.PgError
	// The probe's dedicated connection runs only the health query with server- and client-side
	// timeouts. Treat any query cancellation conservatively as indeterminate: matching the message
	// would risk missing a real timeout because PostgreSQL messages may vary by cancellation path.
	if errors.As(err, &postgresErr) && postgresErr.Code == "57014" {
		return SQLHealthTimeoutStatement
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return SQLHealthTimeoutContext
	}
	var networkErr net.Error
	if errors.As(err, &networkErr) && networkErr.Timeout() {
		return SQLHealthTimeoutNetwork
	}
	return SQLHealthTimeoutNone
}

func healthyIPs(health map[string]bool) []string {
	ips := make([]string, 0, len(health))
	for ip, healthy := range health {
		if healthy {
			ips = append(ips, ip)
		}
	}
	slices.Sort(ips)
	return ips
}

func singleHealthyIP(health map[string]bool) (string, bool) {
	leader := ""
	for ip, healthy := range health {
		if !healthy {
			continue
		}
		if leader != "" {
			return "", false
		}
		leader = ip
	}
	return leader, leader != ""
}

func (p *hybridHealthPolicy) observeLeaderLocked(
	source string,
	primaryIPs []string,
	observedAt time.Time,
) *LeaderDetectionComparison {
	if len(primaryIPs) == 0 {
		return nil
	}
	current := make(map[string]struct{}, len(primaryIPs))
	for _, ip := range primaryIPs {
		current[ip] = struct{}{}
	}
	var observation *leaderObservation
	switch source {
	case "patroni":
		observation = &p.patroniLeaderObservation
	case "sql":
		observation = &p.sqlLeaderObservation
	default:
		return nil
	}
	if observation.primaryIPs == nil {
		observation.primaryIPs = current
		return nil
	}
	newLeaders := make([]string, 0, len(current))
	for ip := range current {
		if _, existed := observation.primaryIPs[ip]; !existed {
			newLeaders = append(newLeaders, ip)
		}
	}
	observation.primaryIPs = current
	if len(newLeaders) != 1 {
		return nil
	}

	leaderIP := newLeaders[0]
	if p.pendingLeaderDetections == nil {
		p.pendingLeaderDetections = make(map[string]leaderDetectionPair)
	}
	pair := p.pendingLeaderDetections[leaderIP]
	if source == "sql" {
		pair.sqlAt = observedAt
	} else {
		pair.patroniAt = observedAt
	}
	if pair.sqlAt.IsZero() || pair.patroniAt.IsZero() {
		p.pendingLeaderDetections[leaderIP] = pair
		return nil
	}
	delete(p.pendingLeaderDetections, leaderIP)
	return &LeaderDetectionComparison{
		NewLeaderIP:       leaderIP,
		Speedup:           pair.patroniAt.Sub(pair.sqlAt),
		SQLRoutingEnabled: p.routingEnabled,
	}
}

func (p *hybridHealthPolicy) recordRoutingLeaderLocked(
	source string,
	health map[string]bool,
	observedAt time.Time,
	hybridSource string,
) *LeaderRoutingComparison {
	leaderIP, unique := singleHealthyIP(health)
	if !unique {
		return nil
	}
	var previous *string
	switch source {
	case "patroni":
		previous = &p.patroniRoutingLeaderIP
	case "hybrid":
		previous = &p.hybridRoutingLeaderIP
	default:
		return nil
	}
	if *previous == "" {
		*previous = leaderIP
		return nil
	}
	if *previous == leaderIP {
		return nil
	}
	*previous = leaderIP
	if p.pendingRouting == nil {
		p.pendingRouting = make(map[string]leaderRoutingPair)
	}
	pair := p.pendingRouting[leaderIP]
	if source == "patroni" {
		pair.patroniAt = observedAt
	} else {
		pair.hybridAt = observedAt
		pair.hybridSource = hybridSource
	}
	if pair.patroniAt.IsZero() || pair.hybridAt.IsZero() {
		p.pendingRouting[leaderIP] = pair
		return nil
	}
	// Complete only this leader. Hybrid can advance again before Patroni catches up, leaving a
	// newer leader comparison pending in the same map.
	delete(p.pendingRouting, leaderIP)
	return &LeaderRoutingComparison{
		NewLeaderIP:  leaderIP,
		Improvement:  pair.patroniAt.Sub(pair.hybridAt),
		HybridSource: pair.hybridSource,
	}
}
