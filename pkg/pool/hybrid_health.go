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
	"time"

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
	SQLHealthFallbackStaleEvidence     = "stale_sql_evidence"
)

// LeaderRoutingComparison compares when hybrid and Patroni-only routing select a new leader.
// A positive Improvement means hybrid routing selected the leader first.
// When HasDowntimeComparison is true, both durations start when hybrid routing first
// published no healthy leader. DowntimeReductionPercent compares the actual hybrid
// selection with Patroni, including when hybrid uses Patroni to recover.
type LeaderRoutingComparison struct {
	NewLeaderIP              string
	Improvement              time.Duration
	HybridSource             string
	HybridDowntime           time.Duration
	PatroniDowntime          time.Duration
	DowntimeReductionPercent float64
	HasDowntimeComparison    bool
}

type hybridRoutingDecision struct {
	// Capture the mode with the decision; hooks run after the policy lock is released.
	routingEnabled bool
	routingResult  string
	health         map[string]bool
	publish        bool

	fallbackReason string
	timeoutType    string
	cachedAge      time.Duration
	hasCachedAge   bool

	routingComparison *LeaderRoutingComparison

	// Per-evaluation events, discarded after reporting; this is not retained history.
	routingBanChanges []SQLRoutingBanChange
	routingBannedIPs  map[string]time.Time
}

type routingTimingData struct {
	source            string
	downtimeStartedAt time.Time
}

// hybridHealthPolicy owns the state needed to choose between a complete SQL snapshot and the
// latest Patroni snapshot. It deliberately does not schedule checks or mutate PoolHealth; the
// health worker supplies observations and applies the returned decisions.
// The decision flow and examples are documented at:
// https://github.com/DataDog/pg-proxy/blob/main/pkg/pool/README.md#sql-health-decision-flow
type hybridHealthPolicy struct {
	mu             sync.Mutex
	routingEnabled bool

	patroniHealth    map[string]bool
	patroniUpdatedAt time.Time
	// Keep the last unique leader across empty/ambiguous snapshots. Only a change
	// to a different unique leader advances the SQL evidence freshness boundary.
	lastPatroniLeader      string
	patroniLeaderChangedAt time.Time

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
	// Older leaders superseded before the sources agreed, including across no-primary gaps.
	// Bound this history to current candidates; delayed Patroni B must not replace C.
	supersededSQLLeaders map[string]struct{}

	routingTiming            leaderTiming[routingTimingData]
	routingDowntimeStartedAt time.Time
	routingBannedIPs         sqlRoutingBanlist
	// The source of the last worker publication, including empty/ambiguous routes.
	publishedRoutingResult string
}

func newHybridHealthPolicy(routingEnabled bool) *hybridHealthPolicy {
	return &hybridHealthPolicy{
		routingEnabled:   routingEnabled,
		patroniHealth:    make(map[string]bool),
		routingBannedIPs: make(sqlRoutingBanlist),
	}
}

func (p *hybridHealthPolicy) patroniSnapshot() map[string]bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return maps.Clone(p.patroniHealth)
}

func (p *hybridHealthPolicy) isRoutingEnabled() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.routingEnabled
}

func (p *hybridHealthPolicy) setRoutingEnabled(enabled bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.routingEnabled != enabled {
		// Comparisons cannot span periods where SQL routing evaluation was disabled. Reset both
		// baselines so the next observations establish a fresh pair under the new flag state.
		p.routingTiming = leaderTiming[routingTimingData]{}
		p.routingDowntimeStartedAt = time.Time{}
		p.publishedRoutingResult = ""
		// Leadership history belongs to one uninterrupted routing mode. Keep the
		// latest Patroni snapshot for fallback, but do not carry retention,
		// exclusions, or recovery decisions across a flag transition.
		p.awaitingPatroniLeader = ""
		p.supersededPatroniLeader = ""
		p.sqlLeaderSelectedAt = time.Time{}
		p.supersededSQLLeaders = nil
		p.sqlNoPrimaryAt = time.Time{}
		p.patroniRecoveryActive = false
		p.patroniRecoveryAt = time.Time{}
		p.lastPatroniLeader, _ = singleHealthyIP(p.patroniHealth)
		p.patroniLeaderChangedAt = time.Time{}
	}
	p.routingEnabled = enabled
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
	if leader, unique := singleHealthyIP(health); unique {
		if p.lastPatroniLeader != "" && leader != p.lastPatroniLeader {
			p.patroniLeaderChangedAt = observedAt
		}
		p.lastPatroniLeader = leader
	}
	p.patroniHealth = maps.Clone(health)
	p.patroniUpdatedAt = observedAt
	decision := hybridRoutingDecision{
		routingEnabled: p.routingEnabled,
		routingResult:  SQLRoutingResultShadow,
		health:         maps.Clone(health),
		publish:        !p.routingEnabled,
	}
	if p.routingEnabled {
		decision.routingComparison = p.recordRoutingLeaderLocked("patroni", health, observedAt, "")
		p.reconcilePatroniRoutingLocked(&decision, previousLeader, previousUnique, observedAt)
	}
	return decision
}

// observePublishedPatroniRouting records hybrid publication when Patroni owns routing (including
// cached fallback). Record the publication itself, including when empty SQL discovery restores
// Patroni ownership, rather than waiting for the next refresh.
func (p *hybridHealthPolicy) observePublishedPatroniRouting(health map[string]bool, observedAt time.Time) *LeaderRoutingComparison {
	return p.observePublishedRouting(health, observedAt, SQLRoutingResultCachedFallback)
}

// observePublishedRouting records an actual worker publication, including a ban
// exclusion that preserves the existing routing source and recovery state.
func (p *hybridHealthPolicy) observePublishedRouting(health map[string]bool, observedAt time.Time, routingResult string) *LeaderRoutingComparison {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.routingEnabled {
		return nil
	}
	p.publishedRoutingResult = routingResult
	return p.recordRoutingLeaderLocked("hybrid", health, observedAt, routingResult)
}

// reconcilePatroniRoutingLocked updates the routing decision and recovery state.
// The caller must hold p.mu and supply a decision with the unfiltered Patroni health.
func (p *hybridHealthPolicy) reconcilePatroniRoutingLocked(
	decision *hybridRoutingDecision,
	previousLeader string,
	previousUnique bool,
	observedAt time.Time,
) {
	leader, unique := singleHealthyIP(decision.health)
	_, bannedLeader := p.routingBannedIPs[leader]
	if unique && !bannedLeader && leader == p.awaitingPatroniLeader && observedAt.After(p.sqlLeaderSelectedAt) {
		p.awaitingPatroniLeader = ""
		p.supersededPatroniLeader = ""
		p.supersededSQLLeaders = nil
	}
	// A -> B -> C may happen without this proxy observing Patroni confirm B.
	// Protecting B from stale A must not block a newer unique Patroni leader C.
	_, supersededSQLLeader := p.supersededSQLLeaders[leader]
	patroniAdvancedPastSQLLeader := p.awaitingPatroniLeader != "" && unique && !bannedLeader && !supersededSQLLeader &&
		leader != p.awaitingPatroniLeader && leader != p.supersededPatroniLeader &&
		observedAt.After(p.sqlLeaderSelectedAt)
	p.excludeSupersededLeadersLocked(decision.health)
	p.applyRoutingBanlistLocked(decision.health)
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
			// Recovery establishes a new route, not agreement on old SQL leaders.
			// Keep their exclusions until SQL establishes the topology again.
			p.rememberSupersededSQLLeaderLocked(p.awaitingPatroniLeader)
			p.awaitingPatroniLeader = ""
		}
		decision.publish = true
		decision.routingResult = SQLRoutingResultPatroniRecovery
	}
}

// clearRoutingBansForEmptyDiscovery removes bans for the empty retained SQL set
// without evaluating a SQL routing decision or changing leadership history.
func (p *hybridHealthPolicy) clearRoutingBansForEmptyDiscovery() hybridRoutingDecision {
	p.mu.Lock()
	defer p.mu.Unlock()
	return hybridRoutingDecision{
		routingEnabled: p.routingEnabled,
		routingBanChanges: p.routingBannedIPs.update(
			nil, SQLHealthCheckOutcome{}, SQLHealthResultsSnapshot{},
		),
	}
}

// observeSQLShutdowns records shutdown evidence even when discovery has added an IP
// this poll did not check. Partial evidence cannot select a route or clear a ban
// with a successful role result.
func (p *hybridHealthPolicy) observeSQLShutdowns(candidateIPs []string, fresh SQLHealthResultsSnapshot) hybridRoutingDecision {
	p.mu.Lock()
	defer p.mu.Unlock()
	decision := hybridRoutingDecision{routingEnabled: p.routingEnabled, routingResult: p.publishedRoutingResult}
	if decision.routingResult == "" {
		decision.routingResult = SQLRoutingResultCachedFallback
	}
	var shutdowns []SQLHealthCheckResult
	for _, result := range fresh.Results {
		if isPostgresShutdownError(result.Err) {
			shutdowns = append(shutdowns, result)
		}
	}
	if len(shutdowns) != 0 {
		fresh.Results = shutdowns
		decision.routingBanChanges = p.routingBannedIPs.update(candidateIPs, SQLHealthCheckOutcome{Results: shutdowns}, fresh)
	}
	decision.routingBannedIPs = maps.Clone(p.routingBannedIPs)
	return decision
}

// observeSQLRound separates probe completion (raw detection and bans), evaluation
// (routing history and cache age), and worker publication (hybrid timing). Only a
// successful poll scheduled after a ban may clear it.
func (p *hybridHealthPolicy) observeSQLRound(
	candidateIPs []string,
	outcome SQLHealthCheckOutcome,
	fresh SQLHealthResultsSnapshot,
	evaluatedAt time.Time,
) hybridRoutingDecision {
	return p.observeSQLRoundWithRoutingEvidence(candidateIPs, outcome, nil, fresh, evaluatedAt)
}

// Ban evidence includes recovery results and cached shutdowns from every
// credential. Separate routing evidence contains only normal routing polls.
func (p *hybridHealthPolicy) observeSQLRoundWithRoutingEvidence(
	candidateIPs []string, outcome SQLHealthCheckOutcome, routingEvidence *SQLHealthCheckOutcome,
	fresh SQLHealthResultsSnapshot, evaluatedAt time.Time,
) hybridRoutingDecision {
	p.mu.Lock()
	defer p.mu.Unlock()

	decision := hybridRoutingDecision{
		routingEnabled: p.routingEnabled,
		routingResult:  SQLRoutingResultShadow,
	}
	decision.routingBanChanges = p.routingBannedIPs.update(candidateIPs, outcome, fresh)
	decision.routingBannedIPs = maps.Clone(p.routingBannedIPs)
	routingOutcome := p.routingEligibleOutcomeLocked(outcome)
	if routingEvidence != nil {
		routingOutcome = p.routingEligibleOutcomeLocked(*routingEvidence)
	}
	fallbackReason, timeoutType := sqlHealthFallback(routingOutcome)
	if !p.routingEnabled {
		return decision
	}

	// Completion time cannot prove freshness: a slow check may have observed the
	// old topology before Patroni restored routing. Unknown start times also cannot
	// supersede that recovery. Keep the barrier after accepting a fresh snapshot so
	// another late completion cannot subsequently roll routing back.
	if !p.patroniRecoveryAt.IsZero() && !routingOutcome.StartedAt.After(p.patroniRecoveryAt) {
		decision.routingResult = SQLRoutingResultPatroniRecovery
		return decision
	}
	if fallbackReason == "" && p.sqlPredatesPatroniLeaderLocked(routingOutcome) {
		// An old cached success must not establish new SQL retention or mark the
		// newer Patroni leader superseded. Publish eligible Patroni fallback instead.
		fallbackReason, timeoutType = SQLHealthFallbackStaleEvidence, SQLHealthTimeoutNone
	}
	p.patroniRecoveryActive = false
	for ip := range p.supersededSQLLeaders {
		if !slices.Contains(candidateIPs, ip) {
			delete(p.supersededSQLLeaders, ip)
		}
	}
	p.sqlNoPrimaryAt = time.Time{}
	if fallbackReason == "" && len(routingOutcome.PrimaryIPs) == 0 {
		p.sqlNoPrimaryAt = evaluatedAt
	}
	decision.publish = true
	decision.fallbackReason = fallbackReason
	decision.timeoutType = timeoutType
	if decision.fallbackReason != "" {
		if p.canRetainSQLLeaderLocked(candidateIPs, routingOutcome, fallbackReason) {
			decision.routingResult = SQLRoutingResultRetainedSQL
			decision.health = make(map[string]bool, len(candidateIPs))
			for _, ip := range candidateIPs {
				decision.health[ip] = ip == p.awaitingPatroniLeader
			}
			return decision
		}
		if slices.Contains(candidateIPs, p.awaitingPatroniLeader) {
			// Losing the retained route must not let delayed Patroni revive it.
			p.rememberSupersededSQLLeaderLocked(p.awaitingPatroniLeader)
		}
		p.awaitingPatroniLeader = ""
		decision.routingResult = SQLRoutingResultCachedFallback
		decision.health = maps.Clone(p.patroniHealth)
		p.applyRoutingBanlistLocked(decision.health)
		p.excludeSupersededLeadersLocked(decision.health)
		if (p.supersededPatroniLeader != "" || len(p.supersededSQLLeaders) > 0) && len(healthyIPs(decision.health)) == 0 {
			// Superseded leaders remain excluded, but a different Patroni leader can
			// restore routing immediately through the case-2 recovery path.
			p.sqlNoPrimaryAt = evaluatedAt
		}
		if !p.patroniUpdatedAt.IsZero() {
			decision.cachedAge = evaluatedAt.Sub(p.patroniUpdatedAt)
			if decision.cachedAge < 0 {
				decision.cachedAge = 0
			}
			decision.hasCachedAge = true
		}
		return decision
	}

	p.updateSQLLeaderHistoryLocked(candidateIPs, routingOutcome.PrimaryIPs, evaluatedAt)
	decision.routingResult = SQLRoutingResultConclusive
	decision.health = make(map[string]bool, len(candidateIPs))
	for _, ip := range candidateIPs {
		decision.health[ip] = false
	}
	if len(routingOutcome.PrimaryIPs) == 1 {
		decision.health[routingOutcome.PrimaryIPs[0]] = true
	}
	p.applyRoutingBanlistLocked(decision.health)
	return decision
}

// sqlPredatesPatroniLeaderLocked checks evidence that would contradict an eligible
// Patroni leader. An excluded or banned Patroni observation cannot displace retained SQL,
// and agreement does not establish a new exclusion. The caller holds p.mu.
func (p *hybridHealthPolicy) sqlPredatesPatroniLeaderLocked(outcome SQLHealthCheckOutcome) bool {
	if p.patroniLeaderChangedAt.IsZero() || outcome.StartedAt.After(p.patroniLeaderChangedAt) {
		return false
	}
	leader, unique := singleHealthyIP(p.patroniHealth)
	_, superseded := p.supersededSQLLeaders[leader]
	_, banned := p.routingBannedIPs[leader]
	return unique && !banned && leader != p.supersededPatroniLeader && !superseded &&
		(len(outcome.PrimaryIPs) != 1 || outcome.PrimaryIPs[0] != leader)
}

// updateSQLLeaderHistoryLocked reconciles a conclusive SQL selection with prior leaders.
// The caller must hold p.mu and accept the snapshot's freshness before calling.
func (p *hybridHealthPolicy) updateSQLLeaderHistoryLocked(
	candidateIPs []string,
	primaryIPs []string,
	observedAt time.Time,
) {
	previousSQLLeader := p.awaitingPatroniLeader
	p.awaitingPatroniLeader = ""
	if len(primaryIPs) == 1 {
		leader := primaryIPs[0]
		patroniLeader, unique := singleHealthyIP(p.patroniHealth)
		if unique && patroniLeader == leader {
			p.supersededPatroniLeader = ""
			p.supersededSQLLeaders = nil
		} else {
			if previousSQLLeader != "" && previousSQLLeader != leader && slices.Contains(candidateIPs, previousSQLLeader) {
				p.rememberSupersededSQLLeaderLocked(previousSQLLeader)
			}
			// Fresh conclusive SQL can establish a formerly superseded leader again.
			delete(p.supersededSQLLeaders, leader)
			p.awaitingPatroniLeader = leader
			p.sqlLeaderSelectedAt = observedAt
			if unique {
				previousPatroniLeader := p.supersededPatroniLeader
				if previousPatroniLeader != leader && previousPatroniLeader != patroniLeader &&
					slices.Contains(candidateIPs, previousPatroniLeader) {
					p.rememberSupersededSQLLeaderLocked(previousPatroniLeader)
				}
				p.supersededPatroniLeader = patroniLeader
			}
		}
	} else if slices.Contains(candidateIPs, previousSQLLeader) {
		// B -> no primary -> C must remember B just like B -> C does.
		p.rememberSupersededSQLLeaderLocked(previousSQLLeader)
	}
}

// Retain a previously conclusive SQL leader only through uncertainty. Explicit
// demotion, an unroutable result, removal from the candidate set, or conflicting
// role evidence must not be hidden by the retained route. A successful primary
// result still takes precedence over an allowlisted error for that same IP.
func (p *hybridHealthPolicy) canRetainSQLLeaderLocked(candidateIPs []string, outcome SQLHealthCheckOutcome, fallbackReason string) bool {
	leader := p.awaitingPatroniLeader
	_, banned := p.routingBannedIPs[leader]
	if banned || leader == "" || !slices.Contains(candidateIPs, leader) ||
		fallbackReason == SQLHealthFallbackConflictingRoles || fallbackReason == SQLHealthFallbackMultiplePrimaries ||
		fallbackReason == SQLHealthFallbackStaleEvidence {
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

func (p *hybridHealthPolicy) rememberSupersededSQLLeaderLocked(leader string) {
	if leader == "" {
		return
	}
	if p.supersededSQLLeaders == nil {
		p.supersededSQLLeaders = make(map[string]struct{})
	}
	p.supersededSQLLeaders[leader] = struct{}{}
}

func (p *hybridHealthPolicy) excludeSupersededLeadersLocked(health map[string]bool) {
	if _, present := health[p.supersededPatroniLeader]; present {
		health[p.supersededPatroniLeader] = false
	}
	for ip := range p.supersededSQLLeaders {
		if _, present := health[ip]; present {
			health[ip] = false
		}
	}
}

func (p *hybridHealthPolicy) routingEligibleOutcomeLocked(outcome SQLHealthCheckOutcome) SQLHealthCheckOutcome {
	routingOutcome := SQLHealthCheckOutcome{
		StartedAt:  outcome.StartedAt,
		Results:    make([]SQLHealthCheckResult, 0, len(outcome.Results)),
		PrimaryIPs: make([]string, 0, len(outcome.PrimaryIPs)),
	}
	for _, result := range outcome.Results {
		if _, banned := p.routingBannedIPs[result.IP]; !banned {
			routingOutcome.Results = append(routingOutcome.Results, result)
		}
	}
	for _, ip := range outcome.PrimaryIPs {
		if _, banned := p.routingBannedIPs[ip]; !banned {
			routingOutcome.PrimaryIPs = append(routingOutcome.PrimaryIPs, ip)
		}
	}
	return routingOutcome
}

func (p *hybridHealthPolicy) applyRoutingBanlistLocked(health map[string]bool) {
	for ip := range p.routingBannedIPs {
		if _, present := health[ip]; present {
			health[ip] = false
		}
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

func (p *hybridHealthPolicy) recordRoutingLeaderLocked(source string, health map[string]bool, observedAt time.Time, hybridSource string) *LeaderRoutingComparison {
	side := referenceLeaderSource
	switch source {
	case "hybrid":
		side = candidateLeaderSource
	case "patroni":
	default:
		return nil
	}
	data := routingTimingData{source: hybridSource}
	if side == candidateLeaderSource {
		data.downtimeStartedAt = p.recordRoutingDowntimeLocked(health, observedAt)
	}
	pairs := p.routingTiming.observe(side, health, observedAt, data, oneHealthyLeader)
	if len(pairs) == 0 {
		return nil
	}
	pair := pairs[0]
	comparison := &LeaderRoutingComparison{NewLeaderIP: pair.ip, Improvement: pair.speedup(), HybridSource: pair.candidateData.source}
	startedAt := pair.candidateData.downtimeStartedAt
	if startedAt.IsZero() {
		return comparison
	}
	hybridDowntime := pair.candidateAt.Sub(startedAt)
	patroniDowntime := pair.patroniAt.Sub(startedAt)
	if hybridDowntime < 0 || patroniDowntime <= 0 {
		return comparison
	}
	comparison.HybridDowntime = hybridDowntime
	comparison.PatroniDowntime = patroniDowntime
	comparison.DowntimeReductionPercent = float64(patroniDowntime-hybridDowntime) / float64(patroniDowntime) * 100
	comparison.HasDowntimeComparison = true
	return comparison
}

// recordRoutingDowntimeLocked starts a window only after a published route loses all
// healthy leaders. Attach that start to a replacement leader, not a same-leader recovery.
func (p *hybridHealthPolicy) recordRoutingDowntimeLocked(health map[string]bool, observedAt time.Time) time.Time {
	leaders := healthyIPs(health)
	switch len(leaders) {
	case 0:
		if p.routingDowntimeStartedAt.IsZero() && len(p.routingTiming.candidate.lastNonempty) > 0 {
			p.routingDowntimeStartedAt = observedAt
		}
	case 1:
		startedAt := p.routingDowntimeStartedAt
		p.routingDowntimeStartedAt = time.Time{}
		if !p.routingTiming.candidate.lastNonempty[leaders[0]] {
			return startedAt
		}
	default:
		// An ambiguous publication is not a measured no-leader interval.
		p.routingDowntimeStartedAt = time.Time{}
	}
	return time.Time{}
}
