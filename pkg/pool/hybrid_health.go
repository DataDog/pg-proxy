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
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

const (
	SQLRoutingResultShadow         = "shadow"
	SQLRoutingResultConclusive     = "conclusive_sql"
	SQLRoutingResultCachedFallback = "cached_fallback"

	SQLHealthTimeoutNone      = "none"
	SQLHealthTimeoutContext   = "context_timeout"
	SQLHealthTimeoutNetwork   = "network_timeout"
	SQLHealthTimeoutStatement = "statement_timeout"
	SQLHealthTimeoutMixed     = "mixed"

	SQLHealthFallbackTimeout           = "timeout"
	SQLHealthFallbackIndeterminate     = "indeterminate_error"
	SQLHealthFallbackMultiplePrimaries = "multiple_primaries"
	SQLHealthFallbackConflictingRoles  = "conflicting_roles"

	SQLRoutingBanReasonPostgresShutdown   = "postgres_shutdown"
	SQLRoutingUnbanReasonConclusiveResult = "conclusive_result"
	SQLRoutingUnbanReasonDNSRemoved       = "dns_removed"
)

// SQLRoutingBanChange reports one IP entering or leaving the SQL routing banlist.
type SQLRoutingBanChange struct {
	IP     string
	Banned bool // New state: true on entry, false on recovery or candidate removal.
	Reason string
}

// LeaderDetectionComparison pairs independent SQL and Patroni observations of a new leader.
// When HasDowntimeComparison is true, the downtime fields share the first conclusive SQL
// no-leader observation as their start. DowntimeReductionPercent is
// (PatroniDowntime-SQLDowntime)/PatroniDowntime*100; no standalone detection speedup is exposed.
type LeaderDetectionComparison struct {
	NewLeaderIP              string
	SQLRoutingEnabled        bool
	SQLDowntime              time.Duration
	PatroniDowntime          time.Duration
	DowntimeReductionPercent float64
	HasDowntimeComparison    bool
}

// LeaderRoutingComparison compares when hybrid and Patroni-only routing select a new leader.
// A positive Improvement means hybrid routing selected the leader first.
type LeaderRoutingComparison struct {
	NewLeaderIP  string
	Improvement  time.Duration
	HybridSource string
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

	leaderDetection   *LeaderDetectionComparison
	routingComparison *LeaderRoutingComparison

	// Per-evaluation events, discarded after reporting; this is not retained history.
	routingBanChanges []SQLRoutingBanChange
	routingBannedIPs  map[string]time.Time
}

type sqlDowntimeWindow struct {
	startedAt   time.Time
	recoveredAt time.Time
}

// hybridHealthPolicy owns the state needed to choose between a complete SQL snapshot and the
// latest Patroni snapshot. It deliberately does not schedule checks or mutate PoolHealth; the
// health worker supplies observations and applies the returned decisions.
type hybridHealthPolicy struct {
	mu             sync.Mutex
	routingEnabled bool

	patroniHealth    map[string]bool
	patroniUpdatedAt time.Time

	detectionTiming      leaderTiming[sqlDowntimeWindow]
	routingTiming        leaderTiming[string]
	sqlDowntimeStartedAt time.Time
	routingBannedIPs     map[string]time.Time
}

func newHybridHealthPolicy(routingEnabled bool) *hybridHealthPolicy {
	return &hybridHealthPolicy{
		routingEnabled:   routingEnabled,
		patroniHealth:    make(map[string]bool),
		routingBannedIPs: make(map[string]time.Time),
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
		p.routingTiming = leaderTiming[string]{}
	}
	p.routingEnabled = enabled
}

// observePatroni refreshes the fallback snapshot. Patroni remains authoritative in shadow mode;
// when SQL routing is enabled it is cached without replacing the active route.
func (p *hybridHealthPolicy) observePatroni(health map[string]bool, observedAt time.Time) hybridRoutingDecision {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.patroniHealth = maps.Clone(health)
	p.patroniUpdatedAt = observedAt
	decision := hybridRoutingDecision{
		routingEnabled: p.routingEnabled,
		routingResult:  SQLRoutingResultShadow,
		health:         maps.Clone(health),
		publish:        !p.routingEnabled,
	}
	decision.leaderDetection = p.observeLeaderLocked("patroni", healthyIPs(health), observedAt, sqlDowntimeWindow{})
	if p.routingEnabled {
		decision.routingComparison = p.recordRoutingLeaderLocked("patroni", health, observedAt, "")
	}
	return decision
}

// observeSQL evaluates one complete SQL snapshot. SQL routing is conclusive only when every IP has
// a successful role or a narrowly classified unroutable result. Unknown errors, multiple distinct
// primaries, or conflicting deterministic roles fall back to the most recent Patroni snapshot.
// candidateIPs includes every address retained for this snapshot, not only addresses returned by
// its latest DNS lookup. Banlisted IPs remain probed for recovery but are excluded from SQL
// decisions and cached Patroni fallback. Raw comparison snapshots remain unchanged.
func (p *hybridHealthPolicy) observeSQL(
	candidateIPs []string,
	outcome SQLHealthCheckOutcome,
	observedAt time.Time,
) hybridRoutingDecision {
	return p.observeSQLRound(candidateIPs, outcome, SQLHealthResultsSnapshot{
		ScheduledAt: observedAt, ObservedAt: observedAt, Results: outcome.Results,
	})
}

// observeSQLRound distinguishes this completed poll from cached credential evidence. Only a
// successful poll started after a ban may clear it; an older success is not recovery evidence.
func (p *hybridHealthPolicy) observeSQLRound(
	candidateIPs []string,
	outcome SQLHealthCheckOutcome,
	fresh SQLHealthResultsSnapshot,
) hybridRoutingDecision {
	observedAt := fresh.ObservedAt
	p.mu.Lock()
	defer p.mu.Unlock()

	decision := hybridRoutingDecision{
		routingEnabled: p.routingEnabled,
		routingResult:  SQLRoutingResultShadow,
	}
	decision.routingBanChanges = p.updateRoutingBanlistLocked(candidateIPs, outcome, fresh)
	decision.routingBannedIPs = maps.Clone(p.routingBannedIPs)
	routingOutcome := p.routingEligibleOutcomeLocked(outcome)
	fallbackReason, timeoutType := sqlHealthFallback(routingOutcome)
	if fallbackReason == "" {
		downtime := p.recordSQLDowntimeLocked(routingOutcome.PrimaryIPs, observedAt)
		decision.leaderDetection = p.observeLeaderLocked("sql", routingOutcome.PrimaryIPs, observedAt, downtime)
	}
	if !p.routingEnabled {
		return decision
	}

	decision.publish = true
	decision.fallbackReason = fallbackReason
	decision.timeoutType = timeoutType
	if decision.fallbackReason != "" {
		decision.routingResult = SQLRoutingResultCachedFallback
		decision.health = maps.Clone(p.patroniHealth)
		p.applyRoutingBanlistLocked(decision.health)
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

	decision.routingResult = SQLRoutingResultConclusive
	decision.health = make(map[string]bool, len(candidateIPs))
	for _, ip := range candidateIPs {
		decision.health[ip] = false
	}
	if len(routingOutcome.PrimaryIPs) == 1 {
		decision.health[routingOutcome.PrimaryIPs[0]] = true
	}
	p.applyRoutingBanlistLocked(decision.health)
	decision.routingComparison = p.recordRoutingLeaderLocked(
		"hybrid",
		decision.health,
		observedAt,
		SQLRoutingResultConclusive,
	)
	return decision
}

func (p *hybridHealthPolicy) routingEligibleOutcomeLocked(outcome SQLHealthCheckOutcome) SQLHealthCheckOutcome {
	routingOutcome := SQLHealthCheckOutcome{
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

func (p *hybridHealthPolicy) updateRoutingBanlistLocked(
	candidateIPs []string,
	outcome SQLHealthCheckOutcome,
	fresh SQLHealthResultsSnapshot,
) []SQLRoutingBanChange {
	candidates := make(map[string]struct{}, len(candidateIPs))
	for _, ip := range candidateIPs {
		candidates[ip] = struct{}{}
	}
	successfulResults := make(map[string]struct{})
	shutdownResults := make(map[string]struct{})
	for _, result := range fresh.Results {
		if result.Err == nil {
			successfulResults[result.IP] = struct{}{}
		}
	}
	// Conflicting successful roles cannot establish recovery.
	_, conflictingIPs := aggregateSQLComparisonRoles(outcome.Results)
	for _, ip := range conflictingIPs {
		delete(successfulResults, ip)
	}
	for _, result := range outcome.Results {
		if isPostgresShutdownError(result.Err) {
			shutdownResults[result.IP] = struct{}{}
		}
	}

	var changes []SQLRoutingBanChange
	for ip := range p.routingBannedIPs {
		if _, retained := candidates[ip]; !retained {
			delete(p.routingBannedIPs, ip)
			changes = append(changes, SQLRoutingBanChange{IP: ip, Reason: SQLRoutingUnbanReasonDNSRemoved})
		}
	}
	for ip := range shutdownResults {
		if _, retained := candidates[ip]; !retained {
			continue
		}
		if _, alreadyBanned := p.routingBannedIPs[ip]; !alreadyBanned {
			p.routingBannedIPs[ip] = fresh.ObservedAt
			changes = append(changes, SQLRoutingBanChange{IP: ip, Banned: true, Reason: SQLRoutingBanReasonPostgresShutdown})
		}
	}
	// Repeated fresh shutdown evidence moves the recovery boundary without emitting another
	// transition. Cached shutdown results must not keep moving that boundary on unrelated polls.
	for _, result := range fresh.Results {
		if isPostgresShutdownError(result.Err) {
			if bannedAt, banned := p.routingBannedIPs[result.IP]; banned && fresh.ObservedAt.After(bannedAt) {
				p.routingBannedIPs[result.IP] = fresh.ObservedAt
			}
		}
	}
	for ip := range successfulResults {
		// Credential checks for one host run concurrently, so a success and shutdown result in the
		// same round have no reliable ordering. Keep the IP banned until a later clean round proves
		// that it can conclusively report a PostgreSQL role again.
		if _, shuttingDown := shutdownResults[ip]; shuttingDown {
			continue
		}
		if bannedAt, wasBanned := p.routingBannedIPs[ip]; wasBanned && fresh.ScheduledAt.After(bannedAt) {
			delete(p.routingBannedIPs, ip)
			changes = append(changes, SQLRoutingBanChange{IP: ip, Reason: SQLRoutingUnbanReasonConclusiveResult})
		}
	}
	slices.SortFunc(changes, func(a, b SQLRoutingBanChange) int { return strings.Compare(a.IP, b.IP) })
	return changes
}

func (p *hybridHealthPolicy) applyRoutingBanlistLocked(health map[string]bool) {
	for ip := range p.routingBannedIPs {
		if _, present := health[ip]; present {
			health[ip] = false
		}
	}
}

func isPostgresShutdownError(err error) bool {
	var postgresErr *pgconn.PgError
	if !errors.As(err, &postgresErr) {
		return false
	}
	switch postgresErr.Code {
	case sqlHealthCheckCrashShutdown:
		return true
	case sqlHealthCheckCannotConnectNow:
		return strings.Contains(postgresErr.Message, "the database system is shutting down")
	default:
		return false
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
		// Credential targets describe the same PostgreSQL instance. One successful role, or one
		// result that conclusively makes the IP unroutable, resolves uncertainty from another
		// credential on that IP. Uncertainty on a different IP still requires Patroni fallback.
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

func (p *hybridHealthPolicy) recordSQLDowntimeLocked(primaryIPs []string, observedAt time.Time) sqlDowntimeWindow {
	switch len(primaryIPs) {
	case 0:
		if p.sqlDowntimeStartedAt.IsZero() && len(p.detectionTiming.candidate.lastNonempty) > 0 {
			p.sqlDowntimeStartedAt = observedAt
		}
	case 1:
		if p.sqlDowntimeStartedAt.IsZero() {
			return sqlDowntimeWindow{}
		}
		startedAt := p.sqlDowntimeStartedAt
		p.sqlDowntimeStartedAt = time.Time{}
		// Same-leader recovery has no replacement event to compare with Patroni.
		if p.detectionTiming.candidate.lastNonempty[primaryIPs[0]] {
			return sqlDowntimeWindow{}
		}
		return sqlDowntimeWindow{startedAt: startedAt, recoveredAt: observedAt}
	}
	return sqlDowntimeWindow{}
}

func (p *hybridHealthPolicy) observeLeaderLocked(source string, primaryIPs []string, observedAt time.Time, downtime sqlDowntimeWindow) *LeaderDetectionComparison {
	side := referenceLeaderSource
	switch source {
	case "sql":
		side = candidateLeaderSource
	case "patroni":
	default:
		return nil
	}
	health := make(map[string]bool, len(primaryIPs))
	for _, ip := range primaryIPs {
		health[ip] = true
	}
	pairs := p.detectionTiming.observe(side, health, observedAt, downtime, oneNewLeader)
	if len(pairs) == 0 {
		return nil
	}
	pair := pairs[0]
	comparison := &LeaderDetectionComparison{NewLeaderIP: pair.ip, SQLRoutingEnabled: p.routingEnabled}
	downtime = pair.candidateData
	if downtime.startedAt.IsZero() || !downtime.recoveredAt.Equal(pair.candidateAt) {
		return comparison
	}
	patroniDowntime := pair.patroniAt.Sub(downtime.startedAt)
	sqlDowntime := downtime.recoveredAt.Sub(downtime.startedAt)
	if patroniDowntime <= 0 || sqlDowntime < 0 {
		return comparison
	}
	comparison.SQLDowntime = sqlDowntime
	comparison.PatroniDowntime = patroniDowntime
	comparison.DowntimeReductionPercent = float64(patroniDowntime-sqlDowntime) / float64(patroniDowntime) * 100
	comparison.HasDowntimeComparison = true
	return comparison
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
	pairs := p.routingTiming.observe(side, health, observedAt, hybridSource, oneHealthyLeader)
	if len(pairs) == 0 {
		return nil
	}
	pair := pairs[0]
	return &LeaderRoutingComparison{NewLeaderIP: pair.ip, Improvement: pair.speedup(), HybridSource: pair.candidateData}
}
