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
		routingResult: SQLRoutingResultShadow,
		health:        maps.Clone(health),
		publish:       !p.routingEnabled,
	}
	decision.leaderDetection = p.observeLeaderLocked("patroni", healthyIPs(health), observedAt)
	if p.routingEnabled {
		decision.routingComparison = p.recordRoutingLeaderLocked("patroni", health, observedAt, "")
	}
	return decision
}

// observeSQL evaluates one complete SQL snapshot. SQL routing is conclusive only when every IP has
// a successful role or a narrowly classified unroutable result. Unknown errors, multiple distinct
// primaries, or conflicting deterministic roles fall back to the most recent Patroni snapshot.
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

	decision.publish = true
	decision.fallbackReason = fallbackReason
	decision.timeoutType = timeoutType
	if decision.fallbackReason != "" {
		decision.routingResult = SQLRoutingResultCachedFallback
		decision.health = maps.Clone(p.patroniHealth)
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
