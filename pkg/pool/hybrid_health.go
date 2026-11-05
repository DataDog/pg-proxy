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
	SQLHealthFallbackMultiplePrimaries = "multiple_primaries"
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

// hybridHealthPolicy owns the state needed to choose between a complete SQL snapshot and the
// latest Patroni snapshot. It deliberately does not schedule checks or mutate PoolHealth; the
// health worker supplies observations and applies the returned decisions.
type hybridHealthPolicy struct {
	mu             sync.Mutex
	routingEnabled bool

	patroniHealth    map[string]bool
	patroniUpdatedAt time.Time

	detectionTiming leaderTiming[struct{}]
	routingTiming   leaderTiming[string]
}

func newHybridHealthPolicy(routingEnabled bool) *hybridHealthPolicy {
	return &hybridHealthPolicy{
		routingEnabled: routingEnabled,
		patroniHealth:  make(map[string]bool),
	}
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

// observeSQL evaluates one complete SQL snapshot. Timeout-classified probe errors and multiple
// distinct primaries are indeterminate, so routing falls back to the most recent Patroni snapshot.
// Ordinary non-timeout probe errors are conclusive unhealthy results. candidateIPs includes every
// address retained for this snapshot, not only addresses returned by its latest DNS lookup.
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
	timeoutType := SQLHealthTimeoutNone
	for _, result := range outcome.Results {
		resultTimeoutType := sqlHealthTimeoutType(result.Err)
		if resultTimeoutType == SQLHealthTimeoutNone {
			continue
		}
		if timeoutType == SQLHealthTimeoutNone {
			timeoutType = resultTimeoutType
		} else if timeoutType != resultTimeoutType {
			timeoutType = SQLHealthTimeoutMixed
		}
	}
	if timeoutType != SQLHealthTimeoutNone {
		return SQLHealthFallbackTimeout, timeoutType
	}
	if len(outcome.PrimaryIPs) > 1 {
		return SQLHealthFallbackMultiplePrimaries, SQLHealthTimeoutNone
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

func (p *hybridHealthPolicy) observeLeaderLocked(
	source string,
	primaryIPs []string,
	observedAt time.Time,
) *LeaderDetectionComparison {
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
	pairs := p.detectionTiming.observe(side, health, observedAt, struct{}{}, oneNewLeader)
	if len(pairs) == 0 {
		return nil
	}
	pair := pairs[0]
	return &LeaderDetectionComparison{
		NewLeaderIP:       pair.ip,
		Speedup:           pair.speedup(),
		SQLRoutingEnabled: p.routingEnabled,
	}
}

func (p *hybridHealthPolicy) recordRoutingLeaderLocked(
	source string,
	health map[string]bool,
	observedAt time.Time,
	hybridSource string,
) *LeaderRoutingComparison {
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
	return &LeaderRoutingComparison{
		NewLeaderIP:  pair.ip,
		Improvement:  pair.speedup(),
		HybridSource: pair.candidate.data,
	}
}
