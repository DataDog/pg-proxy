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
// A positive Speedup means SQL observed the leader first. When SQL observed a no-leader interval
// before the transition, the downtime fields share that SQL observation as their start, and
// DowntimeReductionPercent is (PatroniDowntime-SQLDowntime)/PatroniDowntime*100.
type LeaderDetectionComparison struct {
	NewLeaderIP              string
	Speedup                  time.Duration
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
}

type leaderObservation struct {
	// Keep the last nonempty role observation for same-leader downtime accounting.
	primaryIPs map[string]struct{}
	// Comparison state can be invalidated independently so recovery records a fresh timestamp.
	comparisonPrimaryIPs map[string]struct{}
}

// observe returns a newly observed leader, or an empty string for a baseline or no unique change.
func (o *leaderObservation) observe(primaryIPs []string) string {
	if len(primaryIPs) == 0 {
		return ""
	}
	current := make(map[string]struct{}, len(primaryIPs))
	for _, ip := range primaryIPs {
		current[ip] = struct{}{}
	}
	previous := o.comparisonPrimaryIPs
	o.primaryIPs = current
	o.comparisonPrimaryIPs = maps.Clone(current)
	if previous == nil {
		return ""
	}
	newLeaders := make([]string, 0, len(current))
	for ip := range current {
		if _, existed := previous[ip]; !existed {
			newLeaders = append(newLeaders, ip)
		}
	}
	if len(newLeaders) != 1 {
		return ""
	}
	return newLeaders[0]
}

type leaderDetectionPair struct {
	sqlAt     time.Time
	patroniAt time.Time
}

type sqlDowntimeWindow struct {
	startedAt   time.Time
	recoveredAt time.Time
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

	patroniLeaderObservation    leaderObservation
	sqlLeaderObservation        leaderObservation
	pendingLeaderDetections     map[string]leaderDetectionPair
	sqlDowntimeStartedAt        time.Time
	sqlDowntimesAwaitingPatroni map[string]sqlDowntimeWindow

	patroniRoutingObservation leaderObservation
	hybridRoutingObservation  leaderObservation
	pendingRouting            map[string]leaderRoutingPair
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
	if p.routingEnabled != enabled {
		// Comparisons cannot span periods where SQL routing evaluation was disabled. Reset both
		// baselines so the next observations establish a fresh pair under the new flag state.
		p.patroniRoutingObservation = leaderObservation{}
		p.hybridRoutingObservation = leaderObservation{}
		p.pendingRouting = nil
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
		routingEnabled: p.routingEnabled,
		routingResult:  SQLRoutingResultShadow,
	}
	fallbackReason, timeoutType := sqlHealthFallback(outcome)
	if fallbackReason == "" {
		p.recordSQLDowntimeLocked(outcome.PrimaryIPs, observedAt)
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

func (p *hybridHealthPolicy) recordSQLDowntimeLocked(primaryIPs []string, observedAt time.Time) {
	switch len(primaryIPs) {
	case 0:
		// Do not treat startup without a baseline leader as a failover downtime. Once SQL has
		// observed a leader, retain the first no-leader timestamp until SQL recovers.
		if p.sqlDowntimeStartedAt.IsZero() && len(p.sqlLeaderObservation.primaryIPs) > 0 {
			p.sqlDowntimeStartedAt = observedAt
		}
	case 1:
		if p.sqlDowntimeStartedAt.IsZero() {
			return
		}
		leaderIP := primaryIPs[0]
		startedAt := p.sqlDowntimeStartedAt
		p.sqlDowntimeStartedAt = time.Time{}
		if _, wasPreviousLeader := p.sqlLeaderObservation.primaryIPs[leaderIP]; wasPreviousLeader {
			// The same leader recovered, so there is no new-leader event to compare with Patroni.
			return
		}
		if p.sqlDowntimesAwaitingPatroni == nil {
			p.sqlDowntimesAwaitingPatroni = make(map[string]sqlDowntimeWindow)
		}
		p.sqlDowntimesAwaitingPatroni[leaderIP] = sqlDowntimeWindow{
			startedAt:   startedAt,
			recoveredAt: observedAt,
		}
	}
}

func (p *hybridHealthPolicy) observeLeaderLocked(
	source string,
	primaryIPs []string,
	observedAt time.Time,
) *LeaderDetectionComparison {
	var observation *leaderObservation
	switch source {
	case "patroni":
		observation = &p.patroniLeaderObservation
	case "sql":
		observation = &p.sqlLeaderObservation
	default:
		return nil
	}
	// Once a source has left a leader, its unmatched observation cannot be paired with
	// a later promotion of that IP. Preserve observations from the other source, which
	// may legitimately be ahead of this one.
	for ip, pair := range p.pendingLeaderDetections {
		observed := source == "sql" && !pair.sqlAt.IsZero() ||
			source == "patroni" && !pair.patroniAt.IsZero()
		if observed && !slices.Contains(primaryIPs, ip) {
			delete(p.pendingLeaderDetections, ip)
			delete(p.sqlDowntimesAwaitingPatroni, ip)
			delete(observation.comparisonPrimaryIPs, ip)
		}
	}
	leaderIP := observation.observe(primaryIPs)
	if leaderIP == "" {
		return nil
	}

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
	comparison := &LeaderDetectionComparison{
		NewLeaderIP:       leaderIP,
		Speedup:           pair.patroniAt.Sub(pair.sqlAt),
		SQLRoutingEnabled: p.routingEnabled,
	}
	downtime, ok := p.sqlDowntimesAwaitingPatroni[leaderIP]
	delete(p.sqlDowntimesAwaitingPatroni, leaderIP)
	if !ok || !downtime.recoveredAt.Equal(pair.sqlAt) {
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

func (p *hybridHealthPolicy) recordRoutingLeaderLocked(
	source string,
	health map[string]bool,
	observedAt time.Time,
	hybridSource string,
) *LeaderRoutingComparison {
	var observation *leaderObservation
	switch source {
	case "patroni":
		observation = &p.patroniRoutingObservation
	case "hybrid":
		observation = &p.hybridRoutingObservation
	default:
		return nil
	}
	// Once a source leaves a leader, its unmatched timestamp must not be paired with
	// a later promotion of that IP. Keep observations from the other source, which
	// may still be ahead of this one, even when this source has no unique leader.
	for ip, pair := range p.pendingRouting {
		observedBySource := source == "hybrid" && !pair.hybridAt.IsZero() ||
			source == "patroni" && !pair.patroniAt.IsZero()
		if observedBySource && !health[ip] {
			delete(p.pendingRouting, ip)
			delete(observation.comparisonPrimaryIPs, ip)
		}
	}
	leaderIP, unique := singleHealthyIP(health)
	if !unique {
		return nil
	}
	if observation.observe([]string{leaderIP}) == "" {
		return nil
	}
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
	delete(p.pendingRouting, leaderIP)
	return &LeaderRoutingComparison{
		NewLeaderIP:  leaderIP,
		Improvement:  pair.patroniAt.Sub(pair.hybridAt),
		HybridSource: pair.hybridSource,
	}
}
