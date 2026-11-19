// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package pool

import (
	"context"
	"crypto/sha256"
	"errors"
	"maps"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrSQLHealthUpdatesUnsupported reports that a health subscription has no SQL checker.
	ErrSQLHealthUpdatesUnsupported = errors.New("pool does not publish SQL health updates")
	// ErrSQLHealthCheckMissed reports that the previous credential SQL round was still running at the
	// next tick.
	ErrSQLHealthCheckMissed = errors.New("SQL health check missed its scheduled interval")
)

// HealthChecker centralizes health probing across pools. Pools targeting the same host and role
// share discovery, probes, and Patroni state. SQL-enabled subscriptions retain independent
// database- and credential-specific targets under that shared host snapshot.
type HealthChecker interface {
	// Subscribe registers host for periodic probing and returns a channel that
	// receives the shared snapshot after each completed health check. The first Subscribe for a new
	// key runs an inline DNS + probe pass before returning, so an initial snapshot is ready for the
	// caller to receive. Each subscriber gets its own channel so every subscriber observes updates.
	// SQL is nil for subscriptions that do not need centralized SQL health checks.
	Subscribe(host string, isReadReplica bool, dbName string, tags []string, sql *SQLHealthConfig) HealthUpdates
	// Unsubscribe drops a subscription. When the last subscriber for a key
	// unsubscribes the entry is removed and the worker stops probing it.
	Unsubscribe(HealthUpdates)
}

// HealthUpdates delivers the latest shared health snapshot for a subscribed
// host. Updates are coalesced while a consumer is still processing the previous
// notification.
type HealthUpdates <-chan *PoolHealth

// SQLHealthConfig enables centralized SQL checks for one health subscription.
type SQLHealthConfig struct {
	ConnStringForIP func(ip string) string
	// ChecksEnabled is evaluated before each SQL poll. A valid false also restores Patroni
	// routing and latches SQL routing off until host recreation. Invalid evaluations use the
	// callback's last-known value for probing. Nil preserves enabled checks.
	ChecksEnabled func(context.Context) SQLHealthFeatureFlagEvaluation
	// RoutingEnabled allows conclusive aggregated SQL snapshots to own client routing. A valid
	// false latches routing off until host recreation (normally a restart); invalid evaluations
	// preserve the current mode. Nil keeps Patroni authoritative.
	RoutingEnabled func(context.Context) SQLHealthFeatureFlagEvaluation
}

// SQLHealthFeatureFlagEvaluation distinguishes an intentional flag value from an evaluation error.
// Enabled should contain the evaluator's last-known value when Valid is false.
type SQLHealthFeatureFlagEvaluation struct {
	Enabled bool
	Valid   bool
}

// SQLHealthObservation reports a client-facing SQL availability result. A writable observation may
// be published as soon as the first healthy primary responds, before the complete routing snapshot
// finishes. Disabled reports a feature-gated check without classifying it as a database failure.
type SQLHealthObservation struct {
	// ScheduledAt identifies the refresh interval this observation belongs to. Consumers use it
	// to prevent a late completion from an older interval from replacing newer availability state.
	ScheduledAt time.Time
	// ObservedAt is when the SQL result became known. For a missed interval it equals ScheduledAt.
	ObservedAt time.Time
	Writable   bool
	Err        error
	Disabled   bool
}

var errNoSQLHealthCheckResults = errors.New("SQL health check returned no results")

func newSQLHealthObservation(scheduledAt, observedAt time.Time, outcome SQLHealthCheckOutcome) SQLHealthObservation {
	observation := SQLHealthObservation{
		ScheduledAt: scheduledAt,
		ObservedAt:  observedAt,
		Writable:    len(outcome.PrimaryIPs) > 0,
	}
	errs := make([]error, 0, len(outcome.Results))
	for _, result := range outcome.Results {
		if result.Err != nil {
			errs = append(errs, result.Err)
		}
	}
	if len(outcome.Results) == 0 {
		errs = append(errs, errNoSQLHealthCheckResults)
	}
	observation.Err = errors.Join(errs...)
	return observation
}

func sqlHealthObservationScheduledAt(observation SQLHealthObservation) time.Time {
	if !observation.ScheduledAt.IsZero() {
		return observation.ScheduledAt
	}
	// Preserve ordering for callers that construct observations without ScheduledAt.
	return observation.ObservedAt
}

// SQLHealthUpdates is an independent subscription to SQL observations for one PoolHealth.
type SQLHealthUpdates interface {
	Updates() <-chan SQLHealthObservation
	Done() <-chan struct{}
	Close()
}

// HealthProber supplies discovery and health policy. Its methods may run concurrently.
type HealthProber interface {
	Lookup(host string, tags []string) []string
	Probe(ip, host, dbName string, isReadReplica bool, tags []string) (healthy bool, systemID string, err error)
}

// HealthWorkerHooks reports subscription counts and refresh activity.
// Callbacks may run concurrently across hosts. Evicted must not read the snapshot.
type HealthWorkerHooks struct {
	Subscriptions      func(count int, tags []string)
	CheckStarted       func(tags []string)
	CheckFinished      func(elapsed time.Duration, tags []string)
	SQLCheckMissed     func(tags []string)
	SQLPatroniCompared func(SQLPatroniHealthSnapshot, []string)
	// SQLPotentialCompared reports signed snapshot speedup, never a routing decision.
	// It can run concurrently for different hosts and must return promptly.
	SQLPotentialCompared func(SQLPotentialComparison, []string)
	Evicted              func(ip string, tags []string)
}

// SQLHealthResultsSnapshot is one credential target's raw result set for retained IPs. Errors remain wrapped
// so observability hooks can inspect PostgreSQL SQLSTATEs and errors.Is/errors.As chains.
type SQLHealthResultsSnapshot struct {
	// StartedAt is the probe start time used to order evidence against Patroni recovery.
	StartedAt    time.Time
	ScheduledAt  time.Time
	ObservedAt   time.Time
	CandidateIPs []string
	Tags         []string
	Results      []SQLHealthCheckResult
}

// SQLPatroniHealthSnapshot exposes retained raw SQL findings alongside the Patroni snapshot used for
// comparison. SQL results may come from different scheduled polls when credentials finish at
// different rates.
type SQLPatroniHealthSnapshot struct {
	// SQLHealth contains only roles on which the latest determinate credential results agree.
	// Retained addresses with only inconclusive results are unknown and absent from this map.
	SQLHealth map[string]bool
	// SQLConflictingIPs lists addresses observed as both primary and standby by current targets.
	SQLConflictingIPs []string
	SQLRetainedIPs    []string
	SQLResults        []SQLHealthResultsSnapshot
	PatroniHealth     map[string]bool
}

// Matches reports whether every conclusive SQL finding agrees with Patroni and both snapshots have
// the same conclusive address membership. Unknown SQL roles are neutral; conflicting roles
// always produce a mismatch.
func (s SQLPatroniHealthSnapshot) Matches() bool {
	return s.MismatchReason() == SQLPatroniMatch
}

// SQLPatroniMismatchReason classifies comparison evidence without inferring its cause.
type SQLPatroniMismatchReason string

const (
	SQLPatroniMatch                SQLPatroniMismatchReason = "none"
	SQLPatroniPrimaryDisagreement  SQLPatroniMismatchReason = "primary_disagreement"
	SQLPatroniMembershipDifference SQLPatroniMismatchReason = "membership_difference"
	SQLPatroniSQLRoleConflict      SQLPatroniMismatchReason = "sql_role_conflict"
)

// MismatchReason preserves Matches' unknown-role handling. Role conflicts take
// precedence over primary disagreements, which take precedence over differences
// consisting only of extra non-primary addresses. A membership difference does
// not establish that DNS is stale or that the difference is transient.
func (s SQLPatroniHealthSnapshot) MismatchReason() SQLPatroniMismatchReason {
	if len(s.SQLConflictingIPs) > 0 {
		return SQLPatroniSQLRoleConflict
	}
	unknownIPs := make(map[string]struct{})
	for _, ip := range s.SQLRetainedIPs {
		if _, known := s.SQLHealth[ip]; !known {
			unknownIPs[ip] = struct{}{}
		}
	}
	reason := SQLPatroniMatch
	for ip, sqlHealthy := range s.SQLHealth {
		patroniHealthy, ok := s.PatroniHealth[ip]
		if ok && patroniHealthy != sqlHealthy || !ok && sqlHealthy {
			return SQLPatroniPrimaryDisagreement
		}
		if !ok {
			reason = SQLPatroniMembershipDifference
		}
	}
	for ip, patroniHealthy := range s.PatroniHealth {
		if _, unknown := unknownIPs[ip]; unknown {
			continue
		}
		if _, ok := s.SQLHealth[ip]; !ok {
			if patroniHealthy {
				return SQLPatroniPrimaryDisagreement
			}
			reason = SQLPatroniMembershipDifference
		}
	}
	return reason
}

type SQLIteration struct {
	RoutingResult  string
	FallbackReason string
	TimeoutType    string
}

type SQLFallback struct {
	Reason           string
	TimeoutType      string
	CachedPatroniAge time.Duration
	HasCachedPatroni bool
}

// SQLHealthWorkerHooks reports hybrid-routing decisions and leader-publication timing.
type SQLHealthWorkerHooks struct {
	Iteration             func(SQLIteration, []string)
	ConclusiveError       func(SQLHealthCheckResult, []string)
	CachedFallback        func(SQLFallback, []string)
	LeaderRoutingCompared func(LeaderRoutingComparison, []string)
}

// HealthWorkerConfig supplies the application policy for a shared worker.
type HealthWorkerConfig struct {
	Interval         time.Duration
	SQLInterval      time.Duration
	AbsenceThreshold int
	Prober           HealthProber
	NewSQLChecker    func(context.Context) *SQLHealthChecker
	Hooks            HealthWorkerHooks
	SQLHooks         SQLHealthWorkerHooks
}

// NewHealthWorker constructs a worker without starting periodic refreshes.
// Prober is required. Interval must be positive when Run is used.
func NewHealthWorker(cfg HealthWorkerConfig) *HealthWorker {
	return &HealthWorker{
		interval:         cfg.Interval,
		sqlInterval:      cfg.SQLInterval,
		absenceThreshold: cfg.AbsenceThreshold,
		prober:           cfg.Prober,
		hooks:            cfg.Hooks,
		sqlHooks:         cfg.SQLHooks,
		newSQLChecker:    cfg.NewSQLChecker,
		hosts:            make(map[string]*PoolHealth),
		subscriptions:    make(map[HealthUpdates]*healthSubscription),
	}
}

// HealthWorker shares health checks and snapshots among pools targeting the same host.
type HealthWorker struct {
	interval         time.Duration
	sqlInterval      time.Duration
	prober           HealthProber
	absenceThreshold int
	hooks            HealthWorkerHooks
	sqlHooks         SQLHealthWorkerHooks
	newSQLChecker    func(context.Context) *SQLHealthChecker

	mu            sync.Mutex
	hosts         map[string]*PoolHealth
	subscriptions map[HealthUpdates]*healthSubscription
}

// PoolHealth is a shared snapshot for a subscribed host.
type PoolHealth struct {
	host          string
	isReadReplica bool
	dbName        string
	tags          []string
	patroniTags   []string

	mu sync.RWMutex
	// Keep probing until the last pool unsubscribes.
	refcount    int
	subscribers map[HealthUpdates]chan *PoolHealth
	// ready closes after the first inline health check finishes. Concurrent
	// subscribers wait for it instead of observing an empty snapshot.
	ready              chan struct{}
	state              *HealthState // Client-visible routing, independent of both discovery states.
	patroniHealthState *HealthState

	sqlMu          sync.RWMutex
	sqlHealthState *HealthState
	// sqlScheduleMu serializes DNS discovery and starting each tick's idle credential checks. The
	// checks themselves run after this lock is released.
	sqlScheduleMu sync.Mutex
	// sqlComparisonMu protects target lifecycle generations and each credential's latest raw
	// results. Aggregate roles are derived from those results, never cached independently.
	// It also serializes invalidation with retention changes and hybrid policy evaluation.
	// When both are needed, acquire sqlScheduleMu before sqlComparisonMu; DNS lookup itself
	// runs outside sqlComparisonMu so it cannot delay completed polls or invalidation.
	sqlComparisonMu     sync.Mutex
	sqlComparisonLatest map[*sqlHealthTarget]SQLHealthResultsSnapshot
	// Potential timing is telemetry-only and protected by sqlComparisonMu.
	potentialTiming sqlPotentialTimingState
	// subscriptionTargets associates each pool subscription with its SQL connection string.
	subscriptionTargets map[HealthUpdates]*sqlHealthTarget
	// routingMu protects the policy pointer, routing mode, Patroni cache updates, and publication.
	// Lock order: sqlMu -> sqlComparisonMu -> routingMu; none may cover network probes or hooks.
	routingMu          sync.Mutex
	sqlRoutingDisabled bool // A live disable remains latched until this host is recreated.
	sqlRoutingActive   bool // Conclusive or retained SQL owns routing; Patroni decisions can recover it.
	routingChangedAt   time.Time
	// The policy survives removal of SQL targets so re-subscription cannot reset the kill switch.
	hybridPolicy     *hybridHealthPolicy
	routingEnabledFn func(context.Context) SQLHealthFeatureFlagEvaluation
}

type healthSubscription struct {
	poolHealth *PoolHealth
	sqlTarget  *sqlHealthTarget
}

type sqlHealthRoundState struct {
	scheduledAt       time.Time
	running           bool
	writablePublished bool
}

type sqlHealthComparisonSnapshot struct {
	potentialComparisons []SQLPotentialComparison
	conflictingIPs       []string
	health               map[string]bool
	retainedIPs          []string
	results              []SQLHealthResultsSnapshot
}

func (s *PoolHealth) disableSQLComparisonTarget(target *sqlHealthTarget) {
	s.sqlComparisonMu.Lock()
	defer s.sqlComparisonMu.Unlock()
	target.sqlComparisonGeneration++
	s.resetSQLPotentialTargetLocked(target)
	delete(s.sqlComparisonLatest, target)
}

func (s *PoolHealth) retireSQLComparisonTarget(target *sqlHealthTarget) {
	s.sqlComparisonMu.Lock()
	defer s.sqlComparisonMu.Unlock()
	target.sqlComparisonRetired = true
	s.resetSQLPotentialTargetLocked(target)
	delete(s.sqlComparisonLatest, target)
}

func (s *PoolHealth) currentSQLComparisonGeneration(target *sqlHealthTarget) uint64 {
	s.sqlComparisonMu.Lock()
	defer s.sqlComparisonMu.Unlock()
	return target.sqlComparisonGeneration
}

// reserveSQLComparisonPoll validates the lifecycle captured before flag evaluation. Its final
// result distinguishes a stale evaluation from a current tick that finds an active SQL round.
// Disabling and reservation are atomic so an older enabled evaluation cannot adopt a new lifecycle.
func (s *PoolHealth) reserveSQLComparisonPoll(
	target *sqlHealthTarget,
	scheduledAt time.Time,
	expectedGeneration uint64,
	enabled bool,
) (generation uint64, started bool, current bool) {
	s.sqlComparisonMu.Lock()
	defer s.sqlComparisonMu.Unlock()
	if !target.acceptsSQLRoundLocked(expectedGeneration) {
		return 0, false, false
	}
	if !enabled {
		target.sqlComparisonGeneration++
		s.resetSQLPotentialTargetLocked(target)
		delete(s.sqlComparisonLatest, target)
	}
	return target.sqlComparisonGeneration, target.tryStartSQLRound(scheduledAt), true
}

// refreshSQLResolved makes DNS eviction and timing invalidation indivisible to observations.
// The caller holds sqlScheduleMu; DNS lookup happens before taking sqlComparisonMu so a slow
// lookup cannot delay completed SQL or Patroni observations.
func (s *PoolHealth) refreshSQLResolved(trackPotential bool, resolved []string, absenceThreshold int) []string {
	s.sqlComparisonMu.Lock()
	defer s.sqlComparisonMu.Unlock()
	retained := s.sqlHealthState.RefreshResolved(resolved, absenceThreshold, func(ip string) {
		// Eviction removes cached evidence, rather than only hiding it during aggregation.
		// Keep sibling IPs and their original observation timestamps intact.
		for target, snapshot := range s.sqlComparisonLatest {
			snapshot.CandidateIPs = slices.DeleteFunc(snapshot.CandidateIPs, func(candidate string) bool { return candidate == ip })
			snapshot.Results = slices.DeleteFunc(snapshot.Results, func(result SQLHealthCheckResult) bool { return result.IP == ip })
			s.sqlComparisonLatest[target] = snapshot
		}
	})
	if trackPotential && !s.isReadReplica {
		s.potentialTiming.updateRetained(retained, time.Now())
	}
	return retained
}

func (s *PoolHealth) recordSQLComparisonPoll(
	trackPotential bool,
	target *sqlHealthTarget,
	generation uint64,
	scheduledAt time.Time,
	observedAt time.Time,
	candidateIPs []string,
	outcome SQLHealthCheckOutcome,
) *sqlHealthComparisonSnapshot {
	s.sqlComparisonMu.Lock()
	defer s.sqlComparisonMu.Unlock()
	return s.recordSQLComparisonPollLocked(trackPotential, target, generation, scheduledAt, observedAt, candidateIPs, outcome)
}

// recordSQLComparisonPollLocked requires sqlComparisonMu to be held.
func (s *PoolHealth) recordSQLComparisonPollLocked(
	trackPotential bool,
	target *sqlHealthTarget,
	generation uint64,
	scheduledAt time.Time,
	observedAt time.Time,
	candidateIPs []string,
	outcome SQLHealthCheckOutcome,
) *sqlHealthComparisonSnapshot {
	if !target.acceptsSQLRoundLocked(generation) {
		return nil
	}

	retainedIPs := make(map[string]struct{})
	for _, ip := range s.sqlHealthState.Snapshot().Resolved {
		retainedIPs[ip] = struct{}{}
	}

	if s.sqlComparisonLatest == nil {
		s.sqlComparisonLatest = make(map[*sqlHealthTarget]SQLHealthResultsSnapshot)
	}
	// A poll can complete after DNS eviction. Filter owned copies so its callback
	// cannot restore absent IPs or mutate the caller's diagnostic results.
	retainedCandidates := slices.DeleteFunc(slices.Clone(candidateIPs), func(ip string) bool {
		_, retained := retainedIPs[ip]
		return !retained
	})
	retainedResults := slices.DeleteFunc(slices.Clone(outcome.Results), func(result SQLHealthCheckResult) bool {
		_, retained := retainedIPs[result.IP]
		return !retained
	})
	s.sqlComparisonLatest[target] = SQLHealthResultsSnapshot{
		StartedAt:    outcome.StartedAt,
		ScheduledAt:  scheduledAt,
		ObservedAt:   observedAt,
		CandidateIPs: retainedCandidates,
		Tags:         slices.Clone(target.tags),
		Results:      retainedResults,
	}

	snapshot := buildSQLHealthComparisonSnapshot(retainedIPs, s.sqlComparisonLatest)
	if trackPotential && !s.isReadReplica {
		snapshot.potentialComparisons = s.potentialTiming.observeSQLSnapshot(snapshot, observedAt)
	}
	return &snapshot
}

func buildSQLHealthComparisonSnapshot(
	retainedIPs map[string]struct{},
	latest map[*sqlHealthTarget]SQLHealthResultsSnapshot,
) sqlHealthComparisonSnapshot {
	snapshot := sqlHealthComparisonSnapshot{
		retainedIPs: slices.Collect(maps.Keys(retainedIPs)),
		results:     make([]SQLHealthResultsSnapshot, 0, len(latest)),
	}
	slices.Sort(snapshot.retainedIPs)
	var retainedResults []SQLHealthCheckResult
	for _, targetSnapshot := range latest {
		snapshot.results = append(snapshot.results, cloneSQLHealthResultsSnapshot(targetSnapshot))
		for _, result := range targetSnapshot.Results {
			if _, retained := retainedIPs[result.IP]; retained {
				retainedResults = append(retainedResults, result)
			}
		}
	}
	snapshot.health, snapshot.conflictingIPs = aggregateSQLComparisonRoles(retainedResults)
	slices.SortFunc(snapshot.results, func(left, right SQLHealthResultsSnapshot) int {
		if left.ScheduledAt.Before(right.ScheduledAt) {
			return -1
		}
		if left.ScheduledAt.After(right.ScheduledAt) {
			return 1
		}
		if left.ObservedAt.Before(right.ObservedAt) {
			return -1
		}
		if left.ObservedAt.After(right.ObservedAt) {
			return 1
		}
		return 0
	})
	return snapshot
}

// aggregateSQLComparisonRoles applies the same role rules to raw mismatch evidence and
// lifecycle-filtered timing evidence. Unknown roles do not erase a sibling's successful role.
func aggregateSQLComparisonRoles(results []SQLHealthCheckResult) (map[string]bool, []string) {
	health := make(map[string]bool)
	conflicts := make(map[string]struct{})
	for _, result := range filterSupersededSQLRoles(results) {
		if result.Err != nil {
			continue
		}
		role := result.HealthyPrimary()
		if previous, known := health[result.IP]; known && previous != role {
			conflicts[result.IP] = struct{}{}
		}
		health[result.IP] = role
	}
	// Keep conflicts distinct from unknowns and independent of result order.
	var conflictingIPs []string
	for ip := range conflicts {
		delete(health, ip)
		conflictingIPs = append(conflictingIPs, ip)
	}
	slices.Sort(conflictingIPs)
	return health, conflictingIPs
}

func cloneSQLHealthResultsSnapshot(snapshot SQLHealthResultsSnapshot) SQLHealthResultsSnapshot {
	snapshot.CandidateIPs = slices.Clone(snapshot.CandidateIPs)
	snapshot.Tags = slices.Clone(snapshot.Tags)
	snapshot.Results = slices.Clone(snapshot.Results)
	return snapshot
}

// sqlHealthTarget contains the SQL-specific state for one connection string.
type sqlHealthTarget struct {
	poolHealth           *PoolHealth
	connectionStringHash [sha256.Size]byte
	database             string
	tags                 []string
	connStringForIP      func(string) string
	checksEnabled        func(context.Context) SQLHealthFeatureFlagEvaluation
	checker              *SQLHealthChecker
	refcount             int

	// sqlCheckMu serializes all operations that access or mutate checker state: SQL probes,
	// independently discovered probe-pool reconciliation, feature-flag pool release, subscription
	// setup, and shutdown. It also protects sqlClosed. Observation publication must happen after
	// this lock is released so consumers cannot block checker cleanup.
	sqlCheckMu sync.Mutex
	sqlClosed  bool
	// sqlComparisonRetired prevents a check that finishes after its last unsubscribe from restoring
	// the target's raw result in the host-level comparison cache.
	// Both fields are protected by PoolHealth.sqlComparisonMu. Disabling advances the generation
	// so a pre-disable completion is rejected even after re-enable; retirement is permanent.
	sqlComparisonRetired    bool
	sqlComparisonGeneration uint64

	// sqlRoundMu protects the active round separately from sqlCheckMu so the scheduler can decide
	// whether a missed tick represents unavailability while a SQL probe is still running.
	sqlRoundMu sync.Mutex
	sqlRound   sqlHealthRoundState

	subscriberMu   sync.RWMutex
	sqlSubscribers map[*sqlHealthUpdates]struct{}
}

// acceptsSQLRoundLocked requires the owning PoolHealth.sqlComparisonMu to be held.
func (t *sqlHealthTarget) acceptsSQLRoundLocked(generation uint64) bool {
	return !t.sqlComparisonRetired && generation == t.sqlComparisonGeneration
}

func (t *sqlHealthTarget) tryStartSQLRound(scheduledAt time.Time) bool {
	t.sqlRoundMu.Lock()
	defer t.sqlRoundMu.Unlock()
	if t.sqlRound.running {
		return false
	}
	t.sqlRound = sqlHealthRoundState{scheduledAt: scheduledAt, running: true}
	return true
}

func (t *sqlHealthTarget) finishSQLRound(scheduledAt time.Time) {
	t.sqlRoundMu.Lock()
	defer t.sqlRoundMu.Unlock()

	if !t.sqlRound.running || !t.sqlRound.scheduledAt.Equal(scheduledAt) {
		return
	}
	t.sqlRound = sqlHealthRoundState{}
}

func (t *sqlHealthTarget) markSQLRoundWritable(scheduledAt time.Time) bool {
	t.sqlRoundMu.Lock()
	defer t.sqlRoundMu.Unlock()

	if !t.sqlRound.running ||
		!t.sqlRound.scheduledAt.Equal(scheduledAt) ||
		t.sqlRound.writablePublished {
		return false
	}
	t.sqlRound.writablePublished = true
	return true
}

func (t *sqlHealthTarget) activeSQLRoundPublishedWritable() bool {
	t.sqlRoundMu.Lock()
	defer t.sqlRoundMu.Unlock()
	return t.sqlRound.running && t.sqlRound.writablePublished
}

func (t *sqlHealthTarget) sqlRoundRunning() bool {
	t.sqlRoundMu.Lock()
	defer t.sqlRoundMu.Unlock()
	return t.sqlRound.running
}

type sqlHealthUpdates struct {
	source    *sqlHealthTarget
	updates   chan SQLHealthObservation
	done      chan struct{}
	once      sync.Once
	publishMu sync.Mutex
}

func sqlConnectionStringHash(host string, sql *SQLHealthConfig) [sha256.Size]byte {
	return sha256.Sum256([]byte(sql.ConnStringForIP(host)))
}

func (w *HealthWorker) Subscribe(host string, isReadReplica bool, dbName string, tags []string, sql *SQLHealthConfig) HealthUpdates {
	updates := make(chan *PoolHealth, 1)
	subscription := HealthUpdates(updates)
	sqlEnabled := sql != nil && sql.ConnStringForIP != nil && !isReadReplica && w.newSQLChecker != nil
	routingEvaluation := SQLHealthFeatureFlagEvaluation{Valid: true}
	if sqlEnabled && sql.RoutingEnabled != nil {
		routingEvaluation = sql.RoutingEnabled(context.Background())
	}

	w.mu.Lock()
	poolHealth, existed := w.hosts[host]
	if !existed {
		healthTags := slices.Clone(tags)
		if hostTag := "postgres_host:" + host; !slices.Contains(healthTags, hostTag) {
			healthTags = append(healthTags, hostTag)
		}
		poolHealth = &PoolHealth{
			host:                host,
			isReadReplica:       isReadReplica,
			dbName:              dbName,
			tags:                healthTags,
			patroniTags:         patroniHealthTags(host, isReadReplica),
			state:               NewHealthState(),
			patroniHealthState:  NewHealthState(),
			sqlHealthState:      NewHealthState(),
			subscribers:         make(map[HealthUpdates]chan *PoolHealth),
			ready:               make(chan struct{}),
			subscriptionTargets: make(map[HealthUpdates]*sqlHealthTarget),
		}
		w.hosts[host] = poolHealth
	}
	var target *sqlHealthTarget
	if sqlEnabled {
		connectionStringHash := sqlConnectionStringHash(host, sql)
		poolHealth.sqlMu.Lock()
		poolHealth.routingMu.Lock()
		if poolHealth.hybridPolicy == nil {
			enabled := routingEvaluation.Valid && routingEvaluation.Enabled && !poolHealth.sqlRoutingDisabled
			poolHealth.hybridPolicy = newHybridHealthPolicy(enabled)
			poolHealth.routingEnabledFn = sql.RoutingEnabled
			poolHealth.sqlRoutingDisabled = poolHealth.sqlRoutingDisabled || (routingEvaluation.Valid && !routingEvaluation.Enabled)
			poolHealth.routingChangedAt = time.Now()
			poolHealth.hybridPolicy.observePatroni(poolHealth.patroniHealthState.Snapshot().Health, time.Now())
		}
		poolHealth.routingMu.Unlock()
		for _, existing := range poolHealth.subscriptionTargets {
			if existing.connectionStringHash == connectionStringHash {
				target = existing
				break
			}
		}
		if target == nil {
			targetTags := slices.Clone(tags)
			if hostTag := "postgres_host:" + host; !slices.Contains(targetTags, hostTag) {
				targetTags = append(targetTags, hostTag)
			}
			target = &sqlHealthTarget{
				poolHealth:           poolHealth,
				connectionStringHash: connectionStringHash,
				database:             dbName,
				tags:                 targetTags,
				connStringForIP:      sql.ConnStringForIP,
				checksEnabled:        sql.ChecksEnabled,
				checker:              w.newSQLChecker(context.Background()),
				sqlSubscribers:       make(map[*sqlHealthUpdates]struct{}),
			}
		}
		target.refcount++
		poolHealth.sqlMu.Unlock()
	}
	poolHealth.mu.Lock()
	poolHealth.refcount++
	poolHealth.subscribers[subscription] = updates
	subscriptionCount := poolHealth.refcount
	poolHealth.mu.Unlock()
	if target != nil {
		poolHealth.sqlMu.Lock()
		poolHealth.subscriptionTargets[subscription] = target
		poolHealth.sqlMu.Unlock()
	}
	w.subscriptions[subscription] = &healthSubscription{poolHealth: poolHealth, sqlTarget: target}
	w.mu.Unlock()

	if w.hooks.Subscriptions != nil {
		w.hooks.Subscriptions(subscriptionCount, tags)
	}

	if !existed {
		w.checkPoolHealth(poolHealth)
		close(poolHealth.ready)
		return subscription
	}

	<-poolHealth.ready
	poolHealth.mu.Lock()
	select {
	case updates <- poolHealth:
	default:
	}
	poolHealth.mu.Unlock()
	return subscription
}

func (w *HealthWorker) Unsubscribe(subscription HealthUpdates) {
	if subscription == nil {
		return
	}

	w.mu.Lock()
	record, ok := w.subscriptions[subscription]
	if !ok {
		w.mu.Unlock()
		return
	}
	delete(w.subscriptions, subscription)
	poolHealth := record.poolHealth
	poolHealth.mu.Lock()
	updates := poolHealth.subscribers[subscription]
	delete(poolHealth.subscribers, subscription)
	poolHealth.refcount--
	if poolHealth.refcount <= 0 {
		if current, ok := w.hosts[poolHealth.host]; ok && current == poolHealth {
			delete(w.hosts, poolHealth.host)
		}
	}
	subscriptionCount := poolHealth.refcount
	poolHealth.mu.Unlock()
	close(updates)
	w.mu.Unlock()
	if record.sqlTarget != nil {
		poolHealth.removeSQLTarget(record.sqlTarget, subscription)
	}
	if w.hooks.Subscriptions != nil {
		w.hooks.Subscriptions(subscriptionCount, poolHealth.tags)
	}
}

// Run executes the Patroni and centralized SQL health loops. Both operate on the same PoolHealth,
// while Patroni and SQL retain independent address state on their respective cadences.
func (w *HealthWorker) Run() {
	if w.sqlInterval > 0 {
		go w.runCentralizedSQLChecks()
	}
	w.runPatroniChecks()
}

func (w *HealthWorker) runPatroniChecks() {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for range ticker.C {
		w.Refresh()
	}
}

func (w *HealthWorker) runCentralizedSQLChecks() {
	ticker := time.NewTicker(w.sqlInterval)
	defer ticker.Stop()
	for tickAt := range ticker.C {
		w.scheduleSQLRefresh(tickAt)
	}
}

func (s *PoolHealth) trackNewIPs(ips []string) []string {
	return s.patroniHealthState.Track(ips)
}

// publishRoutingSnapshot requires routingMu. Discovery state is never replaced by publication.
func (s *PoolHealth) publishRoutingSnapshot(health map[string]bool, resolved []string) {
	s.state.ReplaceRoutingSnapshot(health, resolved)
	s.notifySubscribers()
}

func (s *PoolHealth) routingPolicy() (*hybridHealthPolicy, func(context.Context) SQLHealthFeatureFlagEvaluation) {
	s.routingMu.Lock()
	defer s.routingMu.Unlock()
	return s.hybridPolicy, s.routingEnabledFn
}

// syncSQLRoutingKillSwitch calls external flag evaluation outside all state locks. A valid disable
// wins over concurrent/stale enables. An evaluation error cannot change the last accepted mode.
func (w *HealthWorker) syncSQLRoutingKillSwitch(s *PoolHealth) {
	_, evaluate := s.routingPolicy()
	if evaluate == nil {
		return
	}
	evaluation := evaluate(context.Background())
	s.routingMu.Lock()
	defer s.routingMu.Unlock()
	if s.hybridPolicy == nil || !evaluation.Valid {
		return
	}
	if !evaluation.Enabled {
		s.disableSQLRoutingLocked()
		return
	}
	if !s.sqlRoutingDisabled && !s.hybridPolicy.isRoutingEnabled() {
		s.hybridPolicy.setRoutingEnabled(true)
		s.routingChangedAt = time.Now()
	}
}

// disableSQLRoutingLocked requires routingMu; it must not wait for an in-flight SQL probe.
func (s *PoolHealth) disableSQLRoutingLocked() {
	routingWasEnabled := s.hybridPolicy != nil && s.hybridPolicy.isRoutingEnabled()
	s.sqlRoutingDisabled = true
	if routingWasEnabled {
		s.hybridPolicy.setRoutingEnabled(false)
		s.routingChangedAt = time.Now()
	}
	if s.sqlRoutingActive || routingWasEnabled {
		s.sqlRoutingActive = false
		snapshot := s.patroniHealthState.Snapshot()
		s.publishRoutingSnapshot(snapshot.Health, snapshot.Resolved)
	}
}

// applyPatroniHealth advances Patroni independently, even while SQL owns client routing. Cache
// updates and publication share routingMu with SQL fallback selection, preventing stale fallback
// copies from overwriting a newer Patroni observation.
func (w *HealthWorker) applyPatroniHealth(s *PoolHealth, health map[string]bool, systemIDs map[string]string, resolved []string, observedAt time.Time) {
	w.syncSQLRoutingKillSwitch(s)
	var evicted []string
	var decision hybridRoutingDecision
	s.routingMu.Lock()
	s.patroniHealthState.Update(health, systemIDs, resolved, w.absenceThreshold, func(ip string) { evicted = append(evicted, ip) })
	patroniHealth := s.patroniHealthState.Snapshot()
	policy := s.hybridPolicy
	routingHealth := patroniHealth.Health
	if policy != nil {
		decision = policy.observePatroni(patroniHealth.Health, observedAt)
		routingHealth = decision.health
	}
	// A rejected stale observation has nil health and must not clear the published route.
	if (policy == nil || decision.health != nil) && (!s.sqlRoutingActive || decision.publish) {
		s.publishRoutingSnapshot(routingHealth, patroniHealth.Resolved)
		if policy != nil && decision.routingResult != SQLRoutingResultPatroniRecovery {
			if comparison := policy.observePublishedPatroniRouting(routingHealth, observedAt); comparison != nil {
				decision.routingComparison = comparison
			}
		}
	}
	s.routingMu.Unlock()
	for _, ip := range evicted {
		if w.hooks.Evicted != nil {
			w.hooks.Evicted(ip, s.tags)
		}
	}
	if policy != nil {
		w.reportHybridDecision(s, decision, false)
	}
}

func (s *PoolHealth) notifySubscribers() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, updates := range s.subscribers {
		select {
		case updates <- s:
		default:
		}
	}
}

func (w *HealthWorker) checkPoolHealth(poolHealth *PoolHealth) {
	start := time.Now()
	if w.hooks.CheckStarted != nil {
		w.hooks.CheckStarted(poolHealth.patroniTags)
	}
	ips := w.prober.Lookup(poolHealth.host, poolHealth.patroniTags)
	tracked := poolHealth.trackNewIPs(ips)

	pghealth := make(map[string]bool, len(tracked))
	systemIDs := make(map[string]string, len(tracked))
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, ip := range tracked {
		wg.Add(1)
		go func(ip string) {
			defer wg.Done()
			ok, systemID, err := w.prober.Probe(
				ip,
				poolHealth.host,
				poolHealth.dbName,
				poolHealth.isReadReplica,
				poolHealth.patroniTags,
			)
			mu.Lock()
			pghealth[ip] = err == nil && ok
			systemIDs[ip] = systemID
			mu.Unlock()
		}(ip)
	}
	wg.Wait()
	observedAt := time.Now()

	if w.hooks.CheckFinished != nil {
		w.hooks.CheckFinished(time.Since(start), poolHealth.patroniTags)
	}
	w.applyPatroniHealth(poolHealth, pghealth, systemIDs, ips, observedAt)
	w.observePatroniPotential(poolHealth, pghealth, observedAt)
}

func patroniHealthTags(host string, isReadReplica bool) []string {
	role := "primary"
	if isReadReplica {
		role = "replica"
	}
	return []string{
		"postgres_host:" + host,
		"pool_role:" + role,
	}
}

// Refresh checks all subscribed hosts in parallel and waits for their snapshots.
// Call it serially; Run already does this on each tick.
func (w *HealthWorker) Refresh() {
	w.mu.Lock()
	allPoolHealths := make([]*PoolHealth, 0, len(w.hosts))
	for _, s := range w.hosts {
		allPoolHealths = append(allPoolHealths, s)
	}
	w.mu.Unlock()
	var wg sync.WaitGroup
	for _, poolHealth := range allPoolHealths {
		wg.Add(1)
		go func(poolHealth *PoolHealth) {
			defer wg.Done()
			w.checkPoolHealth(poolHealth)
		}(poolHealth)
	}
	wg.Wait()
}

type sqlHealthGroup struct {
	poolHealth *PoolHealth
	targets    []*sqlHealthTarget
}

func (w *HealthWorker) sqlHealthGroups() []sqlHealthGroup {
	w.mu.Lock()
	groups := make([]sqlHealthGroup, 0, len(w.hosts))
	for _, poolHealth := range w.hosts {
		select {
		case <-poolHealth.ready:
			targets := make([]*sqlHealthTarget, 0)
			targetsForPool := poolHealth.sqlTargetValues()
			for _, target := range targetsForPool {
				if target.checker != nil {
					targets = append(targets, target)
				}
			}
			if len(targets) > 0 {
				groups = append(groups, sqlHealthGroup{poolHealth: poolHealth, targets: targets})
			}
		default:
			// The initial Patroni discovery and probe have not completed yet.
		}
	}
	w.mu.Unlock()
	return groups
}

// scheduleSQLRefresh starts every idle credential target without waiting. Each target reports a
// comparison and evaluates the hybrid snapshot after all of its IP probes finish.
func (w *HealthWorker) scheduleSQLRefresh(scheduledAt time.Time) {
	for _, group := range w.sqlHealthGroups() {
		go w.runSQLHealthGroup(group, scheduledAt, false)
	}
}

// RefreshSQL resolves each subscribed host once, checks its SQL targets in parallel, and waits for
// the checks it starts.
// It is intended for explicit refreshes and tests; Run uses scheduleSQLRefresh so a slow target
// cannot delay the next interval for unrelated targets.
func (w *HealthWorker) RefreshSQL() {
	scheduledAt := time.Now()
	var wg sync.WaitGroup
	for _, group := range w.sqlHealthGroups() {
		wg.Add(1)
		go func(group sqlHealthGroup) {
			defer wg.Done()
			w.runSQLHealthGroup(group, scheduledAt, true)
		}(group)
	}
	wg.Wait()
}

type sqlHealthCheckPlan struct {
	target     *sqlHealthTarget
	enabled    bool
	generation uint64
}

type completedSQLHealthCheck struct {
	target  *sqlHealthTarget
	outcome SQLHealthCheckOutcome
}

func (w *HealthWorker) checkSQLHealthGroup(group sqlHealthGroup, scheduledAt time.Time) {
	w.runSQLHealthGroup(group, scheduledAt, true)
}

func (w *HealthWorker) runSQLHealthGroup(
	group sqlHealthGroup,
	scheduledAt time.Time,
	waitForStartedChecks bool,
) {
	ctx := context.Background()
	w.syncSQLRoutingKillSwitch(group.poolHealth)

	// Reserve each idle credential target before DNS discovery. A later tick can therefore report a
	// missed or disabled interval immediately instead of queuing behind a slow lookup.
	startedPlans := make([]sqlHealthCheckPlan, 0, len(group.targets))
	checksEnabled := false
	for _, target := range group.targets {
		generation := group.poolHealth.currentSQLComparisonGeneration(target)
		evaluation := SQLHealthFeatureFlagEvaluation{Enabled: true, Valid: true}
		if target.checksEnabled != nil {
			evaluation = target.checksEnabled(ctx)
		}
		enabled := evaluation.Enabled
		generation, started, current := group.poolHealth.reserveSQLComparisonPoll(target, scheduledAt, generation, enabled)
		if !current {
			continue
		}
		if evaluation.Valid && !enabled {
			group.poolHealth.routingMu.Lock()
			group.poolHealth.disableSQLRoutingLocked()
			group.poolHealth.routingMu.Unlock()
		}
		plan := sqlHealthCheckPlan{target: target, enabled: enabled, generation: generation}
		if !started {
			if plan.enabled {
				w.recordSQLCheckMissed(plan.target)
				// Legacy availability consumers use missed intervals to open downtime windows.
				// This publication never enters the credential comparison snapshots.
				publishMissedSQLAvailability(plan.target, scheduledAt)
			} else {
				// Advance consumers' disable cutoff even though this credential's older round is
				// still running. Its eventual completion is rejected by observation ordering.
				plan.target.publishSQLHealth(SQLHealthObservation{
					ScheduledAt: scheduledAt,
					ObservedAt:  scheduledAt,
					Disabled:    true,
				})
			}
			continue
		}
		if plan.enabled {
			checksEnabled = true
		}
		startedPlans = append(startedPlans, plan)
	}
	if len(startedPlans) == 0 {
		return
	}

	var toProbe []string
	resolvedSet := make(map[string]struct{})
	if checksEnabled {
		group.poolHealth.sqlScheduleMu.Lock()
		resolved := w.prober.Lookup(group.poolHealth.host, group.poolHealth.patroniTags)
		toProbe = group.poolHealth.refreshSQLResolved(w.hooks.SQLPotentialCompared != nil, resolved, w.absenceThreshold)
		// Successful probes of retained IPs must gain protection even when DNS omits them.
		for _, ip := range toProbe {
			resolvedSet[ip] = struct{}{}
		}
		group.poolHealth.sqlScheduleMu.Unlock()
	}

	var wg sync.WaitGroup
	for _, plan := range startedPlans {
		wg.Add(1)
		go func(plan sqlHealthCheckPlan) {
			defer wg.Done()
			defer plan.target.finishSQLRound(scheduledAt)
			outcome, completed := w.checkSQLHealth(plan.target, toProbe, plan.enabled, scheduledAt)
			if plan.enabled && completed {
				w.completeSQLHealthCheck(plan.target, plan.generation, scheduledAt, toProbe, resolvedSet, outcome)
			}
		}(plan)
	}
	if !waitForStartedChecks {
		return
	}
	wg.Wait()
}

// completeSQLHealthCheck applies a credential's complete multi-IP poll. Other credentials may
// still be running; their last complete snapshots remain available for host-level evaluation.
func (w *HealthWorker) completeSQLHealthCheck(
	target *sqlHealthTarget,
	generation uint64,
	scheduledAt time.Time,
	candidateIPs []string,
	resolvedSet map[string]struct{},
	outcome SQLHealthCheckOutcome,
) {
	// Preserve probe completion time even when routing-flag evaluation blocks.
	observedAt := time.Now()
	poolHealth := target.poolHealth
	// External flag evaluation must not run under the snapshot/invalidation lock.
	w.syncSQLRoutingKillSwitch(poolHealth)
	var snapshot *sqlHealthComparisonSnapshot
	var decision hybridRoutingDecision
	applied := func() bool {
		poolHealth.sqlComparisonMu.Lock()
		defer poolHealth.sqlComparisonMu.Unlock()
		if !target.acceptsSQLRoundLocked(generation) {
			return false
		}
		// Preserve independent SQL retention updates, but reject late disabled/retired polls
		// before they can mutate retention or hybrid state.
		aggregation := aggregateSQLHealthCheckResults(outcome.Results)
		retainedHealth := make(map[string]bool, len(aggregation.Health))
		for ip, healthy := range aggregation.Health {
			if _, resolved := resolvedSet[ip]; resolved {
				retainedHealth[ip] = healthy
			}
		}
		poolHealth.sqlHealthState.RecordHealthAt(scheduledAt, retainedHealth)
		// A delayed success can restore an IP evicted by DNS while the probe was running.
		// Capture membership and roles only after that retention update.
		snapshot = poolHealth.recordSQLComparisonPollLocked(w.hooks.SQLPotentialCompared != nil, target, generation, scheduledAt, observedAt, candidateIPs, outcome)
		if len(snapshot.retainedIPs) == 0 {
			poolHealth.routingMu.Lock()
			if poolHealth.sqlRoutingActive {
				poolHealth.sqlRoutingActive = false
				patroniHealth := poolHealth.patroniHealthState.Snapshot()
				poolHealth.publishRoutingSnapshot(patroniHealth.Health, patroniHealth.Resolved)
				if policy := poolHealth.hybridPolicy; policy != nil {
					decision.routingEnabled = policy.isRoutingEnabled()
					decision.routingComparison = policy.observePublishedPatroniRouting(patroniHealth.Health, time.Now())
				}
			}
			poolHealth.routingMu.Unlock()
			return false
		}
		// DNS can discover another IP while this credential is running. An older poll
		// must not evaluate a partial IP set using cached data to fill that new address.
		completedIPs := make(map[string]bool, len(outcome.Results))
		for _, result := range outcome.Results {
			completedIPs[result.IP] = true
		}
		for _, ip := range snapshot.retainedIPs {
			if !completedIPs[ip] {
				return false
			}
		}
		outcomes := make([]SQLHealthCheckOutcome, 0, len(snapshot.results))
		for _, credential := range snapshot.results {
			var results []SQLHealthCheckResult
			for _, result := range credential.Results {
				if slices.Contains(snapshot.retainedIPs, result.IP) {
					results = append(results, result)
				}
			}
			if len(results) != 0 {
				outcomes = append(outcomes, SQLHealthCheckOutcome{StartedAt: credential.StartedAt, Results: results})
			}
		}
		poolHealth.routingMu.Lock()
		defer poolHealth.routingMu.Unlock()
		policy := poolHealth.hybridPolicy
		if policy == nil || scheduledAt.Before(poolHealth.routingChangedAt) {
			return false // A poll spanning a mode transition cannot publish under the new mode.
		}
		decision = policy.observeSQL(snapshot.retainedIPs, aggregateSQLHealthCheckOutcomes(outcomes).Outcome, time.Now())
		if decision.publish {
			poolHealth.sqlRoutingActive = decision.routingResult == SQLRoutingResultConclusive || decision.routingResult == SQLRoutingResultRetainedSQL
			resolved := snapshot.retainedIPs
			if !poolHealth.sqlRoutingActive {
				resolved = poolHealth.patroniHealthState.Snapshot().Resolved
			}
			poolHealth.publishRoutingSnapshot(decision.health, resolved)
		}
		return true
	}()
	w.reportSQLPatroniComparison(poolHealth, snapshot)
	if snapshot != nil {
		w.reportSQLPotential(poolHealth, snapshot.potentialComparisons)
	}
	if applied {
		// Cached errors remain evidence for the decision, but are not new probe events.
		w.reportConclusiveErrors([]completedSQLHealthCheck{{target: target, outcome: outcome}}, decision)
	}
	// Empty discovery can publish a Patroni route without evaluating a SQL iteration.
	if applied || decision.routingComparison != nil {
		w.reportHybridDecision(poolHealth, decision, applied)
	}
}

func (w *HealthWorker) reportSQLPatroniComparison(
	poolHealth *PoolHealth,
	snapshot *sqlHealthComparisonSnapshot,
) {
	if snapshot == nil || w.hooks.SQLPatroniCompared == nil {
		return
	}
	comparison := SQLPatroniHealthSnapshot{
		SQLHealth:         maps.Clone(snapshot.health),
		SQLConflictingIPs: slices.Clone(snapshot.conflictingIPs),
		SQLRetainedIPs:    slices.Clone(snapshot.retainedIPs),
		SQLResults:        slices.Clone(snapshot.results),
		PatroniHealth:     poolHealth.patroniHealthState.Snapshot().Health,
	}
	w.hooks.SQLPatroniCompared(comparison, poolHealth.patroniTags)
}

func (w *HealthWorker) sqlRoutingTags(tags []string, routingEnabled bool) []string {
	return append(
		slices.Clone(tags),
		"sql_health_routing_enabled:"+strconv.FormatBool(routingEnabled),
	)
}

func (w *HealthWorker) reportConclusiveErrors(
	checks []completedSQLHealthCheck,
	decision hybridRoutingDecision,
) {
	if decision.routingResult != SQLRoutingResultConclusive || w.sqlHooks.ConclusiveError == nil {
		return
	}
	for _, check := range checks {
		for _, result := range check.outcome.Results {
			if !isConclusiveSQLUnroutableError(result.Err) {
				continue
			}
			w.sqlHooks.ConclusiveError(result, w.sqlRoutingTags(check.target.tags, decision.routingEnabled))
		}
	}
}

func (w *HealthWorker) reportHybridDecision(
	poolHealth *PoolHealth,
	decision hybridRoutingDecision,
	reportIteration bool,
) {
	tags := w.sqlRoutingTags(poolHealth.patroniTags, decision.routingEnabled)
	if reportIteration && w.sqlHooks.Iteration != nil {
		w.sqlHooks.Iteration(SQLIteration{
			RoutingResult:  decision.routingResult,
			FallbackReason: decision.fallbackReason,
			TimeoutType:    decision.timeoutType,
		}, tags)
	}
	if decision.routingResult == SQLRoutingResultCachedFallback && w.sqlHooks.CachedFallback != nil {
		w.sqlHooks.CachedFallback(SQLFallback{
			Reason:           decision.fallbackReason,
			TimeoutType:      decision.timeoutType,
			CachedPatroniAge: decision.cachedAge,
			HasCachedPatroni: decision.hasCachedAge,
		}, tags)
	}
	if decision.routingComparison != nil && w.sqlHooks.LeaderRoutingCompared != nil {
		w.sqlHooks.LeaderRoutingCompared(*decision.routingComparison, tags)
	}
}

func (w *HealthWorker) recordSQLCheckMissed(target *sqlHealthTarget) {
	// The active round already produced this interval's availability observation. It is still
	// finishing its remaining probes, but neither an outcome:missed metric nor an unavailable
	// observation should overwrite the early writable result.
	if target.activeSQLRoundPublishedWritable() {
		return
	}
	if w.hooks.SQLCheckMissed != nil {
		w.hooks.SQLCheckMissed(target.tags)
	}
}

// publishMissedSQLAvailability preserves the existing availability/downtime contract. Comparison
// only consumes completed polls; a missed tick neither replaces results nor invalidates a poll.
func publishMissedSQLAvailability(target *sqlHealthTarget, scheduledAt time.Time) {
	if target.activeSQLRoundPublishedWritable() {
		return
	}
	target.publishSQLHealth(SQLHealthObservation{
		ScheduledAt: scheduledAt,
		ObservedAt:  scheduledAt,
		Err:         ErrSQLHealthCheckMissed,
	})
}

func (w *HealthWorker) checkSQLHealth(
	target *sqlHealthTarget,
	toProbe []string,
	checksEnabled bool,
	scheduledAt time.Time,
) (SQLHealthCheckOutcome, bool) {
	poolHealth := target.poolHealth
	ctx := context.Background()
	var writablePublished atomic.Bool
	var outcome SQLHealthCheckOutcome
	observation, ok := func() (SQLHealthObservation, bool) {
		target.sqlCheckMu.Lock()
		defer target.sqlCheckMu.Unlock()
		if target.sqlClosed || target.checker == nil || target.connStringForIP == nil {
			return SQLHealthObservation{}, false
		}
		if !checksEnabled {
			target.checker.ReleasePools()
			return SQLHealthObservation{
				ScheduledAt: scheduledAt,
				ObservedAt:  time.Now(),
				Disabled:    true,
			}, true
		}

		patroniHealth := poolHealth.patroniHealthState.Snapshot().Health
		outcome = target.checker.Check(
			ctx,
			toProbe,
			target.connStringForIP,
			patroniHealth,
			target.tags,
			func(result SQLHealthCheckResult) {
				// Availability is optimistic: publish as soon as any IP is confirmed writable.
				// The full round still detects and reports multiple primaries separately.
				if !result.HealthyPrimary() ||
					!target.markSQLRoundWritable(scheduledAt) {
					return
				}
				writablePublished.Store(true)
				target.publishSQLHealth(SQLHealthObservation{
					ScheduledAt: scheduledAt,
					ObservedAt:  time.Now(),
					Writable:    true,
				})
			},
		)
		// Reconcile dedicated pools against the current SQL discovery snapshot. Patroni refresh never
		// waits for a slow SQL check to release sqlCheckMu.
		target.checker.RetainIPs(toProbe)
		if writablePublished.Load() {
			return SQLHealthObservation{}, false
		}
		return newSQLHealthObservation(scheduledAt, time.Now(), outcome), true
	}()
	if ok {
		target.publishSQLHealth(observation)
	}
	return outcome, checksEnabled && (ok || writablePublished.Load())
}

func (s *PoolHealth) removeSQLTarget(target *sqlHealthTarget, subscription HealthUpdates) {
	s.sqlMu.Lock()
	delete(s.subscriptionTargets, subscription)
	if target.refcount > 0 {
		target.refcount--
	}
	retired := target.refcount == 0
	if retired {
		// Serialize retirement with aggregation/publication before restoring Patroni. A late poll
		// must not republish SQL after the last subscription has gone away.
		s.sqlComparisonMu.Lock()
		target.sqlComparisonRetired = true
		s.resetSQLPotentialTargetLocked(target)
		delete(s.sqlComparisonLatest, target)
		if len(s.subscriptionTargets) == 0 {
			s.routingMu.Lock()
			s.disableSQLRoutingLocked()
			s.routingMu.Unlock()
		}
		s.sqlComparisonMu.Unlock()
	}
	s.sqlMu.Unlock()
	if retired {
		target.closeSQL()
	}
}

func (s *PoolHealth) closeSQL() {
	targets := s.sqlTargetValues()
	for _, target := range targets {
		target.closeSQL()
	}
}

// sqlTargetValues returns each SQL target once, even when multiple pool subscriptions share it.
func (s *PoolHealth) sqlTargetValues() []*sqlHealthTarget {
	s.sqlMu.RLock()
	defer s.sqlMu.RUnlock()
	seen := make(map[*sqlHealthTarget]struct{})
	targets := make([]*sqlHealthTarget, 0, len(s.subscriptionTargets))
	for _, target := range s.subscriptionTargets {
		if _, ok := seen[target]; ok {
			continue
		}
		seen[target] = struct{}{}
		targets = append(targets, target)
	}
	return targets
}

// SubscribeSQLHealth subscribes to SQL observations for the connection string associated with
// the health snapshot subscription.
func (s *PoolHealth) SubscribeSQLHealth(healthUpdates ...HealthUpdates) (SQLHealthUpdates, error) {
	if s == nil {
		return nil, ErrSQLHealthUpdatesUnsupported
	}
	var target *sqlHealthTarget
	s.sqlMu.RLock()
	if len(healthUpdates) > 0 {
		target = s.subscriptionTargets[healthUpdates[0]]
	}
	s.sqlMu.RUnlock()
	if len(healthUpdates) == 0 {
		if targets := s.sqlTargetValues(); len(targets) == 1 {
			target = targets[0]
		}
	}
	if target == nil {
		return nil, ErrSQLHealthUpdatesUnsupported
	}
	target.sqlCheckMu.Lock()
	defer target.sqlCheckMu.Unlock()
	if target.sqlClosed || target.checker == nil {
		return nil, ErrSQLHealthUpdatesUnsupported
	}
	subscription := &sqlHealthUpdates{
		source:  target,
		updates: make(chan SQLHealthObservation, 1),
		done:    make(chan struct{}),
	}
	target.subscriberMu.Lock()
	target.sqlSubscribers[subscription] = struct{}{}
	target.subscriberMu.Unlock()
	return subscription, nil
}

func (s *sqlHealthUpdates) Updates() <-chan SQLHealthObservation { return s.updates }
func (s *sqlHealthUpdates) Done() <-chan struct{}                { return s.done }

func (s *sqlHealthUpdates) Close() {
	if s == nil {
		return
	}
	s.once.Do(func() {
		s.publishMu.Lock()
		defer s.publishMu.Unlock()

		close(s.done)
		if s.source != nil {
			s.source.subscriberMu.Lock()
			delete(s.source.sqlSubscribers, s)
			s.source.subscriberMu.Unlock()
		}
	})
}

func (s *sqlHealthUpdates) publish(observation SQLHealthObservation) {
	s.publishMu.Lock()
	defer s.publishMu.Unlock()

	// Stop immediately when the subscription has been closed. The default keeps this check
	// nonblocking while the subscription is still active.
	select {
	case <-s.done:
		return
	default:
	}

	// Use the fast path when the one-element buffer has room. If it is full, continue below and
	// coalesce its stale observation instead of blocking the SQL refresh.
	select {
	case s.updates <- observation:
		return
	default:
	}

	// Remove the buffered observation so it can be replaced. Preserve it when its scheduled
	// interval is newer, even if an older check completed later in wall-clock time.
	select {
	case buffered := <-s.updates:
		if sqlHealthObservationScheduledAt(buffered).After(sqlHealthObservationScheduledAt(observation)) {
			observation = buffered
		}
	default:
	}
	// Put the selected newest observation back without blocking. The default is a defensive
	// fallback that keeps publication best-effort if the channel cannot accept the value.
	select {
	case s.updates <- observation:
	default:
	}
}

func (s *sqlHealthTarget) closeSQL() {
	s.sqlCheckMu.Lock()
	if s.sqlClosed {
		s.sqlCheckMu.Unlock()
		return
	}
	s.sqlClosed = true
	subscribers := s.sqlSubscriberValues()
	for _, subscriber := range subscribers {
		subscriber.Close()
	}
	if s.checker != nil {
		s.checker.Close()
	}
	s.sqlCheckMu.Unlock()
}

func (s *sqlHealthTarget) publishSQLHealth(observation SQLHealthObservation) {
	subscribers := s.sqlSubscriberValues()
	for _, subscriber := range subscribers {
		subscriber.publish(observation)
	}
}

func (s *sqlHealthTarget) sqlSubscriberValues() []*sqlHealthUpdates {
	s.subscriberMu.RLock()
	defer s.subscriberMu.RUnlock()
	subscribers := make([]*sqlHealthUpdates, 0, len(s.sqlSubscribers))
	for subscriber := range s.sqlSubscribers {
		subscribers = append(subscribers, subscriber)
	}
	return subscribers
}

// publishSQLHealth is retained for tests and callers that construct a single-target PoolHealth.
func (s *PoolHealth) publishSQLHealth(observation SQLHealthObservation) {
	targets := s.sqlTargetValues()
	for _, target := range targets {
		target.publishSQLHealth(observation)
	}
}

// Snapshot returns copies of the current routing health and retained IPs for the host.
func (s *PoolHealth) Snapshot() HealthStateSnapshot {
	return s.state.Snapshot()
}

// IsHealthy returns whether ip passed its most recent probe. Unknown
// IPs report false.
func (s *PoolHealth) IsHealthy(ip string) bool {
	return s.state.IsHealthy(ip)
}
