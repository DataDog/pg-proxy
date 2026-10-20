package limiter

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	pgproxy "github.com/DataDog/pg-proxy"
)

// Memory-derived global session limit:
// max(floor, (memory hard limit × safety fraction) / bytes per session).
const (
	defaultGaugeInterval = 2 * time.Second

	// BytesPerSession is the marginal per-session memory cost — allocations that scale with
	// the number of open sessions: TLS session state, pgproto3 read/write buffers, the
	// per-connection goroutine stack, and per-connection kernel socket buffers. It is anchored
	// on Δmemory/Δconnections analysis across pods (~50 KiB measured) and set above the measured
	// value to leave room for heavier client workloads.
	BytesPerSession = 60 * 1024 // 60 KiB

	// MemorySafetyFraction accounts for process-wide overhead that does not scale per session:
	// Go runtime/heap fragmentation and GC slack, baseline non-session allocations, and shared
	// mappings. It is multiplicative so larger pods carry proportional slack.
	MemorySafetyFraction = 0.8
)

// ServiceQuota is a per-service session cap. Name is the key RegisterSession
// will look up (already normalized by the embedder).
type ServiceQuota struct {
	Name string
	Max  int32
}

// Memory is an optional memory budget used to derive a global session cap.
// When Available is false, Params.MemoryFallback is used instead.
type Memory struct {
	Bytes     int64
	Available bool
}

// Params constructs a Limiter. GlobalLimit is the configured cap.
// DefaultServiceMax is applied to services that were not pre-registered.
// MemoryFloor is the minimum derived cap; MemoryFallback is used when Memory
// is unavailable.
type Params struct {
	GlobalLimit       int32
	DefaultServiceMax int32
	MemoryFloor       int32
	MemoryFallback    int32
	// MemDerivedGlobalLimitFloor is retained for compatibility with existing
	// embedders. MemoryFloor takes precedence when it is positive.
	MemDerivedGlobalLimitFloor int32
	Memory                     Memory
	Quotas                     []ServiceQuota
	// GaugeInterval controls how often ExportGauges emits limiter state.
	// Values at or below zero use the two-second production default.
	GaugeInterval time.Duration
}

// Limiter tracks global and per-service session counts with mostly lock-free
// concurrency. It leans slightly on the strict side: it may reject a session
// even if the system is right at (not above) the configured limit.
type Limiter struct {
	globalCount atomic.Int32 // Total active sessions across all services
	globalLimit atomic.Int32 // Configured global session cap

	// Global session cap computed from the supplied memory budget at construction.
	// Emitted as a gauge; when enforceMemDerived returns true, RegisterSession enforces
	// min(globalLimit, memDerivedGlobalLimit) so the configured cap remains an upper bound.
	memDerivedGlobalLimit      atomic.Int32
	defaultServiceMax          int32
	memDerivedGlobalLimitFloor int32

	// Optional; when non-nil and returning true, RegisterSession clamps the active cap to
	// memDerivedGlobalLimit. Read on every RegisterSession call.
	enforceMemDerived func() bool
	gaugeInterval     time.Duration

	services sync.Map // map[string]*ServiceState
}

// ServiceState stores active and max session counts for a service.
type ServiceState struct {
	Count atomic.Int32
	Limit atomic.Int32
}

// ErrorType is the category of a session rejection.
type ErrorType string

const (
	GlobalLimitExceeded  ErrorType = "global_limit_exceeded"
	ServiceLimitExceeded ErrorType = "service_limit_exceeded"
)

// CapMode identifies which global cap was active when a session was registered.
const (
	CapModeDefault    = "default"
	CapModeMemDerived = "mem_derived"
)

// Error is returned when a session exceeds a configured limit.
type Error struct {
	Type    ErrorType
	CapMode string
	Err     error
}

func (e *Error) Error() string {
	return e.Err.Error()
}

func (e *Error) Unwrap() error {
	return e.Err
}

// New constructs a Limiter from Params.
func New(p Params) *Limiter {
	gaugeInterval := p.GaugeInterval
	if gaugeInterval <= 0 {
		gaugeInterval = defaultGaugeInterval
	}
	memoryFloor := p.MemoryFloor
	if memoryFloor <= 0 {
		memoryFloor = p.MemDerivedGlobalLimitFloor
	}
	memoryFallback := p.MemoryFallback
	if memoryFallback <= 0 {
		memoryFallback = p.GlobalLimit
	}
	sl := &Limiter{
		defaultServiceMax:          p.DefaultServiceMax,
		memDerivedGlobalLimitFloor: memoryFloor,
		gaugeInterval:              gaugeInterval,
	}
	sl.globalLimit.Store(p.GlobalLimit)
	sl.setMemDerivedGlobalLimit(p.Memory, memoryFallback)
	for _, quota := range p.Quotas {
		if quota.Name != "" {
			sl.RegisterService(quota.Name, quota.Max)
		}
	}
	return sl
}

// ExportGauges reports active per-service and global session counts plus the
// memory-derived global limit until ctx is canceled.
func (sl *Limiter) ExportGauges(ctx context.Context) {
	ticker := time.NewTicker(sl.gaugeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sl.ForEachService(func(service string, count int32) {
				_ = pgproxy.Metrics().Gauge(
					"dd.orgstore.pg_proxy.listener.service_connections_open",
					float64(count),
					[]string{"client_service_account:" + service},
					1,
				)
			})
			_ = pgproxy.Metrics().Gauge(
				"dd.orgstore.pg_proxy.listener.connections_open",
				float64(sl.GetGlobalCount()),
				[]string{},
				1,
			)
			_ = pgproxy.Metrics().Gauge(
				"dd.orgstore.pg_proxy.session_limit.global",
				float64(sl.GetMemDerivedGlobalLimit()),
				[]string{"mode:mem_derived"},
				1,
			)
		}
	}
}

// RegisterSession attempts to increment session counters, rejecting if limits
// are exceeded. Safe to call concurrently.
func (sl *Limiter) RegisterSession(service string) error {
	serviceState, found := sl.lookup(service)
	if !found {
		// When service is not in the supplied quotas, e.g. orgstore-devenv-toolbox.
		serviceState = sl.RegisterService(service, sl.defaultServiceMax)
	}

	globalLimit := sl.globalLimit.Load()
	capMode := CapModeDefault
	if sl.enforceMemDerived != nil && sl.enforceMemDerived() {
		if memLimit := sl.memDerivedGlobalLimit.Load(); memLimit < globalLimit {
			globalLimit = memLimit
			capMode = CapModeMemDerived
		}
	}

	// Optimistically increment global session count.
	// If we exceed the limit, roll back and reject.
	if sl.globalCount.Add(1) > globalLimit {
		sl.globalCount.Add(-1)
		return newError(GlobalLimitExceeded, capMode, fmt.Sprintf("reached global connection capacity of %d (cap_mode=%s)", globalLimit, capMode))
	}

	// Optimistically increment per-service session count.
	// If over limit, roll back both and reject.
	if serviceState.Count.Add(1) > serviceState.Limit.Load() {
		serviceState.Count.Add(-1)
		sl.globalCount.Add(-1)
		return newError(ServiceLimitExceeded, capMode, fmt.Sprintf("%s reached connection capacity of %d", service, serviceState.Limit.Load()))
	}

	return nil
}

// DeregisterSession decrements session counters with basic underflow protection.
// Safe to call concurrently.
func (sl *Limiter) DeregisterSession(service string) error {
	serviceState, found := sl.lookup(service)
	if !found {
		return fmt.Errorf("service %s not found in SessionLimiter", service)
	}

	if serviceState.Count.Load() > 0 {
		serviceState.Count.Add(-1)
	} else {
		// This should never happen, but keep this log to compare with the previous implementation.
		pgproxy.Logger().Warn("attempting to decrement a 0 session count", "service", service)
	}

	if sl.globalCount.Load() > 0 {
		sl.globalCount.Add(-1)
	} else {
		// This should never happen, but keep this log to compare with the previous implementation.
		pgproxy.Logger().Warn("attempting to decrement a 0 global sessions count")
	}

	return nil
}

// ForEachService applies a callback to each registered service's session count.
// Safe to call concurrently; each count is atomically loaded and passed as a copy to the callback.
func (sl *Limiter) ForEachService(callback func(service string, count int32)) {
	sl.services.Range(func(key, value any) bool {
		callback(key.(string), value.(*ServiceState).Count.Load())
		return true
	})
}

// GetGlobalCount returns the total number of active sessions across all services.
func (sl *Limiter) GetGlobalCount() int32 {
	return sl.globalCount.Load()
}

// EffectiveGlobalLimit returns the currently-enforced global session cap and
// the active cap mode. It is the same limit that RegisterSession enforces.
// Safe to call concurrently.
func (sl *Limiter) EffectiveGlobalLimit() (int32, string) {
	globalLimit := sl.globalLimit.Load()
	capMode := CapModeDefault
	if sl.enforceMemDerived != nil && sl.enforceMemDerived() {
		if memLimit := sl.memDerivedGlobalLimit.Load(); memLimit < globalLimit {
			globalLimit = memLimit
			capMode = CapModeMemDerived
		}
	}
	return globalLimit, capMode
}

// GetMemDerivedGlobalLimit returns the current memory-derived global limit.
func (sl *Limiter) GetMemDerivedGlobalLimit() int32 {
	return sl.memDerivedGlobalLimit.Load()
}

// RefreshFromCgroup re-reads the cgroup memory hard limit and republishes the
// derived session cap, so an in-place pod resize is picked up without a restart.
// It reports false if the limit could not be read, in which case the last
// published cap is kept. A cgroup with no hard limit uses the configured global limit.
func (sl *Limiter) RefreshFromCgroup() bool {
	memory, ok := readDefaultHardLimit()
	return sl.applyCgroupRefresh(memory, ok)
}

func (sl *Limiter) applyCgroupRefresh(memory Memory, ok bool) bool {
	if !ok {
		return false
	}
	sl.setMemDerivedGlobalLimit(memory, sl.globalLimit.Load())
	return true
}

func (sl *Limiter) setMemDerivedGlobalLimit(memory Memory, unlimitedFallback int32) {
	if memory.Available {
		sl.memDerivedGlobalLimit.Store(MemDerivedLimit(memory.Bytes, sl.memDerivedGlobalLimitFloor))
		return
	}
	sl.memDerivedGlobalLimit.Store(unlimitedFallback)
}

// SetEnforceMemDerived installs the flag check that RegisterSession uses to
// decide whether to clamp the active cap to the memory-derived limit.
// Must be called before RegisterSession.
func (sl *Limiter) SetEnforceMemDerived(enforce func() bool) {
	sl.enforceMemDerived = enforce
}

// GetCurrentServiceCount returns the active session count for a given service.
func (sl *Limiter) GetCurrentServiceCount(service string) int32 {
	if state, ok := sl.lookup(service); ok {
		return state.Count.Load()
	}
	return 0
}

// ServiceLimit returns the configured cap for a service, if it is registered.
func (sl *Limiter) ServiceLimit(service string) (int32, bool) {
	state, ok := sl.lookup(service)
	if !ok {
		return 0, false
	}
	return state.Limit.Load(), true
}

// RegisterService registers a service with a given session limit or returns the existing state.
// Safe to call concurrently.
func (sl *Limiter) RegisterService(service string, limit int32) *ServiceState {
	serviceState := newServiceState(limit)
	// LoadOrStore prevents duplicate registration if another goroutine initialized this service concurrently.
	actual, _ := sl.services.LoadOrStore(service, serviceState)
	return actual.(*ServiceState)
}

// GetErrorType returns the error type string if err is a Limit Error, otherwise "unknown".
func (sl *Limiter) GetErrorType(err error) string {
	if e, ok := err.(*Error); ok {
		return string(e.Type)
	}
	pgproxy.Logger().Warn("unknown error type in limiter.GetErrorType", "type", fmt.Sprintf("%T", err))
	return "unknown"
}

// GetCapMode returns the global cap mode active when the rejection occurred,
// or "unknown" if err is not a Limit Error.
func (sl *Limiter) GetCapMode(err error) string {
	if e, ok := err.(*Error); ok {
		return e.CapMode
	}
	return "unknown"
}

// MemDerivedLimit applies the memory-cap formula. Split out so it can be
// unit-tested without depending on how memory was discovered.
func MemDerivedLimit(memBytes int64, floor int32) int32 {
	if memBytes <= 0 {
		return floor
	}
	available := float64(memBytes) * MemorySafetyFraction
	limit := int32(available / BytesPerSession)
	if limit < floor {
		return floor
	}
	return limit
}

func (sl *Limiter) lookup(service string) (*ServiceState, bool) {
	value, ok := sl.services.Load(service)
	if !ok {
		return nil, false
	}
	return value.(*ServiceState), true
}

// newServiceState creates a ServiceState with an initial session limit.
func newServiceState(limit int32) *ServiceState {
	serviceState := &ServiceState{}
	serviceState.Count.Store(0)
	serviceState.Limit.Store(limit)
	return serviceState
}

func newError(t ErrorType, capMode, msg string) *Error {
	return &Error{
		Type:    t,
		CapMode: capMode,
		Err:     fmt.Errorf("%s", msg),
	}
}
