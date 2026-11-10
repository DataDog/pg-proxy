// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package limiter

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	pgproxy "github.com/DataDog/pg-proxy"
)

const defaultGaugeInterval = 2 * time.Second

// ServiceQuota is a per-service session cap. Name is the key RegisterSession
// will look up (already normalized by the embedder).
type ServiceQuota struct {
	Name string
	Max  int32
}

// Params constructs a Limiter. GlobalLimit is the configured cap.
// DefaultServiceMax is applied to services that were not pre-registered.
type Params struct {
	GlobalLimit       int32
	DefaultServiceMax int32
	Quotas            []ServiceQuota
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

	defaultServiceMax int32
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

// Error is returned when a session exceeds a configured limit.
type Error struct {
	Type ErrorType
	Err  error
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
	sl := &Limiter{
		defaultServiceMax: p.DefaultServiceMax,
		gaugeInterval:     gaugeInterval,
	}
	sl.globalLimit.Store(p.GlobalLimit)
	for _, quota := range p.Quotas {
		if quota.Name != "" {
			sl.RegisterService(quota.Name, quota.Max)
		}
	}
	return sl
}

// ExportGauges reports active per-service and global session counts plus the
// configured global session limit until ctx is canceled.
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
					"pg_proxy.listener.service_connections_open",
					float64(count),
					[]string{"client_service_account:" + service},
					1,
				)
			})
			_ = pgproxy.Metrics().Gauge(
				"pg_proxy.listener.connections_open",
				float64(sl.GetGlobalCount()),
				[]string{},
				1,
			)
			_ = pgproxy.Metrics().Gauge(
				"pg_proxy.session_limit.global",
				float64(sl.globalLimit.Load()),
				[]string{},
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
		// When service is not in the supplied quotas, e.g. an ad hoc tooling client.
		serviceState = sl.RegisterService(service, sl.defaultServiceMax)
	}

	globalLimit := sl.globalLimit.Load()

	// Optimistically increment global session count.
	// If we exceed the limit, roll back and reject.
	if sl.globalCount.Add(1) > globalLimit {
		sl.globalCount.Add(-1)
		return newError(GlobalLimitExceeded, fmt.Sprintf("reached global connection capacity of %d", globalLimit))
	}

	// Optimistically increment per-service session count.
	// If over limit, roll back both and reject.
	if serviceState.Count.Add(1) > serviceState.Limit.Load() {
		serviceState.Count.Add(-1)
		sl.globalCount.Add(-1)
		return newError(ServiceLimitExceeded, fmt.Sprintf("%s reached connection capacity of %d", service, serviceState.Limit.Load()))
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

func newError(t ErrorType, msg string) *Error {
	return &Error{
		Type: t,
		Err:  fmt.Errorf("%s", msg),
	}
}
