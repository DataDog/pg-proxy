// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package server

import (
	"context"
	"fmt"

	pgproxy "github.com/DataDog/pg-proxy"
	"github.com/DataDog/pg-proxy/pkg/listener"
	"github.com/DataDog/pg-proxy/pkg/mirror"
	"github.com/DataDog/pg-proxy/pkg/session"
	"github.com/DataDog/pg-proxy/pkg/topology"
)

// Backend is the cluster view needed to resolve startup and construct sessions.
// SessionBackend retains any application wrapper used for checkout accounting.
type Backend interface {
	SessionBackend() session.Backend
	ParameterStatuses() map[string]string
	CanPerformDynamicRouting(bool) bool
}

// Inventory supplies current routing and backend state. The caller owns its lifecycle.
// The Warden extraction can implement this interface without server importing Warden.
type Inventory interface {
	GetLogicalDatabase(string) (topology.LogicalDatabase, bool)
	GetBackend(string) (Backend, bool)
}

// SessionInfo contains authenticated connection metadata for optional integrations.
type SessionInfo struct {
	User            string
	LogicalDatabase string
	Service         string
	PassthroughUser bool
}

// SessionExtensions configures optional hooks and link decoration independently.
// Its zero value leaves the portable session path unchanged.
type SessionExtensions struct {
	Hooks        session.Hooks
	DecorateLink bool
}

// SessionExtensionsFactory configures extensions for one authenticated session.
// A nil factory leaves the portable session path unchanged.
type SessionExtensionsFactory func(SessionInfo) SessionExtensions

// RuntimeConfig supplies routing inventory and application session integrations.
type RuntimeConfig struct {
	Inventory Inventory
	Settings  topology.RuntimeConfig
	// ParameterStatuses optionally updates a private copy of backend parameters in place.
	// The callback must not retain the map.
	ParameterStatuses func(map[string]string)
	SessionExtensions SessionExtensionsFactory
	RandomInt         func(int) int
	Mirroring         session.Mirroring
	Metrics           session.Metrics
	// EnableTraceParameters includes raw Bind values in all sessions' sampled query traces.
	// The zero value disables parameter capture. SQL text is unaffected.
	EnableTraceParameters bool
}

// Runtime resolves startup and constructs sessions for the listener.
type Runtime struct {
	cfg          RuntimeConfig
	mirrorBudget *mirror.Budget
}

// ResolvedStartup keeps the backend chosen before authentication for session creation.
type ResolvedStartup struct {
	logicalDatabase topology.LogicalDatabase
	backend         Backend
}

// BackendCluster identifies the backend selected by startup resolution.
func (s ResolvedStartup) BackendCluster() string { return s.logicalDatabase.Cluster }

// NewRuntime composes session construction around the caller-owned inventory.
// Inventory is required. Optional integrations keep session's existing defaults.
func NewRuntime(cfg RuntimeConfig) *Runtime {
	return &Runtime{cfg: cfg, mirrorBudget: mirror.NewBudget(mirror.ComputeBudget())}
}

// ResolveStartup resolves the logical database and copies backend startup parameters.
func (r *Runtime) ResolveStartup(ctx context.Context, request listener.StartupRequest) (listener.StartupResult[ResolvedStartup], error) {
	logicalDB, ok := r.cfg.Inventory.GetLogicalDatabase(request.Database)
	if !ok {
		return listener.StartupResult[ResolvedStartup]{}, fmt.Errorf("%w: %s", listener.ErrLogicalDatabaseNotConfigured, request.Database)
	}
	backend, ok := r.cfg.Inventory.GetBackend(logicalDB.Cluster)
	if !ok {
		pgproxy.LoggerFromContext(ctx).Info("startup backend unavailable", "backend_cluster", fmt.Sprintf("%.256s", logicalDB.Cluster))
		return listener.StartupResult[ResolvedStartup]{}, fmt.Errorf(
			"%w: logical database %q is mapped to cluster %q, which has no entry in the states map",
			listener.ErrBackendNotReady, request.Database, logicalDB.Cluster,
		)
	}
	parameters := make(map[string]string)
	for key, value := range backend.ParameterStatuses() {
		parameters[key] = value
	}
	if r.cfg.ParameterStatuses != nil {
		r.cfg.ParameterStatuses(parameters)
	}
	return listener.StartupResult[ResolvedStartup]{
		LogicalDatabase:   request.Database,
		ParameterStatuses: parameters,
		Data:              ResolvedStartup{logicalDatabase: logicalDB, backend: backend},
	}, nil
}

// NewSession constructs the portable session from authenticated startup data.
func (r *Runtime) NewSession(cfg listener.SessionConfig[ResolvedStartup]) (listener.Session, error) {
	resolved := cfg.Resolved.Data
	backendSupportsDynamicRouting := true
	if cfg.Request.Options.DynamicRoutingEnabled {
		backendSupportsDynamicRouting = resolved.backend.CanPerformDynamicRouting(true)
	}
	dynamicRoutingEnabled := topology.DynamicRoutingEnabled(
		cfg.Request.Options.DynamicRoutingEnabled,
		backendSupportsDynamicRouting,
		resolved.logicalDatabase,
	)
	readStates, _ := r.ResolveReadStates(resolved.logicalDatabase.ReadClusters)
	var extensions SessionExtensions
	if r.cfg.SessionExtensions != nil {
		extensions = r.cfg.SessionExtensions(SessionInfo{
			User:            cfg.Request.User,
			LogicalDatabase: cfg.Resolved.LogicalDatabase,
			Service:         cfg.Service,
			PassthroughUser: cfg.Request.Options.PassthroughUser,
		})
	}
	session := session.New(session.Config{
		Context:                  cfg.Context,
		User:                     cfg.Request.User,
		Database:                 cfg.Resolved.LogicalDatabase,
		LogicalDatabase:          cfg.Resolved.LogicalDatabase,
		PID:                      cfg.PID,
		SecretKey:                cfg.SecretKey,
		Service:                  cfg.Service,
		Client:                   cfg.Client,
		ClientConn:               cfg.ClientConn,
		LivenessProbe:            session.NewTCPLivenessProbe(cfg.ClientConn),
		Backend:                  resolved.backend.SessionBackend(),
		DynamicRoutingEnabled:    dynamicRoutingEnabled,
		PostgresMsgBufferSize:    r.cfg.Settings.PostgresMessageBufferSize,
		ReplyBufferPool:          r.cfg.Settings.ReplyBufferPool,
		PassthroughUser:          cfg.Request.Options.PassthroughUser,
		Priority:                 cfg.Request.Options.Priority,
		ReadStates:               readStates,
		RandomInt:                r.cfg.RandomInt,
		AutoPreparedStmtsEnabled: cfg.Request.Options.AutoCachedPreparedStatements,
		Mirroring:                r.cfg.Mirroring,
		MirrorBudget:             r.mirrorBudget,
		MetricTags:               cfg.MetricTags,
		Metrics:                  r.cfg.Metrics,
		EnableTraceParameters:    r.cfg.EnableTraceParameters,
		Hooks:                    extensions.Hooks,
		DecorateLink:             extensions.DecorateLink,
	})
	return session, nil
}

// ResolveReadStates selects available read backends and preserves configured weights.
func (r *Runtime) ResolveReadStates(reads topology.ReadClusters) ([]session.ReadStateConfig, topology.ReadClusters) {
	var readStates []session.ReadStateConfig
	resolvedReads := make(topology.ReadClusters, len(reads))
	for clusterName, readSettings := range reads {
		backendState, ok := r.cfg.Inventory.GetBackend(clusterName)
		if !ok {
			pgproxy.Logger().Error(fmt.Sprintf("could not find an entry in the states map for the given cluster: %q", clusterName))
			continue
		}
		readStates = append(readStates, session.ReadStateConfig{
			Backend: backendState.SessionBackend(),
			Weight:  topology.Weight(readSettings),
		})
		resolvedReads[clusterName] = readSettings
	}
	return readStates, resolvedReads
}
