// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package server

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/DataDog/pg-proxy/pkg/link"
	"github.com/DataDog/pg-proxy/pkg/listener"
	"github.com/DataDog/pg-proxy/pkg/session"
	"github.com/DataDog/pg-proxy/pkg/topology"
)

type testInventory struct {
	databases map[string]topology.LogicalDatabase
	backends  map[string]Backend
}

func (i *testInventory) GetLogicalDatabase(name string) (topology.LogicalDatabase, bool) {
	database, ok := i.databases[name]
	return database, ok
}
func (i *testInventory) GetBackend(name string) (Backend, bool) {
	backend, ok := i.backends[name]
	return backend, ok
}

type testBackend struct {
	session.Backend
	parameters      map[string]string
	supportsRouting bool
	routingChecks   int
}

func (b *testBackend) SessionBackend() session.Backend      { return b.Backend }
func (b *testBackend) ParameterStatuses() map[string]string { return b.parameters }
func (b *testBackend) CanPerformDynamicRouting(enabled bool) bool {
	if enabled {
		b.routingChecks++
	}
	return b.supportsRouting
}

func newInventoryForTest() *testInventory {
	return &testInventory{
		databases: map[string]topology.LogicalDatabase{"testdb": {Cluster: "primary"}},
		backends:  map[string]Backend{"primary": &testBackend{parameters: map[string]string{"server_version": "16"}}},
	}
}

func newRuntimeForTest() (*Runtime, *testInventory) {
	inventory := newInventoryForTest()
	return NewRuntime(RuntimeConfig{Inventory: inventory}), inventory
}

func TestRuntimeResolveStartup(t *testing.T) {
	runtime, inventory := newRuntimeForTest()
	runtime.cfg.ParameterStatuses = func(parameters map[string]string) {
		parameters["server_version"] = "overridden"
	}
	resolved, err := runtime.ResolveStartup(t.Context(), listener.StartupRequest{Database: "testdb"})
	require.NoError(t, err)
	assert.Equal(t, "testdb", resolved.LogicalDatabase)
	assert.Equal(t, map[string]string{"server_version": "overridden"}, resolved.ParameterStatuses)
	assert.Equal(t, "16", inventory.backends["primary"].ParameterStatuses()["server_version"])

	_, err = runtime.ResolveStartup(t.Context(), listener.StartupRequest{Database: "missing"})
	assert.EqualError(t, err, "no configuration found for logical database missing")
	assert.Equal(t, "primary", resolved.Data.BackendCluster())
	delete(inventory.backends, "primary")
	_, err = runtime.ResolveStartup(t.Context(), listener.StartupRequest{Database: "testdb"})
	assert.EqualError(t, err, `the specified logical database "testdb" is mapped to cluster "primary" but could not find an entry for it in the states map`)
}

func TestRuntimeReadStatesPreserveAvailableWeights(t *testing.T) {
	runtime, inventory := newRuntimeForTest()
	zeroBackend, defaultBackend := &sessionExtensionTestBackend{}, &sessionExtensionTestBackend{}
	inventory.backends["zero"] = &testBackend{Backend: zeroBackend}
	inventory.backends["default"] = &testBackend{Backend: defaultBackend}
	reads := topology.ReadClusters{
		"zero":    {Weight: 0},
		"default": {Weight: topology.NormalizedWeight(nil)},
		"missing": {Weight: 10},
	}
	states, resolved := runtime.ResolveReadStates(reads)
	assert.ElementsMatch(t, []session.ReadStateConfig{
		{Backend: zeroBackend, Weight: 0},
		{Backend: defaultBackend, Weight: 1},
	}, states)
	assert.Equal(t, topology.ReadClusters{"zero": reads["zero"], "default": reads["default"]}, resolved)
	assert.Len(t, reads, 3)
}

func TestRuntimeDynamicRouting(t *testing.T) {
	for _, tc := range []struct {
		name                                 string
		requested, supported, reads, enabled bool
	}{
		{name: "disabled"},
		{name: "supported", requested: true, supported: true, enabled: true},
		{name: "unsupported", requested: true},
		{name: "configured reads", reads: true, enabled: true},
		{name: "reads override unsupported", requested: true, reads: true, enabled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runtime, inventory := newRuntimeForTest()
			backend := inventory.backends["primary"].(*testBackend)
			backend.supportsRouting = tc.supported
			if tc.reads {
				inventory.databases["testdb"] = topology.LogicalDatabase{Cluster: "primary", ReadClusters: topology.ReadClusters{"primary": {Weight: 1}}}
			}
			resolved, err := runtime.ResolveStartup(t.Context(), listener.StartupRequest{Database: "testdb"})
			require.NoError(t, err)
			active, err := runtime.NewSession(listener.SessionConfig[ResolvedStartup]{
				Context: t.Context(), Resolved: resolved,
				Request: listener.StartupRequest{Options: listener.SessionOptions{DynamicRoutingEnabled: tc.requested}},
			})
			require.NoError(t, err)
			assert.Equal(t, tc.enabled, active.DynamicRoutingEnabled())
			if tc.requested {
				assert.Equal(t, 1, backend.routingChecks)
			} else {
				assert.Zero(t, backend.routingChecks)
			}
		})
	}
}

// A concurrent inventory refresh must not replace the backend already resolved for startup.
func TestRuntimeSessionUsesResolvedBackendAndAuthenticatedIdentity(t *testing.T) {
	runtime, inventory := newRuntimeForTest()
	checkoutBackend := &sessionExtensionTestBackend{}
	inventory.backends["primary"].(*testBackend).Backend = checkoutBackend
	resolved, err := runtime.ResolveStartup(t.Context(), listener.StartupRequest{Database: "testdb"})
	require.NoError(t, err)
	inventory.backends["primary"] = &testBackend{Backend: &sessionExtensionTestBackend{}}
	active, err := runtime.NewSession(listener.SessionConfig[ResolvedStartup]{
		Context: t.Context(), Resolved: resolved,
		Request: listener.StartupRequest{User: "db-role", Options: listener.SessionOptions{AutoCachedPreparedStatements: true}},
		PID:     42, SecretKey: []byte{1, 2, 3, 4}, Service: "authenticated-service", MetricTags: []string{"custom:tag"},
	})
	require.NoError(t, err)
	assert.Equal(t, "db-role", active.User())
	assert.Equal(t, "testdb", active.Database())
	assert.Equal(t, "testdb", active.LogicalDatabaseID())
	assert.Equal(t, "authenticated-service", active.Service())
	assert.Equal(t, "42", active.Identifier())
	assert.Equal(t, []byte{1, 2, 3, 4}, active.SecretKey())
	assert.Equal(t, []string{"custom:tag"}, active.MetricTags())
	assert.Same(t, checkoutBackend, active.(*session.Session).Backend())
	assert.True(t, active.(*session.Session).AutoPreparedStmtsEnabled)
}

func TestRuntimeInstallsSessionExtensionsWithAuthenticatedInfo(t *testing.T) {
	runtime, _ := newRuntimeForTest()
	resolved, err := runtime.ResolveStartup(t.Context(), listener.StartupRequest{Database: "testdb"})
	require.NoError(t, err)

	var got SessionInfo
	closed := 0
	runtime.cfg.SessionExtensions = func(info SessionInfo) SessionExtensions {
		got = info
		return SessionExtensions{Hooks: session.Hooks{Closed: func() { closed++ }}}
	}
	created, err := runtime.NewSession(listener.SessionConfig[ResolvedStartup]{
		Context: t.Context(),
		Request: listener.StartupRequest{
			User:     "database-role",
			Database: "testdb",
			Options:  listener.SessionOptions{PassthroughUser: true},
		},
		Resolved: resolved,
		Service:  "client-service",
	})
	require.NoError(t, err)
	created.(*session.Session).Close(nil)

	assert.Equal(t, SessionInfo{
		User:            "database-role",
		LogicalDatabase: "testdb",
		Service:         "client-service",
		PassthroughUser: true,
	}, got)
	assert.Equal(t, 1, closed)
}

type sessionExtensionTestBackend struct {
	session.Backend
	requests []session.LinkRequest
}

func (b *sessionExtensionTestBackend) Acquire(_ context.Context, request session.LinkRequest) (link.Link, error) {
	b.requests = append(b.requests, request)
	return nil, fmt.Errorf("stop after recording checkout")
}

func TestRuntimePassesDecorationToCheckout(t *testing.T) {
	for _, decorate := range []bool{false, true} {
		t.Run(fmt.Sprintf("decorate=%t", decorate), func(t *testing.T) {
			runtime, _ := newRuntimeForTest()
			resolved, err := runtime.ResolveStartup(t.Context(), listener.StartupRequest{Database: "testdb"})
			require.NoError(t, err)
			runtime.cfg.SessionExtensions = func(SessionInfo) SessionExtensions {
				return SessionExtensions{DecorateLink: decorate}
			}
			created, err := runtime.NewSession(listener.SessionConfig[ResolvedStartup]{
				Context: t.Context(), Resolved: resolved,
			})
			require.NoError(t, err)
			active := created.(*session.Session)
			backend := &sessionExtensionTestBackend{}
			active.RemapBackend(backend)
			active.ApplyStateRemapping()
			err = active.HandleAction(session.ClientMessageAction{ProxyMsgToPg: &pgproto3.Query{String: "select 1"}})
			require.ErrorContains(t, err, "stop after recording checkout")
			require.Len(t, backend.requests, 1)
			assert.Equal(t, decorate, backend.requests[0].DecorateLink)
		})
	}
}
