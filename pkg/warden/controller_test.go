// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package warden

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/DataDog/pg-proxy/pkg/topology"
)

type controllerConfig struct {
	host     string
	interval time.Duration
}

func TestControllerRefreshesAndReconcilesConfiguration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	cfg := controllerConfig{host: "host-a", interval: time.Minute}
	version := uint64(1)
	var order []string
	controller := NewController(ControllerConfig[controllerConfig, string]{
		Context:  ctx,
		Interval: time.Hour,
		Source: func(context.Context) (controllerConfig, uint64, error) {
			return cfg, version, nil
		},
		Normalize: func(value controllerConfig) (Snapshot[string], error) {
			order = append(order, "normalize")
			return Snapshot[string]{
				Databases:        map[Identifier]string{"primary": value.host},
				Clusters:         map[Identifier]Cluster{"cluster": {Primary: "primary"}},
				LogicalDatabases: map[Identifier]topology.LogicalDatabase{"logical": {Cluster: "cluster"}},
			}, nil
		},
		RefreshInterval: func(value controllerConfig) (time.Duration, bool) {
			order = append(order, "interval")
			return value.interval, true
		},
		Factory: func(context.Context, string, string, []string) (ManagedBackend, error) {
			order = append(order, "factory")
			return &testManagedBackend{}, nil
		},
		Hooks: ControllerHooks[controllerConfig]{
			BeforeAccept: func(controllerConfig) error {
				order = append(order, "before")
				return nil
			},
			Accepted: func(controllerConfig) {
				order = append(order, "accepted")
			},
		},
	})

	require.NoError(t, controller.Refresh())
	assert.Equal(t, []string{"before", "normalize", "factory", "accepted", "interval"}, order)
	assert.Equal(t, cfg.host, mustDatabase(t, controller.Reconciler(), "primary"))
	_, found := controller.Runtime().GetBackend("cluster")
	assert.True(t, found)

	order = nil
	cfg.host = "host-b"
	require.NoError(t, controller.Refresh(), "same version should be skipped")
	assert.Empty(t, order)
	assert.Equal(t, "host-a", mustDatabase(t, controller.Reconciler(), "primary"))

	version++
	require.NoError(t, controller.Refresh())
	assert.Equal(t, "host-b", mustDatabase(t, controller.Reconciler(), "primary"))
}

func TestControllerStopsBeforeReconciliationOnHookOrNormalizeError(t *testing.T) {
	sentinel := errors.New("rejected")
	controller := NewController(ControllerConfig[string, string]{
		Interval:  time.Hour,
		Normalize: func(string) (Snapshot[string], error) { return Snapshot[string]{}, nil },
		Hooks: ControllerHooks[string]{
			BeforeAccept: func(string) error { return sentinel },
		},
	})

	err := controller.Accept("config")
	require.ErrorIs(t, err, sentinel)
	_, found := controller.Reconciler().GetDatabase("primary")
	assert.False(t, found)

	controller = NewController(ControllerConfig[string, string]{
		Interval: time.Hour,
		Normalize: func(string) (Snapshot[string], error) {
			return Snapshot[string]{}, sentinel
		},
	})
	err = controller.Accept("config")
	require.ErrorIs(t, err, sentinel)
	_, found = controller.Reconciler().GetDatabase("primary")
	assert.False(t, found)
}

func mustDatabase(t *testing.T, reconciler *Reconciler[string], name string) string {
	t.Helper()
	database, found := reconciler.GetDatabase(Identifier(name))
	require.True(t, found)
	return database
}
