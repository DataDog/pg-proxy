package pool

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInstanceRequiresInitializedPool(t *testing.T) {
	instance := NewInstance(InstanceConfig{})
	_, err := instance.AcquireConnection(context.Background())
	assert.EqualError(t, err, "pool unexpectedly not initialized when attempting to acquire connection")
	assert.Nil(t, instance.Stat())
	instance.Reset()
	instance.Close()
}

func TestInstanceEnsurePoolRemovesPrimarySessionAttribute(t *testing.T) {
	var connectionString string
	instance := NewInstance(InstanceConfig{
		Address:          "127.0.0.1",
		Database:         "app",
		ConnectionString: "host=127.0.0.1 target_session_attrs=primary dbname=app",
		PoolFactory: func(_ context.Context, value string) (*pgxpool.Pool, error) {
			connectionString = value
			return nil, errors.New("unavailable")
		},
	})

	assert.False(t, instance.EnsurePool(context.Background()))
	assert.Equal(t, "host=127.0.0.1  dbname=app", connectionString)
}

func TestInstanceHooksObserveHealthAndPoolErrors(t *testing.T) {
	var observedAddress string
	var observedHealthy bool
	var createErr error
	instance := NewInstance(InstanceConfig{
		Address:  "db.test",
		Database: "app",
		PoolFactory: func(context.Context, string) (*pgxpool.Pool, error) {
			return nil, errors.New("boom")
		},
		Hooks: InstanceHooks{
			HealthObserved: func(address string, healthy bool, _ []string) {
				observedAddress, observedHealthy = address, healthy
			},
			PoolCreateError: func(_, _ string, err error) {
				createErr = err
			},
		},
	})
	instance.SetHealthy(true)
	require.True(t, instance.IsHealthy())
	assert.Equal(t, "db.test", observedAddress)
	assert.True(t, observedHealthy)
	assert.False(t, instance.EnsurePool(context.Background()))
	assert.EqualError(t, createErr, "boom")
}
