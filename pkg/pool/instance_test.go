package pool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
		PoolFactory: func(_ context.Context, value string) (ConnectionPool, error) {
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
		PoolFactory: func(context.Context, string) (ConnectionPool, error) {
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

func TestInstanceConcurrentInitialization(t *testing.T) {
	const callers = 20
	var attempts atomic.Int32
	entered := make(chan struct{}, callers)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	physical := &lifecycleTestPool{}
	instance := NewInstance(InstanceConfig{
		PoolFactory: func(context.Context, string) (ConnectionPool, error) {
			attempts.Add(1)
			entered <- struct{}{}
			<-release
			return physical, nil
		},
	})
	results := make(chan bool, callers)
	initialize := func() { results <- instance.EnsurePool(context.Background()) }
	go initialize()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("pool creation did not start")
	}

	started := make(chan struct{}, callers-1)
	for range callers - 1 {
		go func() {
			started <- struct{}{}
			initialize()
		}()
	}
	for range callers - 1 {
		<-started
	}

	// Construction must not hold the lock used for pool and health access.
	responsive := make(chan bool, 1)
	go func() {
		unpublished := instance.Pool() == nil && instance.Stat() == nil
		instance.SetHealthy(true)
		responsive <- unpublished && instance.IsHealthy()
	}()
	select {
	case ok := <-responsive:
		require.True(t, ok, "pool should remain unpublished while health access works")
	case <-time.After(5 * time.Second):
		t.Fatal("pool construction blocked pool or health access")
	}

	// Keep the first construction blocked while competing callers run.
	select {
	case <-entered:
		t.Fatal("concurrent initialization created another pool")
	case <-time.After(100 * time.Millisecond):
	}
	unblock()
	timeout := time.After(5 * time.Second)
	for range callers {
		select {
		case ok := <-results:
			assert.True(t, ok)
		case <-timeout:
			t.Fatal("initialization callers did not finish")
		}
	}
	assert.EqualValues(t, 1, attempts.Load())
	assert.Same(t, physical, instance.Pool())
}

type lifecycleTestPool struct {
	ConnectionPool
	closed     atomic.Bool
	closeCount atomic.Int32
}

func (p *lifecycleTestPool) Close() {
	p.closeCount.Add(1)
	p.closed.Store(true)
}

func TestInstanceCloseBeforeInitialization(t *testing.T) {
	attempts := 0
	instance := NewInstance(InstanceConfig{
		PoolFactory: func(context.Context, string) (ConnectionPool, error) {
			attempts++
			return &lifecycleTestPool{}, nil
		},
	})
	instance.Close()
	assert.False(t, instance.EnsurePool(context.Background()))
	assert.Nil(t, instance.Pool())
	assert.Zero(t, attempts)
}

func TestInstanceClosesPoolCreatedDuringShutdown(t *testing.T) {
	for _, failCreation := range []bool{false, true} {
		name := "successful creation"
		if failCreation {
			name = "failed creation"
		}
		t.Run(name, func(t *testing.T) {
			physical := &lifecycleTestPool{}
			started, release := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			t.Cleanup(unblock)
			var attempts atomic.Int32
			instance := NewInstance(InstanceConfig{
				PoolFactory: func(context.Context, string) (ConnectionPool, error) {
					attempts.Add(1)
					close(started)
					<-release
					if failCreation {
						return nil, errors.New("creation failed")
					}
					return physical, nil
				},
			})
			result := make(chan bool, 1)
			go func() { result <- instance.EnsurePool(context.Background()) }()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("pool creation did not start")
			}
			closed := make(chan struct{})
			go func() { instance.Close(); close(closed) }()
			select {
			case <-closed:
			case <-time.After(time.Second):
				t.Fatal("shutdown waited for pool creation")
			}
			assert.Nil(t, instance.Pool())
			unblock()
			select {
			case ok := <-result:
				assert.False(t, ok, "initialization must not succeed after shutdown")
			case <-time.After(time.Second):
				t.Fatal("initialization did not finish")
			}
			assert.Nil(t, instance.Pool(), "late pool must never be published")
			if failCreation {
				assert.Zero(t, physical.closeCount.Load())
			} else {
				assert.EqualValues(t, 1, physical.closeCount.Load())
			}
			assert.False(t, instance.EnsurePool(context.Background()))
			assert.EqualValues(t, 1, attempts.Load())
			instance.Close()
			if !failCreation {
				assert.EqualValues(t, 1, physical.closeCount.Load())
			}
		})
	}
}

func TestInstanceCloseAfterInitialization(t *testing.T) {
	physical := &lifecycleTestPool{}
	attempts := 0
	instance := NewInstance(InstanceConfig{
		PoolFactory: func(context.Context, string) (ConnectionPool, error) {
			attempts++
			return physical, nil
		},
	})
	require.True(t, instance.EnsurePool(context.Background()))
	require.True(t, instance.EnsurePool(context.Background()))
	assert.Same(t, physical, instance.Pool())
	assert.Equal(t, 1, attempts)
	instance.Close()
	require.Eventually(t, physical.closed.Load, time.Second, time.Millisecond)
	instance.Close()
	assert.False(t, instance.EnsurePool(context.Background()))
	assert.EqualValues(t, 1, physical.closeCount.Load())
	assert.Equal(t, 1, attempts)
}

func TestInstanceRetriesFailedInitialization(t *testing.T) {
	physical := &lifecycleTestPool{}
	attempts := 0
	instance := NewInstance(InstanceConfig{
		PoolFactory: func(context.Context, string) (ConnectionPool, error) {
			attempts++
			if attempts == 1 {
				return nil, errors.New("temporary failure")
			}
			return physical, nil
		},
	})
	t.Cleanup(instance.Close)
	assert.False(t, instance.EnsurePool(context.Background()))
	assert.Nil(t, instance.Pool())
	assert.True(t, instance.EnsurePool(context.Background()))
	assert.Same(t, physical, instance.Pool())
	assert.Equal(t, 2, attempts)
}
