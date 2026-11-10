// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

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

// instanceTestPool isolates the Instance lifecycle from actual PostgreSQL I/O.
type instanceTestPool struct {
	ConnectionPool
	limit     int32
	resizeErr error
	closed    bool
}

func (p *instanceTestPool) Resize(limit int32) error {
	if p.resizeErr != nil {
		return p.resizeErr
	}
	p.limit = limit
	return nil
}

func (p *instanceTestPool) Close() { p.closed = true }

func newResizableTestInstance(factory func(context.Context, string) (ConnectionPool, error)) *Instance {
	return NewInstance(InstanceConfig{
		IsResizable:           true,
		ResizableVirtualLimit: 50,
		PhysicalMaxConns:      100,
		PoolFactory:           factory,
	})
}

func TestInstanceAppliesResizeBeforeInitialization(t *testing.T) {
	pool := &instanceTestPool{limit: 50}
	instance := newResizableTestInstance(func(context.Context, string) (ConnectionPool, error) {
		return pool, nil
	})
	require.NoError(t, instance.ResizeMaxConns(10))
	require.Nil(t, instance.Pool())
	require.True(t, instance.EnsurePool(context.Background()))
	assert.Same(t, pool, instance.Pool())
	assert.EqualValues(t, 10, pool.limit)

	// A downsize does not lower the startup ceiling for subsequent growth.
	require.NoError(t, instance.ResizeMaxConns(80))
	assert.EqualValues(t, 80, pool.limit)
}

func TestInstanceAppliesResizeDuringInitialization(t *testing.T) {
	pool := &instanceTestPool{limit: 50}
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseFactory := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseFactory()
	instance := newResizableTestInstance(func(context.Context, string) (ConnectionPool, error) {
		close(started)
		<-release
		return pool, nil
	})
	initialized := make(chan bool, 1)
	go func() { initialized <- instance.EnsurePool(context.Background()) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("pool construction did not start")
	}
	resized := make(chan error, 1)
	go func() { resized <- instance.ResizeMaxConns(10) }()
	select {
	case err := <-resized:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("resize blocked behind connection establishment")
	}
	require.Nil(t, instance.Pool())
	releaseFactory()
	select {
	case ok := <-initialized:
		require.True(t, ok)
	case <-time.After(time.Second):
		t.Fatal("pool initialization did not complete")
	}
	assert.EqualValues(t, 10, pool.limit)
}

func TestInstanceValidatesResizeBeforeAndAfterInitialization(t *testing.T) {
	for _, initialized := range []bool{false, true} {
		name := "before_initialization"
		if initialized {
			name = "after_initialization"
		}
		t.Run(name, func(t *testing.T) {
			pool := &instanceTestPool{limit: 50}
			instance := newResizableTestInstance(func(context.Context, string) (ConnectionPool, error) {
				return pool, nil
			})
			require.NoError(t, instance.ResizeMaxConns(10))
			if initialized {
				require.True(t, instance.EnsurePool(context.Background()))
			}
			for _, limit := range []int32{-1, 0, 101} {
				require.Error(t, instance.ResizeMaxConns(limit))
			}
			require.True(t, instance.EnsurePool(context.Background()))
			assert.EqualValues(t, 10, pool.limit, "invalid requests must not replace the accepted limit")
		})
	}
}

func TestInstanceRetainsResizeAcrossConstructionFailure(t *testing.T) {
	pool := &instanceTestPool{limit: 50}
	attempts := 0
	instance := newResizableTestInstance(func(context.Context, string) (ConnectionPool, error) {
		attempts++
		if attempts == 1 {
			return nil, errors.New("unavailable")
		}
		return pool, nil
	})
	require.NoError(t, instance.ResizeMaxConns(10))
	require.False(t, instance.EnsurePool(context.Background()))
	require.NoError(t, instance.ResizeMaxConns(5))
	require.True(t, instance.EnsurePool(context.Background()))
	assert.EqualValues(t, 5, pool.limit)
}

func TestInstanceClosesUnpublishedPoolWhenResizeFails(t *testing.T) {
	expected := errors.New("resize failed")
	pool := &instanceTestPool{resizeErr: expected}
	instance := newResizableTestInstance(func(context.Context, string) (ConnectionPool, error) {
		return pool, nil
	})
	var reported error
	instance.hooks.PoolCreateError = func(_, _ string, err error) { reported = err }
	require.False(t, instance.EnsurePool(context.Background()))
	assert.Nil(t, instance.Pool())
	assert.True(t, pool.closed)
	assert.ErrorIs(t, reported, expected)
}

func TestInstanceRegularPoolDoesNotResize(t *testing.T) {
	pool := &instanceTestPool{resizeErr: errors.New("unexpected resize")}
	instance := NewInstance(InstanceConfig{
		PoolFactory: func(context.Context, string) (ConnectionPool, error) { return pool, nil },
	})
	assert.False(t, instance.IsResizablePool())
	require.EqualError(t, instance.ResizeMaxConns(10), "dynamic resizing is disabled")
	require.True(t, instance.EnsurePool(context.Background()))
	assert.Same(t, pool, instance.Pool())
}

func TestTraditionalInstanceIgnoresResizableVirtualLimit(t *testing.T) {
	instance := NewInstance(InstanceConfig{ResizableVirtualLimit: 50})
	assert.False(t, instance.isResizable)
	assert.Zero(t, instance.resizableVirtualLimit)
}
