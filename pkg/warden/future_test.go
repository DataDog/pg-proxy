package warden

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSettledFuture(t *testing.T) {
	pending := newSettledFuture(1)
	assert.True(t, pending.IsReady())
	value, err := pending.Get()
	assert.NoError(t, err)
	assert.Equal(t, 1, value)
	assert.False(t, pending.Cancel())
}

func TestRetriableFutureExhausted(t *testing.T) {
	pending := newRetriableFuture(
		context.Background(),
		func(context.Context, func() (int, error)) (int, error) {
			return 0, errors.New("exhausted")
		},
		func() (int, error) { return 0, errors.New("fail") },
		nil,
	)
	<-pending.done
	assert.False(t, pending.IsReady())
	_, err := pending.Get()
	assert.ErrorIs(t, err, errFutureNotReady)
}

func TestRetriableFutureCancelAfterSuccess(t *testing.T) {
	pending := newRetriableFuture(
		context.Background(),
		nil,
		func() (int, error) { return 42, nil },
		nil,
	)

	<-pending.done
	value, err := pending.Get()
	assert.NoError(t, err)
	assert.Equal(t, 42, value)
	assert.True(t, pending.IsReady())
	assert.False(t, pending.Cancel())
}

func TestRetriableFutureCancelStopsRetries(t *testing.T) {
	var attempts atomic.Int32
	started := make(chan struct{}, 10)
	pending := newRetriableFuture(
		context.Background(),
		func(ctx context.Context, computation func() (int, error)) (int, error) {
			for {
				attempts.Add(1)
				started <- struct{}{}
				if _, err := computation(); err == nil {
					return 1, nil
				}
				select {
				case <-ctx.Done():
					return 0, ctx.Err()
				case <-time.After(time.Millisecond):
				}
			}
		},
		func() (int, error) { return 0, errors.New("fail") },
		nil,
	)

	<-started
	assert.True(t, pending.Cancel())
	<-pending.done
	afterCancel := attempts.Load()
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, afterCancel, attempts.Load())
	assert.False(t, pending.IsReady())
}

func TestRetriableFutureParentContextCancelStopsRetries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var attempts atomic.Int32
	started := make(chan struct{}, 10)
	pending := newRetriableFuture(
		ctx,
		func(ctx context.Context, computation func() (int, error)) (int, error) {
			for {
				attempts.Add(1)
				started <- struct{}{}
				if _, err := computation(); err == nil {
					return 1, nil
				}
				select {
				case <-ctx.Done():
					return 0, ctx.Err()
				case <-time.After(time.Millisecond):
				}
			}
		},
		func() (int, error) { return 0, errors.New("fail") },
		nil,
	)

	<-started
	cancel()
	<-pending.done
	afterCancel := attempts.Load()
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, afterCancel, attempts.Load())
	assert.False(t, pending.IsReady())
}
