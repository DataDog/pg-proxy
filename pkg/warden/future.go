package warden

import (
	"context"
	"errors"
	"sync"
	"time"
)

var errFutureNotReady = errors.New("future not ready")

type future[T any] struct {
	mu      sync.RWMutex
	cancel  context.CancelFunc
	done    chan struct{}
	value   T
	ready   bool
	readyAt time.Time
}

func newRetriableFuture[T any](
	ctx context.Context,
	retry Retry[T],
	computation func() (T, error),
	abandoned func(error),
) *future[T] {
	innerCtx, cancel := context.WithCancel(ctx)
	pending := &future[T]{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer cancel()
		var value T
		var err error
		if retry == nil {
			value, err = computation()
		} else {
			value, err = retry(innerCtx, computation)
		}
		if err != nil {
			if abandoned != nil {
				abandoned(err)
			}
			pending.abandon()
			return
		}
		pending.settle(value)
	}()
	return pending
}

func newSettledFuture[T any](value T) *future[T] {
	pending := &future[T]{done: make(chan struct{})}
	pending.settle(value)
	return pending
}

func (f *future[T]) settle(value T) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancel = nil
	f.value = value
	f.ready = true
	f.readyAt = time.Now()
	close(f.done)
}

func (f *future[T]) abandon() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancel = nil
	close(f.done)
}

func (f *future[T]) IsReady() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.ready
}

func (f *future[T]) Done() <-chan struct{} { return f.done }

func (f *future[T]) ReadyAt() (time.Time, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.readyAt, f.ready
}

func (f *future[T]) Get() (T, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if !f.ready {
		return f.value, errFutureNotReady
	}
	return f.value, nil
}

func (f *future[T]) Cancel() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.ready || f.cancel == nil {
		return false
	}
	f.cancel()
	return true
}
