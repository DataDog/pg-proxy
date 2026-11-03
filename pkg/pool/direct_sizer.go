package pool

import (
	"fmt"
	"sync"
)

// DirectPoolSizer serializes resizing with publication of discovery-created instances.
// Static instances must be registered before exposing the pool to resize requests.
type DirectPoolSizer struct {
	mu    sync.Mutex
	state *DirectState
	limit int32
}

func NewDirectPoolSizer(state *DirectState, limit int32) *DirectPoolSizer {
	return &DirectPoolSizer{state: state, limit: limit}
}

func (s *DirectPoolSizer) Limit() int32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.limit
}

// Store applies the latest accepted limit before making an instance visible.
// The caller retains ownership and must close the instance on failure.
func (s *DirectPoolSizer) Store(address string, instance PostgresInstance) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := resizeDirectInstance(instance, s.limit); err != nil {
		return err
	}
	s.state.Store(address, instance)
	return nil
}

// Resize prevents discovery from publishing a stale limit during the sweep.
func (s *DirectPoolSizer) Resize(limit int32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var resizeErr error
	s.state.Range(func(address string, instance PostgresInstance) bool {
		if err := resizeDirectInstance(instance, limit); err != nil {
			resizeErr = fmt.Errorf("resize instance %s: %w", address, err)
			return false
		}
		return true
	})
	if resizeErr != nil {
		return resizeErr
	}
	s.limit = limit
	return nil
}

func resizeDirectInstance(instance PostgresInstance, limit int32) error {
	sizer, ok := instance.(interface{ ResizeMaxConns(int32) error })
	if !ok {
		return fmt.Errorf("postgres instance does not support dynamic resizing")
	}
	return sizer.ResizeMaxConns(limit)
}
