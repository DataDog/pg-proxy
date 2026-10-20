// Package warden owns portable runtime inventory and lifecycle coordination.
package warden

import (
	"iter"
	"sync"

	"github.com/DataDog/pg-proxy/pkg/session"
)

// Sessions is the concurrent inventory of active client sessions.
type Sessions struct {
	sessions *ConcurrentMap[Identifier, session.SessionHandle]
	wg       sync.WaitGroup
}

// NewSessions creates an empty session inventory.
func NewSessions() *Sessions {
	return &Sessions{sessions: NewConcurrentMap(make(map[Identifier]session.SessionHandle))}
}

// Register adds a session and includes it in shutdown waiting.
func (s *Sessions) Register(id string, handle session.SessionHandle) {
	// TODO: Making duplicate registration or deletion idempotent would change
	// shutdown accounting. Preserve the existing one-Register/one-Delete
	// lifecycle contract until that behavior is changed explicitly.
	s.sessions.Put(Identifier(id), handle)
	s.wg.Add(1)
}

// Delete removes a session and releases its shutdown waiter.
func (s *Sessions) Delete(id string) {
	s.sessions.Delete(Identifier(id))
	s.wg.Done()
}

// Get returns a session by identifier.
func (s *Sessions) Get(id string) (session.SessionHandle, bool) {
	return s.sessions.Get(Identifier(id))
}

// All holds the inventory read lock for the duration of iteration, as before
// extraction. Consume promptly; callbacks must not register or delete sessions.
func (s *Sessions) All() iter.Seq2[Identifier, session.SessionHandle] {
	return s.sessions.All()
}

// Wait blocks until all registered sessions are deleted.
func (s *Sessions) Wait() {
	s.wg.Wait()
}
