// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package session

import (
	"context"

	"github.com/jackc/pgx/v5/pgproto3"

	pgproxy "github.com/DataDog/pg-proxy"
	"github.com/DataDog/pg-proxy/pkg/link"
	"github.com/DataDog/pg-proxy/pkg/pool"
)

const observerPanicMetric = "pg_proxy.session.observer_panic"

// CheckoutMeta describes the backend connection acquired for one checkout.
// Passed by value to observers; it never exposes the link or raw connection.
type CheckoutMeta struct {
	ClusterName   string
	Pool          pool.Pool
	EffectiveRole string
	Rerouted      bool
	ForReadQuery  bool
	// Annotation holds the optional value returned by a backend CheckoutAnnotator.
	Annotation any
}

// AcquiredLink pairs a checked-out link with its checkout metadata.
type AcquiredLink struct {
	Link link.Link
	Meta CheckoutMeta
}

// CheckoutBackend is implemented by backends that return CheckoutMeta alongside
// each checked-out link.
type CheckoutBackend interface {
	Backend
	Checkout(context.Context, LinkRequest) (AcquiredLink, error)
}

// Observer receives read-only session lifecycle and protocol notifications.
//
// Every callback runs synchronously on the session goroutine with no session
// locks held. Implementations must return promptly, must not mutate message
// values, and must copy any message data retained after returning.
//
// If a callback panics, the session recovers, disables only that observer,
// calls its OnClosed once (if not already closed), and continues serving the
// session and any remaining observers.
//
// Embed NoopObserver to implement only the callbacks needed.
type Observer interface {
	OnCheckout(CheckoutMeta)
	OnRelease()
	OnFrontendMessage(pgproto3.FrontendMessage)
	OnBackendMessage(pgproto3.BackendMessage)
	OnClientFlushSucceeded()
	OnClosed()
}

// PanicNotifier is optionally implemented by an Observer that needs to emit its
// own telemetry when the session recovers a panic from one of its callbacks.
type PanicNotifier interface {
	OnPanic(hook string, recovered any)
}

// NoopObserver is an embeddable Observer whose methods do nothing.
type NoopObserver struct{}

func (NoopObserver) OnCheckout(CheckoutMeta)                    {}
func (NoopObserver) OnRelease()                                 {}
func (NoopObserver) OnFrontendMessage(pgproto3.FrontendMessage) {}
func (NoopObserver) OnBackendMessage(pgproto3.BackendMessage)   {}
func (NoopObserver) OnClientFlushSucceeded()                    {}
func (NoopObserver) OnClosed()                                  {}

type observerSlot struct {
	observer Observer
	active   bool
}

func initObserverSlots(observers []Observer) []observerSlot {
	if len(observers) == 0 {
		return nil
	}
	slots := make([]observerSlot, 0, len(observers))
	for _, obs := range observers {
		if obs != nil {
			slots = append(slots, observerSlot{observer: obs, active: true})
		}
	}
	return slots
}

func (s *Session) disableObserverOnPanic(slot *observerSlot, hook string, recovered any) {
	slot.active = false
	s.recordObserverPanic(hook)
	pgproxy.Logger().Error("session observer panicked and was disabled", "hook", hook, "panic", recovered, "session_id", s.pid)
	if notifier, ok := slot.observer.(PanicNotifier); ok {
		s.callObserverPanicNotifier(notifier, hook, recovered)
	}
	if hook != "closed" {
		s.callObserverClosed(slot)
	}
}

func (s *Session) callObserverPanicNotifier(notifier PanicNotifier, hook string, recovered any) {
	defer func() {
		if r := recover(); r != nil {
			pgproxy.Logger().Error("session observer OnPanic panicked", "hook", hook, "panic", r, "session_id", s.pid)
		}
	}()
	notifier.OnPanic(hook, recovered)
}

func (s *Session) notifyCheckout(meta CheckoutMeta) {
	notifyObservers(s, "checkout", meta, Observer.OnCheckout)
}

func (s *Session) notifyRelease() {
	notifyObservers(s, "release", struct{}{}, func(o Observer, _ struct{}) { o.OnRelease() })
}

func (s *Session) notifyFrontendMessage(msg pgproto3.FrontendMessage) {
	notifyObservers(s, "frontend_message", msg, Observer.OnFrontendMessage)
}

func (s *Session) notifyBackendMessage(msg pgproto3.BackendMessage) {
	notifyObservers(s, "backend_message", msg, Observer.OnBackendMessage)
}

func (s *Session) notifyClientFlushSucceeded() {
	notifyObservers(s, "client_flush_succeeded", struct{}{}, func(o Observer, _ struct{}) { o.OnClientFlushSucceeded() })
}

// notifyObservers runs call for each active observer. A panic disables only
// that observer, and hook names the callback in the panic metric and log.
func notifyObservers[T any](s *Session, hook string, arg T, call func(Observer, T)) {
	for i := range s.observers {
		callObserver(s, &s.observers[i], hook, arg, call)
	}
}

func callObserver[T any](s *Session, slot *observerSlot, hook string, arg T, call func(Observer, T)) {
	if !slot.active {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			s.disableObserverOnPanic(slot, hook, r)
		}
	}()
	call(slot.observer, arg)
}

func (s *Session) notifyClosed() {
	for i := range s.observers {
		slot := &s.observers[i]
		if !slot.active {
			continue
		}
		slot.active = false
		s.callObserverClosed(slot)
	}
}

func (s *Session) callObserverClosed(slot *observerSlot) {
	defer func() {
		if r := recover(); r != nil {
			s.disableObserverOnPanic(slot, "closed", r)
		}
	}()
	slot.observer.OnClosed()
}
