// Package server composes the portable listener, session, and runtime inventory.
package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"

	"github.com/DataDog/pg-proxy/pkg/listener"
	"github.com/DataDog/pg-proxy/pkg/protocol"
	"github.com/DataDog/pg-proxy/pkg/session"
	"github.com/DataDog/pg-proxy/pkg/topology"
)

// SessionRegistry owns active session lookup and shutdown coordination.
type SessionRegistry interface {
	Register(string, session.SessionHandle)
	Delete(string)
	Get(string) (session.SessionHandle, bool)
	Wait()
}

// Config contains listener settings, caller-owned inventories, and app integrations.
// NewServer wires startup resolution, session construction, and lifecycle internally.
type Config struct {
	ListenAddress string
	TLSConfig     *tls.Config
	RequireTLS    bool
	Admission     listener.Admission
	Authenticate  func(context.Context, listener.StartupRequest, listener.StartupResult[ResolvedStartup], protocol.Exchange) (listener.AuthenticationResult, error)
	LoadShedder   listener.LoadShedder
	Runtime       RuntimeConfig
	Sessions      SessionRegistry
}

// Server owns the listener and the runtime used to construct its sessions.
type Server struct {
	listener *listener.Listener[ResolvedStartup]
	runtime  *Runtime
}

// NewServer composes and binds a PostgreSQL proxy server.
func NewServer(ctx context.Context, cfg Config) (*Server, error) {
	if cfg.Sessions == nil {
		return nil, errors.New("server session registry is required")
	}
	if cfg.Runtime.Inventory == nil {
		return nil, errors.New("server routing inventory is required")
	}
	runtime := NewRuntime(cfg.Runtime)
	engine, err := listener.New(ctx, listener.Config[ResolvedStartup]{
		ListenAddress: cfg.ListenAddress,
		TLSConfig:     cfg.TLSConfig,
		RequireTLS:    cfg.RequireTLS,
		Admission:     cfg.Admission,
		Hooks: listener.Hooks[ResolvedStartup]{
			ResolveStartup: runtime.ResolveStartup,
			NewSession:     runtime.NewSession,
			Authenticate:   cfg.Authenticate,
			LoadShedder:    cfg.LoadShedder,
			Lifecycle:      sessionLifecycle(cfg.Sessions),
		},
	})
	if err != nil {
		return nil, err
	}
	return &Server{listener: engine, runtime: runtime}, nil
}

// ResolveReadStates exposes the server's read routing view to metrics adapters.
func (s *Server) ResolveReadStates(reads topology.ReadClusters) ([]session.ReadStateConfig, topology.ReadClusters) {
	return s.runtime.ResolveReadStates(reads)
}

func sessionLifecycle(sessions SessionRegistry) listener.Lifecycle {
	return listener.Lifecycle{
		Register: func(active listener.Session) {
			sessions.Register(active.Identifier(), active)
		},
		Delete: func(active listener.Session) {
			sessions.Delete(active.Identifier())
		},
		Lookup: func(pid uint32) (session.SessionHandle, bool) {
			return sessions.Get(fmt.Sprintf("%d", pid))
		},
		WaitAll: sessions.Wait,
	}
}

// Run accepts connections until shutdown.
func (s *Server) Run() error {
	return s.listener.Run()
}

// Close stops accepting new connections.
func (s *Server) Close() error {
	return s.listener.Close()
}

// Addr returns the bound listener address.
func (s *Server) Addr() net.Addr {
	return s.listener.Addr()
}
