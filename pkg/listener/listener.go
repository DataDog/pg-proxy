// Package listener owns the portable PostgreSQL listener and startup engine.
package listener

import (
	"bytes"
	"context"
	crypto_rand "crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgproto3"

	pgproxy "github.com/DataDog/pg-proxy"
	"github.com/DataDog/pg-proxy/pkg/limiter"
	"github.com/DataDog/pg-proxy/pkg/protocol"
	pgsession "github.com/DataDog/pg-proxy/pkg/session"
)

const (
	overCapGateRefreshInterval     = time.Second
	loadSheddingStartupPeekTimeout = time.Second
)

// StartupRequest is the portable information parsed from a startup packet.
type StartupRequest struct {
	User       string
	Database   string
	Parameters map[string]string
	Options    SessionOptions
}

// StartupResult is app-resolved startup data. T remains app-owned and typed.
type StartupResult[T any] struct {
	LogicalDatabase   string
	ParameterStatuses map[string]string
	Data              T
}

// AuthenticationResult is the authenticated client identity.
type AuthenticationResult struct {
	Service string
}

// SessionConfig is the complete portable handoff to the session factory.
type SessionConfig[T any] struct {
	Context    context.Context
	Request    StartupRequest
	Resolved   StartupResult[T]
	PID        uint32
	SecretKey  []byte
	Client     *pgproto3.Backend
	ClientConn net.Conn
	Service    string
	MetricTags []string
}

// Session is the listener-facing portion of a client session.
type Session interface {
	pgsession.SessionHandle
	User() string
	Database() string
	Service() string
	MetricTags() []string
	DynamicRoutingEnabled() bool
}

// LoadShedder holds an admission slot for the complete connection lifetime.
type LoadShedder interface {
	Run(context.Context, func()) error
}

// Admission owns portable global and per-service session accounting.
type Admission interface {
	RegisterSession(string) error
	DeregisterSession(string) error
	GetGlobalCount() int32
	EffectiveGlobalLimit() (int32, string)
	GetErrorType(error) string
	GetCapMode(error) string
}

// Lifecycle keeps app-owned session inventory and shutdown waiting outside the engine.
// TODO: A dedicated RFC/control-plane change may replace these lifecycle callbacks.
type Lifecycle struct {
	Register func(Session)
	Delete   func(Session)
	Lookup   func(uint32) (pgsession.SessionHandle, bool)
	WaitAll  func()
}

// Hooks are the narrow app seams used by the portable listener engine.
type Hooks[T any] struct {
	ResolveStartup func(context.Context, StartupRequest) (StartupResult[T], error)
	Authenticate   func(context.Context, StartupRequest, StartupResult[T], protocol.Exchange) (AuthenticationResult, error)
	NewSession     func(SessionConfig[T]) (Session, error)
	Lifecycle      Lifecycle
	LoadShedder    LoadShedder

	AcceptLoopRejectEnabled func(context.Context) bool
}

// Config contains only portable listener settings and app callbacks.
type Config[T any] struct {
	ListenAddress string
	TLSConfig     *tls.Config
	RequireTLS    bool
	Admission     Admission
	Hooks         Hooks[T]

	// Testing seams. Production leaves these nil.
	Listen func(string, string) (net.Listener, error)
	// TLSConnUpgrader defaults to tls.Server. It cannot be tls.Server itself:
	// tls.Server returns *tls.Conn, and Go function types are not covariant.
	TLSConnUpgrader func(net.Conn, *tls.Config) net.Conn
}

// Listener accepts, negotiates, authenticates, admits, creates, and runs sessions.
type Listener[T any] struct {
	ctx       context.Context
	cfg       Config[T]
	ln        net.Listener
	connCount atomic.Uint32

	cachedAcceptLoopReject atomic.Bool
	effectiveGlobalCap     atomic.Int32
	overCapRejectCount     atomic.Int64
}

// ClientNotified marks an error for which the client was already sent a fatal
// response, such as a session-limit rejection. The marker suppresses a duplicate
// client response and error log further up the connection stack.
func ClientNotified(err error) error { return &clientNotifiedError{cause: err} }

type clientNotifiedError struct{ cause error }

func (e *clientNotifiedError) Error() string {
	return fmt.Sprintf("client notified rejection: %v", e.cause)
}

// Unwrap keeps the underlying admission error visible to errors.Is and errors.As.
func (e *clientNotifiedError) Unwrap() error { return e.cause }

// New creates and binds a listener.
func New[T any](ctx context.Context, cfg Config[T]) (*Listener[T], error) {
	if cfg.Admission == nil {
		return nil, errors.New("listener admission controller is required")
	}
	if cfg.Hooks.ResolveStartup == nil || cfg.Hooks.NewSession == nil {
		return nil, errors.New("listener startup resolver and session factory are required")
	}
	listen := cfg.Listen
	if listen == nil {
		listen = net.Listen
	}
	ln, err := listen("tcp", cfg.ListenAddress)
	if err != nil {
		return nil, err
	}
	return &Listener[T]{ctx: ctx, cfg: cfg, ln: ln}, nil
}

// Addr returns the bound listener address.
func (l *Listener[T]) Addr() net.Addr { return l.ln.Addr() }

// Close stops accepting new connections.
func (l *Listener[T]) Close() error { return l.ln.Close() }

// Run serves connections until shutdown or an accept error.
func (l *Listener[T]) Run() error {
	pgproxy.Logger().Info("Listening", "address", l.cfg.ListenAddress)
	l.startOverCapGateRefresher()
	for {
		select {
		case <-l.ctx.Done():
			_ = l.ln.Close()
			// The listener is now closed but some sessions might still be running.
			// We need to wait for them here, as the process will halt after exiting this function.
			if l.cfg.Hooks.Lifecycle.WaitAll != nil {
				l.cfg.Hooks.Lifecycle.WaitAll()
			}
			return nil
		default:
			conn, err := l.ln.Accept()
			if err != nil {
				return err
			}
			// Note: this gate runs before attemptTLSUpgrade/handleFirstMessage reads the startup
			// packet, so it can't distinguish a CancelRequest (which targets an existing session,
			// not a new one) from any other incoming connection. We deliberately don't special-case
			// it: fleet-wide cancel volume is low (<2/s including misrouted cancels) next to
			// the reject rates this gate exists for, CancelRequest is already best-effort with no
			// client-visible ack, and a lost cancel converges to the same outcome as a client that
			// times out and disconnects — pg-proxy still detects that and closes the backend
			// connection. Treating cancels like any other connection also avoids doing extra
			// per-connection work on exactly the path we're trying to keep cheap under load.
			if l.cachedAcceptLoopReject.Load() {
				if capLimit := l.effectiveGlobalCap.Load(); capLimit > 0 && l.cfg.Admission.GetGlobalCount() >= capLimit {
					l.overCapRejectCount.Add(1)
					// Bare drop, no PG protocol reply: at this point we haven't read a single byte from the client,
					// so we don't yet know the client's request type — each expects a different
					// reply framing. Sending anything meaningful would require reading and parsing that first,
					// which is exactly the TLS/auth-adjacent work this gate exists to skip. A bare close maximizes the
					// sustainable reject rate under a real retry storm.
					_ = conn.Close()
					continue
				}
			}
			configureKeepalive(conn)
			go l.handleConnWithLoadShedding(conn)
		}
	}
}

func configureKeepalive(conn net.Conn) {
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return
	}
	// Enable TCP keepalive to detect network partitions (client disappears
	// without sending FIN/RST). Without keepalive, a silently-dead client
	// holds its proxy session open indefinitely — the kernel never discovers
	// the peer is gone because no probes are sent on an idle socket.
	//
	// When enabled, the kernel sends probes after an idle period and marks
	// the socket as dead if they go unanswered. Our settings give worst-case
	// detection of KEEPIDLE + KEEPCNT*KEEPINTVL = 15s + 3*15s = 60s.
	//
	// Go stdlib reference:
	//   - net.TCPConn.SetKeepAlive:       https://pkg.go.dev/net#TCPConn.SetKeepAlive
	//   - net.TCPConn.SetKeepAlivePeriod: https://pkg.go.dev/net#TCPConn.SetKeepAlivePeriod
	//     (sets both TCP_KEEPIDLE and TCP_KEEPINTVL since Go 1.23)
	//   - syscall.SetsockoptInt for TCP_KEEPCNT: not exposed by net; requires raw fd access.
	if err := tcpConn.SetKeepAlive(true); err != nil {
		pgproxy.Logger().Warn("Failed to enable TCP keepalive", "error", err)
	}
	if err := tcpConn.SetKeepAlivePeriod(15 * time.Second); err != nil {
		pgproxy.Logger().Warn("Failed to set TCP keepalive period", "error", err)
	}
	if rawConn, err := tcpConn.SyscallConn(); err == nil {
		_ = rawConn.Control(func(fd uintptr) {
			if err := syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_KEEPCNT, 3); err != nil {
				pgproxy.Logger().Warn("Failed to set TCP_KEEPCNT", "error", err)
			}
		})
	}
}

func (l *Listener[T]) startOverCapGateRefresher() {
	// Refresh once before the accept loop so the first connection never sees a
	// zero cached cap while the ticker has not yet fired.
	l.refreshOverCapGate()
	go func() {
		ticker := time.NewTicker(overCapGateRefreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-l.ctx.Done():
				return
			case <-ticker.C:
				l.refreshOverCapGate()
			}
		}
	}()
}

func (l *Listener[T]) refreshOverCapGate() {
	enabled := false
	if l.cfg.Hooks.AcceptLoopRejectEnabled != nil {
		enabled = l.cfg.Hooks.AcceptLoopRejectEnabled(l.ctx)
	}
	l.cachedAcceptLoopReject.Store(enabled)
	capLimit, capMode := l.cfg.Admission.EffectiveGlobalLimit()
	l.effectiveGlobalCap.Store(capLimit)
	if count := l.overCapRejectCount.Swap(0); count > 0 {
		_ = pgproxy.Metrics().Count("dd.orgstore.pg_proxy.listener.connection_attempt", count,
			[]string{"outcome:error", "error_type:" + string(limiter.GlobalLimitExceeded), "cap_mode:" + capMode, "placement:accept_reject"}, 1)
	}
}

// handleConnWithLoadShedding wraps connection handling with load shedding.
func (l *Listener[T]) handleConnWithLoadShedding(conn net.Conn) {
	if l.cfg.Hooks.LoadShedder == nil {
		l.handleConn(conn)
		return
	}
	if err := l.cfg.Hooks.LoadShedder.Run(l.ctx, func() { l.handleConn(conn) }); err != nil {
		// Send an error response to a connection rejected by the load shedder.
		handleLoadSheddingRejection(conn)
	}
}

// handleConn accepts a network connection and adds it to the listener for tracking.
func (l *Listener[T]) handleConn(conn net.Conn) {
	processed, first, err := l.attemptTLSUpgrade(conn)
	// If it is non-nil, processed is the connection that should be used for the rest
	// of the session. It may be unchanged or TLS-wrapped. Override conn before
	// deferring Close so the correct connection is closed on exit.
	if processed != nil {
		conn = processed
	}
	defer conn.Close()
	if err != nil {
		l.handleConnError(conn, err, "tls_upgrade_failure")
		return
	}
	session, err := l.handleFirstMessage(conn, first)
	if err != nil {
		l.handleConnError(conn, err, "first_message_failure")
		return
	}

	pgproxy.Logger().Debug("Opened connection", "remote", conn.RemoteAddr().String(), "user", session.User(), "database", session.Database())
	tags := append(session.MetricTags(), "outcome:successful",
		fmt.Sprintf("with_dynamic_routing:%t", session.DynamicRoutingEnabled()))
	_ = pgproxy.Metrics().Count("dd.orgstore.pg_proxy.listener.connection_attempt", 1, tags, 1)
	if l.cfg.Hooks.Lifecycle.Register != nil {
		l.cfg.Hooks.Lifecycle.Register(session)
	}
	defer func() {
		if l.cfg.Hooks.Lifecycle.Delete != nil {
			l.cfg.Hooks.Lifecycle.Delete(session)
		}
		if err := l.cfg.Admission.DeregisterSession(session.Service()); err != nil {
			logDeregisterError(session.Service(), err, false)
		}
	}()

	sessionDone := make(chan struct{})
	defer close(sessionDone)
	l.startDrainWatcher(sessionDone, conn)
	if err := session.Run(); err != nil && shouldReportError(err) {
		logSessionRunError(conn, err)
	}
}

func logSessionRunError(conn net.Conn, err error) {
	message := fmt.Sprintf("running session for connection from %v failed: %v", conn.RemoteAddr().String(), err)
	// PostgreSQL FATAL errors terminate the connection and retain the legacy
	// ERROR-level signal; all other session failures are warnings.
	if strings.Contains(err.Error(), "FATAL") {
		pgproxy.Logger().Error(message)
	} else {
		pgproxy.Logger().Warn(message)
	}
}

func logDeregisterError(service string, err error, rollback bool) {
	if rollback {
		pgproxy.Logger().Warn(fmt.Sprintf("error rolling back session registration for service %v: %v", service, err))
		return
	}
	pgproxy.Logger().Warn(fmt.Sprintf("error deregistering service %v, error: %v", service, err))
}

func (l *Listener[T]) startDrainWatcher(sessionDone <-chan struct{}, conn net.Conn) {
	go func() {
		select {
		// This is necessary because we need to make sure these goroutines are closed if the session
		// is closed to prevent leaking goroutines.
		case <-sessionDone:
		case <-l.ctx.Done():
			_ = conn.SetReadDeadline(time.Now())
		}
	}()
}

// attemptTLSUpgrade optimistically fetches the first client message to check
// whether it is an SSLRequest. If so, the base connection is promoted to TLS;
// otherwise the connection and already-received message are returned unchanged.
//
// This happens outside handleFirstMessage because errors bubble to handleConn,
// which must know which connection to use for the client error response.
func (l *Listener[T]) attemptTLSUpgrade(conn net.Conn) (net.Conn, pgproto3.FrontendMessage, error) {
	pgproxy.Logger().Debug("Accepted connection", "remote", conn.RemoteAddr())
	// Backend because the proxy is the PostgreSQL backend for this client.
	client := pgproto3.NewBackend(conn, conn)
	first, err := client.ReceiveStartupMessage()
	if err != nil {
		return nil, nil, fmt.Errorf("error receiving startup message: %w", err)
	}
	if _, ok := first.(*pgproto3.SSLRequest); !ok {
		return conn, first, nil
	}
	if l.cfg.TLSConfig == nil {
		if _, err = conn.Write([]byte("N")); err != nil {
			return nil, nil, fmt.Errorf("error sending SSL denied request: %w", err)
		}
	} else {
		if _, err = conn.Write([]byte("S")); err != nil {
			return nil, nil, fmt.Errorf("error sending SSL supported request: %w", err)
		}
		upgrade := l.cfg.TLSConnUpgrader
		if upgrade == nil {
			// cannot use tls.Server directly because of type mismatch,
			// as it returns *tls.Conn instead of net.Conn
			upgrade = func(conn net.Conn, cfg *tls.Config) net.Conn { return tls.Server(conn, cfg) }
		}
		conn = upgrade(conn, l.cfg.TLSConfig)
		client = pgproto3.NewBackend(conn, conn)
	}
	first, err = client.ReceiveStartupMessage()
	if err != nil {
		return nil, nil, fmt.Errorf("error receiving post-ssl startup message: %w", err)
	}
	return conn, first, nil
}

// handleFirstMessage handles the first PostgreSQL message: a StartupMessage or
// CancelRequest (an SSLRequest has already been consumed by attemptTLSUpgrade).
func (l *Listener[T]) handleFirstMessage(conn net.Conn, message pgproto3.FrontendMessage) (Session, error) {
	switch msg := message.(type) {
	case *pgproto3.StartupMessage:
		return l.handleStartup(conn, msg)
	case *pgproto3.CancelRequest:
		if err := l.handleCancel(msg); err != nil {
			return nil, fmt.Errorf("failed to cancel request: %w", err)
		}
		return nil, io.EOF
	default:
		return nil, fmt.Errorf("error unknown startup message: %#v", message)
	}
}

// handleStartup creates a new session for an incoming network connection.
func (l *Listener[T]) handleStartup(conn net.Conn, msg *pgproto3.StartupMessage) (Session, error) {
	user, ok := msg.Parameters["user"]
	if !ok {
		return nil, errors.New("user name not provided")
	}
	database, ok := msg.Parameters["database"]
	if !ok {
		database = user
	}
	options, err := ParseSessionOptions(msg.Parameters["options"])
	if err != nil {
		return nil, fmt.Errorf("error parsing message options: %w", err)
	}
	request := StartupRequest{User: user, Database: database, Parameters: msg.Parameters, Options: options}
	resolved, err := l.cfg.Hooks.ResolveStartup(l.ctx, request)
	if err != nil {
		return nil, err
	}
	if resolved.LogicalDatabase == "" {
		resolved.LogicalDatabase = database
	}

	// Backend because the proxy is the PostgreSQL backend for this client.
	client := pgproto3.NewBackend(conn, conn)
	auth := AuthenticationResult{Service: user}
	if l.cfg.Hooks.Authenticate != nil {
		auth, err = l.cfg.Hooks.Authenticate(l.ctx, request, resolved, protocol.NewConnExchange(conn, client))
		if err != nil {
			return nil, err
		}
		if auth.Service == "" {
			auth.Service = user
		}
	}
	tags := []string{
		"user:" + user,
		"database:" + database,
		"client_service_account:" + auth.Service,
		fmt.Sprintf("passthrough_user:%t", options.PassthroughUser),
	}

	// Enforce the session cap AFTER authentication (so the per-service limit sees the
	// authenticated service identity) but BEFORE issuing the backend PID / ReadyForQuery.
	// A rejected client is sent a FATAL 53300 and never receives a PID. This prevents
	// the client from later orphan-cancelling a session that was never created.
	registered := false
	defer func() {
		// Roll back the reserved slot if handleStartup fails after registering but before
		// handing the session to handleConn (which then owns deregistration on success).
		if registered {
			if err := l.cfg.Admission.DeregisterSession(auth.Service); err != nil {
				logDeregisterError(auth.Service, err, true)
			}
		}
	}()
	if err := l.cfg.Admission.RegisterSession(auth.Service); err != nil {
		// Whether the client was notified is tracked as a metric tag rather than logged:
		// under a capacity incident this fires once per rejected connection.
		writeErr := handleSessionLimitExceeded(conn, err)
		metricTags := append(tags, "outcome:error", "error_type:"+l.cfg.Admission.GetErrorType(err),
			"cap_mode:"+l.cfg.Admission.GetCapMode(err), "placement:register_session",
			fmt.Sprintf("client_notified:%t", writeErr == nil))
		_ = pgproxy.Metrics().Count("dd.orgstore.pg_proxy.listener.connection_attempt", 1, metricTags, 1)
		// Suppress a duplicate client response / error log in handleConn — the client
		// was already sent the 53300 above.
		return nil, ClientNotified(err)
	}
	registered = true

	pid := l.connCount.Add(1)
	secretUint32, err := generateSecret()
	if err != nil {
		return nil, fmt.Errorf("error generating secret key: %w", err)
	}
	secret := secretKeyFromUint32(secretUint32)
	// TODO: Preserved the legacy lifecycle ordering during extraction. NewSession can fail
	// after ReadyForQuery has been sent; in a follow-up, either make the factory infallible
	// or construct the session before sending the successful startup response.
	if err := writeStartupResponse(conn, resolved.ParameterStatuses, pid, secret); err != nil {
		return nil, err
	}
	session, err := l.cfg.Hooks.NewSession(SessionConfig[T]{
		Context: l.ctx, Request: request, Resolved: resolved, PID: pid, SecretKey: secret,
		Client: client, ClientConn: conn, Service: auth.Service, MetricTags: tags,
	})
	if err != nil {
		return nil, err
	}
	// Success — hand ownership of the reserved slot to handleConn; don't roll back.
	registered = false
	return session, nil
}

func writeStartupResponse(conn net.Conn, statuses map[string]string, pid uint32, secret []byte) error {
	var buf []byte
	var err error
	buf, err = (&pgproto3.AuthenticationOk{}).Encode(buf)
	if err != nil {
		return fmt.Errorf("error encoding authentication ok message: %v", err)
	}
	keys := make([]string, 0, len(statuses))
	for key := range statuses {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		buf, err = (&pgproto3.ParameterStatus{Name: key, Value: statuses[key]}).Encode(buf)
		if err != nil {
			return fmt.Errorf("error encoding parameter status %q: %v", key, err)
		}
	}
	buf, err = (&pgproto3.BackendKeyData{ProcessID: pid, SecretKey: secret}).Encode(buf)
	if err != nil {
		return fmt.Errorf("error encoding backend key data: %v", err)
	}
	buf, err = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
	if err != nil {
		return fmt.Errorf("error encoding ready for query message: %v", err)
	}
	if _, err = conn.Write(buf); err != nil {
		return fmt.Errorf("error sending ready for query: %w", err)
	}
	return nil
}

func (l *Listener[T]) handleCancel(msg *pgproto3.CancelRequest) error {
	if l.cfg.Hooks.Lifecycle.Lookup == nil {
		return fmt.Errorf("no session found for pid: %d", msg.ProcessID)
	}
	session, ok := l.cfg.Hooks.Lifecycle.Lookup(msg.ProcessID)
	if !ok {
		return fmt.Errorf("no session found for pid: %d", msg.ProcessID)
	}
	if !bytes.Equal(session.SecretKey(), msg.SecretKey) {
		return fmt.Errorf("incorrect cancel request secret key for pid: %d", msg.ProcessID)
	}
	return session.Cancel()
}

// handleLoadSheddingRejection rejects and closes a connection that was shed
// before being accepted. It peeks at the first startup message: if the client
// sent an SSLRequest it replies with "N" (a raw ErrorResponse before TLS would
// be unreadable); otherwise it sends a proper ErrorResponse.
func handleLoadSheddingRejection(conn net.Conn) {
	defer conn.Close()
	err := errors.New("This pg-proxy instance is experiencing high load and has rejected the connection as a protection. Please retry.")
	// Bound reads AND writes so a shed connection that never sends startup bytes
	// (or never drains the error response) cannot block this goroutine indefinitely.
	if deadlineErr := conn.SetDeadline(time.Now().Add(loadSheddingStartupPeekTimeout)); deadlineErr != nil {
		handleConnError(conn, err, "load_shedding")
		return
	}
	defer conn.SetDeadline(time.Time{})
	backend := pgproto3.NewBackend(conn, conn)
	if msg, readErr := backend.ReceiveStartupMessage(); readErr == nil {
		if _, isSSL := msg.(*pgproto3.SSLRequest); isSSL {
			_ = pgproxy.Metrics().Count("dd.orgstore.pg_proxy.listener.connection_attempt", 1,
				[]string{"outcome:error", "placement:load_shedding"}, 1)
			_, _ = conn.Write([]byte("N"))
			return
		}
	}
	handleConnError(conn, err, "load_shedding")
}

// handleSessionLimitExceeded sends a FATAL SQLSTATE 53300 to a client whose
// connection is rejected because a session cap was reached, giving the client
// a clean, retryable backoff signal instead of a bare drop. The message
// distinguishes global proxy capacity from a per-service quota. It returns the
// encode error, or the write error (nil on success).
func handleSessionLimitExceeded(conn net.Conn, err error) error {
	message := "pg-proxy is at capacity. Please retry."
	var limitErr *limiter.Error
	if errors.As(err, &limitErr) && limitErr.Type == limiter.ServiceLimitExceeded {
		message = "Service connection limit reached. Please retry."
	}
	buf, encodeErr := (&pgproto3.ErrorResponse{
		Severity: "FATAL", Code: pgerrcode.TooManyConnections, Message: message,
	}).Encode(nil)
	if encodeErr != nil {
		return encodeErr
	}
	_, writeErr := conn.Write(buf)
	return writeErr
}

func (l *Listener[T]) handleConnError(conn net.Conn, err error, placement string) {
	handleConnError(conn, err, placement)
}

// handleConnError handles a connection error and emits a metric identifying
// where it occurred.
//
// TODO: This preserves the pre-extraction behavior of sending err.Error() to
// the client and reporting every startup failure as 08006 ConnectionFailure.
// Introduce typed client errors carrying a safe message and SQLSTATE, log the
// detailed cause here, and never serialize arbitrary hook errors. PostgreSQL
// authentication failures should use 28P01 invalid_password so drivers do not
// retry them like backend connection failures.
func handleConnError(conn net.Conn, err error, placement string) {
	if !shouldReportError(err) {
		return
	}
	pgproxy.Logger().Error(fmt.Sprintf("failed to open connection from %s: %v", conn.RemoteAddr().String(), err))
	_ = pgproxy.Metrics().Count("dd.orgstore.pg_proxy.listener.connection_attempt", 1,
		[]string{"outcome:error", "placement:" + placement}, 1)
	buf, encodeErr := (&pgproto3.ErrorResponse{
		Severity: "FATAL", Code: pgerrcode.ConnectionFailure, Message: err.Error(),
	}).Encode(nil)
	if encodeErr == nil {
		_, _ = conn.Write(buf)
	}
}

// shouldReportError determines whether a given error should be transmitted to the client application.
// Some errors are expected to happen occasionally and we don't want to forward them
// as customers sometimes get concerned when they see these errors in the logs.
func shouldReportError(err error) bool {
	// Sentinel values are matched with errors.Is (value equality through
	// the error chain). Custom error types like session.ClientDisconnectedError
	// require errors.As (type matching) because there is no single sentinel
	// to compare against.
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return false
	}
	var disconnected *pgsession.ClientDisconnectedError
	if errors.As(err, &disconnected) {
		return false
	}
	// A session-limit rejection already sent the client a FATAL 53300 and emitted its
	// own metric; suppress it here so handleConn doesn't double-respond or double-log.
	var notified *clientNotifiedError
	return !errors.As(err, &notified)
}

// secretKeyFromUint32 encodes a uint32 as a 4-byte big-endian secret key.
func secretKeyFromUint32(value uint32) []byte {
	return binary.BigEndian.AppendUint32(nil, value)
}

// generateSecret returns a cryptographically random uint32 for use as the
// BackendKeyData.SecretKey sent to clients during startup. This secret
// authenticates query-cancel requests (CancelRequest messages): the client
// must echo it back for pg-proxy to accept the cancellation. Using
// crypto/rand — rather than math/rand — ensures the value is unpredictable,
// preventing a malicious client from guessing other sessions' keys and
// cancelling their queries.
func generateSecret() (uint32, error) {
	var secret [4]byte
	if _, err := crypto_rand.Read(secret[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(secret[:]), nil
}
