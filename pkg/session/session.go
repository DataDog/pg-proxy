// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

// Package session owns the portable PostgreSQL client session engine.
package session

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	pgproxy "github.com/DataDog/pg-proxy"
	"github.com/DataDog/pg-proxy/pkg/actions"
	"github.com/DataDog/pg-proxy/pkg/link"
	"github.com/DataDog/pg-proxy/pkg/mirror"
	"github.com/DataDog/pg-proxy/pkg/prepared"
	"github.com/DataDog/pg-proxy/pkg/query"
)

const (
	// maxRetainedReplyBufCap bounds the client reply buffer a session keeps between replies. Buffers
	// up to this size are retained so small replies don't re-allocate; anything larger is released
	// after the reply so an idle connection never pins a big buffer. Set to 1 KiB to match the
	// legacy pgproto3.Backend behaviour of retaining only its sub-1KB write buffer. At ~thousands of
	// connections this is the per-connection idle-memory ceiling for the forwarding path.
	maxRetainedReplyBufCap = 1024 // 1 KiB

	// pooledReplyBufCap is the capacity of a new replyBufPool buffer: twice the default
	// PostgresMessageBufferSize, so that a reply reaches the flush threshold without growing it.
	pooledReplyBufCap = 20 << 10 // 20 KiB
	// maxPooledReplyBufCap bounds the buffers that return to replyBufPool. A buffer that grew
	// past it for a very large message is dropped, so the pool does not keep it.
	maxPooledReplyBufCap = 256 << 10 // 256 KiB
)

// replyBufPool lends buffers to replies that outgrow the session's retained buffer, when
// Config.ReplyBufferPool is set. A reply takes a buffer when it reaches maxRetainedReplyBufCap/2
// bytes, or when one message makes it larger than maxRetainedReplyBufCap, and returns the buffer
// after its last write to the client.
// Thus a reply of many rows does not grow a new buffer each time, and the memory follows the
// large replies in progress, not the number of sessions.
var replyBufPool = sync.Pool{New: func() any {
	buf := make([]byte, 0, pooledReplyBufCap)
	return &buf
}}

// drainReadPollInterval is the client read deadline re-armed while a session is
// being gracefully drained but is not yet idle, so a blocked Receive() wakes
// periodically to make progress / re-check state instead of blocking forever.
const drainReadPollInterval = 100 * time.Millisecond

// isReadTimeout reports whether a client Receive() error is a net read-deadline
// timeout. In this proxy a client read deadline is only ever armed to unblock a
// stuck Receive() during graceful drain (by the listener's drain watcher or by
// drain()), so callers treat a read timeout as "draining" — but this helper only
// asserts the timeout itself, not its cause.
func isReadTimeout(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

// resetDrainReadDeadline re-arms a short client read deadline so a subsequent
// blocked Receive() wakes within drainReadPollInterval. It is a no-op when we do
// not hold the underlying net.Conn (e.g. non-TCP connections / unit tests), in
// which case no drain read deadline is in effect to begin with.
func (s *Session) resetDrainReadDeadline() {
	if s.clientNetConn != nil {
		_ = s.clientNetConn.SetReadDeadline(time.Now().Add(drainReadPollInterval))
	}
}

// receiveMessage reads the next client message, transparently absorbing the read
// deadline timeouts used for graceful drain. Its callers only ever read while they
// already hold in-flight work — a backend checkout (handleTransaction) or a
// discard-until-sync pipeline (drainUntilSync) — so a drain timeout must never
// abort them: it re-arms the deadline and retries until the work is served to
// completion. Only a real message or a genuine (non-timeout) error is returned.
//
// This is the single choke-point for in-flight client reads, so those loops can't
// forget the drain handling. The top-level read in receiveAndHandleMessage does
// NOT go through here: there the session is idle and a drain timeout should let
// Run()/drain() wind the session down, so it handles the timeout itself.
func (s *Session) receiveMessage() (pgproto3.FrontendMessage, error) {
	for {
		msg, err := s.clientConn.Receive()
		// Only absorb the drain read timeout when we hold the underlying net.Conn
		// and can actually re-arm the deadline. Otherwise resetDrainReadDeadline is
		// a no-op, the (past) deadline stays in effect, and this loop would busy-spin
		// on repeated timeouts. A client read deadline is only ever armed on TCP
		// conns (clientNetConn != nil), so a read timeout with no net.Conn is
		// unexpected — surface it rather than spin.
		if err != nil && isReadTimeout(err) && s.clientNetConn != nil {
			s.resetDrainReadDeadline()
			continue
		}
		if err == nil {
			s.recordClientMessageBodyLength(msg)
		}
		return msg, err
	}
}

// A Session owns the lifetime of a client connection in the proxy. It is the core struct of the proxy.
//
// A Session is created by the Listener after a new TCP connection is successfully opened, (optionally)
// encrypted via TLS, and then authenticated. The Session exists until the client's connection is
// terminated.
//
// A Session will be linked to a Postgres connection only for the duration that it's active. It is active
// in the middle of a multi-statement transaction, or during an extended query pipeline. This is roughly
// comparable to PgBouncer's "transaction pooling mode". Once the postgres conn returns to an idle "ready for query"
// state, and after the client has seen all the replies it expected, the link is released and the postgres
// conn is returned to the pool (for use by any session).
type Session struct {
	ctx             context.Context
	user            string // Postgres user of the connected client
	database        string // Postgres database of the connected client
	logicalDatabase string // Name of the logical DB the session was created for
	pid             uint32 // PID sent back to the client (may be used for query cancellation)
	secretKey       []byte // SecretKey sent back to the client (may be used for query cancellation)
	service         string // User's service that requested the session

	clientConn    ClientTransport     // The client's post-authentication protocol transport
	livenessProbe ClientLivenessProbe // non-nil for TCP connections; nil for non-TCP (e.g. tests)

	// clientNetConn is the underlying client connection (same destination clientConn writes to).
	// It is used to set read deadlines while draining, and the high-volume PG->client forwarding
	// path in flushPostgres buffers encoded replies in clientReplyBuf and writes them here directly,
	// reusing that buffer across the flushes of a single reply. This avoids pgproto3.Backend.Flush,
	// which discards (and then re-grows) any write buffer larger than 1KB on every flush — a
	// production profile showed that churn in DataRow.Encode dominated the proxy's allocations.
	clientNetConn net.Conn
	// clientReplyBuf is the reusable PG->client forwarding buffer. It is reused across the flushes
	// of a reply AND retained across replies on this connection so small replies don't re-allocate.
	// It stays within maxRetainedReplyBufCap so an idle connection never pins a large buffer: a
	// reply that outgrows it grows its own buffer, or, with Config.ReplyBufferPool, continues in
	// a buffer from replyBufPool.
	clientReplyBuf []byte
	link           link.Link                        // non-nil when this session is linked to a postgres connection (session goroutine only)
	abortHandle    atomic.Pointer[link.AbortHandle] // out-of-band Cancel/ForceClose handle for the active link

	backend                      Backend
	readStates                   []*ReadState
	readStatesFunc               func() []ReadStateConfig
	randomInt                    func(int) int
	dynamicRoutingEnabled        bool
	lastWrite                    time.Time // If dynamic routing is enabled, denotes when this session last received a write query
	postgresMsgBufferSize        int       // The number of bytes the proxy buffers from PG before flushing to the client
	replyBufferPool              bool      // replies that outgrow clientReplyBuf use replyBufPool; see Config.ReplyBufferPool
	passthroughUser              bool
	priority                     Priority
	discardUntilSync             bool // whether the Session should discard all messages it receives until it gets a Sync message
	pStmtState                   prepared.PreparedStatementState
	AutoPreparedStmtsEnabled     bool
	autoPreparedStmtsEnabledFunc func() bool

	// The client's unnamed prepared statement, which Postgres keeps until the next
	// unnamed Parse, Close of it, or simple Query, and whether the current link's
	// connection has it. Releasing a link discards it, so it is re-created on the
	// next link that a Bind or Describe uses.
	unnamedStmt       prepared.PreparedStatement
	unnamedStmtOnLink bool
	// unnamedGen counts the client messages that replace or destroy the unnamed
	// statement. Replies arrive after later messages are sent, so each unnamed
	// Parse sent to Postgres records the generation it belongs to, in send order;
	// a ParseComplete from an older generation must not restore a statement that a
	// later message replaced or destroyed.
	unnamedGen         uint64
	unnamedPendingGens []uint64

	// reportMu orders remaps and their hook reports. Remap* callers hold it to queue
	// and report; the session goroutine holds it to apply and report, and is the only
	// writer of backend and readStates. Other goroutines read those under it.
	reportMu sync.Mutex
	// pending holds the remaps queued for the next message boundary, or nil. It is
	// replaced under reportMu and read without a lock.
	pending atomic.Pointer[pendingRemap]

	obsVals               *SessionObservabilityValues
	metrics               Metrics
	hooks                 Hooks
	observers             []observerSlot
	decorateLink          bool
	enableTraceParameters bool
	paramTypes            *bindParamTypes // allocated on the first statement Describe

	clientDisconnected                 *atomic.Bool    // set when a client disconnect is detected
	acquireCtx                         context.Context // context for connection acquisition, set by client monitor; in timer mode it lasts for the session, not one checkout
	clientDisconnectTimer              bool            // check for disconnects with timerMonitor; see Config.ClientDisconnectTimer
	timerMonitor                       *timerMonitor   // created at the first checkout when clientDisconnectTimer is set
	livenessProbeChanged               bool            // set by SetLivenessProbe so the next checkout rebuilds timerMonitor
	backendDestroyedOnClientDisconnect atomic.Bool     // true after recording that this session destroyed its backend because the client disconnected

	mirroring Mirroring

	mirror         *mirror.Queue  // per-session mirror queue; nil until the first mirrored link
	mirrorBudget   *mirror.Budget // shared, process-wide byte budget for all mirror queues (set by the listener)
	mirrorWorker   *mirror.Worker // drains s.mirror onto the shadow DB; started with s.mirror
	mirrorTap      *mirror.Tap    // per-checkout mirroring tap; nil when mirroring is off or no link is held
	hooksCloseOnce sync.Once
}

var _ SessionHandle = (*Session)(nil)

// pendingRemap is a snapshot of the queued remaps. A nil field has nothing queued.
type pendingRemap struct {
	backend    Backend
	readStates *[]ReadStateConfig
}

// pendingRemaps returns a copy of the queued remaps.
func (s *Session) pendingRemaps() pendingRemap {
	if pending := s.pending.Load(); pending != nil {
		return *pending
	}
	return pendingRemap{}
}

// ClientTransport is the post-authentication PostgreSQL frontend transport.
type ClientTransport interface {
	Receive() (pgproto3.FrontendMessage, error)
	Send(pgproto3.BackendMessage)
	Flush() error
}

// LinkRequest is the portable checkout state passed to the app backend adapter.
type LinkRequest struct {
	DynamicRoutingEnabled bool
	ForReadQuery          bool
	SessionLastWrite      time.Time
	PassthroughRoleName   string
	MaxPreparedStmts      int
	MetricTags            []string
	TraceState            *query.TraceState
	// DecorateLink asks the backend to run LinkDecorator for this checkout.
	// The listener sets it on the session independently of notification hooks.
	DecorateLink bool
}

// LoadShedder gates one complete backend checkout.
type LoadShedder interface {
	Run(context.Context, func() error, Priority) (fnErr error, shedErr error)
}

// Backend acquires links and supplies checkout-scoped load shedding.
type Backend interface {
	Acquire(context.Context, LinkRequest) (link.Link, error)
	LoadShedder() LoadShedder
}

// ReadStateConfig holds the backend and weight for a read cluster, used when
// remapping read routing on existing sessions after a read-cluster config change.
type ReadStateConfig struct {
	Backend Backend
	Weight  uint8
}

// SessionHandle is the narrow handle retained by the host to run, identify,
// cancel, and reconfigure a session.
type SessionHandle interface {
	Identifier() string
	LogicalDatabaseID() string
	Run() error
	RemapBackend(Backend)
	// RemapReadStates signals the session to update its read-cluster routing on
	// the next idle point.
	RemapReadStates([]ReadStateConfig)
	Cancel() error
	SecretKey() []byte
}

// MirrorPolicy is a snapshot of app-owned mirroring configuration.
type MirrorPolicy struct {
	Enabled        bool
	SourceDatabase string
	ShadowDatabase string
	DisableAt      time.Time
	QueueSize      int
}

// Mirroring leaves policy sources, experiments, and shadow state resolution to the embedder.
type Mirroring struct {
	Policy        func(context.Context) MirrorPolicy
	AcquireShadow func(context.Context, string, LinkRequest) (link.Link, error)
}

// Hooks expose optional session state transition notifications. Callbacks run
// synchronously on the session goroutine, except where noted, and must return
// promptly. They must not mutate message values. A callback that retains message
// data after it returns must copy that data first.
type Hooks struct {
	LinkChanged          func(link.Link)
	RoutedLinkChanged    func(link.Link)
	FrontendMessage      func(pgproto3.FrontendMessage)
	BackendMessage       func(pgproto3.BackendMessage)
	ClientFlushSucceeded func()
	Closed               func()
	// BackendChanged and ReadStatesChanged report remaps:
	//   - Queued: (current, pending) on the Remap* caller's goroutine. pending is
	//     never nil: RemapReadStates(nil) reports an empty slice, and
	//     RemapBackend(nil), which cancels a queued backend, reports the current one.
	//   - Applied: (applied, nil) on the session goroutine, at a message boundary.
	//   - Order: one session's reports never overlap and arrive in the order the
	//     remaps take effect. Each report holds a lock the other goroutine needs, so
	//     keep reports fast.
	//   - Calls: reports must not call RemapBackend or RemapReadStates. Reports on
	//     the caller's goroutine may call Backend and PendingBackend, but not
	//     CurrentLink.
	BackendChanged    func(current Backend, pending Backend)
	ReadStatesChanged func(current []ReadStateConfig, pending []ReadStateConfig)
	DiscardChanged    func(bool)
}

// Config contains only portable session inputs.
type Config struct {
	Context               context.Context
	User                  string
	Database              string
	LogicalDatabase       string
	PID                   uint32
	SecretKey             []byte
	Service               string
	Client                ClientTransport
	ClientConn            net.Conn
	LivenessProbe         ClientLivenessProbe
	Backend               Backend
	ReadStates            []ReadStateConfig
	ReadStatesFunc        func() []ReadStateConfig
	RandomInt             func(int) int
	DynamicRoutingEnabled bool
	PostgresMsgBufferSize int
	// ReplyBufferPool makes a reply that reaches half of the session's retained
	// reply buffer, or whose message outgrows it, continue in a buffer from
	// replyBufPool. The pool is shared by all sessions. Off by default: then each
	// reply larger than maxRetainedReplyBufCap grows its own buffer.
	ReplyBufferPool bool
	PassthroughUser bool
	// Priority is the query-level workload priority for backend load shedding.
	Priority                     Priority
	AutoPreparedStmtsEnabled     bool
	AutoPreparedStmtsEnabledFunc func() bool
	Mirroring                    Mirroring
	MirrorBudget                 *mirror.Budget
	PreparedStatements           prepared.PreparedStatementState
	TraceState                   *query.TraceState
	// EnableTraceParameters includes raw Bind values in sampled query traces for every backend.
	// The zero value disables parameter capture. SQL text is unaffected.
	EnableTraceParameters bool
	MetricTags            []string
	Metrics               Metrics
	ClientDisconnected    *atomic.Bool
	// ClientDisconnectTimer selects how the session checks LivenessProbe during a
	// checkout. False, the default, starts a goroutine and a ticker for each
	// checkout. True keeps one timer and one acquire context for the session and
	// arms the timer for each checkout.
	ClientDisconnectTimer bool
	Hooks                 Hooks
	Observers             []Observer
	// DecorateLink requests decoration when the session checks out a routed link.
	// The listener sets this explicitly; installing hooks does not enable it.
	DecorateLink             bool
	InitialLink              link.Link
	InitialCheckoutMeta      CheckoutMeta
	InitialClientReplyBuffer []byte
	InitialDiscardUntilSync  bool
}

// New constructs the canonical production session engine.
func New(cfg Config) *Session {
	readStates := make([]*ReadState, 0, len(cfg.ReadStates))
	for _, rs := range cfg.ReadStates {
		readStates = append(readStates, &ReadState{backend: rs.Backend, weight: rs.Weight})
	}
	tags := cfg.MetricTags
	if tags == nil {
		tags = []string{
			fmt.Sprintf("user:%s", cfg.User),
			fmt.Sprintf("database:%s", cfg.Database),
			fmt.Sprintf("client_service_account:%s", cfg.Service),
			fmt.Sprintf("passthrough_user:%t", cfg.PassthroughUser),
		}
	}
	preparedStatements := cfg.PreparedStatements
	if preparedStatements == nil {
		preparedStatements = prepared.CreateProxyPreparedStmtsState(tags)
	}
	disconnected := cfg.ClientDisconnected
	if disconnected == nil {
		disconnected = &atomic.Bool{}
	}
	randomInt := cfg.RandomInt
	if randomInt == nil {
		randomInt = rand.Intn
	}
	s := &Session{
		ctx: cfg.Context, user: cfg.User, database: cfg.Database,
		logicalDatabase: cfg.LogicalDatabase, pid: cfg.PID,
		secretKey: append([]byte(nil), cfg.SecretKey...), service: cfg.Service,
		clientConn: cfg.Client, clientNetConn: cfg.ClientConn, livenessProbe: cfg.LivenessProbe,
		clientReplyBuf: append([]byte(nil), cfg.InitialClientReplyBuffer...),
		backend:        cfg.Backend, readStates: readStates,
		readStatesFunc: cfg.ReadStatesFunc, randomInt: randomInt, dynamicRoutingEnabled: cfg.DynamicRoutingEnabled,
		postgresMsgBufferSize: cfg.PostgresMsgBufferSize, replyBufferPool: cfg.ReplyBufferPool,
		passthroughUser:              cfg.PassthroughUser,
		priority:                     cfg.Priority,
		discardUntilSync:             cfg.InitialDiscardUntilSync,
		pStmtState:                   preparedStatements,
		AutoPreparedStmtsEnabled:     cfg.AutoPreparedStmtsEnabled,
		autoPreparedStmtsEnabledFunc: cfg.AutoPreparedStmtsEnabledFunc,
		obsVals:                      createSessionObservabilityValues(cfg.User, cfg.Database, cfg.Service, cfg.PassthroughUser),
		metrics:                      resolveMetrics(cfg.Metrics),
		mirroring:                    cfg.Mirroring, mirrorBudget: cfg.MirrorBudget,
		clientDisconnected: disconnected, hooks: cfg.Hooks, observers: initObserverSlots(cfg.Observers),
		decorateLink:          cfg.DecorateLink,
		enableTraceParameters: cfg.EnableTraceParameters,
		clientDisconnectTimer: cfg.ClientDisconnectTimer,
	}
	if cfg.TraceState != nil {
		s.obsVals.traceState = cfg.TraceState
	}
	if cfg.MetricTags != nil {
		s.obsVals.setMetricTags(cfg.MetricTags)
	}
	if cfg.InitialLink != nil {
		s.attachCheckout(AcquiredLink{Link: cfg.InitialLink, Meta: cfg.InitialCheckoutMeta})
	}
	return s
}

// ClientDisconnectedError indicates the PG connection was discarded because
// the client disconnected during query processing.
type ClientDisconnectedError struct {
	cause error
}

func (e *ClientDisconnectedError) Error() string {
	return fmt.Sprintf("client disconnected: %v", e.cause)
}

// Unwrap exposes the original socket or acquisition error to errors.As/errors.Is.
func (e *ClientDisconnectedError) Unwrap() error {
	return e.cause
}

// NewClientDisconnectedError marks an error as caused by a client disconnect.
func NewClientDisconnectedError(err error) error {
	return &ClientDisconnectedError{cause: err}
}

type clientDisconnectedError = ClientDisconnectedError

// SessionObservabilityValues is a struct that holds session values helpful for o11y
type SessionObservabilityValues struct {
	traceState          *query.TraceState
	metricTags          []string // Important tags for metrics that we'd like to keep track of
	baseMetricTagsCache MetricTags
	// Extended version of metricTags for message counters and latency distributions,
	// keyed by message type. Each cached slice appends message:<type> to the baseline.
	messageMetricTagCache map[string]MetricTags

	user                 string
	database             string
	clientServiceAccount string
}

func createSessionObservabilityValues(user, database, clientServiceAccount string, passthroughUser bool) *SessionObservabilityValues {
	values := &SessionObservabilityValues{
		user:                 user,
		database:             database,
		clientServiceAccount: clientServiceAccount,
		traceState:           &query.TraceState{},
	}
	values.setMetricTags([]string{
		fmt.Sprintf("user:%s", user),
		fmt.Sprintf("database:%s", database),
		fmt.Sprintf("client_service_account:%s", clientServiceAccount),
		fmt.Sprintf("passthrough_user:%t", passthroughUser),
	})
	return values
}

// ReadState contains the states and related information with which to distribute reads
type ReadState struct {
	backend Backend
	weight  uint8
}

const (
	// PostgreSQL Error Codes (See: https://www.postgresql.org/docs/15/errcodes-appendix.html)
	pgErrInsufficientPrivilege      = "42501"
	pgErrTransactionRollback        = "40000"
	pgErrInvalidSqlStatement        = "26000"
	pgErrDuplicatePreparedStatement = "42P05"
	pgErrSyntaxError                = "42601"
)

// This error response is used when a session's state is remapped during a transaction
func buildTransactionRollbackErrorResponse() *pgproto3.ErrorResponse {
	// See https://www.postgresql.org/docs/15/protocol-error-fields.html
	// and https://pkg.go.dev/github.com/jackc/pgproto3/v2#ErrorResponse
	return &pgproto3.ErrorResponse{
		Severity: "FATAL",
		Code:     pgErrTransactionRollback,
		Message:  "The connection to the Postgres server was terminated",
		Hint:     "Please retry the query",
	}
}

// This error response is used when a client attempts to re-use a named prepared statement.
func buildPreparedStatementsAlreadyExistsErrorResponse(name string) *pgproto3.ErrorResponse {
	return &pgproto3.ErrorResponse{
		Severity: "ERROR",
		Code:     pgErrDuplicatePreparedStatement,
		Message:  "Named prepared statement with this name already exists. It must be deallocated before reusing it.",
		Detail:   fmt.Sprintf("Received named prepared statement with name: `%s`", name),
		Hint:     "Please use a new named prepared statement name or deallocate the existing named prepared statement.",
	}
}

// This error response is used when a client attempts to use a named prepared statement that does not exist.
func buildPreparedStatementsDoesNotExistErrorResponse(name string) *pgproto3.ErrorResponse {
	return &pgproto3.ErrorResponse{
		Severity: "ERROR",
		Code:     pgErrInvalidSqlStatement,
		Message:  "Named prepared statement with this name does not exist.",
		Detail:   fmt.Sprintf("Received named prepared statement with name: `%s`", name),
		Hint:     "Please prepare the statement name with the given name before attempting to use it.",
	}
}

// This error response is used when a client attempts to execute a query that cannot be parsed.
func buildFailedToParseQueryErrorResponse(query string) *pgproto3.ErrorResponse {
	return &pgproto3.ErrorResponse{
		Severity: "ERROR",
		Code:     pgErrSyntaxError,
		Message:  "Failed to parse query",
		Detail:   fmt.Sprintf("Received query: `%s`", query),
		Hint:     "Please check your query for syntax errors",
	}
}

func buildReadyForQueryIdleMessage() pgproto3.BackendMessage {
	return &pgproto3.ReadyForQuery{TxStatus: 'I'}
}

// isExtendedQueryMessage reports whether msg is part of the Postgres extended query
// protocol (Parse/Bind/Execute/Describe/Close). These messages require ReadyForQuery
// to be sent only after a Sync, unlike simple queries which reply immediately.
func isExtendedQueryMessage(msg pgproto3.FrontendMessage) bool {
	switch msg.(type) {
	case *pgproto3.Parse, *pgproto3.Bind, *pgproto3.Execute, *pgproto3.Describe, *pgproto3.Close:
		return true
	default:
		return false
	}
}

func (s *Session) LogicalDatabaseID() string   { return s.logicalDatabase }
func (s *Session) User() string                { return s.user }
func (s *Session) Database() string            { return s.database }
func (s *Session) Service() string             { return s.service }
func (s *Session) PID() uint32                 { return s.pid }
func (s *Session) SecretKey() []byte           { return s.secretKey }
func (s *Session) DynamicRoutingEnabled() bool { return s.dynamicRoutingEnabled }
func (s *Session) MetricTags() []string        { return s.obsVals.metricTags }
func (s *Session) TraceState() *query.TraceState {
	return s.obsVals.traceState
}

// CurrentLink and Backend take no lock. Call them on the session goroutine, or
// from a hook as Hooks allows.
func (s *Session) CurrentLink() link.Link { return s.link }
func (s *Session) ClientDisconnected() bool {
	return s.clientDisconnected.Load()
}
func (s *Session) ClientDisconnectTimer() bool { return s.clientDisconnectTimer }
func (s *Session) PreparedStatements() prepared.PreparedStatementState {
	return s.pStmtState
}
func (s *Session) DiscardUntilSync() bool  { return s.discardUntilSync }
func (s *Session) Backend() Backend        { return s.backend }
func (s *Session) PendingBackend() Backend { return s.pendingRemaps().backend }
func (s *Session) ReadStates() []ReadStateConfig {
	return readStatesToConfig(s.currentReadStates())
}

// SetLivenessProbe replaces the client liveness probe. The next checkout uses
// it. Call it from the goroutine that runs the session.
func (s *Session) SetLivenessProbe(probe ClientLivenessProbe) {
	s.livenessProbe = probe
	s.livenessProbeChanged = true
}
func (s *Session) SetAutoPreparedStatementsEnabled(enabled bool) {
	s.AutoPreparedStmtsEnabled = enabled
}
func (s *Session) SetPostgresMessageBufferSize(size int) {
	s.postgresMsgBufferSize = size
}
func (s *Session) autoPreparedStatementsEnabled() bool {
	if s.autoPreparedStmtsEnabledFunc != nil {
		return s.autoPreparedStmtsEnabledFunc()
	}
	return s.AutoPreparedStmtsEnabled
}
func (s *Session) ReceiveAndHandleMessage() error { return s.receiveAndHandleMessage() }
func (s *Session) FlushPostgres() (bool, error)   { return s.flushPostgres() }
func (s *Session) HandleAction(action actions.ClientMessageAction) error {
	return s.handleAction(action)
}

// Close tears the session down. Call it from the goroutine that runs the
// session, not from a link or a hook that may run on another goroutine.
func (s *Session) Close(err error)               { s.close(err) }
func (s *Session) QueryLoadShedder() LoadShedder { return s.queryLoadShedder() }
func (s *Session) ApplyStateRemapping() bool     { return s.handleStateRemapping() }
func (s *Session) RecordNewPreparedStatement(action actions.ClientMessageAction) (bool, error) {
	return s.handleRecordNewPreparedStmt(action)
}
func (s *Session) ForceCloseBackendOnClientDisconnect(backendLink link.Link) {
	s.forceCloseHandleOnClientDisconnect(link.AbortHandleFor(backendLink))
}

func (s *Session) setLink(backendLink link.Link) {
	s.abortHandle.Store(link.AbortHandleFor(backendLink))
	s.link = backendLink
	s.unnamedStmtOnLink = false
	if backendLink == nil && s.hooks.RoutedLinkChanged != nil {
		s.hooks.RoutedLinkChanged(nil)
	}
	if s.hooks.LinkChanged != nil {
		s.hooks.LinkChanged(backendLink)
	}
}

// attachCheckout publishes a checked-out link and notifies observers.
func (s *Session) attachCheckout(acquired AcquiredLink) {
	s.setLink(acquired.Link)
	s.notifyCheckout(acquired.Meta)
}

func (s *Session) releaseCurrentLink(recordClientDisconnect bool) {
	if s.link == nil {
		return
	}
	s.mirrorTap.OnRelease()
	s.mirrorTap = nil
	destroyedByRelease := recordClientDisconnect && !s.link.Idle()
	// Fence out-of-band Cancel and ForceClose before the connection goes back to
	// the pool. MarkReleased is idempotent, so a link that also fences itself in
	// Release is unaffected.
	s.abortHandle.Swap(nil).MarkReleased()
	releaseErr := s.link.Release()
	if destroyedByRelease {
		s.recordBackendDestroyedOnClientDisconnect("release")
	}
	if releaseErr != nil {
		pgproxy.Logger().Warn("error releasing session link", "err", releaseErr)
	}
	s.setLink(nil)
	s.notifyRelease()
}

func (s *Session) sendToLink(msg pgproto3.FrontendMessage, expectation query.ReplyExpectation) {
	s.link.Send(msg, expectation)
	s.mirrorTap.OnSend(msg, expectation)
}

func (s *Session) checkoutBackend(ctx context.Context, routed Backend, request LinkRequest) (AcquiredLink, error) {
	if cb, ok := routed.(CheckoutBackend); ok {
		return cb.Checkout(ctx, request)
	}
	backendLink, err := routed.Acquire(ctx, request)
	if err != nil {
		return AcquiredLink{}, err
	}
	return AcquiredLink{Link: backendLink}, nil
}

func (s *Session) setDiscardUntilSync(discard bool) {
	s.discardUntilSync = discard
	if s.hooks.DiscardChanged != nil {
		s.hooks.DiscardChanged(discard)
	}
}

// Cancel runs on the listener's cancel-request goroutine. It touches only the
// active link's AbortHandle, which safely becomes a no-op once Release starts.
// With no link held the handle is nil and Cancel does nothing; the request may
// have arrived after the query completed.
// TODO(enhancement): if the session is currently waiting to acquire a link, we should definitely cancel that!
func (s *Session) Cancel() error {
	return s.abortHandle.Load().Cancel()
}

// Idle reports whether the session holds no busy link. Call on the session goroutine.
func (s *Session) Idle() bool {
	return s.link == nil || s.link.Idle()
}

// mirroringApplicable reports whether traffic mirroring should run for this session.
// Cheap field checks gate the feature-flag lookup so it's skipped when mirroring is off.
func (s *Session) mirroringApplicable() bool {
	if s.mirroring.Policy == nil {
		return false
	}
	m := s.mirroring.Policy(s.ctx)
	return m.Enabled &&
		s.logicalDatabase == m.SourceDatabase &&
		!m.DisableAt.IsZero() && time.Now().Before(m.DisableAt)
}

// handleOneMessage records metrics and delegates to handleClientMessage.
// On error it wraps as clientDisconnectedError when clientDisconnected is set,
// calls s.close, finishes the root trace, and returns a wrapped error.
// Note: messageCounter counts only messages that reach this function. Terminate
// and state-remapping skips bypass it and are NOT counted; pgx health-check pings
// bypass the load shedder but are still handled via this function, so they ARE
// counted.
func (s *Session) handleOneMessage(msg pgproto3.FrontendMessage) error {
	start := time.Now()
	message := query.MessageToText(msg)
	s.recordMessageStarted(message)

	handleErr := s.handleClientMessage(msg)
	if handleErr != nil {
		// Safety net: if the client monitor detected a disconnect and the error
		// wasn't already classified (e.g. from Flush rather than Receive), wrap
		// it so close() treats it as a benign disconnect.
		var cde *clientDisconnectedError
		if s.clientDisconnected.Load() && !errors.As(handleErr, &cde) {
			handleErr = &clientDisconnectedError{cause: handleErr}
		}
		s.close(handleErr)
		s.obsVals.traceState.FinishRootTrace(handleErr)
		return fmt.Errorf("error handling message %#v: %w", msg, handleErr)
	}
	s.recordMessageFinished(message, time.Since(start))
	return nil
}

// interceptBypassingMessage handles messages that must not go through the standard
// per-message dispatcher, returning (intercepted, err):
//   - Terminate: closes the session and returns (true, wrapped io.EOF) — terminal.
//   - pgx ping health check at an idle boundary: handled via handleOneMessage; returns
//     (true, err) where err is nil on success.
//   - any other message: returns (false, nil).
//
// Caller contract: intercepted == true means the message was handled here and must
// NOT be dispatched again. err != nil means the interception is terminal and the
// caller must stop (return err). When intercepted && err == nil the message was
// handled and processing may continue — the top-level loop returns nil to fetch the
// next message, and the inner loops (handleTransaction, drainUntilSync) `continue`.
// Pings with a held link follow the normal dispatcher, preserving pending work
// and transaction state. During error recovery they are discarded until Sync.
func (s *Session) interceptBypassingMessage(msg pgproto3.FrontendMessage) (bool, error) {
	if _, ok := msg.(*pgproto3.Terminate); ok {
		err := fmt.Errorf("client requested terminate: %w", io.EOF)
		// Mark the client as disconnected so link teardown treats this as a
		// client-initiated close (preserving main's pre-restructure Terminate
		// behavior), not a normal idle release.
		s.clientDisconnected.Store(true)
		s.close(err)
		return true, err
	}
	if actions.IsIdlePGXPing(msg, s.actionState()) {
		return true, s.handleOneMessage(msg)
	}
	return false, nil
}

// handleTransaction processes the initial message and then continues receiving
// and handling messages until the session link is released.
// Invariant: consumes all messages until the link is released, then returns.
// This is the function passed to the load shedder's Run; it holds the RIF slot
// for the full duration of a backend connection checkout.
func (s *Session) handleTransaction(initialMsg pgproto3.FrontendMessage) error {
	if err := s.handleOneMessage(initialMsg); err != nil {
		return err
	}
	for s.link != nil {
		msg, err := s.receiveMessage()
		if err != nil {
			if s.clientDisconnected.Load() {
				err = &clientDisconnectedError{cause: err}
			}
			s.close(err)
			return fmt.Errorf("error receiving client message: %w", err)
		}
		if s.handleStateRemapping() {
			return nil
		}
		// Terminate must close the session even while the checkout is held.
		if intercepted, err := s.interceptBypassingMessage(msg); intercepted {
			if err != nil {
				return err
			}
			continue
		}
		if err := s.handleOneMessage(msg); err != nil {
			return err
		}
	}
	return nil
}

// rejectWithLoadShedding sends a load-shedding rejection response to the client
// and, for extended query messages, drains the pipeline until the client sends Sync.
// NOTE: FinishRootTrace is intentionally omitted — the fn passed to
// loadShedder.Run never ran, so no root trace was started.
func (s *Session) rejectWithLoadShedding(msg pgproto3.FrontendMessage) error {
	pgproxy.Logger().Debug("query rejected by load shedding", "session_id", s.pid)
	s.recordLoadSheddingRejected()
	s.clientConn.Send(&pgproto3.ErrorResponse{
		Severity: "ERROR",
		Code:     pgerrcode.InsufficientResources,
		Message:  "This pg-proxy instance is experiencing high load and has rejected to serve the query as a protection. Please retry.",
	})
	if isExtendedQueryMessage(msg) {
		s.setDiscardUntilSync(true)
		s.clientConn.Flush()
		return s.drainUntilSync()
	}
	s.clientConn.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	s.clientConn.Flush()
	return nil
}

func (s *Session) queryLoadShedder() LoadShedder {
	return s.backend.LoadShedder()
}

// drainUntilSync processes messages while discardUntilSync is set, discarding
// all messages until a Sync clears the flag. Terminate closes the session; a pgx
// ping (like every other non-Sync message here) is turned into a no-op discard by
// DecideClientMessageAction, so it is ignored until Sync.
func (s *Session) drainUntilSync() error {
	for s.discardUntilSync {
		msg, err := s.receiveMessage()
		if err != nil {
			if s.clientDisconnected.Load() {
				err = &clientDisconnectedError{cause: err}
			}
			s.close(err)
			return fmt.Errorf("error receiving client message: %w", err)
		}
		if intercepted, err := s.interceptBypassingMessage(msg); intercepted {
			if err != nil {
				return err
			}
			continue
		}
		if err := s.handleOneMessage(msg); err != nil {
			return err
		}
	}
	return nil
}

func (s *Session) receiveAndHandleMessage() error {
	msg, err := s.clientConn.Receive()
	if err != nil {
		if isReadTimeout(err) {
			// Read deadline set while draining to unblock Receive(); return nil so Run()
			// re-checks the context and drains.
			return nil
		}
		// Record disconnect-shaped failures at the client socket. Backend I/O
		// failures may wrap the same errors, so close() cannot infer the cause.
		var netErr net.Error
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.As(err, &netErr) {
			s.clientDisconnected.Store(true)
		}
		s.close(err)
		return fmt.Errorf("error receiving client message: %w", err)
	}

	s.recordClientMessageBodyLength(msg)

	// The session may have been reset while waiting for a message.
	if shouldSkipMessage := s.handleStateRemapping(); shouldSkipMessage {
		return nil
	}

	// Bypass the load shedder for Terminate and idle pgx health-check pings.
	// Terminate must not reach the shedder — it would reply with an ErrorResponse
	// instead of cleanly closing the connection. Idle pgx pings (no held link, not
	// discarding until Sync) are answered locally so they don't count against load;
	// pings on a held link, empty queries and ";" are NOT bypassed.
	if intercepted, err := s.interceptBypassingMessage(msg); intercepted {
		return err
	}

	// Start client monitoring if a liveness probe is configured (set by the listener
	// when the feature flag is enabled and the connection is TCP).
	if s.livenessProbe != nil {
		monitor, monitorCtx := s.startClientMonitor()
		s.acquireCtx = monitorCtx
		defer func() {
			monitor.stop()
			s.acquireCtx = nil
		}()
	}

	loadShedder := s.queryLoadShedder()
	fnErr, lsErr := loadShedder.Run(s.ctx, func() error {
		return s.handleTransaction(msg)
	}, s.priority)
	if lsErr != nil {
		return s.rejectWithLoadShedding(msg)
	}
	return fnErr
}

// Run begins the session event loop, and returns only after the session is closed and fully cleaned up.
//
// This event loop proxies messages between the client and postgres, transforming messages as required
// (e.g. for statement caching). The event loop is entirely driven by the client. We only receive messages
// from postgres on client-triggered flush events.
func (s *Session) Run() error {
	for {
		select {
		case <-s.ctx.Done(): // This is the shutdown signal for the session
			// On shutdown we want to stop servicing traffic as soon as we can.
			return s.drain()
		default:
			err := s.receiveAndHandleMessage()
			if err != nil {
				return err
			}
		}
	}
}

// drainMessage identifies a drain error that clients may retry on a new connection.
// Send it only between calls, after any open transaction has finished.
// Keep the "Pg-proxy session has been terminated" prefix and this behavior
// compatible with deployed clients, which match on it.
const drainMessage = "Pg-proxy session has been terminated. Reconnect to continue."

// drain finishes the active call or transaction, then sends drainMessage
// and closes the client connection.
func (s *Session) drain() error {
	for {
		if s.Idle() {
			s.clientConn.Send(&pgproto3.ErrorResponse{
				Severity: "FATAL",
				Code:     pgerrcode.AdminShutdown,
				Message:  drainMessage,
			})
			s.clientConn.Flush()
			s.close(io.EOF)
			s.recordDrained()
			return nil
		}

		// We're draining but not yet idle (e.g. mid-transaction). Set a short read
		// deadline so we potentially finish a transaction instead of blocking forever.
		s.resetDrainReadDeadline()
		err := s.receiveAndHandleMessage()
		if err != nil {
			return err
		}
	}
}

// handleStateRemapping applies any pending warden config updates queued via
// RemapReadStates / RemapBackend before the next client message. The read-cluster
// remap is link-safe (only affects future acquisitions). A primary-cluster
// remap with a busy link force-releases the link (aborting any open tx) and
// sends the client a fresh ReadyForQuery; returns true in that case to signal
// the caller to skip the current message.
func (s *Session) handleStateRemapping() bool {
	if s.pending.Load() == nil {
		// Nothing to do here, business as usual
		return false
	}
	if !s.applyPendingRemaps() {
		return false
	}

	if s.link == nil {
		// if the link wasn't busy we can just pretend nothing happened and process the received message
		return false
	}
	// The link is currently busy, so we have to forcefully kill it.
	// This will close the underlying connection to PG as well so no
	// need to explicitly abort the ongoing transaction.
	s.releaseCurrentLink(false)

	// We need to handle in-flight transactions

	pgproxy.Logger().Warn("session link remapped during transaction", "session_id", s.pid)

	// Inform the client about the reset
	rollbackError := buildTransactionRollbackErrorResponse()
	if s.hooks.BackendMessage != nil {
		s.hooks.BackendMessage(rollbackError)
	}
	s.notifyBackendMessage(rollbackError)
	s.clientConn.Send(rollbackError)
	readyForQuery := buildReadyForQueryIdleMessage()
	if s.hooks.BackendMessage != nil {
		s.hooks.BackendMessage(readyForQuery)
	}
	s.notifyBackendMessage(readyForQuery)
	s.clientConn.Send(readyForQuery)
	if s.hooks.ClientFlushSucceeded == nil && len(s.observers) == 0 {
		s.clientConn.Flush()
	} else if s.flushClientConn() == nil {
		if s.hooks.ClientFlushSucceeded != nil {
			s.hooks.ClientFlushSucceeded()
		}
		s.notifyClientFlushSucceeded()
	}

	// TODO: this abort does not drain an in-flight extended-query pipeline
	// nor reset checkout-scoped state (pending prepared statements, root trace). After this
	// ReadyForQuery, a pipeline tail (Bind/Execute/Sync) can be admitted as a new
	// checkout out of protocol order, and can be independently load-shed. This is a
	// pre-existing gap, not introduced by the connection-checkout RIF change.
	// Skip processing the message
	return true
}

// applyPendingRemaps applies and reports the queued remaps, and returns whether the
// backend changed. Only the session goroutine calls it, after seeing a pending remap.
func (s *Session) applyPendingRemaps() bool {
	s.reportMu.Lock()
	defer s.reportMu.Unlock()
	remap := s.pending.Swap(nil)

	// Apply read states update first — no link disruption needed since this only
	// affects routing decisions for future link acquisitions.
	if remap.readStates != nil {
		newReadStates := make([]*ReadState, 0, len(*remap.readStates))
		for _, rs := range *remap.readStates {
			newReadStates = append(newReadStates, &ReadState{backend: rs.Backend, weight: rs.Weight})
		}
		s.readStates = newReadStates
		if len(newReadStates) > 0 {
			s.dynamicRoutingEnabled = true
		}
		if s.hooks.ReadStatesChanged != nil {
			s.hooks.ReadStatesChanged(readStatesToConfig(newReadStates), nil)
		}
	}
	if remap.backend == nil {
		return false
	}
	s.backend = remap.backend
	if s.hooks.BackendChanged != nil {
		s.hooks.BackendChanged(s.backend, nil)
	}
	return true
}

// RemapReadStates queues read routing for the next message boundary; the last call wins.
func (s *Session) RemapReadStates(readStates []ReadStateConfig) {
	if readStates == nil {
		readStates = []ReadStateConfig{} // a nil pending value marks the applied report
	}
	s.reportMu.Lock()
	defer s.reportMu.Unlock()
	next := s.pendingRemaps()
	next.readStates = &readStates
	s.pending.Store(&next)
	if s.hooks.ReadStatesChanged != nil {
		s.hooks.ReadStatesChanged(readStatesToConfig(s.readStates), readStates)
	}
}

func (s *Session) handleClientMessage(msg pgproto3.FrontendMessage) error {
	discardingWithoutLink := s.discardUntilSync && s.link == nil
	// Start the root trace before dispatching ONLY when we're not in
	// discard-until-sync mode. During a discard pipeline the messages
	// are silently dropped until we receive a Sync, whose handling is
	// fast-tracked and doesn't create a span.
	if !s.discardUntilSync {
		s.obsVals.traceState.StartRootTrace(msg)
	}

	action, err := s.DecideClientMessageAction(msg)
	if err != nil {
		return fmt.Errorf("error processing client message: %w", err)
	}
	// The no-link recovery Sync is handled before the decision engine's normal
	// observation callback. Expose that accepted boundary while continuing to
	// omit the messages discarded before it.
	if discardingWithoutLink && !action.DiscardUntilSync {
		if s.hooks.FrontendMessage != nil {
			s.hooks.FrontendMessage(msg)
		}
		s.notifyFrontendMessage(msg)
	}

	return s.handleAction(action)
}

func (s *Session) flushClientConn() error {
	err := s.clientConn.Flush()
	if err != nil {
		s.clientDisconnected.Store(true)
	}
	return err
}

func (s *Session) handleAction(action ClientMessageAction) error {
	s.setDiscardUntilSync(action.DiscardUntilSync)

	if action.InvalidateUnnamedPreparedStmt {
		s.invalidateUnnamedStmt()
	}

	if len(action.ReplyMsgsToClient) > 0 {
		for _, msg := range action.ReplyMsgsToClient {
			if s.hooks.BackendMessage != nil {
				s.hooks.BackendMessage(msg)
			}
			s.notifyBackendMessage(msg)
			s.clientConn.Send(msg)
		}
		err := s.flushClientConn()
		if err != nil {
			return fmt.Errorf("error flushing proxied msg(s) to client: %w", err)
		}
		if s.hooks.ClientFlushSucceeded != nil {
			s.hooks.ClientFlushSucceeded()
		}
		s.notifyClientFlushSucceeded()
	}

	if err := s.handleProxyMsgToPg(action); err != nil {
		return err
	}

	if action.UpdateAutoCachedUnnamedPreparedStmt != nil {
		err := s.pStmtState.AddConfirmedNamedStmt(action.UpdateAutoCachedUnnamedPreparedStmt)
		if err != nil {
			return err
		}
	}

	if action.FlushPostgres {
		s.obsVals.traceState.FinishTrace()
		pgDiscardedUntilSync, err := s.flushPostgres()
		if err != nil {
			return err // should be well-annotated by flushPostgres()
		}

		// if flushing all messages to pg did not result in PG returning an error, safe to perform the afterFlushAction
		if afterFlushAction := action.AfterFlushAction; !pgDiscardedUntilSync && afterFlushAction != nil {
			if err := s.handleAction(*afterFlushAction); err != nil {
				return err
			}
		}
	} else if action.AfterFlushAction != nil {
		pgproxy.Logger().Warn("unexpected after-flush action without flush")
	}

	if err := s.handleDeallocatePreparedStmt(action); err != nil {
		return err
	}

	return nil
}

// startMirrorTap returns a per-checkout mirroring tap when traffic mirroring
// applies to this session, or nil otherwise. The session owns the queue: it is
// created lazily on the first mirrored checkout and reused until mirroring is
// disabled (torn down here) or the session closes.
func (s *Session) startMirrorTap() *mirror.Tap {
	if !s.mirroringApplicable() {
		s.stopMirroring()
		return nil
	}
	m := s.mirroring.Policy(s.ctx)
	tags := append(append([]string(nil), s.obsVals.metricTags...), "source_database:"+m.SourceDatabase)
	if s.mirror == nil {
		s.mirror = mirror.NewQueue(m.QueueSize, s.mirrorBudget)
		s.startMirrorWorker(tags)
	}
	return mirror.NewTap(s.mirror, tags)
}

// stopMirroring releases the queue and worker when mirroring turns off mid-session, so a
// session that no longer mirrors stops holding a slice of the shared budget and stops
// counting acquire failures as replay errors. A later acquire rebuilds both from scratch.
func (s *Session) stopMirroring() {
	if s.mirror == nil {
		return // mirroring was never configured, or is already torn down
	}
	s.mirror.Close() // drops the undrained tail and returns its bytes to the budget
	worker := s.mirrorWorker
	s.mirror, s.mirrorWorker = nil, nil
	if worker != nil {
		// Off the client path, but still bounded so a worker wedged on a dead shadow link
		// gets its link force-closed rather than outliving the session unchecked.
		go worker.Stop(mirror.TeardownDrainTimeout)
	}
}

// startMirrorWorker launches the goroutine that drains s.mirror onto the shadow DB.
func (s *Session) startMirrorWorker(tags []string) {
	// Pin the shadow DB at worker start so a mid-session mirroring change can't
	// redirect queued traffic; app state is still looked up live for pool rebuilds.
	shadowDB := s.mirroring.Policy(s.ctx).ShadowDatabase
	acquire := func() (link.Link, error) {
		// Re-check per batch: stop replaying once mirroring is disabled or past DisableAt.
		if !s.mirroringApplicable() || s.mirroring.AcquireShadow == nil {
			return nil, fmt.Errorf("mirroring no longer applicable for this session")
		}
		req := LinkRequest{MaxPreparedStmts: link.DefaultPreparedStmtsPerConnection, MetricTags: s.obsVals.metricTags}
		if s.passthroughUser {
			req.PassthroughRoleName = s.user // replay under the source's role
		}
		// Bound acquire so a slow/exhausted shadow pool fails fast, not hanging teardown.
		ctx, cancel := context.WithTimeout(s.ctx, mirror.WorkerDrainTimeout)
		defer cancel()
		return s.mirroring.AcquireShadow(ctx, shadowDB, req)
	}
	// Snapshot on the session goroutine: the worker must not read pStmtState itself.
	// Confirmed-only is enough: this runs only with s.link == nil, and the release path
	// invalidates pending stmts just before clearing the link. The backend-remap abort does
	// not, so a stmt left pending there costs one batch, counted by catalog_miss.
	s.mirrorWorker = mirror.NewWorker(s.mirror, acquire, tags, s.pStmtState.ListConfirmedNamedStmts())
	go s.mirrorWorker.Run()
}

func (s *Session) handleProxyMsgToPg(action ClientMessageAction) error {
	if action.ProxyMsgToPg == nil {
		return nil
	}
	if s.link == nil {
		// TODO: RIF acquired on s.state (primary) in receiveAndHandleMessage;
		// read-routed links held on different cluster. Revisit when cross-cluster routing is active.
		routed := s.router(action.HandleReadQuery)

		request := LinkRequest{
			DynamicRoutingEnabled: s.dynamicRoutingEnabled,
			ForReadQuery:          action.HandleReadQuery,
			SessionLastWrite:      s.lastWrite,
			MaxPreparedStmts:      link.DefaultPreparedStmtsPerConnection,
			MetricTags:            s.obsVals.metricTags,
			TraceState:            s.obsVals.traceState,
			DecorateLink:          s.decorateLink,
		}

		if s.passthroughUser {
			request.PassthroughRoleName = s.user
		}

		ctx := context.Background()
		if s.acquireCtx != nil {
			ctx = s.acquireCtx
		}

		acquired, err := s.checkoutBackend(ctx, routed, request)
		if err != nil {
			if s.clientDisconnected.Load() {
				return &clientDisconnectedError{cause: err}
			}
			return fmt.Errorf("error linking session to a postgres conn: %w", err)
		}
		s.mirrorTap = s.startMirrorTap()
		s.setLink(acquired.Link)
		if s.hooks.RoutedLinkChanged != nil {
			s.hooks.RoutedLinkChanged(acquired.Link)
		}
		s.notifyCheckout(acquired.Meta)

		// The monitor may have fired between checkoutBackend returning and
		// setLink publishing abortHandle, seen no handle, skipped ForceClose, and
		// exited. Re-check so we don't block on Receive() with a dead client.
		if s.clientDisconnected.Load() {
			s.forceCloseHandleOnClientDisconnect(s.abortHandle.Load())
			return &clientDisconnectedError{cause: context.Canceled}
		}
	}

	if proxyMsgToPostgres, err := s.handleRecordNewPreparedStmt(action); err != nil {
		return err
	} else if !proxyMsgToPostgres {
		return nil
	}

	if err := s.handleCheckExistingPreparedStmt(action); err != nil {
		return err
	}
	s.handleEnsureUnnamedPreparedStmt(action)

	// A mirroring checkout needs the marks whatever the source link uses: the
	// shadow's release reads them, and its reset mode is not known until the
	// worker acquires it.
	if s.mirrorTap != nil || s.link.UsesSessionMarks() {
		if marks := action.SessionMarks(s.pStmtState, s.unnamedStmt); marks != 0 {
			s.link.MarkSession(marks)
			s.mirrorTap.OnMarkSession(marks)
		}
	}
	s.sendToLink(action.ProxyMsgToPg, action.AddReplyExpectation)

	if s.dynamicRoutingEnabled && !action.HandleReadQuery {
		s.lastWrite = time.Now()
	}
	return nil
}

func (s *Session) handleRecordNewPreparedStmt(action ClientMessageAction) (proxyMsgToPostgres bool, err error) {
	preparedStatement := action.RecordNewPreparedStmt
	if preparedStatement == nil {
		return true, nil
	}
	if !preparedStatement.IsProxyNamedPreparedStmt() {
		// Postgres drops the previous unnamed statement before parsing a new one,
		// so a failed Parse leaves none; ParseComplete records the new one.
		s.invalidateUnnamedStmt()
	}
	// if the connection already has this pending named statement, can just send back ParseComplete
	if preparedStatement.IsProxyNamedPreparedStmt() && s.link.HasExistingNamedPreparedStmt(preparedStatement) {
		if s.mirrorTap != nil {
			// No Parse is forwarded, but the shadow uses a different connection that may
			// lack it. Must precede the flush below, or the batch can end on an
			// unanswered ParseReply and release a non-idle shadow link.
			s.mirrorTap.OnSuppressedParse(&pgproto3.Parse{
				Name:          preparedStatement.GetProxyName(),
				Query:         preparedStatement.GetSQLQuery(),
				ParameterOIDs: preparedStatement.GetParamOIDs(),
			})
		}
		// first flush all buffered messages to Postgres
		s.sendToLink(&pgproto3.Flush{}, query.NoReply)
		discardedUntilSync, err := s.flushPostgres()
		if err != nil {
			return false, err // should be well-annotated by flushPostgres()
		}
		// if no previous messages resulted in an error, proxy back a ParseComplete to the client
		if !discardedUntilSync {
			if err := s.pStmtState.AddConfirmedNamedStmt(preparedStatement); err != nil {
				return false, err
			}
			parseComplete := &pgproto3.ParseComplete{}
			if s.hooks.BackendMessage != nil {
				s.hooks.BackendMessage(parseComplete)
			}
			s.notifyBackendMessage(parseComplete)
			s.clientConn.Send(parseComplete)
			if err := s.flushClientConn(); err != nil {
				return false, fmt.Errorf("error flushing proxied msg(s) to client: %w", err)
			}
			if s.hooks.ClientFlushSucceeded != nil {
				s.hooks.ClientFlushSucceeded()
			}
			s.notifyClientFlushSucceeded()
			return false, nil
		}
	}
	s.pStmtState.EnqueuePendingStmt(preparedStatement, false)
	if !preparedStatement.IsProxyNamedPreparedStmt() {
		s.unnamedPendingGens = append(s.unnamedPendingGens, s.unnamedGen)
	}
	return true, nil
}

// Checks for a prepared statement that has been created during the session, but its unknown whether it was created on this SessionLink or not.
// If it was already created, it updates the SessionLink's usage stats.
// If not, it will send a message to Parse message to PG and expect a corresponding ParseComplete in the future.
// The sequence of events where this occurs are:
//  1. The client starts a tx (implicit or explicit), and a SessionLink is created for the Session.
//     The tx includes a Parse message for a named prepared statement.
//  2. The Parse message is eventually flushed to Postgres,
//     and the Session receives a ParseComplete from Postgres acknowledging the named prepared statement is valid.
//     The Session records in its PreparedStmtState that the Session contains this named prepared statement.
//  3. The SessionLink that was associated with the tx from steps 1 & 2 is released when the tx completes.
//  4. The client starts a new tx, and the underlying SessionLink is either the same or different from the tx steps 1 & 2,
//     so we check.
//  5. The client sends a Bind or Describe message referencing the named prepared statement from step 1.
//     This results in:
//     5a. If the SessionLink is the same as in steps 1 & 2, only usage stats are updated because the named prepared statement was already created on this SessionLink.
//     5b. If the SessionLink is different, the prepared statement will be created on the new SessionLink.
func (s *Session) handleCheckExistingPreparedStmt(action ClientMessageAction) error {
	preparedStatement := action.CheckExistingPreparedStmt
	if preparedStatement == nil {
		return nil
	}
	if preparedStatement.IsProxyNamedPreparedStmt() && s.link.HasExistingNamedPreparedStmt(preparedStatement) {
		err := s.link.RecordExistingNamedPreparedStmtUsed(preparedStatement)
		if err != nil {
			return fmt.Errorf("error recording existing prepared statement usage: %w", err)
		}
		return nil
	}
	s.sendToLink(&pgproto3.Parse{Name: preparedStatement.GetProxyName(), Query: preparedStatement.GetSQLQuery(), ParameterOIDs: preparedStatement.GetParamOIDs()}, query.ParseReply)
	s.pStmtState.EnqueuePendingStmt(preparedStatement, true)
	return nil
}

// handleEnsureUnnamedPreparedStmt re-creates the client's unnamed prepared statement
// on the link's connection before a Bind or Describe that refers to it. The client
// may have parsed it on a link that has since been released (for example psql's
// \gdesc sends Parse+Sync, then Describe+Sync). The release reset is a simple Query,
// and any simple Query discards the unnamed statement; a release that skips it
// leaves the next link to close the statement first. The ParseComplete is not
// forwarded to the client.
func (s *Session) handleEnsureUnnamedPreparedStmt(action ClientMessageAction) {
	if !action.EnsureUnnamedPreparedStmt || s.unnamedStmt == nil || s.unnamedStmtOnLink {
		return
	}
	s.sendToLink(&pgproto3.Parse{Query: s.unnamedStmt.GetSQLQuery(), ParameterOIDs: s.unnamedStmt.GetParamOIDs()}, query.ParseReply)
	s.pStmtState.EnqueuePendingStmt(s.unnamedStmt, true)
	s.unnamedPendingGens = append(s.unnamedPendingGens, s.unnamedGen)
	s.unnamedStmtOnLink = true
}

// invalidateUnnamedStmt forgets the client's unnamed prepared statement and
// its described types, and starts a new generation, so ParseComplete and
// ParameterDescription replies for messages sent before this point cannot
// restore them.
func (s *Session) invalidateUnnamedStmt() {
	s.unnamedStmt = nil
	if s.paramTypes != nil {
		s.paramTypes.unnamed = s.paramTypes.unnamed[:0]
	}
	s.unnamedGen++
}

// confirmUnnamedStmt records the unnamed statement a ParseComplete confirms, if
// no later message replaced or destroyed it. It stores the plain statement: the
// confirmed value wraps it for the pending queue, and keeping the wrapper would
// nest another one on each reuse. The plain statement keeps any cached verdict.
func (s *Session) confirmUnnamedStmt(confirmed prepared.PreparedStatement) {
	if len(s.unnamedPendingGens) == 0 {
		return
	}
	generation := s.unnamedPendingGens[0]
	s.unnamedPendingGens = s.unnamedPendingGens[1:]
	if generation != s.unnamedGen {
		return
	}
	s.unnamedStmt = prepared.Unwrap(confirmed)
	s.unnamedStmtOnLink = true
}

// invalidatePendingStmts drops the pending statement queue and the unnamed
// generations recorded alongside it. An unnamed Parse still pending did not
// create the statement, so the next Bind or Describe of it re-creates it.
func (s *Session) invalidatePendingStmts(fromError bool) {
	s.pStmtState.InvalidatePendingStmts(fromError)
	if len(s.unnamedPendingGens) > 0 {
		s.unnamedStmtOnLink = false
	}
	s.unnamedPendingGens = nil
}

func (s *Session) handleDeallocatePreparedStmt(action ClientMessageAction) error {
	preparedStmt := action.DeallocatePreparedStmt
	if preparedStmt == nil {
		return nil
	}
	if preparedStmt.IsProxyNamedPreparedStmt() {
		s.pStmtState.DeallocateNamedStmtFromClientName(preparedStmt.GetClientName())
	}
	return nil
}

// iterates through messages that it expected PG to send back to the proxy and handles them accordingly.
// returns whether Postgres had to discard messages until a Sync was received and whether an error occurred.
func (s *Session) flushPostgres() (bool, error) {
	s.obsVals.traceState.StartTrace(query.ForwardOpName)
	if s.link == nil {
		return false, fmt.Errorf("expected non-nil session link to flush")
	}
	start := time.Now()
	err := s.link.Flush()
	elapsed := time.Since(start).Microseconds()
	s.recordFlushPostgresFinished(time.Duration(elapsed) * time.Microsecond)
	if err != nil {
		if s.clientDisconnected.Load() {
			return false, &clientDisconnectedError{cause: err}
		}
		return false, fmt.Errorf("error flushing messages to linked postgres conn: %w", err)
	}
	s.mirrorTap.OnFlushSucceeded()
	s.obsVals.traceState.FinishTrace(err)

	timesFlushed := 0
	clientWriteSucceeded := false
	discardedUntilSync := false
	// pgSkippingUntilSync is whether Postgres reported an error that no ReadyForQuery
	// has followed, so Postgres is skipping messages until the client's Sync.
	pgSkippingUntilSync := false

	// replyBuf accumulates encoded messages destined for the client. It is seeded from the
	// connection's retained buffer (so small replies don't re-allocate) and reused across the
	// flushes of this reply (reset to [:0], capacity kept) — avoiding
	// pgproto3.Backend.Flush's discard-and-regrow of any buffer over 1KB on every flush, the churn
	// a production profile attributed to DataRow.Encode. With Config.ReplyBufferPool, a reply that
	// outgrows the retained buffer continues in pooledBuf, a buffer from replyBufPool, so it does
	// not grow a new buffer either. The defer below returns pooledBuf to the pool and keeps the
	// retained buffer for the next reply, unless that buffer is larger than maxRetainedReplyBufCap,
	// in which case it is released so an idle connection never pins a large buffer.
	replyBuf := s.clientReplyBuf[:0]
	// pooledBuf is set when the reply outgrows the retained buffer. It holds the buffer that
	// replyBufPool lent, or nil when the reply continues in a buffer that Encode grew.
	var pooledBuf *[]byte
	// keepRetained keeps buf for the next reply if it is small enough, so that the next reply
	// does not grow the retained buffer again.
	keepRetained := func(buf []byte) {
		if cap(buf) <= maxRetainedReplyBufCap {
			s.clientReplyBuf = buf[:0]
		}
	}
	usePooledBuf := func() {
		keepRetained(replyBuf)
		pooledBuf = replyBufPool.Get().(*[]byte)
		replyBuf = append((*pooledBuf)[:0], replyBuf...)
	}
	defer func() {
		// Deliver any messages staged since the last flush before unwinding. When an error
		// aborts the loop (e.g. rows/notices arrived before a FATAL or a protocol violation),
		// close() only flushes clientConn's own buffer, which never held them, so without this
		// the partial reply is truncated. Skip when the client is gone (close() won't write to
		// it either) or when the reply drained cleanly (replyBuf was reset by the last flush).
		if len(replyBuf) > 0 && !s.clientDisconnected.Load() {
			_, _ = s.clientNetConn.Write(replyBuf)
		}
		if pooledBuf != nil {
			// The last write to the client is done, so no write still reads these buffers.
			// A buffer that grew past maxPooledReplyBufCap is dropped; the pool then gets back
			// the buffer it lent, if any.
			if cap(replyBuf) <= maxPooledReplyBufCap {
				*pooledBuf = replyBuf[:0]
			}
			if cap(*pooledBuf) > 0 {
				replyBufPool.Put(pooledBuf)
			}
			replyBuf = s.clientReplyBuf
		}
		if cap(replyBuf) <= maxRetainedReplyBufCap {
			s.clientReplyBuf = replyBuf[:0]
		} else {
			s.clientReplyBuf = nil
		}
	}()
	flushToClient := func() error {
		if len(replyBuf) == 0 {
			return nil
		}
		_, err := s.clientNetConn.Write(replyBuf)
		replyBuf = replyBuf[:0] // keep capacity for the next flush within this call
		if err != nil {
			s.clientDisconnected.Store(true)
			s.obsVals.traceState.FinishTrace(err)
			return fmt.Errorf("error flushing proxied msg(s) to client: %w", err)
		}
		clientWriteSucceeded = true
		return nil
	}

	s.obsVals.traceState.StartTrace(query.QueryExecutionAndProxying)
	for {
		expectation := s.link.NextExpectation()
		if expectation == query.NoReply {
			break
		}

		msg, err := s.link.Receive()
		if err != nil {
			if s.clientDisconnected.Load() {
				return discardedUntilSync, &clientDisconnectedError{cause: err}
			}
			return discardedUntilSync, fmt.Errorf("error receiving message from linked postgres conn: %w", err)
		}
		if s.hooks.BackendMessage != nil {
			s.hooks.BackendMessage(msg)
		}
		s.notifyBackendMessage(msg)
		action, err := query.DecidePgMessageAction(msg, expectation, s.obsVals.traceState)
		if err != nil {
			return discardedUntilSync, fmt.Errorf("error handling postgres message: %w", err)
		}
		if s.paramTypes != nil && len(s.paramTypes.describing) > 0 {
			s.observeBackendMessage(msg)
		}

		if action.AddNewPreparedStatement {
			confirmedPreparedStmt := s.pStmtState.ConfirmPendingStmt()
			if confirmedPreparedStmt != nil && confirmedPreparedStmt.IsExistingPreparedStmt() {
				action.ProxyMsgToClient = nil
			}
			if confirmedPreparedStmt != nil && !confirmedPreparedStmt.IsProxyNamedPreparedStmt() {
				s.confirmUnnamedStmt(confirmedPreparedStmt)
			}
			err := s.link.RecordNewNamedPreparedStmt(confirmedPreparedStmt)
			if err != nil {
				return discardedUntilSync, fmt.Errorf("error recording new prepared statement: %w", err)
			}
		}

		if action.ProxyMsgToClient != nil {
			if s.replyBufferPool && pooledBuf == nil && len(replyBuf) >= maxRetainedReplyBufCap/2 {
				// The reply is about to outgrow the retained buffer. Continue it in a pooled
				// buffer before the next message makes Encode grow the retained one.
				usePooledBuf()
			}
			encoded, encErr := action.ProxyMsgToClient.Encode(replyBuf)
			if encErr != nil {
				return discardedUntilSync, fmt.Errorf("error encoding message to forward to client: %w", encErr)
			}
			s.obsVals.traceState.AddForwarded(action.ProxyMsgToClient, len(encoded)-len(replyBuf))
			if s.replyBufferPool && pooledBuf == nil && cap(encoded) > maxRetainedReplyBufCap {
				// The reply's buffer is now larger than maxRetainedReplyBufCap: Encode grew a new
				// one for a message that did not fit, or the session started with a larger buffer.
				// replyBuf is still the buffer from before Encode, so keep it for the next reply.
				keepRetained(replyBuf)
				replyBuf = encoded
				if cap(replyBuf) >= pooledReplyBufCap {
					// The buffer is at least as large as a pooled one. Continue in it, and give
					// it to the pool when the reply ends, so the message is not copied again.
					pooledBuf = new([]byte)
				} else {
					usePooledBuf()
				}
			} else {
				replyBuf = encoded
			}
			if len(replyBuf) >= s.postgresMsgBufferSize {
				timesFlushed++
				err = flushToClient()
				if err != nil {
					return discardedUntilSync, err
				}
			}
		}
		if action.ResolveReplyExpectation {
			s.link.ResolveNextExpectation()
		}
		if action.DiscardUntilSync {
			s.link.DiscardUntilSync()
			// there can only have up to one sync message buffered at once, so no additional messages can be processed.
			// this means there is no possible state where discardedUntilSync becomes false before this loop exits.
			discardedUntilSync = true
			pgSkippingUntilSync = true
			s.invalidatePendingStmts(true)
		}
		if action.SetTxStatus != query.NoTxStatus {
			s.link.HandleReadyForQuery(action.SetTxStatus)
			pgSkippingUntilSync = false
		}
		if action.ReleaseLinkIfIdle && s.link.Idle() {
			s.invalidatePendingStmts(false)
			s.releaseCurrentLink(false)
			break
		}
	}
	// ensure any remaining messages are also flushed
	if len(replyBuf) > 0 {
		err := flushToClient()
		if err != nil {
			s.obsVals.traceState.FinishTrace(err)
			return discardedUntilSync, fmt.Errorf("error flushing messages to client: %w", err)
		}
		s.recordBufferFlushFinished(timesFlushed + 1)
		s.obsVals.traceState.FinishTrace()
	}
	s.obsVals.traceState.FinishRootTrace()
	if clientWriteSucceeded {
		if s.hooks.ClientFlushSucceeded != nil {
			s.hooks.ClientFlushSucceeded()
		}
		s.notifyClientFlushSucceeded()
	}
	if pgSkippingUntilSync {
		// The error came before the client's Sync, as in a pipeline flushed with
		// Flush. Postgres ignores the client's messages until that Sync, so the
		// session must too: it must not reply to them or record their effects.
		s.setDiscardUntilSync(true)
	}

	return discardedUntilSync, nil
}

// RemapBackend queues a backend for the next message boundary; the last call wins.
func (s *Session) RemapBackend(backend Backend) {
	s.reportMu.Lock()
	defer s.reportMu.Unlock()
	next := s.pendingRemaps()
	next.backend = backend
	s.pending.Store(&next)
	if s.hooks.BackendChanged != nil {
		if backend == nil {
			// Cancelled: the next boundary keeps the current backend. A nil pending
			// value marks the applied report.
			backend = s.backend
		}
		s.hooks.BackendChanged(s.backend, backend)
	}
}

// router selects a state from readsState if we wish to route a read query
// based on configured traffic weights
func (s *Session) router(forRead bool) Backend {
	if s.hasReadStates() && forRead {
		backend := s.selectReadState()
		if backend != nil {
			return backend
		}
	}

	return s.backend
}

// selectState distributes reads to read states according to their associated traffic weights
func (s *Session) selectReadState() Backend {
	readStates := s.currentReadStates()
	totalWeight := totalReadWeight(readStates)

	if s.hasReadStates() && totalWeight > 0 {
		randInt := s.randomInt(totalWeight)
		cumulatedWeight := 0

		for _, readState := range readStates {
			cumulatedWeight += int(readState.weight)
			if randInt < cumulatedWeight {
				return readState.backend
			}
		}
	}

	return nil
}

func totalReadWeight(readStates []*ReadState) int {
	var total int
	for _, readState := range readStates {
		total += int(readState.weight)
	}
	return total
}

func readStatesToConfig(readStates []*ReadState) []ReadStateConfig {
	result := make([]ReadStateConfig, 0, len(readStates))
	for _, readState := range readStates {
		result = append(result, ReadStateConfig{Backend: readState.backend, Weight: readState.weight})
	}
	return result
}

func (s *Session) hasReadStates() bool {
	return len(s.currentReadStates()) > 0
}

func (s *Session) currentReadStates() []*ReadState {
	if s.readStatesFunc == nil {
		return s.readStates
	}
	configs := s.readStatesFunc()
	readStates := make([]*ReadState, 0, len(configs))
	for _, config := range configs {
		readStates = append(readStates, &ReadState{
			backend: config.Backend,
			weight:  config.Weight,
		})
	}
	return readStates
}

func (s *Session) close(err error) {
	defer func() {
		s.hooksCloseOnce.Do(func() {
			if s.hooks.Closed != nil {
				s.hooks.Closed()
			}
		})
		s.notifyClosed()
	}()

	clientDisconnected := s.clientDisconnected.Load()

	// A client EOF or Terminate received between checkouts bypasses the client
	// monitor. Release closes the link when that disconnect leaves it non-idle.
	s.releaseCurrentLink(clientDisconnected)
	if s.timerMonitor != nil {
		s.timerMonitor.close()
	}
	if s.mirror != nil {
		s.mirror.Close() // wakes the worker so it drops the tail and exits
	}
	if s.mirrorWorker != nil {
		s.mirrorWorker.Stop(mirror.WorkerDrainTimeout)
	}
	if err == nil {
		return
	}
	// The underlying conn is closed by the parent listener
	var cde *clientDisconnectedError
	suppressCloseError := errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.As(err, &cde)
	if !suppressCloseError {
		s.recordCloseError(err)
		s.clientConn.Send(closeErrorResponse(err))
		_ = s.clientConn.Flush()
	}
}

func (s *Session) forceCloseHandleOnClientDisconnect(handle *link.AbortHandle) {
	if handle.ForceClose() {
		s.recordBackendDestroyedOnClientDisconnect("force_close")
	}
}

func (s *Session) recordBackendDestroyedOnClientDisconnect(path string) {
	if s.backendDestroyedOnClientDisconnect.CompareAndSwap(false, true) {
		s.recordBackendDestroyed(path)
	}
}

func closeErrorResponse(err error) *pgproto3.ErrorResponse {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && query.IsSwitchoverSignature(pgErr.Code, pgErr.Message) {
		return &pgproto3.ErrorResponse{
			Severity: "FATAL",
			Code:     pgErr.Code,
			Message:  pgErr.Message + query.LeaderSwitchoverHint,
		}
	}
	return &pgproto3.ErrorResponse{
		Severity: "FATAL",
		Code:     pgerrcode.ConnectionFailure,
		Message:  fmt.Sprintf("closing pg-proxy session with err: %v", err),
	}
}

func (s *Session) Identifier() string { return fmt.Sprintf("%d", s.pid) }
