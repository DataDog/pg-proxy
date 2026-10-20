package listener

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pgproxy "github.com/DataDog/pg-proxy"
	"github.com/DataDog/pg-proxy/pkg/limiter"
)

type listenerMetricCall struct {
	name  string
	value int64
	tags  []string
	rate  float64
}

type listenerRecordingMetrics struct {
	statsd.NoOpClient
	mu     sync.Mutex
	counts []listenerMetricCall
}

func (m *listenerRecordingMetrics) Count(name string, value int64, tags []string, rate float64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counts = append(m.counts, listenerMetricCall{name: name, value: value, tags: append([]string(nil), tags...), rate: rate})
	return nil
}

type bufferConn struct {
	read  *bytes.Reader
	write bytes.Buffer
}

func newBufferConn(input []byte) *bufferConn { return &bufferConn{read: bytes.NewReader(input)} }
func (c *bufferConn) Read(p []byte) (int, error) {
	return c.read.Read(p)
}
func (c *bufferConn) Write(p []byte) (int, error) { return c.write.Write(p) }
func (c *bufferConn) Close() error                { return nil }
func (c *bufferConn) LocalAddr() net.Addr         { return testAddr("local") }
func (c *bufferConn) RemoteAddr() net.Addr        { return testAddr("remote") }
func (c *bufferConn) SetDeadline(time.Time) error { return nil }
func (c *bufferConn) SetReadDeadline(time.Time) error {
	return nil
}
func (c *bufferConn) SetWriteDeadline(time.Time) error {
	return nil
}

type testAddr string

func (a testAddr) Network() string { return string(a) }
func (a testAddr) String() string  { return string(a) }

func TestListenerConnectionAttemptMetrics(t *testing.T) {
	metrics := &listenerRecordingMetrics{}
	restore := pgproxy.Bootstrap(pgproxy.Dependencies{Metrics: metrics})
	t.Cleanup(restore)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	engine, admission, _, err := newTestListener(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	engine.overCapRejectCount.Store(3)
	engine.refreshOverCapGate()

	handleConnError(newBufferConn(nil), errors.New("open failed"), "tls_upgrade_failure")

	ssl, err := (&pgproto3.SSLRequest{}).Encode(nil)
	require.NoError(t, err)
	handleLoadSheddingRejection(newBufferConn(ssl))

	admission.RegisterService("limited", 0)
	rejected := newBufferConn(nil)
	_, err = engine.handleStartup(rejected, &pgproto3.StartupMessage{Parameters: map[string]string{
		"user": "limited", "database": "testdb",
	}})
	require.Error(t, err)

	startup, err := (&pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters:      map[string]string{"user": "successful", "database": "testdb"},
	}).Encode(nil)
	require.NoError(t, err)
	engine.handleConn(newBufferConn(startup))

	assert.Equal(t, []listenerMetricCall{
		{
			name: "dd.orgstore.pg_proxy.listener.connection_attempt", value: 3,
			tags: []string{"outcome:error", "error_type:" + string(limiter.GlobalLimitExceeded), "cap_mode:default", "placement:accept_reject"}, rate: 1,
		},
		{
			name: "dd.orgstore.pg_proxy.listener.connection_attempt", value: 1,
			tags: []string{"outcome:error", "placement:tls_upgrade_failure"}, rate: 1,
		},
		{
			name: "dd.orgstore.pg_proxy.listener.connection_attempt", value: 1,
			tags: []string{"outcome:error", "placement:load_shedding"}, rate: 1,
		},
		{
			name: "dd.orgstore.pg_proxy.listener.connection_attempt", value: 1,
			tags: []string{"user:limited", "database:testdb", "client_service_account:limited", "passthrough_user:false", "outcome:error", "error_type:service_limit_exceeded", "cap_mode:default", "placement:register_session", "client_notified:true"}, rate: 1,
		},
		{
			name: "dd.orgstore.pg_proxy.listener.connection_attempt", value: 1,
			tags: []string{"user:successful", "database:testdb", "client_service_account:successful", "passthrough_user:false", "outcome:successful", "with_dynamic_routing:false"}, rate: 1,
		},
	}, metrics.counts)
}

func TestListenerLogsUseBootstrappedHandler(t *testing.T) {
	var output bytes.Buffer
	restore := pgproxy.Bootstrap(pgproxy.Dependencies{
		LogHandler: slog.NewTextHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug}),
	})
	t.Cleanup(restore)
	conn := newBufferConn(nil)

	handleConnError(conn, errors.New("open failed"), "test")
	logDeregisterError("service-a", errors.New("deregister failed"), false)
	logDeregisterError("service-b", errors.New("rollback failed"), true)
	logSessionRunError(conn, errors.New("ordinary failure"))
	logSessionRunError(conn, errors.New("PostgreSQL FATAL failure"))

	logs, err := io.ReadAll(&output)
	require.NoError(t, err)
	text := string(logs)
	assert.Contains(t, text, `level=ERROR msg="failed to open connection from remote: open failed"`)
	assert.Contains(t, text, `level=WARN msg="error deregistering service service-a, error: deregister failed"`)
	assert.Contains(t, text, `level=WARN msg="error rolling back session registration for service service-b: rollback failed"`)
	assert.Contains(t, text, `level=WARN msg="running session for connection from remote failed: ordinary failure"`)
	assert.Contains(t, text, `level=ERROR msg="running session for connection from remote failed: PostgreSQL FATAL failure"`)
}
