package link

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/DataDog/pg-proxy/pkg/prepared"
	"github.com/DataDog/pg-proxy/pkg/query"
)

// The threshold for the number of named prepared statements on a connection which will trigger deallocations when the connection is released.
// Each prepared statement utilizes memory on Postgres, and we do not want to leave the number unbounded and OOM-kill the server.
// Default PG instance has 8GB of memory, 6GB available.
// A prepared statement for a complex query is ~100KB.
// Default pg-proxy configuration is 3 replicas * 20 max connections = 60 connections.
// Default configuration max memory usage = 60 connections * 100 prepared statements * 100KB = 600MB.
// Configurations with greater than 600 connections will require scaling their PG instance.
const DefaultPreparedStmtsPerConnection = 100

// Link defines the behavior of PG conn <-> Session.
type Link interface {
	Send(msg pgproto3.FrontendMessage, expectation query.ReplyExpectation)
	Flush() error
	Receive() (pgproto3.BackendMessage, error)
	NextExpectation() query.ReplyExpectation
	ResolveNextExpectation()
	DiscardUntilSync()
	Idle() bool
	Cancel() error
	Release() error
	ForceClose() bool // Force-close the underlying PG connection to unblock Receive; reports whether it closed a live connection
	DebugExpectationString() string
	TxStatus() query.TxStatus
	SetTxStatus(txStatus query.TxStatus)
	HandleReadyForQuery(txStatus query.TxStatus)
	RecordNewNamedPreparedStmt(preparedStatement prepared.PreparedStatement) error
	RecordExistingNamedPreparedStmtUsed(preparedStatement prepared.PreparedStatement) error
	HasExistingNamedPreparedStmt(preparedStatement prepared.PreparedStatement) bool
}

// RoleSession changes the Postgres role after acquire and restores it before the
// conn returns to the pool. Nil Options.Role means no role management.
type RoleSession interface {
	// Set changes the Postgres role after acquire; returns opaque token for Reset.
	Set(ctx context.Context, conn *pgxpool.Conn, tags []string) (token string, err error)
	// Reset restores the role before the conn returns to the pool.
	Reset(ctx context.Context, conn *pgxpool.Conn, token string, tags []string) error
}

// Options configure a SessionLink built by New.
type Options struct {
	MaxPreparedStmts int
	MetricTags       []string
	Role             RoleSession // nil = no role management
}

// New wraps an already-acquired conn. The embedder acquires from its pool first.
func New(conn *pgxpool.Conn, opts Options) (Link, error) {
	setUpConnMetaData(conn)

	sl := &SessionLink{
		conn:                       conn,
		pgConn:                     conn.Conn().PgConn().Frontend(),
		txStatus:                   query.Idle,
		replyExpectations:          []query.ReplyExpectation{},
		discardingUntilSync:        false,
		readyForQuery:              true,
		namedPreparedStmtThreshold: opts.MaxPreparedStmts,
		createdTime:                time.Now().Nanosecond(),
		sessionLinkTags:            opts.MetricTags,
		role:                       opts.Role,
	}
	sl.exportStats()

	if opts.Role != nil {
		token, err := opts.Role.Set(context.TODO(), conn, opts.MetricTags)
		if err != nil {
			return nil, err
		}
		sl.token = token
	}

	return sl, nil
}

// NewFrontendLink builds a link with only a Frontend (conn nil) for unit tests.
func NewFrontendLink(fe *pgproto3.Frontend) *SessionLink {
	return &SessionLink{
		conn:                nil,
		pgConn:              fe,
		txStatus:            query.Idle,
		replyExpectations:   []query.ReplyExpectation{},
		discardingUntilSync: false,
		readyForQuery:       true,
	}
}
