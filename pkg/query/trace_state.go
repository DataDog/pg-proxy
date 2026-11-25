// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package query

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/jackc/pgx/v5/pgproto3"

	pgproxy "github.com/DataDog/pg-proxy"
	"github.com/DataDog/pg-proxy/pkg/bindparams"
)

type RequestType = string

const (
	Query                     RequestType = "query"
	Parse                     RequestType = "parse"
	TransactionOpName         string      = "Proxy forwarding "
	HandlingMessagesOpName    string      = "Handling messages"
	ForwardOpName             string      = "Forwarding to postgres"
	QueryExecutionAndProxying string      = "Query Execution and proxying"
	BatchExecutionAndProxying string      = "Batch execution and proxying"
	DataRoundTrips                        = "Returning rows to client"
)

// MaxTrackedMessages We keep track of messages to understand what is going on at the postgres level protocol
// tracking more than 10 messages does not really make sense as they would be repeated until the "ReadyForQuery" one
const MaxTrackedMessages = 15

// TraceState maintains trace state for a single query
type TraceState struct {
	rootQuery            string
	usingFullPropagation bool // query contains parent trace information (making the query unique)
	isBatch              bool
	trace                pgproxy.Span
	rootTrace            pgproxy.Span
	operationMetrics     pgproxy.OperationMetricsReporter
	operation            string
	operationStarted     time.Time
	keepChildSpans       bool
	rowsNumber           int
	rowsSize             int
	// Fixed-size buffer for messages - keeps first MaxTrackedMessages messages
	messages     [MaxTrackedMessages]string
	messageCount int // Number of messages currently stored
	// The root trace's Bind values, rendered on the root span when it finishes.
	// It is allocated on the first sampled capture, so sessions without
	// parameter capture do not carry it.
	bindParams *bindparams.Capture
}

// AddMessage records msg in the trace's message buffer, so it can be attached to
// the span when the trace finishes. Data-bearing messages (DataRow, CopyData) are
// skipped — their volume would swamp MaxTrackedMessages for no diagnostic value.
func (t *TraceState) AddMessage(msg interface{}) {
	if t.operationMetrics != nil && !t.keepChildSpans {
		return
	}
	// Skip data messages
	switch msg.(type) {
	case *pgproto3.DataRow, *pgproto3.CopyData:
		return
	}

	if t.messageCount < MaxTrackedMessages {
		t.messages[t.messageCount] = traceMessageType(msg)
		t.messageCount++
	} else {
		t.SetTag("messages_truncated", true)
	}
}

// AddForwarded records msg, encoded as n bytes, as forwarded to the client.
// Only DataRows count toward the trace's rowsReturned/Affected and
// rowsTotalSize tags.
func (t *TraceState) AddForwarded(msg pgproto3.BackendMessage, n int) {
	if t.operationMetrics != nil && !t.keepChildSpans {
		return
	}
	if _, ok := msg.(*pgproto3.DataRow); ok {
		t.rowsNumber++
		t.rowsSize += n
	}
}

func traceMessageType(msg interface{}) string {
	switch msg.(type) {
	case *pgproto3.AuthenticationCleartextPassword:
		return "*pgproto3.AuthenticationCleartextPassword"
	case *pgproto3.AuthenticationGSS:
		return "*pgproto3.AuthenticationGSS"
	case *pgproto3.AuthenticationGSSContinue:
		return "*pgproto3.AuthenticationGSSContinue"
	case *pgproto3.AuthenticationMD5Password:
		return "*pgproto3.AuthenticationMD5Password"
	case *pgproto3.AuthenticationOk:
		return "*pgproto3.AuthenticationOk"
	case *pgproto3.AuthenticationSASL:
		return "*pgproto3.AuthenticationSASL"
	case *pgproto3.AuthenticationSASLContinue:
		return "*pgproto3.AuthenticationSASLContinue"
	case *pgproto3.AuthenticationSASLFinal:
		return "*pgproto3.AuthenticationSASLFinal"
	case *pgproto3.BackendKeyData:
		return "*pgproto3.BackendKeyData"
	case *pgproto3.Bind:
		return "*pgproto3.Bind"
	case *pgproto3.BindComplete:
		return "*pgproto3.BindComplete"
	case *pgproto3.CancelRequest:
		return "*pgproto3.CancelRequest"
	case *pgproto3.Close:
		return "*pgproto3.Close"
	case *pgproto3.CloseComplete:
		return "*pgproto3.CloseComplete"
	case *pgproto3.CommandComplete:
		return "*pgproto3.CommandComplete"
	case *pgproto3.CopyBothResponse:
		return "*pgproto3.CopyBothResponse"
	case *pgproto3.CopyData:
		return "*pgproto3.CopyData"
	case *pgproto3.CopyDone:
		return "*pgproto3.CopyDone"
	case *pgproto3.CopyFail:
		return "*pgproto3.CopyFail"
	case *pgproto3.CopyInResponse:
		return "*pgproto3.CopyInResponse"
	case *pgproto3.CopyOutResponse:
		return "*pgproto3.CopyOutResponse"
	case *pgproto3.DataRow:
		return "*pgproto3.DataRow"
	case *pgproto3.Describe:
		return "*pgproto3.Describe"
	case *pgproto3.EmptyQueryResponse:
		return "*pgproto3.EmptyQueryResponse"
	case *pgproto3.ErrorResponse:
		return "*pgproto3.ErrorResponse"
	case *pgproto3.Execute:
		return "*pgproto3.Execute"
	case *pgproto3.Flush:
		return "*pgproto3.Flush"
	case *pgproto3.FunctionCall:
		return "*pgproto3.FunctionCall"
	case *pgproto3.FunctionCallResponse:
		return "*pgproto3.FunctionCallResponse"
	case *pgproto3.GSSEncRequest:
		return "*pgproto3.GSSEncRequest"
	case *pgproto3.NoData:
		return "*pgproto3.NoData"
	case *pgproto3.NoticeResponse:
		return "*pgproto3.NoticeResponse"
	case *pgproto3.NotificationResponse:
		return "*pgproto3.NotificationResponse"
	case *pgproto3.ParameterDescription:
		return "*pgproto3.ParameterDescription"
	case *pgproto3.ParameterStatus:
		return "*pgproto3.ParameterStatus"
	case *pgproto3.Parse:
		return "*pgproto3.Parse"
	case *pgproto3.ParseComplete:
		return "*pgproto3.ParseComplete"
	case *pgproto3.PortalSuspended:
		return "*pgproto3.PortalSuspended"
	case *pgproto3.Query:
		return "*pgproto3.Query"
	case *pgproto3.ReadyForQuery:
		return "*pgproto3.ReadyForQuery"
	case *pgproto3.RowDescription:
		return "*pgproto3.RowDescription"
	case *pgproto3.SSLRequest:
		return "*pgproto3.SSLRequest"
	case *pgproto3.StartupMessage:
		return "*pgproto3.StartupMessage"
	case *pgproto3.Sync:
		return "*pgproto3.Sync"
	case *pgproto3.Terminate:
		return "*pgproto3.Terminate"
	default:
		return fmt.Sprintf("%T", msg)
	}
}

// RootTrace returns the current root span, or nil if none is active.
func (t *TraceState) RootTrace() pgproxy.Span {
	return t.rootTrace
}

// Trace returns the current child span, or nil if none is active.
func (t *TraceState) Trace() pgproxy.Span {
	return t.trace
}

// UsingFullPropagation reports whether the current root query already carries
// parent trace information via a SQL comment.
func (t *TraceState) UsingFullPropagation() bool {
	return t.usingFullPropagation
}

// RootQuery returns the query string associated with the current root trace.
func (t *TraceState) RootQuery() string {
	return t.rootQuery
}

func (t *TraceState) getMessages() []string {
	if t.messageCount == 0 {
		return nil
	}

	return t.messages[:t.messageCount]
}

func (t *TraceState) clearMessages() {
	t.messages = [MaxTrackedMessages]string{}
	t.messageCount = 0
}

// isPgxPing reports whether sql is the health-check ping pgx clients send.
func isPgxPing(sql string) bool {
	return sql == "-- ping"
}

// StartTrace traces the handling of a client message given the sql query and the client message type
func (t *TraceState) StartRootTrace(msg pgproto3.FrontendMessage) {
	query := ""
	switch parseMsg := msg.(type) {
	case *pgproto3.Parse:
		query = parseMsg.Query
	case *pgproto3.Query:
		query = parseMsg.String
	}

	if t.rootTrace != nil {
		if query != "" {
			t.isBatch = true
		}
		return
	}

	if isPgxPing(query) {
		return
	}

	t.rootQuery = query
	t.rootTrace, t.usingFullPropagation = pgproxy.Tracing().StartRoot(TransactionOpName, query)
	t.operationMetrics, _ = t.rootTrace.(pgproxy.OperationMetricsReporter)
	if t.operationMetrics != nil {
		t.keepChildSpans = t.operationMetrics.KeepChildSpans()
	}

	t.StartTrace(HandlingMessagesOpName)
}

func (t *TraceState) StartTrace(operationName string) {
	// Since Batches have multiple query executions, we group all of them in one trace, to avoid multiple spans.
	if operationName == QueryExecutionAndProxying && t.isBatch {
		operationName = BatchExecutionAndProxying
	}
	if t.trace != nil || t.operation != "" {
		pgproxy.Logger().Debug("previous trace did not complete before starting a new trace", "operation", operationName)
		t.FinishTrace()
	}
	if t.rootTrace == nil {
		return
	}
	if t.operationMetrics != nil {
		t.operation = operationName
		t.operationStarted = time.Now()
		if !t.keepChildSpans {
			return
		}
	}
	t.trace = t.rootTrace.StartChild(operationName)
}

func (t *TraceState) SetTag(key string, value interface{}) {
	if trace := t.trace; trace != nil {
		trace.SetTag(key, value)
	} else if t.operation == "" {
		pgproxy.Logger().Debug("trace not started, unable to set tag", "tag", key, "value", value)
	}
}

// Sampled reports whether the current root trace will be kept, so callers can
// skip building tag values the tracer would drop. Roots that do not implement
// pgproxy.SamplingReporter are treated as kept.
func (t *TraceState) Sampled() bool {
	if t.rootTrace == nil {
		return false
	}
	reporter, ok := t.rootTrace.(pgproxy.SamplingReporter)
	return !ok || reporter.Sampled()
}

// WantsBindParameters reports whether RecordBindParameters would keep a Bind:
// the root trace is sampled, since the tracer would drop an unsampled trace's
// tags, and none of its Binds was recorded yet.
func (t *TraceState) WantsBindParameters() bool {
	return !t.bindParams.Recorded() && t.Sampled()
}

// RecordBindParameters copies b's values to render as db.parameters.N tags on
// the root span when it finishes. It ignores b unless WantsBindParameters, so
// only a sampled root's first Bind with parameters is kept; a Bind with none,
// such as SELECT 1's, leaves the slot to a later one. Callers may check
// WantsBindParameters first to skip gathering parseOIDs and describedOIDs.
func (t *TraceState) RecordBindParameters(b *pgproto3.Bind, parseOIDs, describedOIDs []uint32) {
	if len(b.Parameters) == 0 || !t.WantsBindParameters() {
		return
	}
	if t.bindParams == nil {
		t.bindParams = &bindparams.Capture{}
	}
	t.bindParams.Record(b, parseOIDs, describedOIDs)
}

func (t *TraceState) FinishTrace(errs ...error) {
	t.finishTrace(pgproxy.OperationProxyError, errs...)
}

func (t *TraceState) finishTrace(failure pgproxy.OperationOutcome, errs ...error) {
	var elapsed time.Duration
	if t.operation != "" {
		elapsed = time.Since(t.operationStarted)
	}
	messages := t.getMessages()
	t.trace = finishSpan(t.trace, messages, t.rowsNumber, t.rowsSize, errs...)
	if t.operation != "" {
		outcome := pgproxy.OperationSucceeded
		if !containsOnlyBenignEOF(errs...) && errors.Join(errs...) != nil {
			outcome = failure
		}
		t.operationMetrics.RecordOperation(t.operation, elapsed, outcome)
		t.operation = ""
		t.operationStarted = time.Time{}
	}
	t.clearMessages()
	t.rowsSize = 0
	t.rowsNumber = 0
}

func (t *TraceState) FinishRootTrace(errs ...error) {
	t.FinishTrace(errs...) // Ensure trace is cleaned up first
	// RecordBindParameters keeps a Bind only under a sampled root.
	if t.bindParams.Recorded() {
		t.bindParams.Render(t.rootTrace.SetTag)
	}
	t.rootTrace = finishSpan(t.rootTrace, nil, 0, 0, errs...)
	t.operationMetrics = nil

	t.rootQuery = ""
	t.isBatch = false
	t.usingFullPropagation = false
}

func finishSpan(span pgproxy.Span, messages []string, rowsNumber int, rowsTotalSize int, errs ...error) pgproxy.Span {
	if span == nil {
		return nil
	}

	if len(messages) > 0 {
		span.SetTag("messages", strings.Join(messages, ", "))
	}

	if rowsTotalSize > 0 {
		span.SetTag("rowsReturned/Affected", rowsNumber)
		span.SetTag("rowsTotalSize", humanize.Bytes(uint64(rowsTotalSize)))
	}

	if containsOnlyBenignEOF(errs...) {
		span.Finish(nil)
	} else {
		span.Finish(errors.Join(errs...))
	}
	return nil
}

func containsOnlyBenignEOF(errs ...error) bool {
	if len(errs) == 0 {
		return true
	}
	for _, err := range errs {
		if !(errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)) {
			return false
		}
	}
	return true
}
