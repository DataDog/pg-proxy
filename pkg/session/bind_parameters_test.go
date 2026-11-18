// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package session

import (
	"bytes"
	"encoding/binary"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"

	"github.com/DataDog/dd-trace-go/v2/ddtrace/mocktracer"
	pgproxy "github.com/DataDog/pg-proxy"
	"github.com/DataDog/pg-proxy/pkg/bindparams"
	proxydatadog "github.com/DataDog/pg-proxy/pkg/datadog"
	"github.com/DataDog/pg-proxy/pkg/query"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// exchange sends one client cycle and plays a Postgres that answers each
// forwarded message until Sync, describing statements with describedOIDs. It
// reads what the proxy forwards rather than expecting the client's messages,
// since the proxy may add messages of its own. It returns a copy of the last
// Bind Postgres received, or nil.
func exchange(t *testing.T, client *pgproto3.Frontend, postgres *pgproto3.Backend, describedOIDs []uint32, sends ...pgproto3.FrontendMessage) *pgproto3.Bind {
	t.Helper()
	for _, msg := range sends {
		client.Send(msg)
	}
	// Flush while reading what Postgres receives: the pipes are bounded, so a
	// large message would otherwise block both sides.
	flushed := make(chan error, 1)
	go func() { flushed <- client.Flush() }()
	var forwarded *pgproto3.Bind
	for done := false; !done; {
		msg, err := postgres.Receive()
		require.NoError(t, err)
		switch m := msg.(type) {
		case *pgproto3.Parse:
			postgres.Send(&pgproto3.ParseComplete{})
		case *pgproto3.Describe:
			postgres.Send(&pgproto3.ParameterDescription{ParameterOIDs: describedOIDs})
			postgres.Send(&pgproto3.NoData{})
		case *pgproto3.Bind:
			// pgproto3 reuses the message and its read buffer; copy it.
			forwarded = &pgproto3.Bind{ParameterFormatCodes: slices.Clone(m.ParameterFormatCodes)}
			for _, v := range m.Parameters {
				forwarded.Parameters = append(forwarded.Parameters, slices.Clone(v)) // keeps nil (NULL) apart from empty
			}
			postgres.Send(&pgproto3.BindComplete{})
		case *pgproto3.Execute:
			postgres.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
		case *pgproto3.Sync:
			// In a transaction, so the session keeps the one link the test
			// backend can hand out.
			postgres.Send(&pgproto3.ReadyForQuery{TxStatus: 'T'})
			done = true
		default:
			t.Fatalf("unexpected forwarded message %T", msg)
		}
	}
	require.NoError(t, <-flushed)
	require.NoError(t, postgres.Flush())
	for {
		msg, err := client.Receive()
		require.NoError(t, err)
		if _, ok := msg.(*pgproto3.ReadyForQuery); ok {
			return forwarded
		}
	}
}

func int8Param(v int64) []byte { return binary.BigEndian.AppendUint64(nil, uint64(v)) }

// rootTags returns the tags of the finished root spans, in order.
func rootTags(tracer mocktracer.Tracer) []map[string]any {
	var roots []map[string]any
	for _, span := range tracer.FinishedSpans() {
		if span.OperationName() == query.TransactionOpName {
			roots = append(roots, span.Tags())
		}
	}
	return roots
}

func startMockTracer(t *testing.T) mocktracer.Tracer {
	tracer := mocktracer.Start()
	t.Cleanup(tracer.Stop)
	t.Cleanup(pgproxy.Bootstrap(pgproxy.Dependencies{Tracer: proxydatadog.Tracer{}}))
	return tracer
}

func TestBindParametersUseParseTypes(t *testing.T) {
	tracer := startMockTracer(t)
	_, client, postgres, _, closeSession := newRunningTestSessionWithBackend(t, func(cfg *Config) {
		cfg.EnableTraceParameters = true
	})
	defer closeSession()
	exchange(t, client, postgres, nil,
		&pgproto3.Parse{Query: parameterQuery + " AND $2", ParameterOIDs: []uint32{pgtype.Int8OID, 0}},
		&pgproto3.Bind{ParameterFormatCodes: []int16{1, 0}, Parameters: [][]byte{int8Param(-42), nil}},
		&pgproto3.Execute{},
		&pgproto3.Sync{})
	closeSession()

	roots := rootTags(tracer)
	require.Len(t, roots, 1)
	assert.Equal(t, "-42", roots[0]["db.parameters.0"])
	assert.Equal(t, "null", roots[0]["db.parameters.1"])
	assert.NotContains(t, roots[0], "db.parameters.truncated")
}

// pgx's describe-exec mode parses a statement without types, learns them from
// the backend's ParameterDescription, and binds binary values in a later cycle.
// (Named statements take the same path, but need a pooled connection the test
// backend does not have.)
func TestBindParametersUseDescribedTypes(t *testing.T) {
	tracer := startMockTracer(t)
	_, client, postgres, _, closeSession := newRunningTestSessionWithBackend(t, func(cfg *Config) {
		cfg.EnableTraceParameters = true
	})
	defer closeSession()
	described := []uint32{pgtype.Int8OID, pgtype.BoolOID}
	exchange(t, client, postgres, described,
		&pgproto3.Parse{Query: parameterQuery + " AND $2"},
		&pgproto3.Describe{ObjectType: 'S'},
		&pgproto3.Sync{})
	exchange(t, client, postgres, described,
		&pgproto3.Bind{ParameterFormatCodes: []int16{1}, Parameters: [][]byte{int8Param(7), {1}}},
		&pgproto3.Execute{},
		&pgproto3.Sync{})
	closeSession()

	roots := rootTags(tracer)
	require.Len(t, roots, 2)
	assert.NotContains(t, roots[0], "db.parameters.0", "the describe cycle has no Bind")
	assert.Equal(t, "7", roots[1]["db.parameters.0"])
	assert.Equal(t, "true", roots[1]["db.parameters.1"])
}

func TestBindParametersKeepFirstBindOfRoot(t *testing.T) {
	tracer := startMockTracer(t)
	_, client, postgres, _, closeSession := newRunningTestSessionWithBackend(t, func(cfg *Config) {
		cfg.EnableTraceParameters = true
	})
	defer closeSession()
	exchange(t, client, postgres, nil,
		&pgproto3.Parse{Query: parameterQuery},
		&pgproto3.Bind{Parameters: [][]byte{[]byte("first")}},
		&pgproto3.Execute{},
		&pgproto3.Bind{Parameters: [][]byte{[]byte("second")}},
		&pgproto3.Execute{},
		&pgproto3.Sync{})
	closeSession()

	roots := rootTags(tracer)
	require.Len(t, roots, 1)
	assert.Equal(t, "first", roots[0]["db.parameters.0"])
}

func TestBindParametersOnlyOnSampledRoots(t *testing.T) {
	for _, tc := range parameterCaptureCases {
		t.Run(tc.name, func(t *testing.T) {
			tracer := startMockTracer(t)
			runParameterQuery(t, tc.sql, tc.enable)
			roots := rootTags(tracer)
			require.Len(t, roots, 1)
			if tc.wantParameters {
				assert.Equal(t, privateParameter, roots[0]["db.parameters.0"])
			} else {
				assert.NotContains(t, roots[0], "db.parameters.0")
			}
		})
	}
}

func TestDescribeTrackingIsBounded(t *testing.T) {
	s := New(Config{EnableTraceParameters: true})
	for range maxPendingDescribes + 10 {
		s.observeClientMessage(&pgproto3.Describe{ObjectType: 'S'})
	}
	assert.Len(t, s.paramTypes.describing, maxPendingDescribes)
	s.observeBackendMessage(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	assert.Empty(t, s.paramTypes.describing, "unanswered describes are dropped at the end of the cycle")

	for i := range maxDescribedStmts + 10 {
		s.paramTypes.describing = append(s.paramTypes.describing, pendingDescribe{stmt: describedStmt{key: strconv.Itoa(i)}})
		s.observeBackendMessage(&pgproto3.ParameterDescription{ParameterOIDs: []uint32{pgtype.Int4OID}})
		s.observeBackendMessage(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	}
	assert.Len(t, s.paramTypes.described, maxDescribedStmts, "a full cache evicts one entry, not all of them")
	assert.Contains(t, s.paramTypes.described, strconv.Itoa(maxDescribedStmts+9), "the newest statement is kept")
}

func TestParameterDescriptionsPairWithDescribesInOrder(t *testing.T) {
	s := New(Config{EnableTraceParameters: true})
	s.paramTypes.describing = []pendingDescribe{{stmt: describedStmt{key: "a"}}, {stmt: describedStmt{key: "b"}}}
	reused := []uint32{pgtype.Int4OID}
	s.observeBackendMessage(&pgproto3.ParameterDescription{ParameterOIDs: reused})
	reused[0] = pgtype.TextOID // pgproto3 reuses the slice for the next message
	s.observeBackendMessage(&pgproto3.ParameterDescription{ParameterOIDs: reused})
	s.observeBackendMessage(&pgproto3.ParameterDescription{ParameterOIDs: []uint32{pgtype.BoolOID}})
	assert.Equal(t, map[string][]uint32{"a": {pgtype.Int4OID}, "b": {pgtype.TextOID}}, s.paramTypes.described,
		"a reply with no Describe left is ignored")
}

func TestDescribedTypesSkipFullyDeclaredStatements(t *testing.T) {
	s := New(Config{EnableTraceParameters: true})
	s.paramTypes.describing = []pendingDescribe{{stmt: describedStmt{key: "a"}, declared: declaredPrefix([]uint32{pgtype.Int8OID})}}
	s.observeBackendMessage(&pgproto3.ParameterDescription{ParameterOIDs: []uint32{pgtype.Int8OID}})
	assert.Empty(t, s.paramTypes.described, "Parse already declared every type")
}

func TestUnnamedStatementTypesAreForgottenWhenReplaced(t *testing.T) {
	s := New(Config{EnableTraceParameters: true})
	describe := func(oids ...uint32) {
		s.observeClientMessage(&pgproto3.Describe{ObjectType: 'S'})
		s.observeBackendMessage(&pgproto3.ParameterDescription{ParameterOIDs: oids})
	}
	s.observeClientMessage(&pgproto3.Parse{})
	describe(pgtype.Int4OID)
	assert.Equal(t, []uint32{pgtype.Int4OID}, s.paramTypes.unnamedDescribed)

	s.observeClientMessage(&pgproto3.Parse{})
	assert.Empty(t, s.paramTypes.unnamedDescribed)

	s.observeClientMessage(&pgproto3.Parse{ParameterOIDs: []uint32{pgtype.Int4OID}})
	s.observeClientMessage(&pgproto3.Query{String: "SELECT 1"})
	assert.Empty(t, s.paramTypes.unnamed, "a simple query destroys the unnamed statement")
}

// In a pipeline, a reply can describe an unnamed statement that a later Parse
// has already replaced; its types must not be taken for the new statement.
func TestLateDescriptionOfReplacedUnnamedStatementIsDropped(t *testing.T) {
	s := New(Config{EnableTraceParameters: true})
	s.observeClientMessage(&pgproto3.Parse{Query: "SELECT $1"})
	s.observeClientMessage(&pgproto3.Describe{ObjectType: 'S'})
	s.observeClientMessage(&pgproto3.Parse{Query: "SELECT $1::text"})
	s.observeBackendMessage(&pgproto3.ParameterDescription{ParameterOIDs: []uint32{pgtype.Int8OID}})
	assert.Empty(t, s.paramTypes.unnamedDescribed)
	stmt, _, ok := s.findStmt("")
	require.True(t, ok)
	assert.Nil(t, s.paramTypes.describedOIDs(stmt))
}

// A Bind naming a statement the proxy does not know must not borrow another
// statement's types.
func TestBindOfUnknownStatementHasNoTypes(t *testing.T) {
	traceState := &query.TraceState{}
	s := New(Config{EnableTraceParameters: true, TraceState: traceState})
	s.observeClientMessage(&pgproto3.Parse{})
	s.observeClientMessage(&pgproto3.Describe{ObjectType: 'S'})
	s.observeBackendMessage(&pgproto3.ParameterDescription{ParameterOIDs: []uint32{pgtype.Int8OID}})

	tracer := &recordingTracer{}
	t.Cleanup(pgproxy.Bootstrap(pgproxy.Dependencies{Tracer: tracer}))
	traceState.StartRootTrace(&pgproto3.Parse{Query: "SELECT 1"})
	s.observeClientMessage(&pgproto3.Bind{PreparedStatement: "missing", ParameterFormatCodes: []int16{1}, Parameters: [][]byte{int8Param(7)}})
	traceState.FinishRootTrace()
	assert.Equal(t, `\x0000000000000007`, tracer.rootTags(0)["db.parameters.0"], "unknown type, so hex")
}

// recordingTracer keeps the tags set on each root span, in start order. The
// session sets root tags after replying to the client, so access is locked.
type recordingTracer struct {
	mu    sync.Mutex
	roots []map[string]any
}

func (t *recordingTracer) StartRoot(string, string) (pgproxy.Span, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.roots = append(t.roots, map[string]any{})
	return recordingSpan{tracer: t, root: len(t.roots) - 1}, false
}
func (*recordingTracer) HasQueryContext(string) bool                       { return false }
func (*recordingTracer) WarnNamedPreparedStatement(string, string, string) {}

// rootTags returns the tags of the i-th root span, or nil.
func (t *recordingTracer) rootTags(i int) map[string]any {
	t.mu.Lock()
	defer t.mu.Unlock()
	if i >= len(t.roots) {
		return nil
	}
	return t.roots[i]
}

// recordingSpan records tags on its root; children (root < 0) drop them.
type recordingSpan struct {
	tracer *recordingTracer
	root   int
}

func (s recordingSpan) StartChild(string) pgproxy.Span {
	return recordingSpan{tracer: s.tracer, root: -1}
}
func (s recordingSpan) SetTag(key string, value any) {
	if s.root < 0 {
		return
	}
	s.tracer.mu.Lock()
	defer s.tracer.mu.Unlock()
	s.tracer.roots[s.root][key] = value
}
func (recordingSpan) Finish(error) {}

// fuzzOIDs are the types the fuzzer picks from: every type the renderer
// decodes, arrays of them, types it does not, and no type at all.
var fuzzOIDs = []uint32{
	0, pgtype.BoolOID, pgtype.Int2OID, pgtype.Int4OID, pgtype.Int8OID, pgtype.OIDOID,
	pgtype.Float4OID, pgtype.Float8OID, pgtype.NumericOID, pgtype.UUIDOID, pgtype.DateOID,
	pgtype.TimestampOID, pgtype.TimestamptzOID, pgtype.TextOID, pgtype.VarcharOID,
	pgtype.JSONBOID, pgtype.ByteaOID, pgtype.Int8ArrayOID, pgtype.TextArrayOID,
	pgtype.NumericArrayOID, pgtype.UUIDArrayOID, pgtype.IntervalOID,
}

// fuzzBind builds a Bind from fuzz input: values are separated by 0xFF, a lone
// 0xFE is NULL, and formats is none, one code for all, or a code per value.
func fuzzBind(values, formats []byte) *pgproto3.Bind {
	b := &pgproto3.Bind{}
	for i, v := range bytes.Split(values, []byte{0xFF}) {
		if i == bindparams.MaxParams+20 {
			break
		}
		if bytes.Equal(v, []byte{0xFE}) {
			v = nil
		}
		b.Parameters = append(b.Parameters, v)
	}
	switch len(formats) {
	case 0:
	case 1:
		b.ParameterFormatCodes = []int16{int16(formats[0] & 1)}
	default:
		for i := range b.Parameters {
			b.ParameterFormatCodes = append(b.ParameterFormatCodes, int16(formats[i%len(formats)]&1))
		}
	}
	return b
}

// FuzzBindParameterCapture drives Binds through a running session, with types
// declared in Parse or described by the backend a cycle earlier, and checks
// what must hold whatever the client sends: Postgres receives the values
// unchanged, and the root carries contiguous, bounded, valid UTF-8 tags.
func FuzzBindParameterCapture(f *testing.F) {
	int8s := func(vs ...int64) []byte {
		var out [][]byte
		for _, v := range vs {
			out = append(out, int8Param(v))
		}
		return bytes.Join(out, []byte{0xFF})
	}
	array, err := pgtype.NewMap().Encode(pgtype.TextArrayOID, pgtype.BinaryFormatCode, []string{"a,b", "c"}, nil)
	require.NoError(f, err)
	f.Add([]byte{4, 4}, int8s(7, -1), []byte{1}, true)                                                 // pgx: described, binary
	f.Add([]byte{4, 0}, int8s(7, 8), []byte{1, 0}, false)                                              // declared, mixed formats
	f.Add([]byte{18}, array, []byte{1}, true)                                                          // binary text[]
	f.Add([]byte{13, 13}, []byte("plain\xFFtext\xc3"), []byte{}, false)                                // text, invalid UTF-8
	f.Add([]byte{0, 3}, []byte{0xFE, 0xFF, 1, 2, 3}, []byte{1}, false)                                 // NULL, malformed int4
	f.Add([]byte{8}, bytes.Repeat([]byte{0xFF}, 300), []byte{0}, false)                                // past MaxParams
	f.Add([]byte{13}, bytes.Repeat(append(bytes.Repeat([]byte("x"), 300), 0xFF), 20), []byte{0}, true) // past the render cap

	f.Fuzz(func(t *testing.T, types, values, formats []byte, describe bool) {
		tracer := &recordingTracer{}
		t.Cleanup(pgproxy.Bootstrap(pgproxy.Dependencies{Tracer: tracer}))
		var oids []uint32
		for _, b := range types {
			oids = append(oids, fuzzOIDs[int(b)%len(fuzzOIDs)])
		}
		bind := fuzzBind(values, formats)
		// The test pipes carry at most 8 KiB per flush; the copy budget and
		// larger Binds are covered by the bindparams tests.
		if len(values)+4*len(bind.Parameters)+2*len(bind.ParameterFormatCodes)+4*len(oids) > 7<<10 {
			t.Skip("too large for the test pipe")
		}
		_, client, postgres, _, closeSession := newRunningTestSessionWithBackend(t, func(cfg *Config) {
			cfg.EnableTraceParameters = true
		})
		defer closeSession()

		parse := &pgproto3.Parse{Query: "SELECT 1", ParameterOIDs: oids}
		bindRoot := 0
		if describe {
			parse.ParameterOIDs = nil
			exchange(t, client, postgres, oids, parse, &pgproto3.Describe{ObjectType: 'S'}, &pgproto3.Sync{})
			bindRoot = 1
		}
		sends := []pgproto3.FrontendMessage{bind, &pgproto3.Execute{}, &pgproto3.Sync{}}
		if !describe {
			sends = append([]pgproto3.FrontendMessage{parse}, sends...)
		}
		forwarded := exchange(t, client, postgres, oids, sends...)
		closeSession() // joins the session, so the Bind cycle's root has finished
		tags := tracer.rootTags(bindRoot)
		require.NotNil(t, tags, "the Bind cycle has a root span")

		require.NotNil(t, forwarded)
		require.Equal(t, bind.Parameters, forwarded.Parameters, "values reach Postgres unchanged")
		require.Equal(t, bind.ParameterFormatCodes, forwarded.ParameterFormatCodes)

		positions, total := 0, 0
		for key, value := range tags {
			if key == bindparams.TruncatedTag || !strings.HasPrefix(key, bindparams.TagPrefix) {
				continue
			}
			positions++
			s, ok := value.(string)
			require.True(t, ok, "%s is a %T", key, value)
			require.True(t, utf8.ValidString(s), "%s is not valid UTF-8: %q", key, s)
			total += len(s)
		}
		for i := range positions {
			value, ok := tags[bindparams.TagPrefix+strconv.Itoa(i)]
			require.True(t, ok, "positions are contiguous from 0, missing %d of %d", i, positions)
			v := bind.Parameters[i]
			binary := len(bind.ParameterFormatCodes) == 1 && bind.ParameterFormatCodes[0] == 1 ||
				len(bind.ParameterFormatCodes) > 1 && bind.ParameterFormatCodes[i] == 1
			switch {
			case v == nil:
				require.Equal(t, "null", value)
			case !binary && len(v) <= bindparams.ValueLimit && utf8.Valid(v):
				require.Equal(t, string(v), value, "short valid text is shown as sent")
			}
		}
		require.LessOrEqual(t, total, bindparams.TotalLimit)
		require.LessOrEqual(t, positions, bindparams.MaxParams)
		_, truncated := tags[bindparams.TruncatedTag]
		require.Equal(t, positions < len(bind.Parameters), truncated, "truncated is set exactly when positions are dropped")
	})
}
