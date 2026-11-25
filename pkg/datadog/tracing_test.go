// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package datadog_test

import (
	"testing"

	"github.com/DataDog/dd-trace-go/v2/ddtrace/ext"
	"github.com/DataDog/dd-trace-go/v2/ddtrace/mocktracer"
	"github.com/stretchr/testify/require"

	pgproxy "github.com/DataDog/pg-proxy"
	proxydatadog "github.com/DataDog/pg-proxy/pkg/datadog"
)

func TestSpanSampled(t *testing.T) {
	const parent = "00-0000000000000000000000000000002a-000000000000000b-"
	testCases := []struct {
		name, sql string
		tag       string
		want      bool
	}{
		// mocktracer bypasses the sampler, so a local root has no priority.
		{name: "undecided priority counts as kept", sql: "SELECT 1", want: true},
		{name: "sampled parent", sql: "SELECT 1 /*traceparent='" + parent + "01'*/", want: true},
		{name: "unsampled parent", sql: "SELECT 1 /*traceparent='" + parent + "00'*/", want: false},
		{name: "manual drop", sql: "SELECT 1 /*traceparent='" + parent + "01'*/", tag: ext.ManualDrop, want: false},
		{name: "manual keep", sql: "SELECT 1 /*traceparent='" + parent + "00'*/", tag: ext.ManualKeep, want: true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mt := mocktracer.Start()
			defer mt.Stop()
			root, _ := proxydatadog.Tracer{}.StartRoot("query", tc.sql)
			defer root.Finish(nil)
			if tc.tag != "" {
				root.SetTag(tc.tag, true)
			}
			require.Equal(t, tc.want, root.(pgproxy.SamplingReporter).Sampled())
			require.Equal(t, tc.want, root.StartChild("child").(pgproxy.SamplingReporter).Sampled(), "children share the trace decision")
		})
	}
	t.Run("tracer not started", func(t *testing.T) {
		root, _ := proxydatadog.Tracer{}.StartRoot("query", "SELECT 1")
		require.False(t, root.(pgproxy.SamplingReporter).Sampled())
	})
}

func TestSpanDefaultsAndSQLCommentParent(t *testing.T) {
	const comment = "/*traceparent='00-0000000000000000000000000000002a-000000000000000b-01'*/"
	for _, tc := range []struct {
		name       string
		sql        string
		wantParent bool
	}{
		{name: "empty"},
		{name: "no comment", sql: "SELECT 'traceparent'"},
		{name: "ordinary comment", sql: "SELECT 1 /*ordinary comment*/"},
		{name: "malformed parent", sql: "SELECT 1 /*traceparent='invalid'*/"},
		{name: "leading parent", sql: comment + " SELECT 1", wantParent: true},
		{name: "embedded parent", sql: "SELECT " + comment + " 1", wantParent: true},
		{name: "trailing parent after ordinary comment", sql: "/*ordinary comment*/ SELECT 1 " + comment, wantParent: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mt := mocktracer.Start()
			defer mt.Stop()
			tracing := proxydatadog.Tracer{}
			require.Equal(t, tc.wantParent, tracing.HasQueryContext(tc.sql))
			root, propagated := tracing.StartRoot("query", tc.sql)
			require.Equal(t, tc.wantParent, propagated)
			child := root.StartChild("child")
			child.Finish(nil)
			root.Finish(nil)

			spans := mt.FinishedSpans()
			require.Len(t, spans, 2)
			gotChild, gotRoot := spans[0], spans[1]
			require.Equal(t, gotRoot.SpanID(), gotChild.ParentID())
			if tc.wantParent {
				require.Equal(t, uint64(11), gotRoot.ParentID())
				require.Equal(t, uint64(42), gotChild.TraceID())
			} else {
				require.Zero(t, gotRoot.ParentID())
			}
			for _, span := range spans {
				require.Equal(t, ext.SpanKindServer, span.Tag(ext.SpanKind))
				require.Equal(t, "postgres", span.Tag(ext.DBSystem))
			}
			require.Equal(t, tc.sql, gotRoot.Tag(ext.DBStatement))
			require.Nil(t, gotChild.Tag(ext.DBStatement), "root query must not mutate the shared options")
		})
	}
}
