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
