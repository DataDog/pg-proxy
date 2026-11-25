// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package datadog

import (
	"strings"
	"testing"
	"time"

	"github.com/DataDog/dd-trace-go/v2/ddtrace/ext"
	"github.com/DataDog/dd-trace-go/v2/ddtrace/mocktracer"
	"github.com/stretchr/testify/require"

	pgproxy "github.com/DataDog/pg-proxy"
	"github.com/DataDog/pg-proxy/pkg/query"
)

func TestOperationTracerKeepsPropagatedAndUndecidedChildren(t *testing.T) {
	mt := mocktracer.Start()
	defer mt.Stop()
	for _, tc := range []struct {
		name    string
		mode    ChildSpanMode
		sql     string
		dropped bool
		keep    bool
	}{
		{"shadow keeps all", ChildSpansShadow, "SELECT 1", true, true},
		{"sampled omits rejected local", ChildSpansSampled, "SELECT 1", true, false},
		{"sampled keeps undecided", ChildSpansSampled, "SELECT 1", false, true},
		{"sampled keeps rejected propagated", ChildSpansSampled, "SELECT 1 /*traceparent='00-0000000000000000000000000000002a-000000000000000b-00'*/", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tracer, err := NewTracer(tc.mode)
			require.NoError(t, err)
			root, _ := tracer.StartRoot("query", tc.sql)
			defer root.Finish(nil)
			if tc.dropped {
				root.SetTag(ext.ManualDrop, true)
			}
			require.Equal(t, tc.keep, root.(pgproxy.OperationMetricsReporter).KeepChildSpans())
		})
	}
}

func TestNewTracerDefaultsToFull(t *testing.T) {
	tracer, err := NewTracer("")
	require.NoError(t, err)
	require.IsType(t, Tracer{}, tracer)
}

type operationMetricsTestSink struct {
	pgproxy.NoopMetrics
	counters      map[string]int
	distributions map[string][]float64
}

func (s *operationMetricsTestSink) Incr(name string, tags []string, _ float64) error {
	s.counters[name+" "+strings.Join(tags, ",")]++
	return nil
}

func (s *operationMetricsTestSink) Distribution(name string, value float64, tags []string, _ float64) error {
	key := name + " " + strings.Join(tags, ",")
	s.distributions[key] = append(s.distributions[key], value)
	return nil
}

func TestOperationMetricsReportBoundedSeries(t *testing.T) {
	sink := &operationMetricsTestSink{counters: map[string]int{}, distributions: map[string][]float64{}}
	t.Cleanup(pgproxy.Bootstrap(pgproxy.Dependencies{Metrics: sink}))
	root := operationRoot{}
	root.RecordOperation(query.ForwardOpName, 3*time.Microsecond, pgproxy.OperationSucceeded)
	root.RecordOperation(query.ForwardOpName, 5*time.Microsecond, pgproxy.OperationProxyError)
	root.RecordOperation(query.ForwardOpName, 7*time.Microsecond, pgproxy.OperationBackendError)
	root.RecordOperation("unrecognized operation", 100*time.Microsecond, pgproxy.OperationSucceeded)
	require.Equal(t, map[string]int{
		"datadog.pg_proxy.trace_operations.hits operation:forwarding_to_postgres":                   3,
		"datadog.pg_proxy.trace_operations.errors operation:forwarding_to_postgres,source:proxy":    1,
		"datadog.pg_proxy.trace_operations.errors operation:forwarding_to_postgres,source:postgres": 1,
	}, sink.counters)
	require.Equal(t, map[string][]float64{
		"datadog.pg_proxy.trace_operations.latency_us operation:forwarding_to_postgres": {3, 5, 7},
	}, sink.distributions)
}
