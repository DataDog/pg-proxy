// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package datadog

import (
	"fmt"
	"os"
	"strings"
	"time"

	pgproxy "github.com/DataDog/pg-proxy"
	"github.com/DataDog/pg-proxy/pkg/query"
)

// ChildSpanMode selects how native Datadog query phases are observed.
type ChildSpanMode string

const (
	ChildSpansFull    ChildSpanMode = "full"
	ChildSpansShadow  ChildSpanMode = "shadow"
	ChildSpansSampled ChildSpanMode = "sampled"
)

// Validate checks the mode and its compatibility with single-span sampling.
func (m ChildSpanMode) Validate() error {
	switch m {
	case "", ChildSpansFull, ChildSpansShadow:
		return nil
	case ChildSpansSampled:
		for _, key := range []string{"DD_SPAN_SAMPLING_RULES", "DD_SPAN_SAMPLING_RULES_FILE"} {
			if strings.TrimSpace(os.Getenv(key)) != "" {
				return fmt.Errorf("sampled child spans cannot be combined with %s", key)
			}
		}
		return nil
	default:
		return fmt.Errorf("child span mode must be full, shadow, or sampled")
	}
}

func (m ChildSpanMode) enabled() bool {
	return m == ChildSpansShadow || m == ChildSpansSampled
}

type operationTracer struct {
	Tracer
	mode ChildSpanMode
}

func (t operationTracer) StartRoot(operation, sql string) (pgproxy.Span, bool) {
	root, propagated := startRoot(operation, sql)
	return operationRoot{span: root, canSuppress: t.mode == ChildSpansSampled && !propagated}, propagated
}

type operationRoot struct {
	span
	canSuppress bool
}

func (s operationRoot) KeepChildSpans() bool {
	return !s.canSuppress || s.Sampled()
}

// Operation tags are static and bounded. Resource and peer tags on Datadog
// APM statistics cannot be reconstructed from these metrics.
var operationTags = map[string][3][]string{
	query.HandlingMessagesOpName:    tagsForOperation("handling_messages"),
	query.ForwardOpName:             tagsForOperation("forwarding_to_postgres"),
	query.QueryExecutionAndProxying: tagsForOperation("query_execution_and_proxying"),
	query.BatchExecutionAndProxying: tagsForOperation("batch_execution_and_proxying"),
	query.DataRoundTrips:            tagsForOperation("returning_rows_to_client"),
}

func tagsForOperation(operation string) [3][]string {
	tag := "operation:" + operation
	return [3][]string{
		pgproxy.OperationSucceeded:    {tag},
		pgproxy.OperationProxyError:   {tag, "source:proxy"},
		pgproxy.OperationBackendError: {tag, "source:postgres"},
	}
}

func (operationRoot) RecordOperation(operation string, elapsed time.Duration, outcome pgproxy.OperationOutcome) {
	tags, ok := operationTags[operation]
	if !ok {
		return
	}
	metrics := pgproxy.Metrics()
	all := tags[pgproxy.OperationSucceeded]
	_ = metrics.Incr("pg_proxy.trace_operations.hits", all, 1)
	if outcome != pgproxy.OperationSucceeded {
		_ = metrics.Incr("pg_proxy.trace_operations.errors", tags[outcome], 1)
	}
	_ = metrics.Distribution("pg_proxy.trace_operations.latency_us", float64(elapsed)/float64(time.Microsecond), all, 1)
}
