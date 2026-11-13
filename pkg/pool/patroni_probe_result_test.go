// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package pool

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClassifyPatroniProbeResult(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want PatroniProbeResult
	}{
		{name: "response", want: PatroniProbeResponse},
		{name: "deadline", err: context.DeadlineExceeded, want: PatroniProbeTimeout},
		{name: "wrapped HTTP deadline", err: &url.Error{Op: "Get", URL: "https://example.invalid", Err: fmt.Errorf("request: %w", context.DeadlineExceeded)}, want: PatroniProbeTimeout},
		{name: "network timeout", err: &net.DNSError{IsTimeout: true}, want: PatroniProbeTimeout},
		{name: "other error", err: errors.New("connection refused"), want: PatroniProbeError},
		{name: "cancellation is not a timeout", err: context.Canceled, want: PatroniProbeError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, classifyPatroniProbeResult(tc.err))
		})
	}
}

func TestSQLPatroniProbeResult(t *testing.T) {
	for _, tc := range []struct {
		name      string
		sql       map[string]bool
		patroni   map[string]bool
		results   map[string]PatroniProbeResult
		retained  []string
		conflicts []string
		want      PatroniProbeResult
	}{
		{name: "no observations", want: PatroniProbeUnknown},
		{name: "SQL discovers primary before Patroni", sql: map[string]bool{"B": true}, want: PatroniProbeUnknown},
		{name: "explicit role disagreement", sql: map[string]bool{"B": true}, patroni: map[string]bool{"B": false}, results: map[string]PatroniProbeResult{"B": PatroniProbeResponse}, want: PatroniProbeResponse},
		{name: "primary timed out", sql: map[string]bool{"B": true}, patroni: map[string]bool{"B": false}, results: map[string]PatroniProbeResult{"B": PatroniProbeTimeout}, want: PatroniProbeTimeout},
		{name: "primary error", sql: map[string]bool{"B": true}, patroni: map[string]bool{"B": false}, results: map[string]PatroniProbeResult{"B": PatroniProbeError}, want: PatroniProbeError},
		{name: "standby timeout does not explain primary disagreement", sql: map[string]bool{"B": true, "C": false}, patroni: map[string]bool{"B": false, "C": false}, results: map[string]PatroniProbeResult{"B": PatroniProbeResponse, "C": PatroniProbeTimeout}, want: PatroniProbeResponse},
		{name: "matching snapshots include standby timeouts", sql: map[string]bool{"B": true, "C": false}, patroni: map[string]bool{"B": true, "C": false}, results: map[string]PatroniProbeResult{"B": PatroniProbeResponse, "C": PatroniProbeTimeout}, want: PatroniProbeTimeout},
		{name: "additional Patroni primary", sql: map[string]bool{"B": true}, patroni: map[string]bool{"A": true, "B": true}, results: map[string]PatroniProbeResult{"A": PatroniProbeResponse, "B": PatroniProbeTimeout}, want: PatroniProbeResponse},
		{name: "unknown SQL IP is neutral", sql: map[string]bool{"B": true}, patroni: map[string]bool{"A": true, "B": false}, retained: []string{"A", "B"}, results: map[string]PatroniProbeResult{"A": PatroniProbeTimeout, "B": PatroniProbeResponse}, want: PatroniProbeResponse},
		{name: "membership uses missing IP", sql: map[string]bool{"B": true, "C": false}, patroni: map[string]bool{"B": true}, results: map[string]PatroniProbeResult{"B": PatroniProbeResponse}, want: PatroniProbeUnknown},
		{name: "extra Patroni member", sql: map[string]bool{"B": true}, patroni: map[string]bool{"B": true, "C": false}, results: map[string]PatroniProbeResult{"B": PatroniProbeResponse, "C": PatroniProbeError}, want: PatroniProbeError},
		{name: "role conflict uses conflicting IP", sql: map[string]bool{"B": true}, patroni: map[string]bool{"B": true, "C": false}, conflicts: []string{"C"}, results: map[string]PatroniProbeResult{"B": PatroniProbeTimeout, "C": PatroniProbeResponse}, want: PatroniProbeResponse},
		{name: "timeout precedes error and unknown", sql: map[string]bool{"A": true, "B": true, "C": true}, patroni: map[string]bool{"A": false, "B": false, "C": false}, results: map[string]PatroniProbeResult{"A": PatroniProbeError, "B": PatroniProbeTimeout}, want: PatroniProbeTimeout},
		{name: "error precedes unknown", sql: map[string]bool{"A": true, "B": true}, patroni: map[string]bool{"A": false, "B": false}, results: map[string]PatroniProbeResult{"A": PatroniProbeError}, want: PatroniProbeError},
		{name: "unobserved retained IP in matching snapshot", sql: map[string]bool{"B": true}, patroni: map[string]bool{"B": true}, retained: []string{"B", "C"}, results: map[string]PatroniProbeResult{"B": PatroniProbeResponse}, want: PatroniProbeUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := SQLPatroniHealthSnapshot{SQLHealth: tc.sql, PatroniHealth: tc.patroni, PatroniProbeResults: tc.results, SQLRetainedIPs: tc.retained, SQLConflictingIPs: tc.conflicts}
			assert.Equal(t, tc.want, snapshot.PatroniProbeResult())
		})
	}
}

func TestPatroniComparisonKeepsCompletedObservationDuringRetry(t *testing.T) {
	const ip = "10.0.0.1"
	p := newMockProber(map[string][]string{"primary.local": {ip}})
	p.probeErrors[ip] = context.DeadlineExceeded
	w := newTestWorker(p)
	health := receivePoolHealth(t, w.Subscribe("primary.local", false, "db", nil, nil))
	sql := &sqlHealthComparisonSnapshot{health: map[string]bool{ip: true}, retainedIPs: []string{ip}}
	var comparisons []SQLPatroniHealthSnapshot
	w.hooks.SQLPatroniCompared = func(snapshot SQLPatroniHealthSnapshot, tags []string) {
		assert.Contains(t, tags, "patroni_probe_result:"+string(snapshot.PatroniProbeResult()))
		comparisons = append(comparisons, snapshot)
		// Hook tags must not overwrite the host's tags through spare slice capacity.
		tags[len(tags)-1] = "hook:changed"
	}
	w.reportSQLPatroniComparison(health, sql)
	require.Len(t, comparisons, 1)
	assert.Equal(t, PatroniProbeTimeout, comparisons[0].PatroniProbeResult())

	// The embedder retries a timed-out HTTP attempt, then receives a valid 503. While it
	// retries, comparisons must continue using the previous completed health/result pair.
	retrying, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	requests := 0
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		requests++
		if requests == 1 {
			close(retrying)
			<-release
			return nil, context.DeadlineExceeded
		}
		return &http.Response{StatusCode: http.StatusServiceUnavailable, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
	})}
	p.mockHealthPing = func(string) (bool, error) {
		ok, _, _, err := ProbePatroni(PatroniProbe{IP: ip, Path: "/master", Client: client})
		if err != nil {
			ok, _, _, err = ProbePatroni(PatroniProbe{IP: ip, Path: "/master", Client: client})
		}
		return ok, err
	}
	done := make(chan struct{})
	go func() { defer close(done); w.Refresh() }()
	<-retrying
	w.reportSQLPatroniComparison(health, sql)
	assert.Equal(t, PatroniProbeTimeout, comparisons[1].PatroniProbeResult())
	unblock()
	<-done
	w.reportSQLPatroniComparison(health, sql)
	require.Len(t, comparisons, 3, "one callback per SQL comparison, with no additional per-IP counts")
	assert.Equal(t, 2, requests)
	assert.Equal(t, PatroniProbeResponse, comparisons[2].PatroniProbeResult())
	assert.Equal(t, SQLPatroniPrimaryDisagreement, comparisons[2].MismatchReason(), "503 still disagrees with SQL primary")
	assert.Equal(t, PatroniProbeTimeout, comparisons[0].PatroniProbeResult(), "older snapshots must remain stable")
	assert.NotContains(t, health.patroniTags, "hook:changed")

	comparisons[2].PatroniHealth[ip] = true
	comparisons[2].PatroniProbeResults[ip] = PatroniProbeTimeout
	w.reportSQLPatroniComparison(health, sql)
	assert.False(t, comparisons[3].PatroniHealth[ip], "hook mutation must not affect worker state")
	assert.Equal(t, PatroniProbeResponse, comparisons[3].PatroniProbeResult())
}

func TestPatroniComparisonObservationAtomic(t *testing.T) {
	const ip = "10.0.0.1"
	w := newTestWorker(newMockProber(map[string][]string{"primary.local": {ip}}, ip))
	health := receivePoolHealth(t, w.Subscribe("primary.local", false, "db", nil, nil))
	sql := &sqlHealthComparisonSnapshot{health: map[string]bool{ip: true}, retainedIPs: []string{ip}}
	w.hooks.SQLPatroniCompared = func(snapshot SQLPatroniHealthSnapshot, _ []string) {
		want := PatroniProbeTimeout
		if snapshot.PatroniHealth[ip] {
			want = PatroniProbeResponse
		}
		assert.Equal(t, want, snapshot.PatroniProbeResults[ip], "health and result must belong to the same refresh")
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range 1000 {
			healthy, result := i%2 == 0, PatroniProbeTimeout
			if healthy {
				result = PatroniProbeResponse
			}
			w.updateHealthAndCleanup(health, map[string]bool{ip: healthy}, nil, map[string]PatroniProbeResult{ip: result}, []string{ip})
		}
	}()
	for range 1000 {
		w.reportSQLPatroniComparison(health, sql)
	}
	wg.Wait()
}

func TestPatroniProbeResultsEvictedWithHealth(t *testing.T) {
	const ip = "10.0.0.1"
	p := newMockProber(map[string][]string{"primary.local": {ip}})
	p.probeErrors[ip] = context.DeadlineExceeded
	w := newTestWorker(p)
	health := receivePoolHealth(t, w.Subscribe("primary.local", false, "db", nil, nil))
	p.ipsByHost["primary.local"] = nil
	for range maxFailedHostLookups {
		w.Refresh()
	}
	patroniHealth, results := health.patroniComparisonSnapshot()
	assert.Empty(t, patroniHealth)
	assert.Empty(t, results, "evicted IPs must not retain stale probe results")
}
