// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

//go:build integration

package integration

import (
	"maps"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/DataDog/pg-proxy/internal/itest"
)

// postgresMajor holds the expectations that differ between PostgreSQL majors.
type postgresMajor struct {
	// startupParameters are the parameters PostgreSQL reports with
	// ParameterStatus at startup.
	startupParameters []string
	// unproxiedStartupParameters are the startupParameters the proxy does not
	// report: it reports a fixed list, which predates them.
	unproxiedStartupParameters []string
}

// startupParameters14 are the parameters PostgreSQL 14 reports at startup.
var startupParameters14 = []string{
	"DateStyle", "IntervalStyle", "TimeZone", "application_name", "client_encoding", "default_transaction_read_only",
	"in_hot_standby", "integer_datetimes", "is_superuser", "server_encoding", "server_version", "session_authorization",
	"standard_conforming_strings",
}

// postgresMajors has an entry for each major the suite runs on (see
// internal/itest/images.go). Record here anything a test expects only on some
// majors, so that a new major, or a new difference, is added explicitly.
var postgresMajors = map[int]postgresMajor{
	14: {startupParameters: startupParameters14},
	18: {
		// PostgreSQL 16 added scram_iterations, and 18 search_path.
		startupParameters:          append(slices.Clone(startupParameters14), "scram_iterations", "search_path"),
		unproxiedStartupParameters: []string{"scram_iterations", "search_path"},
	},
}

// expectedOfMajor returns the expectations for the backend's major.
func expectedOfMajor(t *testing.T) postgresMajor {
	t.Helper()
	expected, ok := postgresMajors[backendMajor]
	require.True(t, ok, "postgresMajors has no entry for PostgreSQL %d", backendMajor)
	return expected
}

// TestStartupParameterStatus verifies the parameters reported with
// ParameterStatus at startup, directly and through the proxy, and that each
// reported value is the server's.
func TestStartupParameterStatus(t *testing.T) {
	expected := expectedOfMajor(t)
	proxied := slices.DeleteFunc(slices.Clone(expected.startupParameters), func(name string) bool {
		return slices.Contains(expected.unproxiedStartupParameters, name)
	})
	proxy := startProxy(t)
	for target, test := range map[string]struct {
		url  string
		want []string
	}{
		"direct": {backend.URL("postgres"), expected.startupParameters},
		"proxy":  {proxy.URL(dbPooled, itest.PostgresUser), proxied},
	} {
		t.Run(target, func(t *testing.T) {
			c := connectRaw(t, test.url)
			defer c.close()
			require.ElementsMatch(t, test.want, slices.Collect(maps.Keys(c.parameters)))
			requireParameterView(t, c)
		})
	}
}
