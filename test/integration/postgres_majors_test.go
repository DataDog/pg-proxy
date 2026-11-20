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
	// report, a known bug (see unproxiedStartupParametersBug).
	unproxiedStartupParameters []string
}

// unproxiedStartupParametersBug is a known bug. The proxy run of
// TestStartupParameterStatus expects only the parameters the proxy reports;
// set PG_PROXY_ITEST_RUN_KNOWN_BUGS=1 to expect all of PostgreSQL's.
const unproxiedStartupParametersBug = "TODO: proxy reports a fixed list of startup parameters, " +
	"which lacks those that PostgreSQL 16 and later add: %v"

// Parameters PostgreSQL reports at startup, from the major that first does.
var (
	startupParameters14 = []string{
		"DateStyle", "IntervalStyle", "TimeZone", "application_name", "client_encoding", "default_transaction_read_only",
		"in_hot_standby", "integer_datetimes", "is_superuser", "server_encoding", "server_version", "session_authorization",
		"standard_conforming_strings",
	}
	startupParameters16 = append(slices.Clone(startupParameters14), "scram_iterations")
	startupParameters18 = append(slices.Clone(startupParameters16), "search_path")
)

// postgresMajors has an entry for each supported major. CI runs those with a
// pinned image (see internal/itest/images.go); run the others with
// PG_PROXY_TEST_POSTGRES_IMAGE. Record here anything a test expects only on
// some majors, so that a new major, or a new difference, is added explicitly.
var postgresMajors = map[int]postgresMajor{
	14: {startupParameters: startupParameters14},
	15: {startupParameters: startupParameters14},
	16: {startupParameters: startupParameters16, unproxiedStartupParameters: []string{"scram_iterations"}},
	17: {startupParameters: startupParameters16, unproxiedStartupParameters: []string{"scram_iterations"}},
	18: {startupParameters: startupParameters18, unproxiedStartupParameters: []string{"scram_iterations", "search_path"}},
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
	proxy := startProxy(t)
	for target, url := range map[string]string{
		"direct": backend.URL("postgres"),
		"proxy":  proxy.URL(dbPooled, itest.PostgresUser),
	} {
		t.Run(target, func(t *testing.T) {
			want := expected.startupParameters
			if target == "proxy" && len(expected.unproxiedStartupParameters) > 0 && !runKnownBugs {
				t.Logf(unproxiedStartupParametersBug, expected.unproxiedStartupParameters)
				want = slices.DeleteFunc(slices.Clone(want), func(name string) bool {
					return slices.Contains(expected.unproxiedStartupParameters, name)
				})
			}
			c := connectRaw(t, url)
			defer c.close()
			require.ElementsMatch(t, want, slices.Collect(maps.Keys(c.parameters)))
			requireParameterView(t, c)
		})
	}
}
