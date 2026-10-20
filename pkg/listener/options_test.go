package listener

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"

	pgsession "github.com/DataDog/pg-proxy/pkg/session"
)

func TestParseSessionOptions_ErrorCases(t *testing.T) {
	options, err := ParseSessionOptions("abc%xy")
	assert.Error(t, err)
	assert.Equal(t, SessionOptions{}, options)
	assert.ErrorContains(t, err, "invalid URL escape")
}

func TestParseSessionOptions_WarningCases_NoError(t *testing.T) {
	for _, test := range []struct {
		name  string
		input string
	}{
		{name: "Empty input string", input: "   "},
		{name: "No pg_proxy flags present", input: "some_flag=123 other_flag=true"},
	} {
		t.Run(test.name, func(t *testing.T) {
			options, err := ParseSessionOptions(test.input)
			assert.NoError(t, err)
			assert.Equal(t, SessionOptions{}, options)
		})
	}
}

// TestParseSessionOptions_AbsentPriorityFoldsToDefault documents the
// observable-vs-structural distinction in Priority: an absent
// pg_proxy.priority option leaves SessionOptions.Priority at its zero
// (unset) value — it is never explicitly assigned — but reading it through
// Level() still observably yields DefaultPriority.
func TestParseSessionOptions_AbsentPriorityFoldsToDefault(t *testing.T) {
	options, err := ParseSessionOptions(url.QueryEscape("-c pg_proxy.dynamic_routing=true"))
	assert.NoError(t, err)
	assert.Equal(t, pgsession.Priority{}, options.Priority,
		"an absent pg_proxy.priority option must leave the underlying Priority unset (the zero value), not an explicit default")
	assert.Equal(t, pgsession.DefaultPriority.Level(), options.Priority.Level(),
		"an absent pg_proxy.priority option must observably fold to the default priority via Level()")
}

func TestParseSessionOptions_SuccessCases(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected SessionOptions
	}{
		{
			name:     "dynamic_routing only",
			input:    "-c pg_proxy.dynamic_routing=true",
			expected: SessionOptions{DynamicRoutingEnabled: true},
		},
		{
			name:     "passthrough_user option only",
			input:    "-c pg_proxy.passthrough_user=true",
			expected: SessionOptions{PassthroughUser: true},
		},
		{
			name:  "both known flags",
			input: "-c pg_proxy.dynamic_routing=true -c pg_proxy.passthrough_user=true",
			expected: SessionOptions{
				DynamicRoutingEnabled: true,
				PassthroughUser:       true,
			},
		},
		{
			name:     "auto_cached_prepared_statements true",
			input:    "-c pg_proxy.auto_cached_prepared_statements=true",
			expected: SessionOptions{AutoCachedPreparedStatements: true},
		},
		{
			name:     "auto_cached_prepared_statements false",
			input:    "-c pg_proxy.auto_cached_prepared_statements=false",
			expected: SessionOptions{},
		},
		{
			name:  "auto_cached_prepared_statements with dynamic_routing",
			input: "-c pg_proxy.auto_cached_prepared_statements=true -c pg_proxy.dynamic_routing=true",
			expected: SessionOptions{
				DynamicRoutingEnabled:        true,
				AutoCachedPreparedStatements: true,
			},
		},
		{
			name:  "auto_cached_prepared_statements with passthrough user",
			input: "-c pg_proxy.auto_cached_prepared_statements=true -c pg_proxy.passthrough_user=true",
			expected: SessionOptions{
				PassthroughUser:              true,
				AutoCachedPreparedStatements: true,
			},
		},
		{
			name:  "all three known flags",
			input: "-c pg_proxy.dynamic_routing=true -c pg_proxy.passthrough_user=true -c pg_proxy.auto_cached_prepared_statements=true",
			expected: SessionOptions{
				DynamicRoutingEnabled:        true,
				PassthroughUser:              true,
				AutoCachedPreparedStatements: true,
			},
		},
		{
			name:     "unknown flag only",
			input:    "-c pg_proxy.debug=true",
			expected: SessionOptions{},
		},
		{
			name:     "known + unknown flag",
			input:    "-c pg_proxy.passthrough_user=true -c pg_proxy.debug=true",
			expected: SessionOptions{PassthroughUser: true},
		},
		{
			name:     "auto_cached_prepared_statements with unknown flag",
			input:    "-c pg_proxy.auto_cached_prepared_statements=true -c pg_proxy.debug=true",
			expected: SessionOptions{AutoCachedPreparedStatements: true},
		},
		{
			name:     "priority 0 is the highest priority, not unset",
			input:    "-c pg_proxy.priority=0",
			expected: SessionOptions{Priority: pgsession.P0},
		},
		{
			name:     "priority 1",
			input:    "-c pg_proxy.priority=1",
			expected: SessionOptions{Priority: pgsession.P1},
		},
		{
			name:     "priority 2",
			input:    "-c pg_proxy.priority=2",
			expected: SessionOptions{Priority: pgsession.P2},
		},
		{
			name:     "priority 3",
			input:    "-c pg_proxy.priority=3",
			expected: SessionOptions{Priority: pgsession.P3},
		},
		{
			name:     "priority 4",
			input:    "-c pg_proxy.priority=4",
			expected: SessionOptions{Priority: pgsession.P4},
		},
		{
			name:     "priority 5 is the lowest priority",
			input:    "-c pg_proxy.priority=5",
			expected: SessionOptions{Priority: pgsession.P5},
		},
		{
			name:     "priority non-integer present but unparseable falls back to the default",
			input:    "-c pg_proxy.priority=notanumber",
			expected: SessionOptions{Priority: pgsession.DefaultPriority},
		},
		{
			name:     "priority out of range (6) falls back to the default",
			input:    "-c pg_proxy.priority=6",
			expected: SessionOptions{Priority: pgsession.DefaultPriority},
		},
		{
			name:     "priority alphanumeric value matches the options regex but fails to parse, falls back to the default",
			input:    "-c pg_proxy.priority=3x",
			expected: SessionOptions{Priority: pgsession.DefaultPriority},
		},
		{
			name:     "priority negative value is not matched by the options regex, behaves like priority absent",
			input:    "-c pg_proxy.priority=-1",
			expected: SessionOptions{},
		},
		{
			name:     "priority float value is rejected whole, not truncated to its integer prefix",
			input:    "-c pg_proxy.priority=1.5",
			expected: SessionOptions{Priority: pgsession.DefaultPriority},
		},
		{
			name:     "priority range suffix is rejected whole, not truncated to its integer prefix",
			input:    "-c pg_proxy.priority=1-5",
			expected: SessionOptions{Priority: pgsession.DefaultPriority},
		},
		{
			name:     "priority comma suffix is rejected whole, not truncated to its integer prefix",
			input:    "-c pg_proxy.priority=1,5",
			expected: SessionOptions{Priority: pgsession.DefaultPriority},
		},
		{
			name:     "repeated priority: the last occurrence wins (0 then 5 yields P5)",
			input:    "-c pg_proxy.priority=0 -c pg_proxy.priority=5",
			expected: SessionOptions{Priority: pgsession.P5},
		},
		{
			name:     "repeated priority: last occurrence wins in reverse order too (5 then 0 yields P0)",
			input:    "-c pg_proxy.priority=5 -c pg_proxy.priority=0",
			expected: SessionOptions{Priority: pgsession.P0},
		},
		{
			name:     "repeated priority: malformed last occurrence falls back to the default",
			input:    "-c pg_proxy.priority=0 -c pg_proxy.priority=1.5",
			expected: SessionOptions{Priority: pgsession.DefaultPriority},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options, err := ParseSessionOptions(url.QueryEscape(test.input))
			assert.NoError(t, err)
			assert.Equal(t, test.expected, options)
		})
	}
}

func TestParseBoolOrFalse(t *testing.T) {
	for _, test := range []struct {
		value    string
		expected bool
	}{
		{"true", true},
		{"false", false},
		{"notabool", false},
		{"", false},
	} {
		assert.Equal(t, test.expected, parseBoolOrFalse("ignored_key", test.value))
	}
}
