// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package listener

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	pgproxy "github.com/DataDog/pg-proxy"
	pgsession "github.com/DataDog/pg-proxy/pkg/session"
)

// These are the pg_proxy option keys (without the "pg_proxy." prefix)
const (
	passthroughUserOption              = "passthrough_user"
	dynamicRoutingOption               = "dynamic_routing"
	autoCachedPreparedStatementsOption = "auto_cached_prepared_statements"
	priorityOption                     = "priority"
)

// Uses custom options that are consumed by Postgres to use with extensions.
// This allows custom parameters in the Postgres URL to propagate from clients
// to the proxy.
// See https://www.postgresql.org/docs/current/config-setting.html#CONFIG-SETTING-SHELL
// and https://www.postgresql.org/docs/current/runtime-config-custom.html
// for more information on the format, particularly the "-c" parameter and the
// pg-proxy.<option> syntax.
//
// TODO: Remove `-c` because it's not needed and adds complexity.
// Matches flags like: pg_proxy.<key>=<value> where both are alphanumeric/underscore
var optionsRegex = regexp.MustCompile(`-c pg_proxy\.(\w+)=([\w$]+)`)

// SessionOptions are pg_proxy options supplied in a PostgreSQL startup packet.
type SessionOptions struct {
	DynamicRoutingEnabled        bool
	PassthroughUser              bool
	AutoCachedPreparedStatements bool
	// Priority is the client-requested query-level workload priority (see
	// pgsession.Priority). Reading it via Level() folds an absent
	// pg_proxy.priority option, or an invalid supplied value, to
	// pgsession.DefaultPriority.
	Priority pgsession.Priority
}

// ParseSessionOptions parses the PostgreSQL startup "options" parameter.
func ParseSessionOptions(options string) (SessionOptions, error) {
	unescaped, err := url.QueryUnescape(options)
	if err != nil {
		pgproxy.Logger().Warn("error parsing options", "reason", "url_decode_failed")
		return SessionOptions{}, errors.New("failed to URL-decode options string")
	}
	matches, err := parsedOptions(unescaped)
	if err != nil {
		pgproxy.Logger().Warn("error parsing options", "reason", "invalid_match")
		return SessionOptions{}, err
	}

	var result SessionOptions
	for key, value := range matches {
		switch key {
		case dynamicRoutingOption:
			result.DynamicRoutingEnabled = parseBoolOrFalse(key, value)
		case passthroughUserOption:
			result.PassthroughUser = parseBoolOrFalse(key, value)
		case autoCachedPreparedStatementsOption:
			result.AutoCachedPreparedStatements = parseBoolOrFalse(key, value)
		case priorityOption:
			result.Priority = parsePriorityOrDefault(unescaped, key, value)
		default:
			pgproxy.Logger().Warn("unknown option", "reason", "unknown_option")
		}
	}
	return result, nil
}

func parsedOptions(options string) (map[string]string, error) {
	if strings.TrimSpace(options) == "" {
		return map[string]string{}, nil
	}
	matches := optionsRegex.FindAllStringSubmatch(options, -1)
	if len(matches) == 0 {
		pgproxy.Logger().Warn("no pg_proxy options found (expected `-c pg_proxy.<option_name>=<value>`)", "reason", "unrecognized_options")
		return map[string]string{}, nil
	}
	result := make(map[string]string, len(matches))
	for _, match := range matches {
		// Ideally should never happen, but there for sanity check.
		// Expected format: []string{"-c pg_proxy.<key>=<value>", "<key>", "<value>"}
		// Example: []string{"-c pg_proxy.dynamic_routing=true", "dynamic_routing", "true"}
		if len(match) != 3 {
			return nil, fmt.Errorf("invalid match format: expected 3 items, got %d", len(match))
		}
		result[match[1]] = match[2]
	}
	return result, nil
}

// parseBoolOrFalse tries to parse value (a string) as a boolean.
// Returns false and logs a warning if the value is invalid.
func parseBoolOrFalse(key, value string) bool {
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		pgproxy.Logger().Warn("invalid bool", "key", key, "reason", "invalid_bool")
		return false
	}
	return parsed
}

// priorityTokenRegex captures the raw token following `-c pg_proxy.priority=`
// up to the next whitespace delimiter. optionsRegex's value class ([\w$]+)
// stops at punctuation, so a malformed value like "1.5" or "1-5" would
// silently truncate to "1" and elevate the workload to P1 — the opposite of
// failing safe. parsePriorityOrDefault therefore re-extracts the untruncated
// token with this regex and rejects anything that is not a clean integer.
var priorityTokenRegex = regexp.MustCompile(`-c pg_proxy\.priority=([^\s]*)`)

// parsePriorityOrDefault re-extracts value from the raw options string with
// priorityTokenRegex (to defeat the truncation optionsRegex's value class
// applies to punctuation) and parses it as a pgsession.Priority level via
// pgsession.PriorityFromLevel. When the option is repeated, the LAST
// occurrence wins, matching the options map (later assignments overwrite
// earlier ones) and Postgres's own later-wins resolution of repeated `-c`
// GUCs — so both parse layers always agree on the same occurrence. Returns
// pgsession.DefaultPriority and logs a warning if the raw token is missing
// or is not a valid non-negative integer in pgsession.PriorityFromLevel's
// accepted range. Note that 0 (pgsession.P0, the highest priority) is a
// valid, distinct value from "unset" — only a missing key or an invalid
// value falls back to the default.
func parsePriorityOrDefault(rawOptions, key, value string) pgsession.Priority {
	priorityTokens := priorityTokenRegex.FindAllStringSubmatch(rawOptions, -1)
	if len(priorityTokens) == 0 {
		// The key was seen in the parsed map but the raw token is gone —
		// impossible in practice; fail safe.
		pgproxy.Logger().Warn("invalid priority", "key", key, "reason", "missing_priority")
		return pgsession.DefaultPriority
	}
	raw := priorityTokens[len(priorityTokens)-1][1]
	parsed, err := strconv.ParseUint(raw, 10, 32)
	if err == nil {
		var priority pgsession.Priority
		if priority, err = pgsession.PriorityFromLevel(uint32(parsed)); err == nil {
			return priority
		}
	}
	pgproxy.Logger().Warn("invalid priority", "key", key, "reason", "invalid_priority")
	return pgsession.DefaultPriority
}
