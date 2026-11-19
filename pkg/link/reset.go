// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package link

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"

	"github.com/DataDog/pg-proxy/pkg/config"
)

const (
	metricReset             = "pg_proxy.sessions.connections.reset"
	metricResetWouldLeak    = "pg_proxy.sessions.connections.reset.would_leak"
	metricResetFullCheck    = "pg_proxy.sessions.connections.reset.full_check"
	metricCrossCheckoutRead = "pg_proxy.sessions.sequence_state.cross_checkout_read"
	// sqlstateNotInPrerequisiteState is object_not_in_prerequisite_state, which
	// currval and lastval raise when the session has no value yet.
	sqlstateNotInPrerequisiteState = "55000"
)

// SessionMarks record what a checkout may have done to session state. Each
// checkout starts with none, because the previous release left the connection
// at its baseline.
type SessionMarks uint8

const (
	// MarkClientSQL: client SQL that the session scanner flagged ran in this
	// checkout.
	MarkClientSQL SessionMarks = 1 << iota
	// MarkPreparedStmt: a Bind used a statement whose text the scanner
	// flagged, or that the proxy could not match to a Parse.
	MarkPreparedStmt
	// MarkParameterStatus: the backend reported a changed setting.
	MarkParameterStatus
	// MarkReadsSequenceState: client SQL may have called currval or lastval.
	// It does not make a checkout dirty; it scopes the cross-checkout read
	// metric.
	MarkReadsSequenceState
)

// ResetPolicy is how a SessionLink resets its connection when it returns to the
// pool. Build one per pool with NewResetPolicy and share it across that pool's
// links. The zero value resets on every release.
type ResetPolicy struct {
	mode config.ResetMode
	// auditSQL reads the settings in auditColumns, in order. ResetAudit runs it
	// before the reset on releases that ResetDynamic would skip.
	auditSQL     string
	auditColumns []string
	// fullCheckRate is the share of audited releases that also run fullCheckSQL.
	fullCheckRate float64
}

// auditedSettings are the settings ResetAudit compares on every audited release:
// those that Postgres does not report with ParameterStatus and whose leak changes
// another client's results, errors, messages, timeouts or durability. A reported
// setting cannot change without marking the checkout. A setting that a version
// lacks reads as NULL (missing_ok). The sampled full check finds the settings
// this list misses.
var auditedSettings = []string{
	// Query results and meaning.
	"search_path", // reported only from PostgreSQL 18
	"row_security",
	"session_replication_role",
	"transform_null_equals",
	"default_text_search_config",
	"lc_monetary",
	"lc_numeric",
	"lc_time",
	"bytea_output",
	"extra_float_digits",
	"xmlbinary",
	"xmloption",
	"array_nulls",
	"backslash_quote",
	"quote_all_identifiers",
	"gin_fuzzy_search_limit",
	// Transactions and DDL.
	"default_transaction_isolation",
	"default_transaction_deferrable",
	"check_function_bodies",
	"default_tablespace",
	"temp_tablespaces",
	// What the client sees.
	"client_min_messages",
	"lc_messages",
	// Timeouts.
	"statement_timeout",
	"lock_timeout",
	"idle_in_transaction_session_timeout",
	"idle_session_timeout",
	"transaction_timeout", // PostgreSQL 17 and later
	// Durability.
	"synchronous_commit",
}

const auditColumnRole = "current_user"

// fullCheckSQL names every setting the checkout left different from its reset
// value. Reading pg_settings costs about 1 ms of backend time, against about
// 0.1 ms for auditSQL, so it runs only on a sample of audited releases. It does
// not see custom settings, which pg_settings does not list, or the role; the
// audit columns cover both.
const fullCheckSQL = "SELECT pg_catalog.string_agg(name, ',' ORDER BY name) FROM pg_catalog.pg_settings " +
	"WHERE source = 'session' AND setting IS DISTINCT FROM reset_val"

// defaultFullCheckRate is the share of audited releases that run fullCheckSQL.
const defaultFullCheckRate = 0.01

// NewResetPolicy validates mode and the custom auditSettings, and for
// ResetAudit builds the audit query once.
func NewResetPolicy(mode config.ResetMode, auditSettings []string) (ResetPolicy, error) {
	if err := (config.DatabaseSettings{ResetMode: mode, ResetAuditSettings: auditSettings}).ValidateReset(); err != nil {
		return ResetPolicy{}, err
	}
	policy := ResetPolicy{mode: mode.Resolve()}
	if policy.mode != config.ResetAudit {
		return policy, nil
	}
	var columns, exprs []string
	add := func(name, expr string) {
		columns = append(columns, name)
		exprs = append(exprs, expr)
	}
	for _, name := range auditedSettings {
		add(name, fmt.Sprintf("pg_catalog.current_setting('%s', true)", name))
	}
	// pg_catalog. keeps a leaked search_path from redirecting the check itself.
	// current_user is a keyword, not a function.
	add(auditColumnRole, "current_user")
	for _, name := range auditSettings {
		if slices.Contains(columns, name) {
			continue
		}
		// ValidateReset accepted only identifier characters and dots.
		add(name, fmt.Sprintf("pg_catalog.current_setting('%s', true)", name))
	}
	policy.auditSQL = "SELECT " + strings.Join(exprs, ", ")
	policy.auditColumns = columns
	policy.fullCheckRate = defaultFullCheckRate
	return policy, nil
}

// Mode returns the resolved mode: never config.ResetDefault.
func (p ResetPolicy) Mode() config.ResetMode { return p.mode.Resolve() }

// sampleFullCheck reports whether this audited release also runs fullCheckSQL.
// The draw is random, so a periodic workload cannot fall between samples.
func (p ResetPolicy) sampleFullCheck() bool {
	return p.fullCheckRate > 0 && rand.Float64() < p.fullCheckRate
}

// resetSQL returns the reset statement. roleManaged says an embedder RoleSession
// has already reset the role.
//
// RESET ALL skips role and session_authorization (GUC_NO_RESET_ALL). Without a
// RoleSession, the same simple query resets identity first: any user may run
// RESET SESSION AUTHORIZATION, and RESET ROLE restores a connection-time role
// (ALTER ROLE ... SET role, or a startup option), which Postgres releases before
// the CVE-2024-10978 fix drop.
//
// Every mode sends the same statement, so ResetAudit changes nothing a client
// sees and ResetDynamic differs from ResetAlways only in whether it is sent.
// Closing held cursors and discarding sequence state (OSQQUERY-823, -826) would
// change every mode at once.
func resetSQL(roleManaged bool) string {
	if roleManaged {
		return "RESET ALL"
	}
	return "RESET SESSION AUTHORIZATION; RESET ROLE; RESET ALL"
}

// Reset reasons, the reason tag of the reset metric.
const (
	resetReasonAlways          = "always"
	resetReasonOff             = "off"
	resetReasonClientSQL       = "client_sql"
	resetReasonPreparedStmt    = "prepared_stmt"
	resetReasonParameterStatus = "parameter_status"
	resetReasonAudit           = "audit" // dynamic would skip this release
	resetReasonClean           = "clean"
)

// resetDecision reports whether a release with marks sends the reset, and why.
func (p ResetPolicy) resetDecision(marks SessionMarks) (send bool, reason string) {
	mode := p.Mode()
	switch {
	case mode == config.ResetAlways:
		return true, resetReasonAlways
	case mode == config.ResetOff:
		return false, resetReasonOff
	case marks&MarkClientSQL != 0:
		return true, resetReasonClientSQL
	case marks&MarkPreparedStmt != 0:
		return true, resetReasonPreparedStmt
	case marks&MarkParameterStatus != 0:
		return true, resetReasonParameterStatus
	case mode == config.ResetAudit:
		return true, resetReasonAudit
	default:
		return false, resetReasonClean
	}
}

// auditBaselineKey keys a connection's auditBaseline in pgconn's CustomData,
// which lives as long as the backend connection.
const auditBaselineKey = "github.com/DataDog/pg-proxy/pkg/link.auditBaseline"

// auditBaseline is the audit row a connection reads right after a reset: what
// every audited setting reads at the connection's baseline.
type auditBaseline struct {
	sql    string // the audit query the values answer
	values [][]byte
}

// changedColumns returns the audited columns whose values differ between the
// row read before the reset and the baseline.
func (p ResetPolicy) changedColumns(before, after [][]byte) []string {
	var changed []string
	for i, name := range p.auditColumns {
		if i >= len(before) || i >= len(after) {
			break
		}
		if !slices.Equal(before[i], after[i]) || (before[i] == nil) != (after[i] == nil) {
			changed = append(changed, name)
		}
	}
	return changed
}
