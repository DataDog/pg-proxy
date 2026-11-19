// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package link

import (
	"fmt"
	"slices"
	"strings"

	"github.com/DataDog/pg-proxy/pkg/config"
)

const (
	metricReset             = "pg_proxy.sessions.connections.reset"
	metricResetWouldLeak    = "pg_proxy.sessions.connections.reset.would_leak"
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
	// auditSQL reads the settings in auditColumns, in order. It runs before and
	// after the reset on releases that ResetDynamic would skip.
	auditSQL     string
	auditColumns []string
}

// auditedSettings are settings that Postgres does not report with
// ParameterStatus and whose leak changes another client's results, access or
// errors. A reported setting cannot change without marking the checkout. The
// version-specific ones use missing_ok, which reads NULL where they don't
// exist.
var auditedSettings = []string{
	"search_path", // reported only from PostgreSQL 18
	"statement_timeout",
	"lock_timeout",
	"idle_in_transaction_session_timeout",
	"default_transaction_isolation",
	"row_security",
	"session_replication_role",
	"bytea_output",
	"extra_float_digits",
	"transaction_timeout", // PostgreSQL 17 and later
}

const (
	auditColumnRole       = "current_user"
	auditColumnHeldCursor = "held_cursor"
)

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
	add(auditColumnHeldCursor, "(SELECT pg_catalog.count(*) FROM pg_catalog.pg_cursors WHERE is_holdable)")
	for _, name := range auditSettings {
		if slices.Contains(columns, name) {
			continue
		}
		// ValidateReset accepted only identifier characters and dots.
		add(name, fmt.Sprintf("pg_catalog.current_setting('%s', true)", name))
	}
	policy.auditSQL = "SELECT " + strings.Join(exprs, ", ")
	policy.auditColumns = columns
	return policy, nil
}

// Mode returns the resolved mode: never config.ResetDefault.
func (p ResetPolicy) Mode() config.ResetMode { return p.mode.Resolve() }

// resetSQL returns the reset statement for this policy. roleManaged says an
// embedder RoleSession has already reset the role.
//
// RESET ALL skips role and session_authorization (GUC_NO_RESET_ALL). Without a
// RoleSession, the same simple query resets identity first: any user may run
// RESET SESSION AUTHORIZATION, and RESET ROLE restores a connection-time role
// (ALTER ROLE ... SET role, or a startup option), which Postgres releases before
// the CVE-2024-10978 fix drop.
//
// ResetAudit and ResetDynamic also close held cursors and discard sequence
// state, which ResetAlways does not do yet. CLOSE ALL and DISCARD SEQUENCES,
// unlike DISCARD ALL, can run in the implicit transaction of a multi-statement
// query.
func (p ResetPolicy) resetSQL(roleManaged bool) string {
	sql := "RESET ALL"
	if !roleManaged {
		sql = "RESET SESSION AUTHORIZATION; RESET ROLE; RESET ALL"
	}
	switch p.Mode() {
	case config.ResetAudit, config.ResetDynamic:
		sql += "; CLOSE ALL; DISCARD SEQUENCES"
	}
	return sql
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

// changedColumns returns the audited columns whose values differ between the
// rows read before and after the reset.
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
