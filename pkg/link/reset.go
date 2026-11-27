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
	metricReset          = "pg_proxy.sessions.connections.reset"
	metricResetWouldLeak = "pg_proxy.sessions.connections.reset.would_leak"
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
)

// ResetPolicy is how a SessionLink resets its connection when it returns to the
// pool. Build one per pool with NewResetPolicy and share it across that pool's
// links. The zero value resets on every release. It is one pointer, nil for
// ResetAlways, so a link that resets every time is no larger than before
// reset_mode existed.
type ResetPolicy struct {
	*resetPolicy
}

type resetPolicy struct {
	mode config.ResetMode // resolved, and never ResetAlways
	// auditSQL reads the settings in auditColumns, in order. ResetAudit runs it
	// before the reset on a sample of the releases that ResetDynamic would skip.
	auditSQL     string
	auditColumns []string
	// auditRate and fullCheckRate are the shares of the releases ResetDynamic
	// would skip that run auditSQL and, also, fullCheckSQL. Only an audited
	// release runs the full check, so a full-check rate above the audit rate
	// gives every audited release the full check.
	auditRate     float64
	fullCheckRate float64
}

// auditedSettings are the settings ResetAudit compares on every audited release:
// those that Postgres does not report with ParameterStatus and whose leak changes
// another client's results, errors, messages, timeouts or durability. A reported
// setting cannot change without marking the checkout. A setting that a version
// lacks reads as NULL (missing_ok). The sampled full check finds the settings
// this list misses, including default_transaction_read_only, which Postgres
// reports only from PostgreSQL 14.
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
// 0.1 ms for auditSQL, so it runs on fewer releases. It does not see custom
// settings, which pg_settings does not list, or the role; the audit columns
// cover both.
const fullCheckSQL = "SELECT pg_catalog.string_agg(name, ',' ORDER BY name) FROM pg_catalog.pg_settings " +
	"WHERE source = 'session' AND setting IS DISTINCT FROM reset_val"

// defaultAuditRate and defaultFullCheckRate are the shares of the releases
// ResetDynamic would skip that ResetAudit checks and, also, full-checks, when
// the configuration does not set reset_audit_rate.
const (
	defaultAuditRate     = 0.1
	defaultFullCheckRate = 0.01
)

// NewResetPolicy validates the reset fields of settings, and for ResetAudit
// builds the audit query once.
func NewResetPolicy(settings config.DatabaseSettings) (ResetPolicy, error) {
	if err := settings.ValidateReset(); err != nil {
		return ResetPolicy{}, err
	}
	mode := settings.ResetMode.Resolve()
	if mode == config.ResetAlways {
		return ResetPolicy{}, nil
	}
	policy := ResetPolicy{&resetPolicy{mode: mode}}
	if mode != config.ResetAudit {
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
	for _, name := range settings.ResetAuditSettings {
		if slices.Contains(columns, name) {
			continue
		}
		// ValidateReset accepted only identifier characters and dots.
		add(name, fmt.Sprintf("pg_catalog.current_setting('%s', true)", name))
	}
	policy.auditSQL = "SELECT " + strings.Join(exprs, ", ")
	policy.auditColumns = columns
	policy.auditRate = settings.ResetAuditRate
	if policy.auditRate == 0 {
		policy.auditRate = defaultAuditRate
	}
	policy.fullCheckRate = defaultFullCheckRate
	return policy, nil
}

// Mode returns the resolved mode: never config.ResetDefault.
func (p ResetPolicy) Mode() config.ResetMode {
	if p.resetPolicy == nil {
		return config.ResetAlways
	}
	return p.mode
}

// usesMarks reports whether releases read the checkout's session marks. Only
// these modes need the session to scan SQL text.
func (p ResetPolicy) usesMarks() bool {
	mode := p.Mode()
	return mode == config.ResetAudit || mode == config.ResetDynamic
}

// reports reports whether releases emit the reset metrics. ResetAlways emits
// none, as before reset_mode existed.
func (p ResetPolicy) reports() bool { return p.Mode() != config.ResetAlways }

// sampleAudit draws what ResetAudit does on a release that ResetDynamic would
// skip: one draw picks the full check, the audit check alone, or the plain
// reset. The draw is random, so a periodic workload cannot fall between samples.
func (p ResetPolicy) sampleAudit() resetAction {
	switch draw := rand.Float64(); {
	case draw < min(p.fullCheckRate, p.auditRate):
		return resetFullAudited
	case draw < p.auditRate:
		return resetAudited
	default:
		return resetSend
	}
}

// resetStatements returns the reset statements and the one simple query that
// runs them, the same in every mode. roleManaged says an embedder RoleSession
// has already reset the role. RESET ALL skips role and session_authorization
// (GUC_NO_RESET_ALL), so without one the query resets identity first: any user
// may run RESET SESSION AUTHORIZATION, and RESET ROLE restores a
// connection-time role (ALTER ROLE ... SET role, or a startup option), which
// releases before the CVE-2024-10978 fix drop. Closing held cursors and
// discarding sequence state (OSQQUERY-823, -826) would change every mode at
// once. The slices are shared: callers must not modify them.
func resetStatements(roleManaged bool) (statements []string, sql string) {
	if roleManaged {
		return roleResetSQL, roleResetQuery
	}
	return identityResetSQL, identityResetQuery
}

var (
	roleResetSQL       = []string{"RESET ALL"}
	identityResetSQL   = []string{"RESET SESSION AUTHORIZATION", "RESET ROLE", "RESET ALL"}
	roleResetQuery     = strings.Join(roleResetSQL, "; ")
	identityResetQuery = strings.Join(identityResetSQL, "; ")
)

// Reset reasons, the reason tag of the reset metric.
const (
	resetReasonAlways          = "always"
	resetReasonOff             = "off"
	resetReasonClientSQL       = "client_sql"
	resetReasonPreparedStmt    = "prepared_stmt"
	resetReasonParameterStatus = "parameter_status"
	resetReasonServerVersion   = "server_version" // the server may drop the role on a clean checkout
	resetReasonAudit           = "audit"          // dynamic would skip this release
	resetReasonClean           = "clean"
)

// resetAction is what a release does about the reset.
type resetAction uint8

const (
	resetSkip        resetAction = iota
	resetSend                    // send the reset
	resetAudited                 // send the reset inside the audit check
	resetFullAudited             // send the reset inside the audit check and the full check
)

// resetDecision reports what a release with marks does about the reset, and why.
// rolePersists says the role set at connection time survives the client's own
// RESET SESSION AUTHORIZATION or DISCARD ALL, which the scanner does not count;
// see resetDropsRole.
func (p ResetPolicy) resetDecision(marks SessionMarks, rolePersists bool) (action resetAction, reason string) {
	mode := p.Mode()
	switch {
	case mode == config.ResetAlways:
		return resetSend, resetReasonAlways
	case mode == config.ResetOff:
		return resetSkip, resetReasonOff
	case marks&MarkClientSQL != 0:
		return resetSend, resetReasonClientSQL
	case marks&MarkPreparedStmt != 0:
		return resetSend, resetReasonPreparedStmt
	case marks&MarkParameterStatus != 0:
		return resetSend, resetReasonParameterStatus
	case !rolePersists:
		return resetSend, resetReasonServerVersion
	case mode == config.ResetAudit:
		return p.sampleAudit(), resetReasonAudit
	default:
		return resetSkip, resetReasonClean
	}
}

// resetDropsRole reports whether a server whose server_version is version drops
// a role set at connection time (ALTER ROLE ... SET role, or a startup option)
// when a client runs RESET SESSION AUTHORIZATION, which DISCARD ALL also runs.
// The releases before the CVE-2024-10978 fix do, and from PostgreSQL 14 they
// send no ParameterStatus for it when the session user does not change: 14.0 to
// 14.13, 15.0 to 15.8, 16.0 to 16.4 and 17.0. Earlier releases report
// session_authorization on every RESET, which marks the checkout. A version that
// does not start with a number counts as one that drops the role.
func resetDropsRole(version string) bool {
	major, n := leadingInt(version)
	if n == 0 {
		return true
	}
	var fixedMinor int
	switch major {
	case 14:
		fixedMinor = 14
	case 15:
		fixedMinor = 9
	case 16:
		fixedMinor = 5
	case 17:
		fixedMinor = 1
	default:
		return false
	}
	minor := 0 // a beta or release candidate precedes the .0 release
	if n < len(version) && version[n] == '.' {
		minor, _ = leadingInt(version[n+1:])
	}
	return minor < fixedMinor
}

// leadingInt returns the decimal number s starts with and its length in bytes.
func leadingInt(s string) (v, n int) {
	for n < len(s) && s[n] >= '0' && s[n] <= '9' {
		v = v*10 + int(s[n]-'0')
		n++
	}
	return v, n
}

// reasonTags returns the reset metrics' tags for a release's decision: its
// reason and, on a release ResetDynamic would skip in ResetAudit, the check it
// ran: none, list or full.
func reasonTags(action resetAction, reason string) []string {
	if reason != resetReasonAudit {
		return []string{"reason:" + reason}
	}
	check := "none"
	switch action {
	case resetAudited:
		check = "list"
	case resetFullAudited:
		check = "full"
	}
	return []string{"reason:" + reason, "check:" + check}
}

// auditBaseline, kept in pgconn's CustomData for the life of the backend
// connection, is the audit row the connection reads right after a reset: what
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
		if !slices.Equal(before[i], after[i]) || (before[i] == nil) != (after[i] == nil) {
			changed = append(changed, name)
		}
	}
	return changed
}
