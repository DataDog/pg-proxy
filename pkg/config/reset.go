// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package config

import (
	"errors"
	"fmt"
	"strings"
)

// ResetMode selects how pg-proxy restores session state when a backend
// connection returns to the pool at the end of an autocommit statement or a
// transaction.
type ResetMode string

const (
	// ResetDefault leaves the choice to the library. It is distinct from
	// ResetAlways so the default can change without rewriting configurations
	// that chose ResetAlways explicitly. Today it means ResetAlways.
	ResetDefault ResetMode = ""
	// ResetAlways runs the reset on every release.
	ResetAlways ResetMode = "always"
	// ResetAudit runs the reset on every release, and on each release that
	// ResetDynamic would skip it also reports the settings the reset changed:
	// the settings ResetDynamic would have leaked.
	ResetAudit ResetMode = "audit"
	// ResetDynamic runs the reset only when something in the checkout may have
	// changed session state. It does not see changes made inside functions or
	// triggers that the client's SQL does not name, or sequence state.
	ResetDynamic ResetMode = "dynamic"
	// ResetOff never sends a reset. A client's SET carries over to the next
	// client of the connection.
	ResetOff ResetMode = "off"
)

// ResetModes lists the accepted non-empty values, in documentation order.
var ResetModes = []ResetMode{ResetAlways, ResetAudit, ResetDynamic, ResetOff}

// Resolve returns the mode that m selects, replacing ResetDefault with the
// library default.
func (m ResetMode) Resolve() ResetMode {
	if m == ResetDefault {
		return ResetAlways
	}
	return m
}

// ValidateReset checks the reset_mode and reset_audit_settings fields. Readers
// do not call it; applications and the pg-proxy binary call it with their other
// semantic checks.
func (s DatabaseSettings) ValidateReset() error {
	var issues []error
	if s.ResetMode != ResetDefault && !isResetMode(s.ResetMode) {
		allowed := make([]string, len(ResetModes))
		for i, m := range ResetModes {
			allowed[i] = string(m)
		}
		issues = append(issues, fmt.Errorf("reset_mode: unknown mode %q; use one of %s", s.ResetMode, strings.Join(allowed, ", ")))
	}
	for i, name := range s.ResetAuditSettings {
		if !isCustomSettingName(name) {
			issues = append(issues, fmt.Errorf("reset_audit_settings[%d]: %q is not a custom setting name of the form prefix.name", i, name))
		}
	}
	return errors.Join(issues...)
}

func isResetMode(m ResetMode) bool {
	for _, known := range ResetModes {
		if m == known {
			return true
		}
	}
	return false
}

// isCustomSettingName reports whether name is two or more simple identifiers
// joined by dots, the form PostgreSQL requires of custom settings. Accepting
// nothing else keeps the name safe to embed in the audit query.
func isCustomSettingName(name string) bool {
	parts := strings.Split(name, ".")
	if len(parts) < 2 {
		return false
	}
	for _, part := range parts {
		if part == "" || !isIdentStart(part[0]) {
			return false
		}
		for i := 1; i < len(part); i++ {
			if c := part[i]; !isIdentStart(c) && (c < '0' || c > '9') && c != '$' {
				return false
			}
		}
	}
	return true
}

func isIdentStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}
