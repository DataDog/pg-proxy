// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package config

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestResetFieldsReadFromYAMLAndJSON(t *testing.T) {
	fromYAML, err := ReadYAML(strings.NewReader(`databases:
- name: write
  reset_mode: audit
  reset_audit_settings: [app.tenant_id]
`))
	if err != nil {
		t.Fatal(err)
	}
	fromJSON, err := ReadJSON(strings.NewReader(`{"databases":[{"name":"write","resetMode":"audit","resetAuditSettings":["app.tenant_id"]}]}`))
	if err != nil {
		t.Fatal(err)
	}
	for format, cfg := range map[string]Config{"yaml": fromYAML, "json": fromJSON} {
		db := cfg.Databases[0]
		if db.ResetMode != ResetAudit || !reflect.DeepEqual(db.ResetAuditSettings, []string{"app.tenant_id"}) {
			t.Fatalf("%s: lost reset settings: %+v", format, db.DatabaseSettings)
		}
	}
}

// Unset fields stay out of serialized configuration, so a document written by
// this version still reads in a strict reader that predates them.
func TestUnsetResetFieldsAreOmitted(t *testing.T) {
	data, err := json.Marshal(Config{Databases: []Database{{DatabaseSettings: DatabaseSettings{Name: "write"}}}})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "reset") {
		t.Fatalf("unset reset fields were serialized: %s", data)
	}
}

func TestResetModeResolve(t *testing.T) {
	if got := ResetDefault.Resolve(); got != ResetAlways {
		t.Fatalf("default resolves to %q", got)
	}
	for _, m := range ResetModes {
		if got := m.Resolve(); got != m {
			t.Fatalf("%q resolves to %q", m, got)
		}
	}
}

func TestValidateReset(t *testing.T) {
	valid := []DatabaseSettings{
		{},
		{ResetMode: ResetAlways},
		{ResetMode: ResetAudit, ResetAuditSettings: []string{"app.tenant_id", "pg_trgm.similarity_threshold", "a.b.c", "x_1.y$2"}},
		{ResetMode: ResetDynamic},
		{ResetMode: ResetOff},
	}
	for _, s := range valid {
		if err := s.ValidateReset(); err != nil {
			t.Errorf("%+v: %v", s, err)
		}
	}
	invalid := map[string]DatabaseSettings{
		`reset_mode: unknown mode "Always"; use one of always, audit, dynamic, off`: {ResetMode: "Always"},
		`reset_audit_settings[0]: "work_mem"`:                                       {ResetAuditSettings: []string{"work_mem"}},
		`reset_audit_settings[1]: "app.x'); DROP TABLE t; --"`:                      {ResetAuditSettings: []string{"app.ok", "app.x'); DROP TABLE t; --"}},
		`reset_audit_settings[0]: "app."`:                                           {ResetAuditSettings: []string{"app."}},
		`reset_audit_settings[0]: "1app.x"`:                                         {ResetAuditSettings: []string{"1app.x"}},
	}
	for want, s := range invalid {
		err := s.ValidateReset()
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%+v: got %v, want an error containing %q", s, err, want)
		}
	}
}
