// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package config

import (
	"errors"
	"fmt"

	"github.com/DataDog/pg-proxy/pkg/scram"
)

// Secret holds an inline password or SCRAM verifier in the portable config, so
// standalone deployments can supply credentials without a secret-store adapter.
// It reads a normal JSON/YAML string but redacts JSON, YAML and fmt output to keep
// credentials out of config dumps and logs. Authentication and connection builders
// explicitly convert it with string(s) when they need the credential value.
// Marshaling produces a diagnostic representation that cannot restore credentials.
type Secret string

func (Secret) String() string               { return "[REDACTED]" }
func (Secret) GoString() string             { return "[REDACTED]" }
func (Secret) MarshalJSON() ([]byte, error) { return []byte(`"[REDACTED]"`), nil }
func (Secret) MarshalYAML() (any, error)    { return "[REDACTED]", nil }

func (c StaticCredentials) validate() error {
	if c.Password != "" && c.SCRAMSHA256 != "" {
		return errors.New("password and scram_sha_256 (scramSHA256) are mutually exclusive")
	}
	if (c.Password != "" || c.SCRAMSHA256 != "") && c.Username == "" {
		return errors.New("static credentials require username")
	}
	if c.SCRAMSHA256 != "" && !validSCRAMSHA256(string(c.SCRAMSHA256)) {
		return errors.New("invalid SCRAM-SHA-256 verifier")
	}
	return nil
}

// Reuse the exchange's parser so configuration and authentication accept the
// same stored-verifier format and bounds.
func validSCRAMSHA256(value string) bool {
	_, err := scram.ParseVerifier(value)
	return err == nil
}

// ValidateCredentials checks credential sources only. Runtime defaults, topology
// validation and backend construction remain the caller's responsibility.
func (c Config) ValidateCredentials() error {
	for i, db := range c.Databases {
		if db.Password != "" && db.Username == "" {
			return fmt.Errorf("database %d: password requires username", i)
		}
	}
	for i, user := range c.Users {
		if err := user.StaticCredentials.validate(); err != nil {
			return fmt.Errorf("user %d: %w", i, err)
		}
	}
	return nil
}
