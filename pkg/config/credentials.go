package config

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
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
		return errors.New("password and scram_sha_256 are mutually exclusive")
	}
	if (c.Password != "" || c.SCRAMSHA256 != "") && c.Username == "" {
		return errors.New("static credentials require username")
	}
	if c.SCRAMSHA256 != "" && !validSCRAMSHA256(string(c.SCRAMSHA256)) {
		return errors.New("invalid SCRAM-SHA-256 verifier")
	}
	return nil
}

// Validate the PostgreSQL stored-verifier form without deriving keys or exposing
// the supplied verifier in errors. Authentication with it remains separate work.
func validSCRAMSHA256(value string) bool {
	parts := strings.Split(value, "$")
	if len(parts) != 3 || parts[0] != "SCRAM-SHA-256" {
		return false
	}
	iterationText, saltText, ok := strings.Cut(parts[1], ":")
	if !ok {
		return false
	}
	iterations, err := strconv.ParseInt(iterationText, 10, 32)
	if err != nil || iterations <= 0 {
		return false
	}
	salt, err := base64.StdEncoding.DecodeString(saltText)
	if err != nil || len(salt) == 0 {
		return false
	}
	keys := strings.Split(parts[2], ":")
	if len(keys) != 2 {
		return false
	}
	for _, key := range keys {
		decoded, err := base64.StdEncoding.DecodeString(key)
		if err != nil || len(decoded) != 32 {
			return false
		}
	}
	return true
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
