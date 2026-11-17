// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"

	"gopkg.in/yaml.v3"
)

// ReadJSON reads one configuration document. Unknown fields are rejected, so
// misspelled routing settings cannot silently select defaults. Inline passwords
// and user verifiers are retained as Secret values, which redact log output.
func ReadJSON(r io.Reader) (Config, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return Config{}, errors.New("could not read configuration JSON")
	}
	var cfg Config
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cfg); err != nil {
		// Decoder errors can contain supplied values, including misplaced secrets.
		return Config{}, configurationError(data, "json", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return Config{}, errors.New("expected one configuration document")
	}
	if err := cfg.ValidateCredentials(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// ReadYAML uses the existing snake_case YAML field names. Unknown settings and
// extra documents fail instead of silently ignoring misspelled options.
func ReadYAML(r io.Reader) (Config, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return Config{}, errors.New("could not read configuration YAML")
	}
	var cfg Config
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&cfg); err != nil {
		return Config{}, configurationError(data, "yaml", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return Config{}, errors.New("expected one configuration document")
	}
	if err := cfg.ValidateCredentials(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}
