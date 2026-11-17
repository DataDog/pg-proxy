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
	"reflect"

	"gopkg.in/yaml.v3"
)

// ReadJSON reads one configuration document. Unknown fields are rejected, so
// misspelled routing settings cannot silently select defaults. Inline passwords
// and user verifiers are retained as Secret values, which redact log output.
func ReadJSON(r io.Reader) (Config, error) {
	cfg, err := ReadJSONAs[Config](r)
	if err != nil {
		return Config{}, err
	}
	if err := cfg.ValidateCredentials(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// ReadJSONAs reads one strict JSON document with diagnostics for the supplied
// schema. Applications using ConfigSections can decode their own section types
// before resolving credentials and applying application-specific validation.
func ReadJSONAs[T any](r io.Reader) (T, error) {
	var cfg, zero T
	data, err := io.ReadAll(r)
	if err != nil {
		return zero, errors.New("could not read configuration JSON")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cfg); err != nil {
		return zero, configurationError(data, "json", err, reflect.TypeFor[T]())
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return zero, errors.New("expected one configuration document")
	}
	return cfg, nil
}

// ReadYAML uses the existing snake_case YAML field names. Unknown settings and
// extra documents fail instead of silently ignoring misspelled options.
func ReadYAML(r io.Reader) (Config, error) {
	cfg, err := ReadYAMLAs[Config](r)
	if err != nil {
		return Config{}, err
	}
	if err := cfg.ValidateCredentials(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// ReadYAMLAs reads one strict YAML document with diagnostics for the supplied
// schema. Like ReadJSONAs, it leaves credential and semantic checks to the caller.
func ReadYAMLAs[T any](r io.Reader) (T, error) {
	var cfg, zero T
	data, err := io.ReadAll(r)
	if err != nil {
		return zero, errors.New("could not read configuration YAML")
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&cfg); err != nil {
		return zero, configurationError(data, "yaml", err, reflect.TypeFor[T]())
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return zero, errors.New("expected one configuration document")
	}
	return cfg, nil
}
