// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"

	"gopkg.in/yaml.v3"
)

// ReadJSON reads one configuration document. Unknown fields are rejected, so
// misspelled routing settings cannot silently select defaults. Inline passwords
// and user verifiers are retained as Secret values, which redact log output.
func ReadJSON(r io.Reader) (Config, error) {
	return withValidCredentials(ReadJSONAs[Config](r))
}

// ReadJSONAs reads one strict JSON document with diagnostics for the supplied
// schema. Applications using ConfigSections can decode their own section types
// before resolving credentials and applying application-specific validation.
func ReadJSONAs[T any](r io.Reader) (T, error) {
	return readStrict[T](r, "json", func(r io.Reader) decoder {
		d := json.NewDecoder(r)
		d.DisallowUnknownFields()
		return d
	})
}

// ReadYAML uses the existing snake_case YAML field names. Unknown settings and
// extra documents fail instead of silently ignoring misspelled options.
func ReadYAML(r io.Reader) (Config, error) {
	return withValidCredentials(ReadYAMLAs[Config](r))
}

// ReadYAMLAs reads one strict YAML document with diagnostics for the supplied
// schema. Like ReadJSONAs, it leaves credential and semantic checks to the caller.
func ReadYAMLAs[T any](r io.Reader) (T, error) {
	return readStrict[T](r, "yaml", func(r io.Reader) decoder {
		d := yaml.NewDecoder(r)
		d.KnownFields(true)
		return d
	})
}

type decoder interface{ Decode(any) error }

// readStrict decodes exactly one document. The input is buffered so a decode
// failure can be diagnosed without forwarding decoder text, which may quote secrets.
func readStrict[T any](r io.Reader, format string, newDecoder func(io.Reader) decoder) (T, error) {
	var cfg, zero T
	data, err := io.ReadAll(r)
	if err != nil {
		return zero, fmt.Errorf("could not read configuration %s", strings.ToUpper(format))
	}
	d := newDecoder(bytes.NewReader(data))
	if err := d.Decode(&cfg); err != nil {
		return zero, configurationError(data, format, err, reflect.TypeFor[T]())
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return zero, errors.New("expected one configuration document")
	}
	return cfg, nil
}

func withValidCredentials(cfg Config, err error) (Config, error) {
	if err == nil {
		err = cfg.ValidateCredentials()
	}
	if err != nil {
		return Config{}, err
	}
	return cfg, nil
}
