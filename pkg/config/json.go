package config

import (
	"encoding/json"
	"errors"
	"io"

	"gopkg.in/yaml.v3"
)

// ReadJSON reads one configuration document. Unknown fields are rejected, so
// misspelled routing settings cannot silently select defaults. Inline passwords
// and user verifiers are retained as Secret values, which redact log output.
func ReadJSON(r io.Reader) (Config, error) {
	var cfg Config
	decoder := json.NewDecoder(r)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cfg); err != nil {
		// Decoder errors can contain supplied values, including misplaced secrets.
		return Config{}, errors.New("invalid configuration JSON or unknown field")
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
	var cfg Config
	decoder := yaml.NewDecoder(r)
	decoder.KnownFields(true)
	if err := decoder.Decode(&cfg); err != nil {
		return Config{}, errors.New("invalid configuration YAML or unknown field")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return Config{}, errors.New("expected one configuration document")
	}
	if err := cfg.ValidateCredentials(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}
