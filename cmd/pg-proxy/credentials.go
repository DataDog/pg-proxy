// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"gopkg.in/yaml.v3"

	pgconfig "github.com/DataDog/pg-proxy/pkg/config"
)

// fileConfig is the OSS binary's file format: the library schema plus file and
// environment references for credentials. The library types stay free of
// credential sources; resolve converts this format into a pgconfig.Config.
type fileConfig pgconfig.ConfigSections[
	fileUser, fileDatabase, pgconfig.Cluster,
	pgconfig.LogicalDatabase, pgconfig.Proxy, pgconfig.Mirroring,
]

type fileDatabase struct {
	pgconfig.Database `yaml:",inline"`
	PasswordFile      string `json:"passwordFile,omitempty" yaml:"password_file,omitempty"`
	PasswordEnv       string `json:"passwordEnv,omitempty" yaml:"password_env,omitempty"`
}

type fileUser struct {
	pgconfig.UserSettings `yaml:",inline"`
	StaticCredentials     fileStaticCredentials `json:"staticCredentials,omitempty" yaml:"static_credentials,omitempty"`
}

type fileStaticCredentials struct {
	pgconfig.StaticCredentials `yaml:",inline"`
	PasswordFile               string `json:"passwordFile,omitempty" yaml:"password_file,omitempty"`
	PasswordEnv                string `json:"passwordEnv,omitempty" yaml:"password_env,omitempty"`
	SCRAMSHA256File            string `json:"scramSHA256File,omitempty" yaml:"scram_sha_256_file,omitempty"`
	SCRAMSHA256Env             string `json:"scramSHA256Env,omitempty" yaml:"scram_sha_256_env,omitempty"`
}

// readConfigJSON and readConfigYAML keep the library readers' strictness:
// unknown fields and extra documents fail, and decode errors are fixed strings
// because parser errors can contain supplied values, including misplaced secrets.
func readConfigJSON(r io.Reader) (pgconfig.Config, error) {
	var cfg fileConfig
	decoder := json.NewDecoder(r)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cfg); err != nil {
		return pgconfig.Config{}, errors.New("invalid configuration JSON or unknown field")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return pgconfig.Config{}, errors.New("expected one configuration document")
	}
	return cfg.resolve()
}

func readConfigYAML(r io.Reader) (pgconfig.Config, error) {
	var cfg fileConfig
	decoder := yaml.NewDecoder(r)
	decoder.KnownFields(true)
	if err := decoder.Decode(&cfg); err != nil {
		return pgconfig.Config{}, errors.New("invalid configuration YAML or unknown field")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return pgconfig.Config{}, errors.New("expected one configuration document")
	}
	return cfg.resolve()
}

// resolve reads every credential reference once, at startup. Errors name the
// setting but never the referenced path, variable, or value.
func (c fileConfig) resolve() (pgconfig.Config, error) {
	result := pgconfig.Config{
		Clusters:         c.Clusters,
		LogicalDatabases: c.LogicalDatabases,
		Proxy:            c.Proxy,
		Mirroring:        c.Mirroring,
	}
	if c.Databases != nil {
		result.Databases = make([]pgconfig.Database, len(c.Databases))
	}
	for i, database := range c.Databases {
		password, err := resolveSecret("password", database.Password, database.PasswordFile, database.PasswordEnv)
		if err != nil {
			return pgconfig.Config{}, fmt.Errorf("database %d: %w", i, err)
		}
		result.Databases[i] = database.Database
		result.Databases[i].Password = password
	}
	if c.Users != nil {
		result.Users = make([]pgconfig.User, len(c.Users))
	}
	for i, user := range c.Users {
		credentials := user.StaticCredentials
		password, err := resolveSecret("password", credentials.Password, credentials.PasswordFile, credentials.PasswordEnv)
		if err != nil {
			return pgconfig.Config{}, fmt.Errorf("user %d: %w", i, err)
		}
		verifier, err := resolveSecret("scram_sha_256", credentials.SCRAMSHA256, credentials.SCRAMSHA256File, credentials.SCRAMSHA256Env)
		if err != nil {
			return pgconfig.Config{}, fmt.Errorf("user %d: %w", i, err)
		}
		result.Users[i] = pgconfig.User{UserSettings: user.UserSettings, StaticCredentials: credentials.StaticCredentials}
		result.Users[i].StaticCredentials.Password = password
		result.Users[i].StaticCredentials.SCRAMSHA256 = verifier
	}
	if err := result.ValidateCredentials(); err != nil {
		return pgconfig.Config{}, err
	}
	return result, nil
}

// resolveSecret accepts at most one of an inline value, a file path, or an
// environment variable name. A referenced file or variable must exist and be
// nonempty. File contents lose one trailing newline (\n or \r\n), as mounted
// secrets often end with one; environment values are used as-is.
func resolveSecret(setting string, inline pgconfig.Secret, file, env string) (pgconfig.Secret, error) {
	sources := 0
	for _, source := range []string{string(inline), file, env} {
		if source != "" {
			sources++
		}
	}
	if sources > 1 {
		return "", fmt.Errorf("%[1]s, %[1]s_file, and %[1]s_env are mutually exclusive", setting)
	}
	switch {
	case file != "":
		data, err := os.ReadFile(file)
		if err != nil {
			return "", fmt.Errorf("%s_file: %s", setting, fileErrorReason(err))
		}
		value := string(data)
		if trimmed, ok := strings.CutSuffix(value, "\r\n"); ok {
			value = trimmed
		} else {
			value = strings.TrimSuffix(value, "\n")
		}
		if value == "" {
			return "", fmt.Errorf("%s_file: file is empty", setting)
		}
		return pgconfig.Secret(value), nil
	case env != "":
		value := os.Getenv(env)
		if value == "" {
			return "", fmt.Errorf("%s_env: environment variable is not set or empty", setting)
		}
		return pgconfig.Secret(value), nil
	}
	return inline, nil
}

func fileErrorReason(err error) string {
	switch {
	case errors.Is(err, os.ErrNotExist):
		return "file not found"
	case errors.Is(err, os.ErrPermission):
		return "permission denied"
	}
	return "could not read file"
}
