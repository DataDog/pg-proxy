// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

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

// Decode the command schema with the same strictness and safe field diagnostics
// as the library, then resolve local credential references before validation.
func readConfigJSON(r io.Reader) (pgconfig.Config, error) {
	cfg, err := pgconfig.ReadJSONAs[fileConfig](r)
	if err != nil {
		return pgconfig.Config{}, err
	}
	return cfg.resolve()
}

func readConfigYAML(r io.Reader) (pgconfig.Config, error) {
	cfg, err := pgconfig.ReadYAMLAs[fileConfig](r)
	if err != nil {
		return pgconfig.Config{}, err
	}
	return cfg.resolve()
}

// credentialResolutionError marks a decoded document whose local credentials
// could not all be resolved. Other independent settings can still be checked.
type credentialResolutionError struct{ error }

// resolve reads every credential reference once, at startup. Errors name the
// setting but never the referenced path, variable, or value.
func (c fileConfig) resolve() (pgconfig.Config, error) {
	var issues []error
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
			issues = append(issues, fmt.Errorf("database %d: %w", i, err))
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
			issues = append(issues, fmt.Errorf("user %d: %w", i, err))
		}
		verifier, err := resolveSecret("scram_sha_256", credentials.SCRAMSHA256, credentials.SCRAMSHA256File, credentials.SCRAMSHA256Env)
		if err != nil {
			issues = append(issues, fmt.Errorf("user %d: %w", i, err))
		}
		result.Users[i] = pgconfig.User{UserSettings: user.UserSettings, StaticCredentials: credentials.StaticCredentials}
		result.Users[i].StaticCredentials.Password = password
		result.Users[i].StaticCredentials.SCRAMSHA256 = verifier
	}
	issues = append(issues, result.ValidateCredentials())
	if err := errors.Join(issues...); err != nil {
		return result, credentialResolutionError{err}
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
