// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package config

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfigurationDiagnostics(t *testing.T) {
	for _, tc := range []struct{ name, format, input, path, detail string }{
		{"yaml unknown", "yaml", "proxy:\n  tls_requred: private-value\n", "proxy.tls_requred", "tls_required"},
		{"json unknown", "json", `{"proxy":{"tlsRequred":"private-value"}}`, "proxy.tlsRequred", "tlsRequired"},
		{"yaml type", "yaml", "databases:\n- port: private-value\n", "databases[0].port", "int32"},
		{"json type", "json", `{"databases":[{"port":"private-value"}]}`, "databases[0].port", "int32"},
		{"yaml range", "yaml", "databases:\n- port: 99999999999999999999\n", "databases[0].port", "int32"},
		{"yaml duration", "yaml", "databases:\n- max_conn_lifetime: private-value\n", "databases[0].max_conn_lifetime", "time.Duration"},
		{"yaml map", "yaml", "clusters: private-value\n", "clusters", "mapping"},
		{"yaml list", "yaml", "databases: private-value\n", "databases", "sequence"},
		{"yaml nested", "yaml", "logical_databases:\n  app:\n    read_clusters:\n      replica:\n        weight: private-value\n", `logical_databases["app"].read_clusters["replica"].weight`, "uint8"},
		{"yaml secret", "yaml", "databases:\n- password: [private-value]\n", "databases[0].password", "config.Secret"},
		{"json secret", "json", `{"users":[{"staticCredentials":{"password":["private-value"]}}]}`, "users[0].staticCredentials.password", "config.Secret"},
		{"yaml duplicate", "yaml", "proxy:\n  tls_required: false\n  tls_required: private-value\n", "proxy", "duplicate field"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var read func(io.Reader) (Config, error) = ReadYAML
			if tc.format == "json" {
				read = ReadJSON
			}
			_, err := read(strings.NewReader(tc.input))
			require.ErrorContains(t, err, tc.path)
			require.ErrorContains(t, err, tc.detail)
			require.ErrorContains(t, err, "line ")
			require.ErrorContains(t, err, "column ")
			require.NotContains(t, err.Error(), "private-value")
		})
	}
}

func TestConfigurationSyntaxDiagnostics(t *testing.T) {
	_, err := ReadJSON(strings.NewReader("{\"proxy\": {\"tlsRequired\": @private-value}}"))
	require.ErrorContains(t, err, "byte ")
	require.NotContains(t, err.Error(), "private-value")
	_, err = ReadYAML(strings.NewReader("proxy:\n  tls_required: [private-value\n"))
	require.ErrorContains(t, err, "line ")
	require.NotContains(t, err.Error(), "private-value")
}

func TestDiagnosticWalkPreservesYAMLAliasesAndMerges(t *testing.T) {
	_, err := ReadYAML(strings.NewReader("clusters:\n  one: &cluster {primary: db}\n  two: {<<: *cluster}\n"))
	require.NoError(t, err)
	_, err = ReadYAML(strings.NewReader("databases:\n- &db {port: private-value}\n- *db\n"))
	require.ErrorContains(t, err, "databases[0].port")
	require.NotContains(t, err.Error(), "private-value")
}

func TestConfigurationReportsAllSchemaIssues(t *testing.T) {
	for _, tc := range []struct{ format, input string }{
		{"yaml", "proxy:\n  tls_requred: private-value\n  postgres_msg_buffer_size: private-value\ndatabases:\n- port: private-value\n- max_conns: private-value\n"},
		{"json", `{"proxy":{"tls_requred":"private-value","postgresMsgBufferSize":"private-value"},"databases":[{"port":"private-value"},{"maxConns":"private-value"}]}`},
	} {
		t.Run(tc.format, func(t *testing.T) {
			read := ReadYAML
			bufferField, maxField := "postgres_msg_buffer_size", "max_conns"
			if tc.format == "json" {
				read = ReadJSON
				bufferField, maxField = "postgresMsgBufferSize", "maxConns"
			}
			_, err := read(strings.NewReader(tc.input))
			require.Error(t, err)
			for _, field := range []string{"proxy.tls_requred", "proxy." + bufferField, "databases[0].port", "databases[1]." + maxField} {
				require.Contains(t, err.Error(), field)
			}
			require.Len(t, strings.Split(err.Error(), "\n"), 4)
			require.NotContains(t, err.Error(), "private-value")
			_, again := read(strings.NewReader(tc.input))
			require.EqualError(t, again, err.Error())
		})
	}
}

func TestConfigurationReportsAllInvalidSections(t *testing.T) {
	for _, document := range []string{`{"proxy":false,"databases":[false,false]}`, "proxy: false\ndatabases:\n- false\n- false\n"} {
		read := ReadYAML
		if strings.HasPrefix(document, "{") {
			read = ReadJSON
		}
		_, err := read(strings.NewReader(document))
		for _, path := range []string{"(proxy)", "(databases[0])", "(databases[1])"} {
			require.ErrorContains(t, err, path)
		}
		require.Len(t, strings.Split(err.Error(), "\n"), 3)
	}
}

func TestEmbeddedPointerSchemaDiagnostics(t *testing.T) {
	type database struct {
		*DatabaseSettings `yaml:",inline"`
	}
	type application struct {
		Databases []database `json:"databases" yaml:"databases"`
	}
	for _, format := range []string{"json", "yaml"} {
		t.Run(format, func(t *testing.T) {
			read := ReadJSONAs[application]
			valid := `{"databases":[{"name":"db","host":"localhost"}]}`
			invalid := `{"databases":[{"name":"db","typo":"private-value","port":"private-value"}]}`
			if format == "yaml" {
				read = ReadYAMLAs[application]
				valid = "databases:\n- name: db\n  host: localhost\n"
				invalid = "databases:\n- name: db\n  typo: private-value\n  port: private-value\n"
			}
			cfg, err := read(strings.NewReader(valid))
			require.NoError(t, err)
			require.NotNil(t, cfg.Databases[0].DatabaseSettings)
			require.Equal(t, "localhost", cfg.Databases[0].Host)
			require.NotPanics(t, func() { _, err = read(strings.NewReader(invalid)) })
			require.ErrorContains(t, err, "databases[0].typo")
			require.ErrorContains(t, err, "databases[0].port")
			require.NotContains(t, err.Error(), "private-value")
		})
	}
}
