// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package main

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Execute the real entry point in a subprocess: a valid offline config must
// return before telemetry initialization, pool creation, or listener startup.
func TestValidateConfigProcess(t *testing.T) {
	if path := os.Getenv("PG_PROXY_VALIDATE_TEST_CONFIG"); path != "" {
		os.Args = []string{"pg-proxy", "-config", path, "-validate-config", "-telemetry", "otel"}
		main()
		os.Exit(0)
	}
	executable, err := os.Executable()
	require.NoError(t, err)
	shared := ossConfig()
	shared.Databases[0].Host = "192.0.2.1"
	shared.Databases[0].Port = 1
	data, err := json.Marshal(shared)
	require.NoError(t, err)
	for _, tc := range []struct {
		name, contents, message string
		valid                   bool
	}{
		{"valid without network", string(data), "configuration valid", true},
		{"invalid schema", `{"proxy":{"tlsRequred":true}}`, "proxy.tlsRequred", false},
		{"invalid topology", `{"databases":[]}`, "must not be empty", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := writeConfig(t, "json", tc.contents)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, executable, "-test.run=^TestValidateConfigProcess$")
			cmd.Env = append(os.Environ(), "PG_PROXY_VALIDATE_TEST_CONFIG="+path, "OTEL_EXPORTER_OTLP_ENDPOINT=invalid-endpoint")
			output, err := cmd.CombinedOutput()
			require.NoError(t, ctx.Err(), "validation did not exit promptly: %s", output)
			if tc.valid {
				require.NoError(t, err, "%s", output)
			} else {
				var exit *exec.ExitError
				require.ErrorAs(t, err, &exit)
				require.Equal(t, 1, exit.ExitCode())
			}
			require.Contains(t, string(output), tc.message)
			require.NotContains(t, string(output), "test-password")
		})
	}
}

func TestValidateConfigUsesStartupChecks(t *testing.T) {
	for _, tc := range []struct {
		name    string
		modify  func(*config)
		message string
	}{
		{"missing primary", func(c *config) { c.clusters["orders"] = "missing" }, `clusters["orders"].primary`},
		{"missing cluster", func(c *config) { c.logicalDatabases["client-db"] = c.logicalDatabases["missing"] }, `logical_databases["client-db"].cluster`},
		{"invalid listener", func(c *config) { c.listenAddress = "0.0.0.0:5432" }, "proxy.listener_address"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := (config{}).withSharedConfig(ossConfig())
			require.NoError(t, err)
			tc.modify(&cfg)
			require.ErrorContains(t, cfg.validate(), tc.message)
		})
	}
	data, err := json.Marshal(ossConfig())
	require.NoError(t, err)
	path := writeConfig(t, "json", string(data))
	cfg, err := parseConfig([]string{"-config", path, "-validate-config", "-telemetry", "none"})
	require.NoError(t, err)
	require.True(t, cfg.validateOnly)
	_, err = parseConfig([]string{"-validate-config"})
	require.ErrorContains(t, err, "-config is required")
	shared := ossConfig()
	shared.Databases[0].MinConns = 10
	shared.Databases[0].MaxConns = 2
	_, err = (config{}).withSharedConfig(shared)
	require.ErrorContains(t, err, "databases[0]")
	require.ErrorContains(t, err, "max_conns")
	shared = ossConfig()
	shared.Proxy.TLSCertFile = "does-not-exist.pem"
	shared.Proxy.TLSKeyFile = "does-not-exist.key"
	_, err = (config{}).withSharedConfig(shared)
	require.ErrorContains(t, err, "proxy.tls_cert_file")
}

func TestValidateConfigReportsAllLocalIssues(t *testing.T) {
	document := `databases:
  - name: primary
    host: localhost
    dbname: postgres
    username: postgres
    password_file: /missing-secret-sentinel/password
    port: 70000
    max_conns: -1
    min_conns: -1
  - name: other
    dbname: postgres
    username: postgres
clusters:
  orders:
    primary: primary
    replicas:
      - missing-replica
      - primary
  broken:
    primary: missing
logical_databases:
  app:
    cluster: absent
    read_clusters:
      missing-read:
        weight: 1
proxy:
  listener_address: invalid-address
  max_sessions: -1
  default_max_user_sessions: -1
  shutdown_grace_period: invalid-duration
  postgres_msg_buffer_size: -1
  tls_cert_file: /missing-secret-sentinel/cert
  tls_key_file: /missing-secret-sentinel/key
`
	path := writeConfig(t, "yaml", document)
	args := []string{"-config", path, "-validate-config", "-telemetry", "none"}
	cfg, err := parseConfig(args)
	require.Error(t, err)
	require.Nil(t, cfg.databases, "invalid configuration must not be usable")
	expected := []string{
		"database 0: password_file: file not found",
		"databases[0]: database port is out of range",
		"databases[0]: database max_conns must not be negative",
		"databases[0]: database min_conns must not be negative",
		"databases[1]: database host is required",
		`clusters["broken"].primary`, `logical_databases["app"].cluster`,
		`clusters["orders"].replicas[0]`, `clusters["orders"].replicas[1]`,
		`logical_databases["app"].read_clusters["missing-read"]`,
		"proxy.listener_address", "proxy.max_sessions", "proxy.default_max_user_sessions",
		"proxy.shutdown_grace_period", "postgres_msg_buffer_size", "proxy.tls_cert_file", "proxy.tls_key_file",
	}
	for _, message := range expected {
		require.Contains(t, err.Error(), message)
	}
	require.NotContains(t, err.Error(), "missing-secret-sentinel")
	require.NotContains(t, err.Error(), `clusters["orders"].primary`, "invalid pool settings must not look like an unknown database")
	_, again := parseConfig(args)
	require.EqualError(t, again, err.Error())
	executable, err := os.Executable()
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestValidateConfigProcess$")
	cmd.Env = append(os.Environ(), "PG_PROXY_VALIDATE_TEST_CONFIG="+path)
	output, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	require.ErrorAs(t, err, &exit)
	require.Equal(t, 1, exit.ExitCode())
	require.NoError(t, ctx.Err())
	require.Contains(t, string(output), "invalid configuration:\n")
	for _, message := range expected {
		require.Contains(t, string(output), message)
	}
	require.NotContains(t, string(output), `\n`, "issues should be printed on separate lines")
}
