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
