// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package main

import (
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	pgconfig "github.com/DataDog/pg-proxy/pkg/config"
)

func ossConfig() pgconfig.Config {
	return pgconfig.Config{
		Databases: []pgconfig.Database{{
			Username: "postgres", Password: "test-password",
			DatabaseSettings: pgconfig.DatabaseSettings{Name: "primary", Host: "localhost", DBName: "postgres", SSLMode: "disable"},
		}},
		Clusters:         map[string]pgconfig.Cluster{"orders": {Primary: "primary"}},
		LogicalDatabases: map[string]pgconfig.LogicalDatabase{"client-db": {Cluster: "orders"}},
		Proxy:            pgconfig.Proxy{ProxySettings: pgconfig.ProxySettings{ListenerAddress: "127.0.0.1:0", PostgresMsgBufferSize: 4096}},
	}
}

func writeConfig(t *testing.T, extension, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "proxy."+extension)
	require.NoError(t, os.WriteFile(path, []byte(contents), 0600))
	return path
}

func TestReadSharedConfigFiles(t *testing.T) {
	for _, signal := range []string{"TRACES", "METRICS", "LOGS"} {
		t.Setenv("OTEL_"+signal+"_EXPORTER", "")
	}
	t.Setenv("PG_PROXY_BACKEND_URL", "invalid-and-must-be-ignored")
	for extension, contents := range map[string]string{
		"json": `{"databases":[{"name":"primary","host":"localhost","dbName":"postgres","username":"postgres","password":"test-password","maxConns":2,"connectionOptions":{"statement_timeout":"5s"}}],"clusters":{"orders":{"primary":"primary"}},"logicalDatabases":{"client-db":{"cluster":"orders"}},"proxy":{"listenerAddress":"127.0.0.1:0","postgresMsgBufferSize":4096}}`,
		"yaml": `databases:
- name: primary
  host: localhost
  dbname: postgres
  username: postgres
  password: test-password
  max_conns: 2
  connection_options: {statement_timeout: 5s}
clusters:
  orders: {primary: primary}
logical_databases:
  client-db: {cluster: orders}
proxy:
  listener_address: '127.0.0.1:0'
  postgres_msg_buffer_size: 4096
`,
	} {
		t.Run(extension, func(t *testing.T) {
			path := writeConfig(t, extension, contents)
			cfg, err := parseConfig([]string{"-config", path})
			require.NoError(t, err)
			require.Equal(t, traceNone, cfg.telemetry.Traces)
			withTracing, err := parseConfig([]string{"-config", path, "-tracing", "otel"})
			require.NoError(t, err)
			require.Equal(t, traceOTel, withTracing.telemetry.Traces)
			_, err = parseConfig([]string{"-config", path, "-tracing", "unknown"})
			require.ErrorContains(t, err, "none or otel")
			require.Equal(t, "orders", cfg.logicalDatabases["client-db"].Cluster)
			require.Equal(t, "primary", cfg.clusters["orders"])
			require.Equal(t, "127.0.0.1:0", cfg.listenAddress)
			require.Equal(t, 4096, cfg.messageBufferSize)
			require.Equal(t, "test-password", cfg.databases["primary"].ConnConfig.Password)
			require.Equal(t, int32(2), cfg.databases["primary"].MaxConns)
			require.Equal(t, "-c statement_timeout=5s", cfg.databases["primary"].ConnConfig.RuntimeParams["options"])
			for _, flag := range []string{"-backend-url", "-listen", "-database", "-max-sessions"} {
				_, err := parseConfig([]string{"-config", path, flag, "ignored"})
				require.ErrorContains(t, err, "flag provided but not defined")
			}
		})
	}
}

func TestTelemetryEnvironmentConfig(t *testing.T) {
	data, err := json.Marshal(ossConfig())
	require.NoError(t, err)
	path := writeConfig(t, "json", string(data))
	for _, tc := range []struct {
		name, traces, metrics, logs, flag, errorKey string
		want                                        telemetryConfig
	}{
		{name: "disabled by default"},
		{name: "enable all", traces: "otlp", metrics: "otlp", logs: "otlp", want: telemetryConfig{Traces: traceOTel, Metrics: metricOTel, Logs: logsOTel}},
		{name: "traces only", traces: "otlp", want: telemetryConfig{Traces: traceOTel}},
		{name: "metrics only", metrics: "otlp", want: telemetryConfig{Metrics: metricOTel}},
		{name: "logs only", logs: "otlp", want: telemetryConfig{Logs: logsOTel}},
		{name: "traces and metrics", traces: "otlp", metrics: "otlp", want: telemetryConfig{Traces: traceOTel, Metrics: metricOTel}},
		{name: "traces and logs", traces: "otlp", logs: "otlp", want: telemetryConfig{Traces: traceOTel, Logs: logsOTel}},
		{name: "metrics and logs", metrics: "otlp", logs: "otlp", want: telemetryConfig{Metrics: metricOTel, Logs: logsOTel}},
		{name: "normalized selectors", traces: " NONE ", metrics: " OTLP ", logs: "None", want: telemetryConfig{Metrics: metricOTel}},
		{name: "flag disables traces", traces: "otlp", flag: "none"},
		{name: "flag enables traces", traces: "none", flag: "otel", want: telemetryConfig{Traces: traceOTel}},
		{name: "flag overrides invalid trace env", traces: "secret-invalid-value", flag: "otel", want: telemetryConfig{Traces: traceOTel}},
		{name: "invalid traces", traces: "secret-invalid-value", errorKey: "OTEL_TRACES_EXPORTER"},
		{name: "invalid metrics", metrics: "secret-invalid-value", errorKey: "OTEL_METRICS_EXPORTER"},
		{name: "invalid logs", logs: "secret-invalid-value", errorKey: "OTEL_LOGS_EXPORTER"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("OTEL_TRACES_EXPORTER", tc.traces)
			t.Setenv("OTEL_METRICS_EXPORTER", tc.metrics)
			t.Setenv("OTEL_LOGS_EXPORTER", tc.logs)
			args := []string{"-config", path}
			if tc.flag != "" {
				args = append(args, "-tracing", tc.flag)
			}
			cfg, err := parseConfig(args)
			if tc.errorKey != "" {
				require.ErrorContains(t, err, tc.errorKey)
				require.NotContains(t, err.Error(), "secret-invalid-value")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, cfg.telemetry)
		})
	}
}

func TestSharedConfigRejectsUnsupportedFeatures(t *testing.T) {
	for name, modify := range map[string]func(*pgconfig.Config){
		"JWT":                      func(c *pgconfig.Config) { c.Proxy.JWTAuthEnabled = true },
		"password authentication":  func(c *pgconfig.Config) { c.Proxy.PasswordAuthEnabled = true },
		"users":                    func(c *pgconfig.Config) { c.Users = []pgconfig.User{{}} },
		"mirroring":                func(c *pgconfig.Config) { c.Mirroring = &pgconfig.Mirroring{} },
		"no databases":             func(c *pgconfig.Config) { c.Databases = nil },
		"duplicate database":       func(c *pgconfig.Config) { c.Databases = append(c.Databases, c.Databases[0]) },
		"unnamed database":         func(c *pgconfig.Config) { c.Databases[0].Name = "" },
		"no clusters":              func(c *pgconfig.Config) { c.Clusters = nil },
		"unnamed cluster":          func(c *pgconfig.Config) { c.Clusters[""] = c.Clusters["orders"] },
		"unnamed logical database": func(c *pgconfig.Config) { c.LogicalDatabases[""] = c.LogicalDatabases["client-db"] },
		"unused database": func(c *pgconfig.Config) {
			database := c.Databases[0]
			database.Name = "unused"
			c.Databases = append(c.Databases, database)
		},
		"unknown primary": func(c *pgconfig.Config) { c.Clusters["orders"] = pgconfig.Cluster{Primary: "unknown"} },
		"replicas": func(c *pgconfig.Config) {
			c.Clusters["orders"] = pgconfig.Cluster{Primary: "primary", Replicas: []string{"primary"}}
		},
		"no logical databases": func(c *pgconfig.Config) { c.LogicalDatabases = nil },
		"unknown cluster": func(c *pgconfig.Config) {
			c.LogicalDatabases["client-db"] = pgconfig.LogicalDatabase{Cluster: "unknown"}
		},
		"read clusters": func(c *pgconfig.Config) {
			c.LogicalDatabases["client-db"] = pgconfig.LogicalDatabase{Cluster: "orders", ReadClusters: map[string]*pgconfig.ReadCluster{"orders": {}}}
		},
		"public listener": func(c *pgconfig.Config) { c.Proxy.ListenerAddress = "0.0.0.0:15432" },
		"negative buffer": func(c *pgconfig.Config) { c.Proxy.PostgresMsgBufferSize = -1 },
	} {
		t.Run(name, func(t *testing.T) {
			shared := ossConfig()
			modify(&shared)
			_, err := (config{}).withSharedConfig(shared)
			require.Error(t, err)
			require.NotContains(t, err.Error(), "test-password")
		})
	}
}

func TestConfigFileDecodeErrors(t *testing.T) {
	for extension, contents := range map[string]string{
		"json": `{"unknown":"test-password"}`,
		"yaml": "unknown: test-password",
		"yml":  "{}\n---\n{}",
		"txt":  "{}",
	} {
		_, err := parseConfig([]string{"-config", writeConfig(t, extension, contents)})
		require.Error(t, err)
		require.NotContains(t, err.Error(), "test-password")
	}
}

func TestConfigRequiredAndDefaults(t *testing.T) {
	t.Setenv("PG_PROXY_BACKEND_URL", "postgres://localhost/postgres")
	_, err := parseConfig(nil)
	require.EqualError(t, err, "-config is required")
	shared := ossConfig()
	shared.Proxy = pgconfig.Proxy{}
	cfg, err := (config{}).withSharedConfig(shared)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1:15432", cfg.listenAddress)
	require.Equal(t, 10240, cfg.messageBufferSize)
}

func TestMultiplePrimaryBackends(t *testing.T) {
	shared := ossConfig()
	database := shared.Databases[0]
	database.Name = "reports-primary"
	database.Username = "reports-user"
	shared.Databases = append(shared.Databases, database)
	shared.Clusters["reports"] = pgconfig.Cluster{Primary: "reports-primary"}
	shared.LogicalDatabases["reports-db"] = pgconfig.LogicalDatabase{Cluster: "reports"}
	shared.LogicalDatabases["client-db-alias"] = shared.LogicalDatabases["client-db"]
	cfg, err := (config{}).withSharedConfig(shared)
	require.NoError(t, err)
	require.Len(t, cfg.databases, 2)
	require.Len(t, cfg.clusters, 2)
	require.Len(t, cfg.logicalDatabases, 3)
	require.Equal(t, "reports", cfg.logicalDatabases["reports-db"].Cluster)
	require.Equal(t, "reports-primary", cfg.clusters["reports"])
	require.Equal(t, "reports-user", cfg.databases["reports-primary"].ConnConfig.User)
	require.Equal(t, cfg.logicalDatabases["client-db"], cfg.logicalDatabases["client-db-alias"])
}

func TestTelemetryPresets(t *testing.T) {
	data, err := json.Marshal(ossConfig())
	require.NoError(t, err)
	path := writeConfig(t, "json", string(data))
	// Explicit presets override the independent exporter selectors.
	for _, signal := range []string{"TRACES", "METRICS", "LOGS"} {
		t.Setenv("OTEL_"+signal+"_EXPORTER", "invalid")
	}
	for mode, want := range map[string]telemetryConfig{
		"datadog": {Traces: traceDatadog, Metrics: metricDogStatsD, Logs: logsJSON},
		"otel":    {Traces: traceOTel, Metrics: metricOTel, Logs: logsOTel},
		"none":    {},
	} {
		cfg, err := parseConfig([]string{"-config", path, "-telemetry", mode})
		require.NoError(t, err)
		require.Equal(t, want, cfg.telemetry)
		_, err = parseConfig([]string{"-config", path, "-telemetry", mode, "-tracing", "none"})
		require.ErrorContains(t, err, "cannot be combined")
	}
	_, err = parseConfig([]string{"-config", path, "-telemetry", "unknown"})
	require.ErrorContains(t, err, "-telemetry must be")
}

func TestTelemetryPresetWarnings(t *testing.T) {
	data, err := json.Marshal(ossConfig())
	require.NoError(t, err)
	path := writeConfig(t, "json", string(data))
	for _, tc := range []struct {
		name, preset, traces, metrics, logs string
		want                                []string
	}{
		{name: "unset none", preset: "none"},
		{name: "unset otel", preset: "otel"},
		{name: "unset datadog", preset: "datadog"},
		{name: "matching none", preset: "none", traces: " NONE ", metrics: "None", logs: "none"},
		{name: "matching otel", preset: "otel", traces: " OTLP ", metrics: "Otlp", logs: "otlp"},
		{name: "matching native logs", preset: "datadog", logs: " NONE "},
		{name: "independent exporters", traces: "otlp", metrics: "none", logs: "otlp"},
		{name: "none overrides otlp", preset: "none", traces: "otlp", metrics: " OTLP ", logs: "Otlp", want: []string{
			"ignoring OTEL_TRACES_EXPORTER: -telemetry none selects disabled tracing",
			"ignoring OTEL_METRICS_EXPORTER: -telemetry none selects disabled metrics",
			"ignoring OTEL_LOGS_EXPORTER: -telemetry none selects stderr logs",
		}},
		{name: "otel overrides none", preset: "otel", traces: "none", metrics: " NONE ", logs: "None", want: []string{
			"ignoring OTEL_TRACES_EXPORTER: -telemetry otel selects OTLP tracing",
			"ignoring OTEL_METRICS_EXPORTER: -telemetry otel selects OTLP metrics",
			"ignoring OTEL_LOGS_EXPORTER: -telemetry otel selects OTLP logs",
		}},
		{name: "datadog overrides exporters", preset: "datadog", traces: "otlp", metrics: "otlp", logs: "otlp", want: []string{
			"ignoring OTEL_METRICS_EXPORTER: -telemetry datadog selects DogStatsD metrics",
			"ignoring OTEL_LOGS_EXPORTER: -telemetry datadog selects JSON stderr logs",
		}},
		{name: "datadog enables metrics", preset: "datadog", metrics: "none", want: []string{
			"ignoring OTEL_METRICS_EXPORTER: -telemetry datadog selects DogStatsD metrics",
		}},
		{name: "native trace setting is not ignored", preset: "datadog", traces: "none"},
		{name: "invalid value omitted", preset: "otel", metrics: "secret-invalid-value", want: []string{
			"ignoring OTEL_METRICS_EXPORTER: -telemetry otel selects OTLP metrics",
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("OTEL_TRACES_EXPORTER", tc.traces)
			t.Setenv("OTEL_METRICS_EXPORTER", tc.metrics)
			t.Setenv("OTEL_LOGS_EXPORTER", tc.logs)
			args := []string{"-config", path}
			if tc.preset != "" {
				args = append(args, "-telemetry", tc.preset)
			}
			cfg, err := parseConfig(args)
			require.NoError(t, err)
			require.Equal(t, tc.want, cfg.telemetryWarnings)
		})
	}
}

func TestTraceParameterSelection(t *testing.T) {
	data, err := json.Marshal(ossConfig())
	require.NoError(t, err)
	path := writeConfig(t, "json", string(data))
	for _, signal := range []string{"TRACES", "METRICS", "LOGS"} {
		t.Setenv("OTEL_"+signal+"_EXPORTER", "")
	}
	for _, mode := range []string{"", "otel", "datadog"} {
		for _, tc := range []struct {
			flag    string
			capture bool
		}{
			{"", false},
			{"-trace-parameters", true},
			{"-trace-parameters=true", true},
			{"-trace-parameters=false", false},
		} {
			args := []string{"-config", path}
			if mode != "" {
				args = append(args, "-telemetry", mode)
			}
			if tc.flag != "" {
				args = append(args, tc.flag)
			}
			cfg, err := parseConfig(args)
			require.NoError(t, err)
			require.Equal(t, tc.capture, cfg.telemetry.CaptureBindParameters, "args: %v", args)
		}
	}
}

func TestLogLevelSelection(t *testing.T) {
	data, err := json.Marshal(ossConfig())
	require.NoError(t, err)
	path := writeConfig(t, "json", string(data))
	for _, mode := range []string{"none", "otel", "datadog"} {
		for _, tc := range []struct {
			value string
			want  slog.Level
		}{
			{"", slog.LevelInfo},
			{"debug", slog.LevelDebug},
			{"INFO", slog.LevelInfo},
			{"Warn", slog.LevelWarn},
			{"error", slog.LevelError},
		} {
			args := []string{"-config", path, "-telemetry", mode}
			if tc.value != "" {
				args = append(args, "-log-level", tc.value)
			}
			cfg, err := parseConfig(args)
			require.NoError(t, err)
			require.Equal(t, tc.want, cfg.telemetry.LogLevel, "args: %v", args)
		}
	}
	for _, value := range []string{"", "verbose", "42"} {
		_, err := parseConfig([]string{"-config", path, "-log-level", value})
		require.ErrorContains(t, err, "log-level")
	}
}
