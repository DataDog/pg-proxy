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
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	pgconfig "github.com/DataDog/pg-proxy/pkg/config"
	"github.com/DataDog/pg-proxy/pkg/pool"
	"github.com/DataDog/pg-proxy/pkg/topology"
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

// namedDatabase returns a copy of the ossConfig database named name.
func namedDatabase(name string) pgconfig.Database {
	database := ossConfig().Databases[0]
	database.Name = name
	return database
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
			require.Equal(t, "primary", cfg.clusters["orders"].Primary)
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
		"unused database":          func(c *pgconfig.Config) { c.Databases = append(c.Databases, namedDatabase("unused")) },
		"unknown primary":          func(c *pgconfig.Config) { c.Clusters["orders"] = pgconfig.Cluster{Primary: "unknown"} },
		"replica is primary": func(c *pgconfig.Config) {
			c.Clusters["orders"] = pgconfig.Cluster{Primary: "primary", Replicas: []string{"primary"}}
		},
		"unknown replica": func(c *pgconfig.Config) {
			c.Clusters["orders"] = pgconfig.Cluster{Primary: "primary", Replicas: []string{"unknown"}}
		},
		"duplicate replica": func(c *pgconfig.Config) {
			c.Databases = append(c.Databases, namedDatabase("replica"))
			c.Clusters["orders"] = pgconfig.Cluster{Primary: "primary", Replicas: []string{"replica", "replica"}}
		},
		"no logical databases": func(c *pgconfig.Config) { c.LogicalDatabases = nil },
		"unknown cluster": func(c *pgconfig.Config) {
			c.LogicalDatabases["client-db"] = pgconfig.LogicalDatabase{Cluster: "unknown"}
		},
		"unknown read cluster": func(c *pgconfig.Config) {
			c.LogicalDatabases["client-db"] = pgconfig.LogicalDatabase{Cluster: "orders", ReadClusters: map[string]*pgconfig.ReadCluster{"unknown": {}}}
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
	for name, file := range map[string]struct{ extension, contents string }{
		"unknown json field":      {"json", `{"unknown":"test-password"}`},
		"unknown yaml field":      {"yaml", "unknown: test-password"},
		"multiple yaml documents": {"yml", "{}\n---\n{}"},
		"unsupported extension":   {"txt", "{}"},
		"negative read weight": {"yaml", `databases:
- {name: primary, host: localhost, dbname: postgres, username: postgres, password: test-password}
clusters:
  orders: {primary: primary}
logical_databases:
  client-db: {cluster: orders, read_clusters: {orders: {weight: -1}}}
`},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := parseConfig([]string{"-config", writeConfig(t, file.extension, file.contents)})
			require.Error(t, err)
			require.NotContains(t, err.Error(), "test-password")
		})
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
	require.Equal(t, "127.0.0.1:5432", cfg.listenAddress)
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
	require.Equal(t, "reports-primary", cfg.clusters["reports"].Primary)
	require.Equal(t, "reports-user", cfg.databases["reports-primary"].ConnConfig.User)
	require.Equal(t, cfg.logicalDatabases["client-db"], cfg.logicalDatabases["client-db-alias"])
}

func TestReplicasAndReadClusters(t *testing.T) {
	shared := ossConfig()
	tuned := namedDatabase("replica-b")
	tuned.MaxConnLifetime = 2 * time.Hour
	shared.Databases = append(shared.Databases, namedDatabase("replica-a"), tuned, namedDatabase("reports-primary"))
	shared.Clusters["orders"] = pgconfig.Cluster{Primary: "primary", Replicas: []string{"replica-a", "replica-b"}}
	shared.Clusters["reports"] = pgconfig.Cluster{Primary: "reports-primary"}
	shared.LogicalDatabases["client-db"] = pgconfig.LogicalDatabase{
		Cluster: "orders",
		ReadClusters: map[string]*pgconfig.ReadCluster{
			"orders":  {Weight: new(uint8(0))},
			"reports": {Weight: new(uint8(3))},
		},
	}
	shared.LogicalDatabases["default-weight"] = pgconfig.LogicalDatabase{
		Cluster:      "orders",
		ReadClusters: map[string]*pgconfig.ReadCluster{"reports": nil},
	}
	cfg, err := (config{}).withSharedConfig(shared)
	require.NoError(t, err)
	require.Equal(t, []string{"replica-a", "replica-b"}, cfg.clusters["orders"].Replicas)
	require.Equal(t, topology.ReadClusters{
		"orders":  {Weight: 0},
		"reports": {Weight: 3},
	}, cfg.logicalDatabases["client-db"].ReadClusters)
	require.Equal(t, topology.ReadClusters{
		"reports": {Weight: topology.DefaultReadClusterWeight},
	}, cfg.logicalDatabases["default-weight"].ReadClusters)
	// Replicas use the library's replica lifetime unless the file sets one.
	require.Equal(t, pool.ReplicaMaxConnLifetime, cfg.databases["replica-a"].MaxConnLifetime)
	require.Equal(t, pool.ReplicaMaxConnLifetimeJitter, cfg.databases["replica-a"].MaxConnLifetimeJitter)
	require.Equal(t, 2*time.Hour, cfg.databases["replica-b"].MaxConnLifetime)
	require.NotEqual(t, pool.ReplicaMaxConnLifetime, cfg.databases["primary"].MaxConnLifetime)
}

func TestRoutingWarnings(t *testing.T) {
	withReplica := func(c *pgconfig.Config) {
		c.Databases = append(c.Databases, namedDatabase("replica"), namedDatabase("reports-primary"))
		c.Clusters["orders"] = pgconfig.Cluster{Primary: "primary", Replicas: []string{"replica"}}
		c.Clusters["reports"] = pgconfig.Cluster{Primary: "reports-primary"}
	}
	withReadClusters := func(reads map[string]*pgconfig.ReadCluster) func(*pgconfig.Config) {
		return func(c *pgconfig.Config) {
			c.LogicalDatabases["client-db"] = pgconfig.LogicalDatabase{Cluster: "orders", ReadClusters: reads}
		}
	}
	withReports := func(c *pgconfig.Config) {
		c.Databases = append(c.Databases, namedDatabase("reports-primary"))
		c.Clusters["reports"] = pgconfig.Cluster{Primary: "reports-primary"}
	}
	for name, tc := range map[string]struct {
		modify []func(*pgconfig.Config)
		want   []string
	}{
		"no read clusters": {},
		"matching replica and read cluster": {
			modify: []func(*pgconfig.Config){withReplica, withReadClusters(map[string]*pgconfig.ReadCluster{"orders": nil, "reports": nil})},
		},
		"different dbname and username": {
			modify: []func(*pgconfig.Config){
				func(c *pgconfig.Config) {
					replica := namedDatabase("replica")
					replica.DBName, replica.Username = "staging", "reporting"
					reports := namedDatabase("reports-primary")
					reports.Username = "reporting"
					c.Databases = append(c.Databases, replica, reports)
					c.Clusters["orders"] = pgconfig.Cluster{Primary: "primary", Replicas: []string{"replica"}}
					c.Clusters["reports"] = pgconfig.Cluster{Primary: "reports-primary"}
				},
				withReadClusters(map[string]*pgconfig.ReadCluster{"reports": nil}),
			},
			want: []string{
				`cluster "orders": replica "replica" uses dbname "staging", but database "primary" uses "postgres"`,
				`cluster "orders": replica "replica" uses a different username than database "primary"`,
				`logical database "client-db": read cluster "reports" primary "reports-primary" uses a different username than database "primary"`,
			},
		},
		"zero weights with replicas": {
			modify: []func(*pgconfig.Config){withReplica, withReadClusters(map[string]*pgconfig.ReadCluster{
				"orders": {Weight: new(uint8(0))}, "reports": {Weight: new(uint8(0))},
			})},
			want: []string{`logical database "client-db": all read_clusters weights are 0, so reads use cluster "orders" and its replicas`},
		},
		"zero weights without replicas": {
			modify: []func(*pgconfig.Config){withReports, withReadClusters(map[string]*pgconfig.ReadCluster{"reports": {Weight: new(uint8(0))}})},
			want:   []string{`logical database "client-db": read_clusters cannot send reads away from the primary of cluster "orders"`},
		},
		"own cluster without replicas": {
			modify: []func(*pgconfig.Config){withReadClusters(map[string]*pgconfig.ReadCluster{"orders": nil})},
			want:   []string{`logical database "client-db": read_clusters cannot send reads away from the primary of cluster "orders"`},
		},
	} {
		t.Run(name, func(t *testing.T) {
			shared := ossConfig()
			for _, modify := range tc.modify {
				modify(&shared)
			}
			cfg, err := (config{}).withSharedConfig(shared)
			require.NoError(t, err)
			warnings := routingWarningsFor(cfg)
			require.Equal(t, tc.want, warnings)
			for _, warning := range warnings {
				require.NotContains(t, warning, "reporting", "usernames should not be logged")
			}
		})
	}
}

func TestReadReplicaConfigFile(t *testing.T) {
	path := writeConfig(t, "yaml", `databases:
- {name: primary, host: localhost, dbname: postgres, username: postgres, password: test-password}
- {name: replica, host: localhost, dbname: postgres, username: postgres, password: test-password}
clusters:
  orders: {primary: primary, replicas: [replica]}
logical_databases:
  client-db:
    cluster: orders
    read_clusters:
      orders: {weight: 2}
`)
	cfg, err := parseConfig([]string{"-config", path})
	require.NoError(t, err)
	require.Equal(t, []string{"replica"}, cfg.clusters["orders"].Replicas)
	require.Equal(t, topology.ReadClusters{"orders": {Weight: 2}}, cfg.logicalDatabases["client-db"].ReadClusters)
}

func TestTelemetryPresets(t *testing.T) {
	data, err := json.Marshal(ossConfig())
	require.NoError(t, err)
	path := writeConfig(t, "json", string(data))
	// Explicit presets override the independent exporter selectors.
	for _, signal := range []string{"TRACES", "METRICS", "LOGS"} {
		t.Setenv("OTEL_"+signal+"_EXPORTER", "invalid")
	}
	t.Setenv("DD_SPAN_SAMPLING_RULES", "")
	t.Setenv("DD_SPAN_SAMPLING_RULES_FILE", "")
	for mode, want := range map[string]telemetryConfig{
		"datadog": {Traces: traceDatadog, Metrics: metricDogStatsD, Logs: logsJSON, SampleChildSpans: true},
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
		nativeProtocol, nativeEnabled       string
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
		{name: "datadog overrides exporters", preset: "datadog", traces: "otlp", nativeProtocol: "0.4", metrics: "otlp", logs: "otlp", want: []string{
			"ignoring OTEL_METRICS_EXPORTER: -telemetry datadog selects DogStatsD metrics",
			"ignoring OTEL_LOGS_EXPORTER: -telemetry datadog selects JSON stderr logs",
		}},
		{name: "datadog enables metrics", preset: "datadog", metrics: "none", want: []string{
			"ignoring OTEL_METRICS_EXPORTER: -telemetry datadog selects DogStatsD metrics",
		}},
		{name: "native trace setting is not ignored", preset: "datadog", traces: "none", nativeEnabled: "true"},
		{name: "invalid value omitted", preset: "otel", metrics: "secret-invalid-value", want: []string{
			"ignoring OTEL_METRICS_EXPORTER: -telemetry otel selects OTLP metrics",
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("OTEL_TRACES_EXPORTER", tc.traces)
			t.Setenv("OTEL_METRICS_EXPORTER", tc.metrics)
			t.Setenv("OTEL_LOGS_EXPORTER", tc.logs)
			t.Setenv("DD_TRACE_AGENT_PROTOCOL_VERSION", tc.nativeProtocol)
			t.Setenv("DD_TRACE_ENABLED", tc.nativeEnabled)
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

func TestSampleChildSpansFlag(t *testing.T) {
	data, err := json.Marshal(ossConfig())
	require.NoError(t, err)
	path := writeConfig(t, "json", string(data))
	for _, env := range []string{"DD_SPAN_SAMPLING_RULES", "DD_SPAN_SAMPLING_RULES_FILE", "OTEL_TRACES_EXPORTER"} {
		t.Setenv(env, "")
	}
	for _, tc := range []struct {
		telemetry, flag string
		want            bool
		wantError       string
	}{
		{telemetry: "datadog", want: true},
		{telemetry: "datadog", flag: "-sample-child-spans", want: true},
		{telemetry: "datadog", flag: "-sample-child-spans=false"},
		{telemetry: "none"},
		{telemetry: "otel"},
		{telemetry: "none", flag: "-sample-child-spans=false"},
		{telemetry: "otel", flag: "-sample-child-spans", wantError: "requires -telemetry datadog"},
		{telemetry: "none", flag: "-sample-child-spans", wantError: "requires -telemetry datadog"},
	} {
		args := []string{"-config", path, "-telemetry", tc.telemetry}
		if tc.flag != "" {
			args = append(args, tc.flag)
		}
		cfg, err := parseConfig(args)
		if tc.wantError != "" {
			require.ErrorContains(t, err, tc.wantError)
		} else {
			require.NoError(t, err)
			require.Equal(t, tc.want, cfg.telemetry.SampleChildSpans, "args: %v", args)
		}
	}
	for key, value := range map[string]string{
		"DD_SPAN_SAMPLING_RULES":      `[{"sample_rate":1}]`,
		"DD_SPAN_SAMPLING_RULES_FILE": "rules.json",
	} {
		t.Run(key, func(t *testing.T) {
			t.Setenv(key, value)
			args := []string{"-config", path, "-telemetry", "datadog"}
			cfg, err := parseConfig(args)
			require.NoError(t, err)
			require.False(t, cfg.telemetry.SampleChildSpans)
			_, err = parseConfig(append(args, "-sample-child-spans=true"))
			require.ErrorContains(t, err, "cannot be combined with "+key)
			cfg, err = parseConfig(append(args, "-sample-child-spans=false"))
			require.NoError(t, err)
			require.False(t, cfg.telemetry.SampleChildSpans)
		})
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

func TestShutdownGracePeriodConfig(t *testing.T) {
	for _, tc := range []struct {
		value   string
		want    time.Duration
		invalid bool
	}{
		{value: "", want: 30 * time.Second},
		{value: "30s", want: 30 * time.Second},
		{value: "2m", want: 2 * time.Minute},
		{value: "250ms", want: 250 * time.Millisecond},
		{value: "0s", invalid: true},
		{value: "-1s", invalid: true},
		{value: "30", invalid: true},
		{value: "invalid-secret-value", invalid: true},
		{value: "999999999999999999h", invalid: true},
	} {
		for _, format := range []string{"json", "yaml"} {
			t.Run(format+"/"+tc.value, func(t *testing.T) {
				shared := ossConfig()
				shared.Proxy.ShutdownGracePeriod = tc.value
				var data []byte
				var err error
				if format == "json" {
					data, err = json.Marshal(shared)
				} else {
					data, err = yaml.Marshal(shared)
				}
				require.NoError(t, err)
				cfg, err := parseConfig([]string{"-config", writeConfig(t, format, string(data)), "-telemetry", "none"})
				if tc.invalid {
					require.ErrorContains(t, err, "proxy.shutdown_grace_period must be a positive duration")
					require.NotContains(t, err.Error(), "invalid-secret-value")
					return
				}
				require.NoError(t, err)
				require.Equal(t, tc.want, cfg.shutdownGracePeriod)
			})
		}
	}
}

func TestSessionLimitConfig(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		global, perUser      int32
		wantGlobal, wantUser int32
		invalid              string
	}{
		{name: "defaults", wantGlobal: 100_000, wantUser: 10_000},
		{name: "configured", global: 50, perUser: 7, wantGlobal: 50, wantUser: 7},
		{name: "negative global", global: -1, invalid: "proxy.max_sessions"},
		{name: "negative user default", perUser: -1, invalid: "proxy.default_max_user_sessions"},
	} {
		for _, format := range []string{"json", "yaml"} {
			t.Run(format+"/"+tc.name, func(t *testing.T) {
				shared := ossConfig()
				shared.Proxy.MaxSessions, shared.Proxy.DefaultMaxUserSessions = tc.global, tc.perUser
				var data []byte
				var err error
				if format == "json" {
					data, err = json.Marshal(shared)
				} else {
					data, err = yaml.Marshal(shared)
				}
				require.NoError(t, err)
				cfg, err := parseConfig([]string{"-config", writeConfig(t, format, string(data)), "-telemetry", "none"})
				if tc.invalid != "" {
					require.ErrorContains(t, err, tc.invalid)
					return
				}
				require.NoError(t, err)
				require.Equal(t, tc.wantGlobal, cfg.sessionLimits.GlobalLimit)
				require.Equal(t, tc.wantUser, cfg.sessionLimits.DefaultServiceMax)
			})
		}
	}
}
