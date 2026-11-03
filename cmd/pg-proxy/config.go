package main

import (
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	pgconfig "github.com/DataDog/pg-proxy/pkg/config"
	"github.com/DataDog/pg-proxy/pkg/pool"
	"github.com/DataDog/pg-proxy/pkg/topology"
)

type traceBackend uint8

const (
	traceNone traceBackend = iota
	traceDatadog
	traceOTel
)

type metricBackend uint8

const (
	metricNone metricBackend = iota
	metricDogStatsD
	metricOTel
)

type logOutput uint8

const (
	logsText logOutput = iota
	logsJSON
	logsOTel // Text stderr plus OTLP export.
)

// telemetryConfig contains resolved runtime choices. Its zero value disables
// export and retains text logs on stderr.
type telemetryConfig struct {
	Traces                traceBackend
	Metrics               metricBackend
	Logs                  logOutput
	LogLevel              slog.Level
	CaptureBindParameters bool
}

func (c telemetryConfig) validate() error {
	if c.Traces > traceOTel {
		return errors.New("invalid trace backend")
	}
	if c.Metrics > metricOTel {
		return errors.New("invalid metric backend")
	}
	if c.Logs > logsOTel {
		return errors.New("invalid log output")
	}
	return nil
}

func parseConfig(args []string) (config, error) {
	var path string
	var tracing, telemetry string
	var traceParameters bool
	var logLevel slog.Level
	flags := flag.NewFlagSet("pg-proxy", flag.ContinueOnError)
	flags.StringVar(&telemetry, "telemetry", "", "all-signal preset: datadog, otel, or none (default: OTEL_*_EXPORTER selection)")
	flags.StringVar(&tracing, "tracing", "none", "query tracing backend: none or otel (overrides OTEL_TRACES_EXPORTER)")
	flags.BoolVar(&traceParameters, "trace-parameters", false, "include raw bind parameter values in query traces (default: disabled; SQL text is always recorded)")
	flags.TextVar(&logLevel, "log-level", slog.LevelInfo, "minimum proxy log level for stderr and OTLP: DEBUG, INFO, WARN, or ERROR")
	flags.StringVar(&path, "config", "", "JSON or YAML configuration file")
	if err := flags.Parse(args); err != nil {
		return config{}, err
	}
	if flags.NArg() != 0 {
		return config{}, errors.New("unexpected positional arguments")
	}
	if path == "" {
		return config{}, errors.New("-config is required")
	}
	file, err := os.Open(path)
	if err != nil {
		return config{}, errors.New("could not open configuration file")
	}
	defer file.Close()
	var shared pgconfig.Config
	switch filepath.Ext(path) {
	case ".json":
		shared, err = pgconfig.ReadJSON(file)
	case ".yaml", ".yml":
		shared, err = pgconfig.ReadYAML(file)
	default:
		return config{}, errors.New("configuration file must use .json, .yaml, or .yml")
	}
	if err != nil {
		return config{}, err
	}
	tracingSet := false
	flags.Visit(func(f *flag.Flag) {
		if f.Name == "tracing" {
			tracingSet = true
		}
	})
	resolved, err := resolveTelemetry(telemetry, tracing, tracingSet, traceParameters)
	if err != nil {
		return config{}, err
	}
	resolved.LogLevel = logLevel
	return (config{telemetry: resolved, telemetryWarnings: telemetryPresetWarnings(telemetry)}).withSharedConfig(shared)
}

// Collect diagnostics during configuration, then log them through the selected
// handler at startup. Never include raw environment values in the warning.
func telemetryPresetWarnings(preset string) []string {
	type selection struct{ key, exporter, backend string }
	var selections []selection
	switch preset {
	case "none":
		selections = []selection{
			{"OTEL_TRACES_EXPORTER", "none", "disabled tracing"},
			{"OTEL_METRICS_EXPORTER", "none", "disabled metrics"},
			{"OTEL_LOGS_EXPORTER", "none", "stderr logs"},
		}
	case "otel":
		selections = []selection{
			{"OTEL_TRACES_EXPORTER", "otlp", "OTLP tracing"},
			{"OTEL_METRICS_EXPORTER", "otlp", "OTLP metrics"},
			{"OTEL_LOGS_EXPORTER", "otlp", "OTLP logs"},
		}
	case "datadog":
		// The native tracer also reads OTEL_TRACES_EXPORTER. Its existing
		// startup checks handle conflicts; do not claim that setting is ignored.
		selections = []selection{
			{"OTEL_METRICS_EXPORTER", "", "DogStatsD metrics"},
			{"OTEL_LOGS_EXPORTER", "none", "JSON stderr logs"},
		}
	}
	var warnings []string
	for _, selection := range selections {
		value := strings.ToLower(strings.TrimSpace(os.Getenv(selection.key)))
		if value != "" && value != selection.exporter {
			warnings = append(warnings, fmt.Sprintf("ignoring %s: -telemetry %s selects %s", selection.key, preset, selection.backend))
		}
	}
	return warnings
}

// resolveTelemetry applies presets and exporter selection once. Startup uses
// only the selected backends, not the flags that selected them.
func resolveTelemetry(preset, tracing string, tracingSet, captureBindParameters bool) (telemetryConfig, error) {
	cfg := telemetryConfig{CaptureBindParameters: captureBindParameters}
	if tracing != "none" && tracing != "otel" {
		return cfg, errors.New("-tracing must be none or otel")
	}
	switch preset {
	case "":
		// Retain independent OTEL_*_EXPORTER selection and the legacy tracing flag.
	case "datadog", "otel", "none":
		if tracingSet {
			return cfg, errors.New("-telemetry cannot be combined with -tracing")
		}
		if preset == "datadog" {
			cfg.Traces, cfg.Metrics, cfg.Logs = traceDatadog, metricDogStatsD, logsJSON
		} else if preset == "otel" {
			cfg.Traces, cfg.Metrics, cfg.Logs = traceOTel, metricOTel, logsOTel
		}
		return cfg, nil
	default:
		return cfg, errors.New("-telemetry must be datadog, otel, or none")
	}
	if tracingSet {
		if tracing == "otel" {
			cfg.Traces = traceOTel
		}
	} else {
		exporter, err := exporterFromEnv("OTEL_TRACES_EXPORTER")
		if err != nil {
			return cfg, err
		}
		if exporter == "otlp" {
			cfg.Traces = traceOTel
		}
	}
	metrics, err := exporterFromEnv("OTEL_METRICS_EXPORTER")
	if err != nil {
		return cfg, err
	}
	if metrics == "otlp" {
		cfg.Metrics = metricOTel
	}
	logs, err := exporterFromEnv("OTEL_LOGS_EXPORTER")
	if err != nil {
		return cfg, err
	}
	if logs == "otlp" {
		cfg.Logs = logsOTel
	}
	return cfg, nil
}

func exporterFromEnv(key string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(key))) {
	case "", "none":
		return "none", nil
	case "otlp":
		return "otlp", nil
	default:
		return "", fmt.Errorf("%s must be none or otlp", key)
	}
}

// withSharedConfig resolves the settings supported by the OSS binary.
// Reject unsupported features before opening pools or the listener.
func (cfg config) withSharedConfig(shared pgconfig.Config) (config, error) {
	if err := shared.ValidateCredentials(); err != nil {
		return config{}, err
	}
	// TODO: wire library TLS and SCRAM support into the OSS binary.
	if shared.Proxy.TLSRequired || shared.Proxy.TLSCertFile != "" || shared.Proxy.TLSKeyFile != "" {
		return config{}, errors.New("listener TLS is not implemented by the OSS binary")
	}
	if shared.Proxy.JWTAuthEnabled || len(shared.Users) > 0 {
		return config{}, errors.New("configured client authentication is not implemented by the OSS binary")
	}
	// TODO(OSQQUERY-715): wire shared traffic mirroring settings into the OSS binary.
	if shared.Mirroring != nil {
		return config{}, errors.New("mirroring configuration is not implemented by the OSS binary")
	}
	cfg.databases = make(map[string]*pgxpool.Config, len(shared.Databases))
	for _, database := range shared.Databases {
		if database.Name == "" {
			return config{}, errors.New("database name is required")
		}
		if _, exists := cfg.databases[database.Name]; exists {
			return config{}, errors.New("database names must be unique")
		}
		settings, err := pool.PGXConfig(database)
		if err != nil {
			return config{}, err
		}
		cfg.databases[database.Name] = settings
	}
	cfg.clusters = make(map[string]string, len(shared.Clusters))
	for name, cluster := range shared.Clusters {
		if len(cluster.Replicas) > 0 {
			return config{}, errors.New("replica routing is not implemented by the OSS binary")
		}
		cfg.clusters[name] = cluster.Primary
	}
	cfg.logicalDatabases = make(map[string]topology.LogicalDatabase, len(shared.LogicalDatabases))
	for name, logical := range shared.LogicalDatabases {
		if len(logical.ReadClusters) > 0 {
			return config{}, errors.New("read-cluster routing is not implemented by the OSS binary")
		}
		cfg.logicalDatabases[name] = topology.LogicalDatabase{Cluster: logical.Cluster}
	}
	cfg.listenAddress = shared.Proxy.ListenerAddress
	if cfg.listenAddress == "" {
		cfg.listenAddress = "127.0.0.1:15432"
	}
	cfg.messageBufferSize = int(shared.Proxy.PostgresMsgBufferSize)
	if cfg.messageBufferSize == 0 {
		cfg.messageBufferSize = topology.DefaultPostgresMessageBufferSize
	}
	return cfg, cfg.validate()
}
