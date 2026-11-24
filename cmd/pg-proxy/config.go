// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package main

import (
	"bytes"
	"crypto"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	pgconfig "github.com/DataDog/pg-proxy/pkg/config"
	proxydatadog "github.com/DataDog/pg-proxy/pkg/datadog"
	"github.com/DataDog/pg-proxy/pkg/limiter"
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

// validate checks only local choices; it never creates telemetry providers.
// Startup and offline configuration validation share this check.
func (c telemetryConfig) validate() error {
	var issues []error
	if c.Traces > traceOTel {
		issues = append(issues, errors.New("invalid trace backend"))
	}
	if c.Metrics > metricOTel {
		issues = append(issues, errors.New("invalid metric backend"))
	}
	if c.Logs > logsOTel {
		issues = append(issues, errors.New("invalid log output"))
	}
	if c.Traces == traceDatadog {
		issues = append(issues, proxydatadog.ValidateTracingEnvironment())
	}
	seen := make(map[string]bool)
	for _, signal := range []struct {
		name    string
		enabled bool
	}{
		{"TRACES", c.Traces == traceOTel}, {"METRICS", c.Metrics == metricOTel}, {"LOGS", c.Logs == logsOTel},
	} {
		if !signal.enabled {
			continue
		}
		if err := validateOTLPProtocol(signal.name); err != nil && !seen[err.Error()] {
			seen[err.Error()] = true
			issues = append(issues, err)
		}
	}
	return errors.Join(issues...)
}

func parseConfig(args []string) (config, error) {
	var path string
	var tracing, telemetry string
	var traceParameters, validateOnly bool
	var logLevel slog.Level
	flags := flag.NewFlagSet("pg-proxy", flag.ContinueOnError)
	flags.StringVar(&telemetry, "telemetry", "", "all-signal preset: datadog, otel, or none (default: OTEL_*_EXPORTER selection)")
	flags.StringVar(&tracing, "tracing", "none", "query tracing backend: none or otel (overrides OTEL_TRACES_EXPORTER)")
	flags.BoolVar(&traceParameters, "trace-parameters", false, "include raw bind parameter values in sampled query traces (default: disabled; SQL text is always recorded)")
	flags.TextVar(&logLevel, "log-level", slog.LevelInfo, "minimum proxy log level for stderr and OTLP: DEBUG, INFO, WARN, or ERROR")
	flags.StringVar(&path, "config", "", "JSON or YAML configuration file")
	flags.BoolVar(&validateOnly, "validate-config", false, "validate configuration and local TLS files, then exit without starting the proxy")
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
		return config{}, fmt.Errorf("could not open configuration file %q: %w", path, err)
	}
	defer file.Close()
	var shared pgconfig.Config
	switch filepath.Ext(path) {
	case ".json":
		shared, err = readConfigJSON(file)
	case ".yaml", ".yml":
		shared, err = readConfigYAML(file)
	default:
		return config{}, errors.New("configuration file must use .json, .yaml, or .yml")
	}
	fileErr := err
	var credentialErr credentialResolutionError
	if err != nil && !errors.As(err, &credentialErr) {
		return config{}, fmt.Errorf("%s: %w", path, err)
	}
	tracingSet := false
	flags.Visit(func(f *flag.Flag) {
		if f.Name == "tracing" {
			tracingSet = true
		}
	})
	resolved, telemetryErr := resolveTelemetry(telemetry, tracing, tracingSet, traceParameters)
	resolved.LogLevel = logLevel
	cfg, settingsErr := (config{validateOnly: validateOnly, telemetry: resolved, telemetryWarnings: telemetryPresetWarnings(telemetry)}).withSharedConfig(shared)
	if err := errors.Join(fileErr, telemetryErr, resolved.validate(), settingsErr); err != nil {
		return config{}, err
	}
	return cfg, nil
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
	var issues []error
	if tracingSet {
		if tracing == "otel" {
			cfg.Traces = traceOTel
		}
	} else {
		exporter, err := exporterFromEnv("OTEL_TRACES_EXPORTER")
		issues = append(issues, err)
		if exporter == "otlp" {
			cfg.Traces = traceOTel
		}
	}
	metrics, err := exporterFromEnv("OTEL_METRICS_EXPORTER")
	issues = append(issues, err)
	if metrics == "otlp" {
		cfg.Metrics = metricOTel
	}
	logs, err := exporterFromEnv("OTEL_LOGS_EXPORTER")
	issues = append(issues, err)
	if logs == "otlp" {
		cfg.Logs = logsOTel
	}
	return cfg, errors.Join(issues...)
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
	var issues []error
	cfg.sessionLimits = limiter.Params{GlobalLimit: shared.Proxy.MaxSessions, DefaultServiceMax: shared.Proxy.DefaultMaxUserSessions}
	for i, user := range shared.Users {
		if user.MaxSessions < 0 {
			issues = append(issues, fmt.Errorf("users[%d].max_sessions must not be negative", i))
		}
		if user.MaxSessions > 0 {
			cfg.sessionLimits.Quotas = append(cfg.sessionLimits.Quotas, limiter.ServiceQuota{Name: user.StaticCredentials.Username, Max: user.MaxSessions})
		}
	}
	cfg.sessionLimits = cfg.sessionLimitParams()
	cfg.shutdownGracePeriod = defaultShutdownGracePeriod
	if shared.Proxy.ShutdownGracePeriod != "" {
		grace, err := time.ParseDuration(shared.Proxy.ShutdownGracePeriod)
		if err != nil || grace <= 0 {
			issues = append(issues, errors.New("proxy.shutdown_grace_period must be a positive duration such as 30s or 2m"))
		} else {
			cfg.shutdownGracePeriod = grace
		}
	}
	var tlsErr error
	cfg.tlsConfig, tlsErr = clientTLSConfig(shared.Proxy)
	issues = append(issues, tlsErr)
	cfg.requireTLS = shared.Proxy.TLSRequired
	cfg.authenticate = nil
	if shared.Proxy.JWTAuthEnabled && shared.Proxy.PasswordAuthEnabled {
		issues = append(issues, errors.New("password_auth_enabled and jwt_auth_enabled are mutually exclusive"))
	}
	if shared.Proxy.JWTAuthEnabled {
		issues = append(issues, errors.New("JWT authentication is not implemented by the OSS binary"))
	}
	if shared.Proxy.PasswordAuthEnabled {
		var err error
		cfg.authenticate, err = passwordAuthentication(shared)
		issues = append(issues, err)
	} else if len(shared.Users) > 0 {
		issues = append(issues, errors.New("configured users require proxy.password_auth_enabled"))
	}
	// TODO: wire shared traffic mirroring settings into the OSS binary.
	if shared.Mirroring != nil {
		issues = append(issues, errors.New("mirroring configuration is not implemented by the OSS binary"))
	}
	replicas := make(map[string]bool)
	for _, cluster := range shared.Clusters {
		for _, replica := range cluster.Replicas {
			replicas[replica] = true
		}
	}
	cfg.databases = make(map[string]*pgxpool.Config, len(shared.Databases))
	for i, database := range shared.Databases {
		if database.Name == "" {
			issues = append(issues, fmt.Errorf("databases[%d].name: database name is required", i))
		}
		_, duplicate := cfg.databases[database.Name]
		if duplicate {
			issues = append(issues, fmt.Errorf("databases[%d].name: database names must be unique", i))
		}
		// Replicas default to the library's replica connection lifetime, so
		// connections spread across replicas after a rolling restart.
		if replicas[database.Name] && database.MaxConnLifetime == 0 {
			database.MaxConnLifetime = pool.ReplicaMaxConnLifetime
			if database.MaxConnLifetimeJitter == 0 {
				database.MaxConnLifetimeJitter = pool.ReplicaMaxConnLifetimeJitter
			}
		}
		settings, err := pool.PGXConfig(database)
		if err != nil {
			issues = append(issues, configurationContext(fmt.Sprintf("databases[%d]", i), err))
		}
		// Keep declared names even when their settings are invalid. Routing checks
		// must not misreport an invalid pool as an unknown database.
		if !duplicate {
			cfg.databases[database.Name] = settings
		}
	}
	cfg.clusters = maps.Clone(shared.Clusters)
	cfg.logicalDatabases = make(map[string]topology.LogicalDatabase, len(shared.LogicalDatabases))
	for name, logical := range shared.LogicalDatabases {
		cfg.logicalDatabases[name] = topology.LogicalDatabase{Cluster: logical.Cluster, ReadClusters: readClusters(logical.ReadClusters)}
	}
	cfg.listenAddress = shared.Proxy.ListenerAddress
	if cfg.listenAddress == "" {
		cfg.listenAddress = "127.0.0.1:5432"
	}
	cfg.messageBufferSize = int(shared.Proxy.PostgresMsgBufferSize)
	if cfg.messageBufferSize == 0 {
		cfg.messageBufferSize = topology.DefaultPostgresMessageBufferSize
	}
	cfg.replyBufferPool = shared.Proxy.ReplyBufferPool
	issues = append(issues, cfg.validateSettings())
	// A TLS load error already explains the missing certificate. Still check the
	// listener address, using the requested authentication policy to avoid cascades.
	if tlsErr != nil {
		issues = append(issues, cfg.validateListenerAddress(shared.Proxy.PasswordAuthEnabled))
	} else {
		issues = append(issues, cfg.validateListener(shared.Proxy.PasswordAuthEnabled))
	}
	if err := errors.Join(issues...); err != nil {
		return config{}, err
	}
	return cfg, nil
}

// readClusters converts read-cluster settings to the runtime view. An empty
// entry (`reports: {}` or `reports: null`) uses the default weight.
func readClusters(settings map[string]*pgconfig.ReadCluster) topology.ReadClusters {
	if len(settings) == 0 {
		return nil
	}
	clusters := make(topology.ReadClusters, len(settings))
	for name, read := range settings {
		var weight *uint8
		if read != nil {
			weight = read.Weight
		}
		clusters[name] = &topology.ReadCluster{Weight: topology.NormalizedWeight(weight)}
	}
	return clusters
}

// clientTLSConfig loads the certificate and key used to accept TLS client connections.
func clientTLSConfig(proxy pgconfig.Proxy) (*tls.Config, error) {
	if proxy.TLSCertFile == "" && proxy.TLSKeyFile == "" {
		if proxy.TLSRequired {
			return nil, errors.New("proxy.tls_required requires tls_cert_file and tls_key_file")
		}
		return nil, nil
	}
	if proxy.TLSCertFile == "" || proxy.TLSKeyFile == "" {
		return nil, errors.New("proxy.tls_cert_file and tls_key_file must be configured together")
	}
	certPEM, certErr := readTLSFile(proxy.TLSCertFile, "proxy.tls_cert_file")
	keyPEM, keyErr := readTLSFile(proxy.TLSKeyFile, "proxy.tls_key_file")
	if err := errors.Join(certErr, keyErr); err != nil {
		return nil, err
	}
	certificate, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, tlsKeyPairError(certPEM, keyPEM)
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{certificate},
	}, nil
}

func readTLSFile(path, setting string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("%s: %s", setting, fileErrorReason(err))
	}
	return data, nil
}

// Classify a failed pair without returning parser errors that can contain PEM
// labels or other file data. tls.X509KeyPair remains the authority on valid pairs.
func tlsKeyPairError(certPEM, keyPEM []byte) error {
	certificate, err := x509.ParseCertificate(tlsPEMBlock(certPEM, "CERTIFICATE"))
	if err != nil {
		return errors.New("proxy.tls_cert_file: invalid PEM certificate")
	}
	keyDER := tlsPEMBlock(keyPEM, "PRIVATE KEY")
	key, err := x509.ParsePKCS8PrivateKey(keyDER)
	if err != nil {
		key, err = x509.ParsePKCS1PrivateKey(keyDER)
	}
	if err != nil {
		key, err = x509.ParseECPrivateKey(keyDER)
	}
	signer, ok := key.(crypto.Signer)
	if err != nil || !ok {
		return errors.New("proxy.tls_key_file: invalid or unsupported PEM private key")
	}
	publicKey, err := x509.MarshalPKIXPublicKey(signer.Public())
	if err != nil {
		return errors.New("proxy.tls_key_file: invalid or unsupported PEM private key")
	}
	if !bytes.Equal(publicKey, certificate.RawSubjectPublicKeyInfo) {
		return errors.New("proxy.tls_key_file: private key does not match proxy.tls_cert_file")
	}
	return errors.New("proxy.tls_cert_file and proxy.tls_key_file: unsupported TLS certificate/key pair")
}

func tlsPEMBlock(data []byte, kind string) []byte {
	for {
		var block *pem.Block
		block, data = pem.Decode(data)
		if block == nil {
			return nil
		}
		if block.Type == kind || (kind == "PRIVATE KEY" && strings.HasSuffix(block.Type, " PRIVATE KEY")) {
			return block.Bytes
		}
	}
}

// Label every issue in a joined error, so multiline output retains its field
// context on each line while preserving the original errors for errors.Is/As.
func configurationContext(path string, err error) error {
	if err == nil {
		return nil
	}
	if group, ok := err.(interface{ Unwrap() []error }); ok {
		var issues []error
		for _, child := range group.Unwrap() {
			issues = append(issues, configurationContext(path, child))
		}
		return errors.Join(issues...)
	}
	return fmt.Errorf("%s: %w", path, err)
}
