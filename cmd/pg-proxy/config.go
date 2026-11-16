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
	flags.BoolVar(&traceParameters, "trace-parameters", false, "include raw bind parameter values in sampled query traces (default: disabled; SQL text is always recorded)")
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
		shared, err = readConfigJSON(file)
	case ".yaml", ".yml":
		shared, err = readConfigYAML(file)
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
	var err error
	cfg.tlsConfig, err = clientTLSConfig(shared.Proxy)
	if err != nil {
		return config{}, err
	}
	cfg.requireTLS = shared.Proxy.TLSRequired
	cfg.authenticate = nil
	if shared.Proxy.JWTAuthEnabled && shared.Proxy.PasswordAuthEnabled {
		return config{}, errors.New("password_auth_enabled and jwt_auth_enabled are mutually exclusive")
	}
	if shared.Proxy.JWTAuthEnabled {
		return config{}, errors.New("JWT authentication is not implemented by the OSS binary")
	}
	if shared.Proxy.PasswordAuthEnabled {
		cfg.authenticate, err = passwordAuthentication(shared)
		if err != nil {
			return config{}, err
		}
	} else if len(shared.Users) > 0 {
		return config{}, errors.New("configured users require proxy.password_auth_enabled")
	}
	// TODO: wire shared traffic mirroring settings into the OSS binary.
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
	cfg.adminAddress = shared.Proxy.AdminAddress
	cfg.messageBufferSize = int(shared.Proxy.PostgresMsgBufferSize)
	if cfg.messageBufferSize == 0 {
		cfg.messageBufferSize = topology.DefaultPostgresMessageBufferSize
	}
	return cfg, cfg.validate()
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
	certPEM, err := readTLSFile(proxy.TLSCertFile, "proxy.tls_cert_file")
	if err != nil {
		return nil, err
	}
	keyPEM, err := readTLSFile(proxy.TLSKeyFile, "proxy.tls_key_file")
	if err != nil {
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
