package main

import (
	"errors"
	"flag"
	"os"
	"path/filepath"

	"github.com/jackc/pgx/v5/pgxpool"

	pgconfig "github.com/DataDog/pg-proxy/pkg/config"
	"github.com/DataDog/pg-proxy/pkg/pool"
	"github.com/DataDog/pg-proxy/pkg/topology"
)

func parseConfig(args []string) (config, error) {
	var path string
	flags := flag.NewFlagSet("pg-proxy", flag.ContinueOnError)
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
	return (config{}).withSharedConfig(shared)
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
