package main

import (
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
			DatabaseSettings: pgconfig.DatabaseSettings{Name: "primary", Connection: pgconfig.Connection{Host: "localhost", Database: "postgres", SSLMode: "disable"}},
		}},
		Clusters:         map[string]pgconfig.Cluster{"orders": {Primary: "primary"}},
		LogicalDatabases: map[string]pgconfig.LogicalDatabase{"client-db": {Cluster: "orders"}},
		Proxy:            pgconfig.Proxy{ListenerAddress: "127.0.0.1:0", PostgresMsgBufferSize: 4096},
	}
}

func writeConfig(t *testing.T, extension, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "proxy."+extension)
	require.NoError(t, os.WriteFile(path, []byte(contents), 0600))
	return path
}

func TestReadSharedConfigFiles(t *testing.T) {
	t.Setenv("PG_PROXY_BACKEND_URL", "invalid-and-must-be-ignored")
	for extension, contents := range map[string]string{
		"json": `{"databases":[{"name":"primary","host":"localhost","dbName":"postgres","username":"postgres","password":"test-password","maxConns":2}],"clusters":{"orders":{"primary":"primary"}},"logicalDatabases":{"client-db":{"cluster":"orders"}},"proxy":{"listenerAddress":"127.0.0.1:0","postgresMsgBufferSize":4096}}`,
		"yaml": `databases:
- name: primary
  host: localhost
  dbname: postgres
  username: postgres
  password: test-password
  max_conns: 2
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
			require.Equal(t, "orders", cfg.logicalDatabases["client-db"].Cluster)
			require.Equal(t, "primary", cfg.clusters["orders"])
			require.Equal(t, "127.0.0.1:0", cfg.listenAddress)
			require.Equal(t, 4096, cfg.messageBufferSize)
			require.Equal(t, "test-password", cfg.databases["primary"].ConnConfig.Password)
			require.Equal(t, int32(2), cfg.databases["primary"].MaxConns)
			for _, flag := range []string{"-backend-url", "-listen", "-database", "-max-sessions"} {
				_, err := parseConfig([]string{"-config", path, flag, "ignored"})
				require.ErrorContains(t, err, "flag provided but not defined")
			}
		})
	}
}

func TestSharedConfigRejectsUnsupportedFeatures(t *testing.T) {
	for name, modify := range map[string]func(*pgconfig.Config){
		"TLS required":             func(c *pgconfig.Config) { c.Proxy.TLSRequired = true },
		"TLS cert":                 func(c *pgconfig.Config) { c.Proxy.TLSCertFile = "certificate.pem" },
		"TLS key":                  func(c *pgconfig.Config) { c.Proxy.TLSKeyFile = "key.pem" },
		"JWT":                      func(c *pgconfig.Config) { c.Proxy.JWTAuthEnabled = true },
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
			c.LogicalDatabases["client-db"] = pgconfig.LogicalDatabase{Cluster: "orders", ReadClusters: map[string]pgconfig.ReadCluster{"orders": {}}}
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
