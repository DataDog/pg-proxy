package pool

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/DataDog/pg-proxy/pkg/config"
)

func directDatabase() config.Database {
	return config.Database{
		DatabaseSettings: config.DatabaseSettings{Connection: config.Connection{Host: "localhost", Port: 5433, Database: "orders", SSLMode: "disable"}},
		Username:         "service", Password: "test-password",
	}
}

func TestPGXConfigConsumesDatabase(t *testing.T) {
	database := directDatabase()
	database.Username = `service' user\name`
	database.Password = `secret' host=unexpected\password`
	database.Database = `orders' database\name`
	database.ApplicationName = "my-proxy"
	database.RuntimeParams = map[string]string{"statement_timeout": "1000", "host": "not-a-connection-host", "application_name": "overridden"}
	database.PoolConfig = config.PoolConfig{
		MaxConns: 12, MinConns: 2, MinIdleConns: 3,
		MaxConnLifetime: 2 * time.Minute, MaxConnLifetimeJitter: 10 * time.Second,
	}
	cfg, err := PGXConfig(database)
	require.NoError(t, err)
	require.Equal(t, database.Host, cfg.ConnConfig.Host)
	require.Equal(t, database.Port, cfg.ConnConfig.Port)
	require.Equal(t, database.Database, cfg.ConnConfig.Database)
	require.Equal(t, database.Username, cfg.ConnConfig.User)
	require.Equal(t, string(database.Password), cfg.ConnConfig.Password)
	require.Nil(t, cfg.ConnConfig.TLSConfig)
	require.Equal(t, "1000", cfg.ConnConfig.RuntimeParams["statement_timeout"])
	require.Equal(t, "my-proxy", cfg.ConnConfig.RuntimeParams["application_name"])
	require.Equal(t, int32(12), cfg.MaxConns)
	require.Equal(t, int32(2), cfg.MinConns)
	require.Equal(t, int32(3), cfg.MinIdleConns)
	require.Equal(t, 2*time.Minute, cfg.MaxConnLifetime)
	require.Equal(t, 10*time.Second, cfg.MaxConnLifetimeJitter)
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = "2000"
	require.Equal(t, "1000", database.RuntimeParams["statement_timeout"])
}

func TestPGXConfigDefaultsAndTLS(t *testing.T) {
	database := directDatabase()
	database.SSLMode = "require"
	cfg, err := PGXConfig(database)
	require.NoError(t, err)
	require.NotNil(t, cfg.ConnConfig.TLSConfig)
	require.Positive(t, cfg.MaxConns)
	require.Positive(t, cfg.MaxConnLifetime)
	require.Zero(t, cfg.MinConns)
	require.Zero(t, cfg.MinIdleConns)
	require.Zero(t, cfg.MaxConnLifetimeJitter)
}

func TestPGXConfigRejectsUnsupportedAndInvalidSettings(t *testing.T) {
	for name, modify := range map[string]func(*config.Database){
		"reference":          func(d *config.Database) { d.Username = ""; d.Password = ""; d.Credentials.Name = "secret-store-entry" },
		"mixed credentials":  func(d *config.Database) { d.Credentials.Name = "secret-store-entry" },
		"host":               func(d *config.Database) { d.Host = "" },
		"database":           func(d *config.Database) { d.Database = "" },
		"username":           func(d *config.Database) { d.Username = "" },
		"postgres host":      func(d *config.Database) { d.PostgresHost = "discovery" },
		"envoy":              func(d *config.Database) { d.RouteThroughEnvoy = true },
		"load balancing":     func(d *config.Database) { d.LoadBalancedServers = []config.LoadBalancedServer{{Host: "other"}} },
		"negative max":       func(d *config.Database) { d.MaxConns = -1 },
		"negative min":       func(d *config.Database) { d.MinConns = -1 },
		"negative min idle":  func(d *config.Database) { d.MinIdleConns = -1 },
		"negative lifetime":  func(d *config.Database) { d.MaxConnLifetime = -time.Second },
		"negative jitter":    func(d *config.Database) { d.MaxConnLifetimeJitter = -time.Second },
		"min above max":      func(d *config.Database) { d.MaxConns = 1; d.MinConns = 2 },
		"min idle above max": func(d *config.Database) { d.MaxConns = 1; d.MinIdleConns = 2 },
		"invalid TLS":        func(d *config.Database) { d.SSLMode = "test-password" },
	} {
		t.Run(name, func(t *testing.T) {
			database := directDatabase()
			modify(&database)
			_, err := PGXConfig(database)
			require.Error(t, err)
			require.NotContains(t, err.Error(), "test-password")
		})
	}
}
