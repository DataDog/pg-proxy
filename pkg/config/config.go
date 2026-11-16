// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

// Package config describes pg-proxy backends and routing independently of their
// configuration source. Config is the complete portable file schema.
// See README.md for credential handling and the field-change checklist.
package config

import "time"

// Config uses the standalone section types.
type Config ConfigSections[User, Database, Cluster, LogicalDatabase, Proxy, Mirroring]

// ConfigSections defines the shared file structure with application-selected section types.
// LogicalDatabase may be a value or pointer type; Mirroring is omitted when nil.
type ConfigSections[User, Database, Cluster, LogicalDatabase, Proxy, Mirroring any] struct {
	Users            []User                     `mapstructure:"users" json:"users" yaml:"users"`
	LogicalDatabases map[string]LogicalDatabase `mapstructure:"logical_databases" json:"logicalDatabases" yaml:"logical_databases"`
	Clusters         map[string]Cluster         `mapstructure:"clusters" json:"clusters" yaml:"clusters"`
	Databases        []Database                 `mapstructure:"databases" json:"databases" yaml:"databases"`
	Proxy            Proxy                      `mapstructure:"proxy" json:"proxy" yaml:"proxy"`
	Mirroring        *Mirroring                 `mapstructure:"mirroring" json:"mirroring,omitempty" yaml:"mirroring,omitempty"`
}

// DatabaseSettings contains shared database fields without credentials.
type DatabaseSettings struct {
	Host string `mapstructure:"host" json:"host" yaml:"host"`
	// PostgresHost is the endpoint used for database discovery.
	PostgresHost string `mapstructure:"postgres_host" json:"postgresHost" yaml:"postgres_host"`
	// RouteThroughEnvoy makes pg-proxy connect through Host instead of directly through PostgresHost.
	RouteThroughEnvoy bool `mapstructure:"route_through_envoy" json:"routeThroughEnvoy" yaml:"route_through_envoy"`
	// DBName is the actual database name in Postgres (e.g. orders)
	DBName  string `mapstructure:"dbname" json:"dbName" yaml:"dbname"`
	Port    int32  `mapstructure:"port" json:"port" yaml:"port"`
	SSLMode string `mapstructure:"sslmode" json:"sslMode" yaml:"sslmode"`
	// Name is the identifier that clusters refer to (e.g. orders_primary)
	Name     string `mapstructure:"name" json:"name" yaml:"name"`
	MaxConns int32  `mapstructure:"max_conns" json:"maxConns" yaml:"max_conns"`
	// LoadBalancedServers allows the proxy to load-balance traffic to individual nodes in
	// CSP replica pools
	LoadBalancedServers []LoadBalancedServer `mapstructure:"load_balanced_servers" json:"loadBalancedServers" yaml:"load_balanced_servers"`
	// ConnectionOptions is a map of connection options to be passed to the Postgres driver
	// as libpq options (-c key=value). Nil inherits PGOPTIONS; an empty map clears it.
	ConnectionOptions map[string]string `mapstructure:"connection_options" json:"connectionOptions" yaml:"connection_options"`
}

// PoolConfig contains additional standalone pool tuning options.
type PoolConfig struct {
	MinConns              int32         `json:"minConns,omitempty" yaml:"min_conns,omitempty"`
	MinIdleConns          int32         `json:"minIdleConns,omitempty" yaml:"min_idle_conns,omitempty"`
	MaxConnLifetime       time.Duration `json:"maxConnLifetime,omitempty" yaml:"max_conn_lifetime,omitempty"`
	MaxConnLifetimeJitter time.Duration `json:"maxConnLifetimeJitter,omitempty" yaml:"max_conn_lifetime_jitter,omitempty"`
}

type LoadBalancedServer struct {
	Host string `mapstructure:"host" json:"host" yaml:"host"`
}

// Database adds standalone credentials to the shared database settings.
type Database struct {
	DatabaseSettings `yaml:",inline" mapstructure:",squash"`
	PoolConfig       `yaml:",inline"`
	ApplicationName  string            `json:"applicationName,omitempty" yaml:"application_name,omitempty"`
	RuntimeParams    map[string]string `json:"runtimeParams,omitempty" yaml:"runtime_params,omitempty"`
	Username         string            `json:"username,omitempty" yaml:"username,omitempty"`
	Password         Secret            `json:"password,omitempty" yaml:"password,omitempty"`
}

type Cluster struct {
	Primary  string   `mapstructure:"primary" json:"primary" yaml:"primary"`
	Replicas []string `mapstructure:"replicas" json:"replicas" yaml:"replicas"`
}

// ReadCluster preserves an omitted weight separately from an explicit zero.
type ReadCluster struct {
	// Weight is a pointer to distinguish between an explicitly set weight of 0 and an unset zero value
	Weight *uint8 `mapstructure:"weight" json:"weight" yaml:"weight"`
}

type LogicalDatabase = LogicalDatabaseSettings[ReadCluster]

// LogicalDatabaseSettings allows applications to keep their own read-cluster behavior.
type LogicalDatabaseSettings[R any] struct {
	Cluster      string        `mapstructure:"cluster" json:"cluster" yaml:"cluster"`
	ReadClusters map[string]*R `mapstructure:"read_clusters" json:"readClusters" yaml:"read_clusters"`
}

// ProxySettings contains listener settings without a certificate provider.
type ProxySettings struct {
	ListenerAddress       string `mapstructure:"listener_address" json:"listenerAddress" yaml:"listener_address"`
	JWTAuthEnabled        bool   `mapstructure:"jwt_auth_enabled" json:"jwtAuthEnabled" yaml:"jwt_auth_enabled"`
	TLSRequired           bool   `mapstructure:"tls_required" json:"tlsRequired" yaml:"tls_required"`
	PostgresMsgBufferSize int32  `mapstructure:"postgres_msg_pg_size" json:"postgresMsgBufferSize" yaml:"postgres_msg_buffer_size"`
}

// Proxy contains listener settings and TLS certificate file paths.
// The pg-proxy executable loads the certificate and key at startup.
type Proxy struct {
	ProxySettings `yaml:",inline" mapstructure:",squash"`
	// PasswordAuthEnabled selects standalone SCRAM authentication independently of
	// JWTAuthEnabled. Applications using ProxySettings keep their own selection.
	PasswordAuthEnabled bool   `json:"passwordAuthEnabled,omitempty" yaml:"password_auth_enabled,omitempty"`
	TLSCertFile         string `json:"tlsCertFile,omitempty" yaml:"tls_cert_file,omitempty"`
	TLSKeyFile          string `json:"tlsKeyFile,omitempty" yaml:"tls_key_file,omitempty"`
	// AdminAddress enables the HTTP health listener (/livez, /readyz) when set.
	AdminAddress string `json:"adminAddress,omitempty" yaml:"admin_address,omitempty"`
}

// UserSettings retains access and session-limit settings. Authentication is caller-owned.
type UserSettings struct {
	Email       string   `mapstructure:"email" json:"email" yaml:"email"`
	Group       string   `mapstructure:"group" json:"group" yaml:"group"`
	PGRoles     []string `mapstructure:"pg_roles" json:"pgRoles" yaml:"pg_roles"`
	Databases   []string `mapstructure:"databases" json:"databases" yaml:"databases"`
	MaxSessions int32    `mapstructure:"max_sessions" json:"maxSessions" yaml:"max_sessions"`
}

// User adds standalone credentials to shared access settings.
type User struct {
	UserSettings      `yaml:",inline" mapstructure:",squash"`
	StaticCredentials StaticCredentials `json:"staticCredentials,omitempty" yaml:"static_credentials,omitempty"`
}

// StaticCredentials accepts either a password or a PostgreSQL SCRAM-SHA-256 verifier.
// SCRAMSHA256 uses SCRAM-SHA-256$iterations:base64-salt$base64-stored-key:base64-server-key.
type StaticCredentials struct {
	Username    string `json:"username,omitempty" yaml:"username,omitempty"`
	Password    Secret `json:"password,omitempty" yaml:"password,omitempty"`
	SCRAMSHA256 Secret `json:"scramSHA256,omitempty" yaml:"scram_sha_256,omitempty"`
}

// Mirroring preserves the existing traffic-mirroring settings and bounds.
type Mirroring struct {
	Enabled bool `mapstructure:"enabled" json:"enabled" yaml:"enabled"`
	// ShadowDatabase must exist in the databases list.
	ShadowDatabase string `mapstructure:"shadow_database" json:"shadowDatabase" yaml:"shadow_database"`
	SourceDatabase string `mapstructure:"source_database" json:"sourceDatabase" yaml:"source_database"`
	// DisableAt is the date and time at which mirroring will be automatically disabled.
	// Applications validate their own activation window.
	DisableAt time.Time `mapstructure:"disable_at" json:"disableAt" yaml:"disable_at"`
	// QueueSize is the max size in bytes of the per-session async queue.
	QueueSize int32 `mapstructure:"queue_size" json:"queueSize" yaml:"queue_size"`
}
