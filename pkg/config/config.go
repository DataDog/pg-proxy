// Package config describes pg-proxy backends and routing independently of their
// configuration source. Config is the complete portable file schema.
// See README.md for credential handling and the field-change checklist.
package config

import "time"

// Config retains the application's sections and field names. Only provider-
// specific settings (Vault, COBS and Emissary) stay with the application.
type Config struct {
	Users            []User                     `json:"users,omitempty" yaml:"users,omitempty"`
	Databases        []Database                 `json:"databases,omitempty" yaml:"databases,omitempty"`
	Clusters         map[string]Cluster         `json:"clusters,omitempty" yaml:"clusters,omitempty"`
	LogicalDatabases map[string]LogicalDatabase `json:"logicalDatabases,omitempty" yaml:"logical_databases,omitempty"`
	Proxy            Proxy                      `json:"proxy,omitempty" yaml:"proxy,omitempty"`
	Mirroring        *Mirroring                 `json:"mirroring,omitempty" yaml:"mirroring,omitempty"`
}

// CredentialRef identifies a caller-owned secret and its version.
type CredentialRef struct {
	Name     string `json:"name,omitempty" yaml:"name,omitempty"`
	Revision string `json:"revision,omitempty" yaml:"revision,omitempty"`
}

// Connection describes a physical database.
type Connection struct {
	Host            string            `json:"host,omitempty" yaml:"host,omitempty"`
	Port            uint16            `json:"port,omitempty" yaml:"port,omitempty"`
	Database        string            `json:"dbName,omitempty" yaml:"dbname,omitempty"`
	SSLMode         string            `json:"sslMode,omitempty" yaml:"sslmode,omitempty"`
	ApplicationName string            `json:"applicationName,omitempty" yaml:"application_name,omitempty"`
	RuntimeParams   map[string]string `json:"connectionOptions,omitempty" yaml:"connection_options,omitempty"`
}

// PoolConfig contains flat database pool settings.
type PoolConfig struct {
	MaxConns              int32         `json:"maxConns,omitempty" yaml:"max_conns,omitempty"`
	MinConns              int32         `json:"minConns,omitempty" yaml:"min_conns,omitempty"`
	MinIdleConns          int32         `json:"minIdleConns,omitempty" yaml:"min_idle_conns,omitempty"`
	MaxConnLifetime       time.Duration `json:"maxConnLifetime,omitempty" yaml:"max_conn_lifetime,omitempty"`
	MaxConnLifetimeJitter time.Duration `json:"maxConnLifetimeJitter,omitempty" yaml:"max_conn_lifetime_jitter,omitempty"`
}

type LoadBalancedServer struct {
	Host string `json:"host" yaml:"host"`
}

// DatabaseSettings contains the credential-free settings shared by file schemas
// and application runtime configuration. Connection and pool fields remain flat.
type DatabaseSettings struct {
	Name                string `json:"name,omitempty" yaml:"name,omitempty"`
	Connection          `yaml:",inline"`
	PoolConfig          `yaml:",inline"`
	PostgresHost        string               `json:"postgresHost,omitempty" yaml:"postgres_host,omitempty"`
	RouteThroughEnvoy   bool                 `json:"routeThroughEnvoy,omitempty" yaml:"route_through_envoy,omitempty"`
	LoadBalancedServers []LoadBalancedServer `json:"loadBalancedServers,omitempty" yaml:"load_balanced_servers,omitempty"`
}

// Database adds standalone credentials to the shared database settings.
type Database struct {
	DatabaseSettings `yaml:",inline"`
	Username         string        `json:"username,omitempty" yaml:"username,omitempty"`
	Password         Secret        `json:"password,omitempty" yaml:"password,omitempty"`
	Credentials      CredentialRef `json:"credentials,omitempty" yaml:"credentials,omitempty"`
}

type Cluster struct {
	Primary  string   `json:"primary,omitempty" yaml:"primary,omitempty"`
	Replicas []string `json:"replicas,omitempty" yaml:"replicas,omitempty"`
}

// ReadCluster preserves an omitted weight separately from an explicit zero.
type ReadCluster struct {
	Weight *uint8 `json:"weight,omitempty" yaml:"weight,omitempty"`
}

type LogicalDatabase struct {
	Cluster      string                 `json:"cluster,omitempty" yaml:"cluster,omitempty"`
	ReadClusters map[string]ReadCluster `json:"readClusters,omitempty" yaml:"read_clusters,omitempty"`
}

// Proxy retains the existing listener options. Certificate files replace the
// application-owned Emissary certificate provider in a standalone deployment.
// The reference command consumes listener address and message buffer size.
// TODO: wire listener TLS; the command currently rejects requested TLS options.
type Proxy struct {
	ListenerAddress       string `json:"listenerAddress,omitempty" yaml:"listener_address,omitempty"`
	JWTAuthEnabled        bool   `json:"jwtAuthEnabled,omitempty" yaml:"jwt_auth_enabled,omitempty"`
	TLSRequired           bool   `json:"tlsRequired,omitempty" yaml:"tls_required,omitempty"`
	TLSCertFile           string `json:"tlsCertFile,omitempty" yaml:"tls_cert_file,omitempty"`
	TLSKeyFile            string `json:"tlsKeyFile,omitempty" yaml:"tls_key_file,omitempty"`
	PostgresMsgBufferSize int32  `json:"postgresMsgBufferSize,omitempty" yaml:"postgres_msg_buffer_size,omitempty"`
}

// User retains access and session-limit settings. Authentication is caller-owned.
type User struct {
	Email             string            `json:"email,omitempty" yaml:"email,omitempty"`
	Group             string            `json:"group,omitempty" yaml:"group,omitempty"`
	PGRoles           []string          `json:"pgRoles,omitempty" yaml:"pg_roles,omitempty"`
	Databases         []string          `json:"databases,omitempty" yaml:"databases,omitempty"`
	MaxSessions       int32             `json:"maxSessions,omitempty" yaml:"max_sessions,omitempty"`
	StaticCredentials StaticCredentials `json:"staticCredentials,omitempty" yaml:"static_credentials,omitempty"`
}

// StaticCredentials accepts a password, a PostgreSQL SCRAM-SHA-256 verifier,
// or a caller-resolved reference. These sources are mutually exclusive.
// SCRAMSHA256 uses SCRAM-SHA-256$iterations:base64-salt$base64-stored-key:base64-server-key.
type StaticCredentials struct {
	Username      string `json:"username,omitempty" yaml:"username,omitempty"`
	Password      Secret `json:"password,omitempty" yaml:"password,omitempty"`
	SCRAMSHA256   Secret `json:"scramSHA256,omitempty" yaml:"scram_sha_256,omitempty"`
	CredentialRef `yaml:",inline"`
}

// Mirroring preserves the existing traffic-mirroring settings and bounds.
type Mirroring struct {
	Enabled        bool      `json:"enabled" yaml:"enabled"`
	ShadowDatabase string    `json:"shadowDatabase" yaml:"shadow_database"`
	SourceDatabase string    `json:"sourceDatabase" yaml:"source_database"`
	DisableAt      time.Time `json:"disableAt" yaml:"disable_at"`
	QueueSize      int32     `json:"queueSize" yaml:"queue_size"`
}
