// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package pool

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/DataDog/pg-proxy/pkg/config"
)

// PGXConfig builds a direct PostgreSQL pool configuration without opening connections.
// Credentials must already be resolved. Zero pool settings retain pgx defaults;
// negative settings are rejected. Connection parsing retains pgx environment defaults.
func PGXConfig(database config.Database) (*pgxpool.Config, error) {
	if err := (config.Config{Databases: []config.Database{database}}).ValidateCredentials(); err != nil {
		return nil, err
	}
	if database.PostgresHost != "" || database.RouteThroughEnvoy || len(database.LoadBalancedServers) > 0 {
		return nil, errors.New("direct pools do not support postgres_host, route_through_envoy, or load_balanced_servers")
	}
	for _, field := range []struct{ name, value string }{
		{"host", database.Host}, {"dbname", database.DBName}, {"username", database.Username},
	} {
		if field.value == "" {
			return nil, fmt.Errorf("database %s is required", field.name)
		}
	}
	switch database.SSLMode {
	case "", "disable", "allow", "prefer", "require", "verify-ca", "verify-full":
	default:
		return nil, errors.New("database sslmode must be disable, allow, prefer, require, verify-ca, or verify-full")
	}
	if database.MaxConns < 0 || database.MinConns < 0 || database.MinIdleConns < 0 || database.MaxConnLifetime < 0 || database.MaxConnLifetimeJitter < 0 {
		return nil, errors.New("database pool settings must not be negative")
	}
	if database.Port < 0 || database.Port > 65535 {
		return nil, errors.New("database port is out of range")
	}
	// Quote each value as a libpq keyword value. Never interpolate unescaped values
	// or runtime parameters into the connection string.
	quote := func(value string) string {
		return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(value) + "'"
	}
	connection := "host=" + quote(database.Host) + " dbname=" + quote(database.DBName) +
		" user=" + quote(database.Username) + " password=" + quote(string(database.Password))
	if database.Port != 0 {
		connection += fmt.Sprintf(" port=%d", database.Port)
	}
	if database.SSLMode != "" {
		connection += " sslmode=" + quote(database.SSLMode)
	}
	cfg, err := pgxpool.ParseConfig(connection)
	if err != nil {
		// Parser errors may contain credentials or other input values.
		return nil, errors.New("invalid database connection configuration: check PG* environment settings and backend TLS files")
	}
	for key, value := range connectionRuntimeParams(database.ConnectionOptions) {
		cfg.ConnConfig.RuntimeParams[key] = value
	}
	for key, value := range database.RuntimeParams {
		cfg.ConnConfig.RuntimeParams[key] = value
	}
	if database.ApplicationName != "" {
		cfg.ConnConfig.RuntimeParams["application_name"] = database.ApplicationName
	}
	if database.MaxConns != 0 {
		cfg.MaxConns = database.MaxConns
	}
	cfg.MinConns = database.MinConns
	cfg.MinIdleConns = database.MinIdleConns
	if cfg.MinConns > cfg.MaxConns || cfg.MinIdleConns > cfg.MaxConns {
		return nil, errors.New("minimum database connections must not exceed max_conns")
	}
	if database.MaxConnLifetime != 0 {
		cfg.MaxConnLifetime = database.MaxConnLifetime
	}
	cfg.MaxConnLifetimeJitter = database.MaxConnLifetimeJitter
	return cfg, nil
}

// Encode libpq options with the same escaping as pgx's keyword parser.
// Nil inherits PGOPTIONS; an empty map clears it.
func connectionRuntimeParams(options map[string]string) map[string]string {
	if options == nil {
		return nil
	}
	escape := strings.NewReplacer(` `, `\ `, `'`, `\'`, `\`, `\\`)
	parts := make([]string, 0, len(options))
	for _, key := range slices.Sorted(maps.Keys(options)) {
		parts = append(parts, fmt.Sprintf("-c %s=%s", key, escape.Replace(options[key])))
	}
	value := strings.Join(parts, " ")
	value = strings.ReplaceAll(strings.ReplaceAll(value, `\\`, `\`), `\'`, `'`)
	return map[string]string{"options": value}
}
