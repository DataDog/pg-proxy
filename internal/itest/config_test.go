//go:build integration

package itest

import (
	"bytes"
	"net/url"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	pgconfig "github.com/DataDog/pg-proxy/pkg/config"
)

// The generated file must round-trip every secret the proxy reads, even though
// Secret redacts itself when marshaled.
func TestEncodeConfigPreservesSecrets(t *testing.T) {
	verifier := "SCRAM-SHA-256$4096:AAECAwQFBgcICQoLDA0ODw==$AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=:ICEiIyQlJicoKSorLC0uLzAxMjM0NTY3ODk6Ozw9Pj8="
	var database pgconfig.Database
	database.Name = "primary"
	database.Username = "postgres"
	database.Password = "database: 'password'"
	var withPassword, withVerifier, withoutCredentials pgconfig.User
	withPassword.StaticCredentials = pgconfig.StaticCredentials{Username: "alice", Password: "[alice] #password"}
	withVerifier.StaticCredentials = pgconfig.StaticCredentials{Username: "bob", SCRAMSHA256: pgconfig.Secret(verifier)}
	cfg := pgconfig.Config{
		Databases: []pgconfig.Database{database},
		Users:     []pgconfig.User{withPassword, withoutCredentials, withVerifier},
	}

	data, err := encodeConfig(cfg)
	require.NoError(t, err)
	require.NotContains(t, string(data), "[REDACTED]")
	decoded, err := pgconfig.ReadYAML(bytes.NewReader(data))
	require.NoError(t, err)
	require.Equal(t, cfg.Databases[0].Password, decoded.Databases[0].Password)
	require.Len(t, decoded.Users, 3)
	require.Equal(t, cfg.Users[0].StaticCredentials, decoded.Users[0].StaticCredentials)
	require.Equal(t, pgconfig.StaticCredentials{}, decoded.Users[1].StaticCredentials)
	require.Equal(t, cfg.Users[2].StaticCredentials, decoded.Users[2].StaticCredentials)
}

func TestPostgresURLSupportsIPv6Hosts(t *testing.T) {
	for _, host := range []string{"127.0.0.1", "localhost", "::1"} {
		t.Run(host, func(t *testing.T) {
			pg := &Postgres{Host: host, Port: 15432}
			u, err := url.Parse(pg.URL("postgres"))
			require.NoError(t, err)
			require.Equal(t, host, u.Hostname())
			require.Equal(t, "15432", u.Port())
			parsed, err := pgx.ParseConfig(pg.URL("postgres"))
			require.NoError(t, err)
			require.Equal(t, host, parsed.Host)
			require.Equal(t, uint16(15432), parsed.Port)
		})
	}
}
