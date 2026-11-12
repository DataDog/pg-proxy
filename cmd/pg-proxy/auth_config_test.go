// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	pgconfig "github.com/DataDog/pg-proxy/pkg/config"
	"github.com/DataDog/pg-proxy/pkg/limiter"
	"github.com/DataDog/pg-proxy/pkg/scram"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestPublishedPasswordAuthenticationExamples(t *testing.T) {
	verifier, err := scram.NewVerifier("example-password")
	require.NoError(t, err)
	for _, extension := range []string{"yaml", "json"} {
		t.Run(extension, func(t *testing.T) {
			document, err := os.ReadFile("../../examples/password-auth." + extension)
			require.NoError(t, err)
			input := strings.NewReader(strings.ReplaceAll(string(document), "<Bob SCRAM verifier>", verifier.Encode()))
			var shared pgconfig.Config
			if extension == "yaml" {
				shared, err = pgconfig.ReadYAML(input)
			} else {
				shared, err = pgconfig.ReadJSON(input)
			}
			require.NoError(t, err)
			shared.Proxy.TLSCertFile, shared.Proxy.TLSKeyFile = clientTLSFiles(t)
			cfg, err := (config{}).withSharedConfig(shared)
			require.NoError(t, err)
			require.NotNil(t, cfg.authenticate)
			require.Len(t, cfg.databases, 2)
			require.Len(t, shared.Users, 2)
		})
	}
}

func passwordConfig(t *testing.T) pgconfig.Config {
	t.Helper()
	shared := ossConfig()
	shared.Proxy.PasswordAuthEnabled, shared.Proxy.TLSRequired = true, true
	shared.Proxy.TLSCertFile, shared.Proxy.TLSKeyFile = clientTLSFiles(t)
	shared.Users = []pgconfig.User{{UserSettings: pgconfig.UserSettings{Databases: []string{"client-db"}}, StaticCredentials: pgconfig.StaticCredentials{Username: "alice", Password: "client-password-sentinel"}}}
	return shared
}

func TestPasswordAuthenticationConfiguration(t *testing.T) {
	for _, format := range []string{"yaml", "json"} {
		shared := passwordConfig(t)
		var document string
		if format == "yaml" {
			document = fmt.Sprintf(`databases:
- {name: primary, host: localhost, dbname: postgres, username: backend}
clusters: {orders: {primary: primary}}
logical_databases: {client-db: {cluster: orders}}
proxy:
  listener_address: '0.0.0.0:15432'
  password_auth_enabled: true
  tls_required: true
  tls_cert_file: %q
  tls_key_file: %q
users:
- static_credentials: {username: alice, password: client-password-sentinel}
  databases: [client-db]
`, shared.Proxy.TLSCertFile, shared.Proxy.TLSKeyFile)
		} else {
			document = fmt.Sprintf(`{"databases":[{"name":"primary","host":"localhost","dbName":"postgres","username":"backend"}],"clusters":{"orders":{"primary":"primary"}},"logicalDatabases":{"client-db":{"cluster":"orders"}},"proxy":{"listenerAddress":"0.0.0.0:15432","passwordAuthEnabled":true,"tlsRequired":true,"tlsCertFile":%q,"tlsKeyFile":%q},"users":[{"staticCredentials":{"username":"alice","password":"client-password-sentinel"},"databases":["client-db"]}]}`, shared.Proxy.TLSCertFile, shared.Proxy.TLSKeyFile)
		}
		cfg, err := parseConfig([]string{"-config", writeConfig(t, format, document), "-telemetry", "none"})
		require.NoError(t, err)
		require.NotNil(t, cfg.authenticate)
		require.Equal(t, "backend", cfg.databases["primary"].ConnConfig.User)
	}
	for name, change := range map[string]func(*pgconfig.Config){
		"both modes":          func(c *pgconfig.Config) { c.Proxy.JWTAuthEnabled = true },
		"disabled with users": func(c *pgconfig.Config) { c.Proxy.PasswordAuthEnabled = false },
		"no users":            func(c *pgconfig.Config) { c.Users = nil },
		"no TLS":              func(c *pgconfig.Config) { c.Proxy.TLSRequired = false },
		"no certificate":      func(c *pgconfig.Config) { c.Proxy.TLSCertFile, c.Proxy.TLSKeyFile = "", "" },
		"duplicate username":  func(c *pgconfig.Config) { c.Users = append(c.Users, c.Users[0]) },
		"no username":         func(c *pgconfig.Config) { c.Users[0].StaticCredentials.Username = "" },
		"no credential":       func(c *pgconfig.Config) { c.Users[0].StaticCredentials.Password = "" },
		"no databases":        func(c *pgconfig.Config) { c.Users[0].Databases = nil },
		"unknown database":    func(c *pgconfig.Config) { c.Users[0].Databases = []string{"primary"} },
		"wildcard database":   func(c *pgconfig.Config) { c.Users[0].Databases = []string{"*"} },
		"roles":               func(c *pgconfig.Config) { c.Users[0].PGRoles = []string{"reader"} },
		"email":               func(c *pgconfig.Config) { c.Users[0].Email = "private-email" },
		"group":               func(c *pgconfig.Config) { c.Users[0].Group = "private-group" },
	} {
		t.Run(name, func(t *testing.T) {
			shared := passwordConfig(t)
			change(&shared)
			_, err := (config{}).withSharedConfig(shared)
			require.Error(t, err)
			require.NotContains(t, err.Error(), "client-password-sentinel")
		})
	}
}

func TestUserSessionLimits(t *testing.T) {
	for _, format := range []string{"yaml", "json"} {
		t.Run(format, func(t *testing.T) {
			shared := passwordConfig(t)
			shared.Proxy.MaxSessions = 4
			shared.Proxy.DefaultMaxUserSessions = 2
			shared.Users[0].MaxSessions = 1
			var data []byte
			var err error
			if format == "json" {
				data, err = json.Marshal(shared)
			} else {
				data, err = yaml.Marshal(shared)
			}
			require.NoError(t, err)
			cfg, err := parseConfig([]string{"-config", writeConfig(t, format, string(data)), "-telemetry", "none"})
			require.NoError(t, err)
			admission := limiter.New(cfg.sessionLimitParams())
			require.NoError(t, admission.RegisterSession("alice"))
			require.Error(t, admission.RegisterSession("alice"))
			require.NoError(t, admission.RegisterSession("bob"))
			require.NoError(t, admission.RegisterSession("bob"))
			require.Error(t, admission.RegisterSession("bob"))
			require.NoError(t, admission.RegisterSession("carol"))
			require.Error(t, admission.RegisterSession("carol"))
			require.NoError(t, admission.DeregisterSession("alice"))
			require.NoError(t, admission.RegisterSession("carol"))
			require.Equal(t, int32(4), admission.GetGlobalCount())
		})
	}
	shared := passwordConfig(t)
	shared.Users[0].MaxSessions = -1
	_, err := (config{}).withSharedConfig(shared)
	require.ErrorContains(t, err, "users[].max_sessions")
	shared.Users[0].MaxSessions = 0
	cfg, err := (config{}).withSharedConfig(shared)
	require.NoError(t, err)
	admission := limiter.New(cfg.sessionLimitParams())
	for i := 0; i < 10_000; i++ {
		require.NoError(t, admission.RegisterSession("alice"))
	}
	require.Error(t, admission.RegisterSession("alice"))
}
