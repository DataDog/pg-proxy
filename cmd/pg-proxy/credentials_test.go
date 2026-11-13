// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	pgconfig "github.com/DataDog/pg-proxy/pkg/config"
	"github.com/DataDog/pg-proxy/pkg/scram"
)

func writeSecret(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "secret")
	require.NoError(t, os.WriteFile(path, []byte(contents), 0600))
	return path
}

func readTestConfig(format, document string) (pgconfig.Config, error) {
	if format == "yaml" {
		return readConfigYAML(strings.NewReader(document))
	}
	return readConfigJSON(strings.NewReader(document))
}

func TestCredentialReferences(t *testing.T) {
	verifier, err := scram.NewVerifier("bob-password")
	require.NoError(t, err)
	backendFile := writeSecret(t, "backend-file-secret\n")
	aliceFile := writeSecret(t, "alice-file-secret\r\n")
	bobFile := writeSecret(t, verifier.Encode()+"\n")
	t.Setenv("PG_PROXY_TEST_BACKEND", "backend-env-secret\n")
	t.Setenv("PG_PROXY_TEST_CAROL", "carol-env-secret")
	t.Setenv("PG_PROXY_TEST_DAVE", verifier.Encode())
	documents := map[string]string{
		"yaml": fmt.Sprintf(`databases:
- {name: file, username: backend, password_file: %q}
- {name: env, username: backend, password_env: PG_PROXY_TEST_BACKEND}
- {name: inline, username: backend, password: backend-inline-secret}
users:
- static_credentials: {username: alice, password_file: %q}
- static_credentials: {username: bob, scram_sha_256_file: %q}
- static_credentials: {username: carol, password_env: PG_PROXY_TEST_CAROL}
- static_credentials: {username: dave, scram_sha_256_env: PG_PROXY_TEST_DAVE}
  databases: [orders]
`, backendFile, aliceFile, bobFile),
		"json": fmt.Sprintf(`{"databases":[
{"name":"file","username":"backend","passwordFile":%q},
{"name":"env","username":"backend","passwordEnv":"PG_PROXY_TEST_BACKEND"},
{"name":"inline","username":"backend","password":"backend-inline-secret"}],
"users":[
{"staticCredentials":{"username":"alice","passwordFile":%q}},
{"staticCredentials":{"username":"bob","scramSHA256File":%q}},
{"staticCredentials":{"username":"carol","passwordEnv":"PG_PROXY_TEST_CAROL"}},
{"staticCredentials":{"username":"dave","scramSHA256Env":"PG_PROXY_TEST_DAVE"},"databases":["orders"]}]}`, backendFile, aliceFile, bobFile),
	}
	for format, document := range documents {
		t.Run(format, func(t *testing.T) {
			cfg, err := readTestConfig(format, document)
			require.NoError(t, err)
			require.Len(t, cfg.Databases, 3)
			require.Equal(t, "backend-file-secret", string(cfg.Databases[0].Password))
			// Environment values are used as-is.
			require.Equal(t, "backend-env-secret\n", string(cfg.Databases[1].Password))
			require.Equal(t, "backend-inline-secret", string(cfg.Databases[2].Password))
			require.Equal(t, "backend", cfg.Databases[0].Username)
			require.Equal(t, "file", cfg.Databases[0].Name)
			require.Len(t, cfg.Users, 4)
			require.Equal(t, "alice-file-secret", string(cfg.Users[0].StaticCredentials.Password))
			require.Equal(t, verifier.Encode(), string(cfg.Users[1].StaticCredentials.SCRAMSHA256))
			require.Equal(t, "carol-env-secret", string(cfg.Users[2].StaticCredentials.Password))
			require.Equal(t, verifier.Encode(), string(cfg.Users[3].StaticCredentials.SCRAMSHA256))
			require.Equal(t, []string{"orders"}, cfg.Users[3].Databases)
		})
	}
}

// Without references, the OSS reader must decode exactly what the library does.
func TestReadersMatchLibraryWithoutReferences(t *testing.T) {
	verifier, err := scram.NewVerifier("bob-password")
	require.NoError(t, err)
	documents := map[string]string{
		"yaml": fmt.Sprintf(`databases:
- name: primary
  host: localhost
  postgres_host: discovery
  port: 5432
  dbname: postgres
  sslmode: disable
  max_conns: 4
  min_conns: 1
  max_conn_lifetime: 1h
  connection_options: {statement_timeout: 5s}
  load_balanced_servers: [{host: node}]
  application_name: app
  runtime_params: {search_path: public}
  username: postgres
  password: backend-password
clusters: {orders: {primary: primary, replicas: [primary]}}
logical_databases: {client-db: {cluster: orders, read_clusters: {orders: {weight: 0}}}}
proxy: {listener_address: '127.0.0.1:0', tls_required: true, postgres_msg_buffer_size: 4096, password_auth_enabled: true, tls_cert_file: cert, tls_key_file: key}
users:
- {email: e, group: g, pg_roles: [r], databases: [client-db], max_sessions: 2, static_credentials: {username: alice, password: alice-password}}
- static_credentials: {username: bob, scram_sha_256: %q}
mirroring: {enabled: true, shadow_database: primary, source_database: primary, disable_at: 2030-01-01T00:00:00Z, queue_size: 10}
`, verifier.Encode()),
		"json": fmt.Sprintf(`{"databases":[{"name":"primary","host":"localhost","postgresHost":"discovery","port":5432,"dbName":"postgres","sslMode":"disable","maxConns":4,"minConns":1,"maxConnLifetime":3600000000000,"connectionOptions":{"statement_timeout":"5s"},"loadBalancedServers":[{"host":"node"}],"applicationName":"app","runtimeParams":{"search_path":"public"},"username":"postgres","password":"backend-password"}],
"clusters":{"orders":{"primary":"primary","replicas":["primary"]}},
"logicalDatabases":{"client-db":{"cluster":"orders","readClusters":{"orders":{"weight":0}}}},
"proxy":{"listenerAddress":"127.0.0.1:0","tlsRequired":true,"postgresMsgBufferSize":4096,"passwordAuthEnabled":true,"tlsCertFile":"cert","tlsKeyFile":"key"},
"users":[{"email":"e","group":"g","pgRoles":["r"],"databases":["client-db"],"maxSessions":2,"staticCredentials":{"username":"alice","password":"alice-password"}},{"staticCredentials":{"username":"bob","scramSHA256":%q}}],
"mirroring":{"enabled":true,"shadowDatabase":"primary","sourceDatabase":"primary","disableAt":"2030-01-01T00:00:00Z","queueSize":10}}`, verifier.Encode()),
	}
	for format, document := range documents {
		t.Run(format, func(t *testing.T) {
			var expected pgconfig.Config
			if format == "yaml" {
				expected, err = pgconfig.ReadYAML(strings.NewReader(document))
			} else {
				expected, err = pgconfig.ReadJSON(strings.NewReader(document))
			}
			require.NoError(t, err)
			actual, err := readTestConfig(format, document)
			require.NoError(t, err)
			require.Equal(t, expected, actual)
			require.Equal(t, "backend-password", string(actual.Databases[0].Password))
			require.Equal(t, int32(1), actual.Databases[0].MinConns)
		})
	}
	for format, document := range map[string]string{"yaml": "{}", "json": "{}"} {
		actual, err := readTestConfig(format, document)
		require.NoError(t, err)
		require.Equal(t, pgconfig.Config{}, actual)
	}
}

func TestCredentialReferenceErrors(t *testing.T) {
	t.Setenv("PG_PROXY_TEST_EMPTY", "")
	t.Setenv("PG_PROXY_TEST_SET", "env-secret-sentinel")
	present := writeSecret(t, "file-secret-sentinel")
	missing := filepath.Join(t.TempDir(), "missing-path-sentinel")
	empty := writeSecret(t, "\n")
	invalidVerifier := writeSecret(t, "verifier-sentinel\n")
	for name, tc := range map[string]struct{ yaml, json, err string }{
		"inline and file": {
			fmt.Sprintf("databases: [{name: db, username: u, password: inline-secret-sentinel, password_file: %q}]", present),
			fmt.Sprintf(`{"databases":[{"name":"db","username":"u","password":"inline-secret-sentinel","passwordFile":%q}]}`, present),
			"database 0: password, password_file, and password_env are mutually exclusive",
		},
		"file and env": {
			fmt.Sprintf("databases: [{name: db, username: u, password_file: %q, password_env: PG_PROXY_TEST_SET}]", present),
			fmt.Sprintf(`{"databases":[{"name":"db","username":"u","passwordFile":%q,"passwordEnv":"PG_PROXY_TEST_SET"}]}`, present),
			"database 0: password, password_file, and password_env are mutually exclusive",
		},
		"missing file": {
			fmt.Sprintf("databases: [{name: db, username: u, password_file: %q}]", missing),
			fmt.Sprintf(`{"databases":[{"name":"db","username":"u","passwordFile":%q}]}`, missing),
			"database 0: password_file: file not found",
		},
		"empty file": {
			fmt.Sprintf("databases: [{name: db, username: u, password_file: %q}]", empty),
			fmt.Sprintf(`{"databases":[{"name":"db","username":"u","passwordFile":%q}]}`, empty),
			"database 0: password_file: file is empty",
		},
		"unset env": {
			"databases: [{name: db, username: u, password_env: PG_PROXY_TEST_UNSET_SENTINEL}]",
			`{"databases":[{"name":"db","username":"u","passwordEnv":"PG_PROXY_TEST_UNSET_SENTINEL"}]}`,
			"database 0: password_env: environment variable is not set or empty",
		},
		"empty env": {
			"databases: [{name: db, username: u, password_env: PG_PROXY_TEST_EMPTY}]",
			`{"databases":[{"name":"db","username":"u","passwordEnv":"PG_PROXY_TEST_EMPTY"}]}`,
			"database 0: password_env: environment variable is not set or empty",
		},
		"referenced password requires username": {
			"databases: [{name: db, password_env: PG_PROXY_TEST_SET}]",
			`{"databases":[{"name":"db","passwordEnv":"PG_PROXY_TEST_SET"}]}`,
			"database 0: password requires username",
		},
		"user password sources": {
			fmt.Sprintf("users: [{static_credentials: {username: alice, password_file: %q, password_env: PG_PROXY_TEST_SET}}]", present),
			fmt.Sprintf(`{"users":[{"staticCredentials":{"username":"alice","passwordFile":%q,"passwordEnv":"PG_PROXY_TEST_SET"}}]}`, present),
			"user 0: password, password_file, and password_env are mutually exclusive",
		},
		"user verifier sources": {
			fmt.Sprintf("users: [{static_credentials: {username: alice, scram_sha_256: inline-secret-sentinel, scram_sha_256_file: %q}}]", present),
			fmt.Sprintf(`{"users":[{"staticCredentials":{"username":"alice","scramSHA256":"inline-secret-sentinel","scramSHA256File":%q}}]}`, present),
			"user 0: scram_sha_256, scram_sha_256_file, and scram_sha_256_env are mutually exclusive",
		},
		"user password and verifier": {
			"users: [{static_credentials: {username: alice, password_env: PG_PROXY_TEST_SET, scram_sha_256_env: PG_PROXY_TEST_SET}}]",
			`{"users":[{"staticCredentials":{"username":"alice","passwordEnv":"PG_PROXY_TEST_SET","scramSHA256Env":"PG_PROXY_TEST_SET"}}]}`,
			"user 0: password and scram_sha_256 are mutually exclusive",
		},
		"user missing verifier file": {
			fmt.Sprintf("users: [{static_credentials: {username: alice, scram_sha_256_file: %q}}]", missing),
			fmt.Sprintf(`{"users":[{"staticCredentials":{"username":"alice","scramSHA256File":%q}}]}`, missing),
			"user 0: scram_sha_256_file: file not found",
		},
		"user invalid verifier file": {
			fmt.Sprintf("users: [{static_credentials: {username: alice, scram_sha_256_file: %q}}]", invalidVerifier),
			fmt.Sprintf(`{"users":[{"staticCredentials":{"username":"alice","scramSHA256File":%q}}]}`, invalidVerifier),
			"user 0: invalid SCRAM-SHA-256 verifier",
		},
		"wrong case": {
			"databases: [{name: db, username: u, passwordFile: x}]",
			`{"databases":[{"name":"db","username":"u","password_file":"x"}]}`,
			"",
		},
	} {
		t.Run(name, func(t *testing.T) {
			for format, document := range map[string]string{"yaml": tc.yaml, "json": tc.json} {
				_, err := readTestConfig(format, document)
				require.Error(t, err, format)
				if tc.err != "" {
					require.EqualError(t, err, tc.err, format)
				}
				for _, sentinel := range []string{"secret-sentinel", "missing-path-sentinel", "UNSET_SENTINEL", "verifier-sentinel", present} {
					require.NotContains(t, err.Error(), sentinel, format)
				}
			}
		})
	}
}

func TestParseConfigResolvesBackendPassword(t *testing.T) {
	t.Setenv("PG_PROXY_TEST_BACKEND", "backend-env-secret")
	cfg, err := parseConfig([]string{"-config", writeConfig(t, "yaml", `databases:
- {name: primary, host: localhost, dbname: postgres, username: postgres, password_env: PG_PROXY_TEST_BACKEND}
clusters: {orders: {primary: primary}}
logical_databases: {client-db: {cluster: orders}}
`), "-telemetry", "none"})
	require.NoError(t, err)
	require.Equal(t, "backend-env-secret", cfg.databases["primary"].ConnConfig.Password)
}
