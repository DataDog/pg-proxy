// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package config

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestExistingYAMLShape(t *testing.T) {
	document := `databases:
- name: write
  host: localhost
  dbname: postgres
  sslmode: disable
  max_conns: 7
  connection_options: {statement_timeout: "5000"}
  load_balanced_servers: [{host: node-a}, {host: node-b}]
clusters:
  cluster: {primary: write}
logical_databases:
  app:
    cluster: cluster
    read_clusters: {cluster: {weight: 0}}
proxy:
  listener_address: 127.0.0.1:15433
  tls_required: true
  tls_cert_file: cert.pem
  tls_key_file: key.pem
  postgres_msg_buffer_size: 8192
users:
- static_credentials: {username: client}
  databases: [app]
  pg_roles: [reader]
  max_sessions: 4
mirroring: {enabled: false}
`
	cfg, err := ReadYAML(strings.NewReader(document))
	if err != nil {
		t.Fatal(err)
	}
	db := cfg.Databases[0]
	if db.DBName != "postgres" || db.MaxConns != 7 || len(db.LoadBalancedServers) != 2 || db.ConnectionOptions["statement_timeout"] != "5000" {
		t.Fatalf("lost flat database settings: %+v", db)
	}
	if !cfg.Proxy.TLSRequired || cfg.Proxy.ListenerAddress != "127.0.0.1:15433" || cfg.Proxy.TLSCertFile != "cert.pem" || cfg.Proxy.TLSKeyFile != "key.pem" || cfg.Proxy.PostgresMsgBufferSize != 8192 {
		t.Fatal("lost listener/TLS settings")
	}
	if len(cfg.Users) != 1 || cfg.Users[0].StaticCredentials.Username != "client" || cfg.Users[0].MaxSessions != 4 || *cfg.LogicalDatabases["app"].ReadClusters["cluster"].Weight != 0 {
		t.Fatal("lost user/routing settings")
	}
	// JSON keeps the existing camelCase names and flat database structure.
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{`"connection":`, `"pool":`, `"mode":`, `"servers":`, `"logical_databases":`} {
		if strings.Contains(string(data), forbidden) {
			t.Fatalf("unexpected config shape: %s", data)
		}
	}
	roundTrip, err := ReadJSON(strings.NewReader(string(data)))
	if err != nil || !reflect.DeepEqual(cfg, roundTrip) {
		t.Fatalf("config did not round-trip: %v", err)
	}
	for _, invalid := range []string{document + "---\n{}", strings.Replace(document, "tls_required:", "tls_requred:", 1), strings.Replace(document, "host: localhost", "password: secret-value", 1)} {
		_, err := ReadYAML(strings.NewReader(invalid))
		if err == nil || strings.Contains(err.Error(), "secret-value") {
			t.Fatalf("unsafe or missing error: %v", err)
		}
	}
}

func TestReadJSONPreservesUnsetAndZero(t *testing.T) {
	cfg, err := ReadJSON(strings.NewReader(`{"databases":[{"name":"db"}],"logicalDatabases":{"db":{"readClusters":{"default":{},"disabled":{"weight":0}}}}}`))
	if err != nil {
		t.Fatal(err)
	}
	reads := cfg.LogicalDatabases["db"].ReadClusters
	if reads["default"].Weight != nil || reads["disabled"].Weight == nil || *reads["disabled"].Weight != 0 {
		t.Fatal("lost omitted versus explicit zero weight")
	}
	if cfg.Databases[0].MaxConns != 0 || cfg.Databases[0].Port != 0 {
		t.Fatal("reader applied runtime defaults")
	}
	for _, document := range []string{`{} {}`, `{} trailing`, `{"unknown":"private-value"}`, `{"proxy":{"tlsRequired":"private-value"}}`} {
		_, err := ReadJSON(strings.NewReader(document))
		if err == nil || strings.Contains(err.Error(), "private-value") {
			t.Fatal("missing or unsafe error", err)
		}
	}
}

func TestClientDisconnectTimerSetting(t *testing.T) {
	fromYAML, err := ReadYAML(strings.NewReader("proxy:\n  client_disconnect_timer: true\n"))
	if err != nil {
		t.Fatal(err)
	}
	fromJSON, err := ReadJSON(strings.NewReader(`{"proxy":{"clientDisconnectTimer":true}}`))
	if err != nil {
		t.Fatal(err)
	}
	if !fromYAML.Proxy.ClientDisconnectTimer || fromYAML.Proxy != fromJSON.Proxy {
		t.Fatalf("YAML and JSON must both enable the timer: %+v, %+v", fromYAML.Proxy, fromJSON.Proxy)
	}
	omitted, err := ReadYAML(strings.NewReader("proxy:\n  listener_address: 127.0.0.1:5432\n"))
	if err != nil {
		t.Fatal(err)
	}
	if omitted.Proxy.ClientDisconnectTimer {
		t.Fatal("a file without the field must keep the goroutine monitor")
	}
	data, err := json.Marshal(omitted)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "clientDisconnectTimer") {
		t.Fatalf("an unset field must not appear in serialized config: %s", data)
	}
}

func TestEmptyConfigSections(t *testing.T) {
	data, err := json.Marshal(Config{})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"users":null,"logicalDatabases":null,"clusters":null,"databases":null,"proxy":{"listenerAddress":"","jwtAuthEnabled":false,"tlsRequired":false,"postgresMsgBufferSize":0}}`
	if string(data) != want {
		t.Fatalf("unexpected empty sections: %s", data)
	}
	cfg, err := ReadJSON(strings.NewReader(string(data)))
	if err != nil || !reflect.DeepEqual(cfg, Config{}) {
		t.Fatalf("empty config did not round-trip: %v", err)
	}
}
