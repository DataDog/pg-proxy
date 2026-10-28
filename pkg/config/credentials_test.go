package config

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func testVerifier() string {
	key := base64.StdEncoding.EncodeToString(make([]byte, 32))
	return "SCRAM-SHA-256$4096:c2FsdA==$" + key + ":" + key
}

func TestInlineCredentialsAndRedaction(t *testing.T) {
	verifier := testVerifier()
	for _, format := range []string{"json", "yaml"} {
		t.Run(format, func(t *testing.T) {
			var cfg Config
			var err error
			if format == "json" {
				cfg, err = ReadJSON(strings.NewReader(fmt.Sprintf(`{"databases":[{"name":"db","host":"localhost","dbName":"postgres","username":"backend","password":"backend-secret"}],"users":[{"staticCredentials":{"username":"alice","password":"client-secret"}},{"staticCredentials":{"username":"bob","scramSHA256":%q}}]}`, verifier)))
			} else {
				cfg, err = ReadYAML(strings.NewReader(fmt.Sprintf("databases:\n- {name: db, host: localhost, dbname: postgres, username: backend, password: backend-secret}\nusers:\n- static_credentials: {username: alice, password: client-secret}\n- static_credentials: {username: bob, scram_sha_256: %q}\n", verifier)))
			}
			if err != nil {
				t.Fatal(err)
			}
			if string(cfg.Databases[0].Password) != "backend-secret" || string(cfg.Users[0].StaticCredentials.Password) != "client-secret" || string(cfg.Users[1].StaticCredentials.SCRAMSHA256) != verifier {
				t.Fatal("lost input credentials")
			}
			j, err := json.Marshal(cfg)
			if err != nil {
				t.Fatal(err)
			}
			y, err := yaml.Marshal(cfg)
			if err != nil {
				t.Fatal(err)
			}
			for _, output := range []string{string(j), string(y), fmt.Sprintf("%v %+v %#v", cfg, cfg, cfg)} {
				for _, secret := range []string{"backend-secret", "client-secret", verifier} {
					if strings.Contains(output, secret) {
						t.Fatal("config output leaked a secret")
					}
				}
				if !strings.Contains(output, "[REDACTED]") {
					t.Fatal("missing redaction")
				}
			}
		})
	}
}

func TestInvalidCredentialSources(t *testing.T) {
	for _, fields := range []string{
		`"password":"private-value"`,
		`"username":"user","password":"private-value","credentials":{"name":"PG"}`,
		`"username":"user","credentials":{"revision":"v1"}`,
	} {
		_, err := ReadJSON(strings.NewReader(`{"databases":[{"name":"db","host":"localhost","dbName":"postgres",` + fields + `}]}`))
		if err == nil || strings.Contains(err.Error(), "private-value") {
			t.Fatal("missing or unsafe error", err)
		}
	}
	for _, creds := range []StaticCredentials{
		{Password: "private-value"},
		{Username: "u", Password: "private-value", SCRAMSHA256: Secret(testVerifier())},
		{Username: "u", Password: "private-value", CredentialRef: CredentialRef{Name: "PG"}},
		{Username: "u", SCRAMSHA256: Secret(testVerifier()), CredentialRef: CredentialRef{Name: "PG"}},
		{Username: "u", SCRAMSHA256: "private-value"},
		{Username: "u", SCRAMSHA256: Secret(strings.Replace(testVerifier(), "4096:", "0:", 1))},
		{Username: "u", SCRAMSHA256: "SCRAM-SHA-256$4096:c2FsdA==$YQ==:YQ=="},
	} {
		err := (Config{Users: []User{{StaticCredentials: creds}}}).ValidateCredentials()
		if err == nil || strings.Contains(err.Error(), "private-value") {
			t.Fatal("missing or unsafe error", err)
		}
	}
}

func TestDatabaseSettingsAllowsApplicationOwnedCredentials(t *testing.T) {
	type appDatabase struct {
		DatabaseSettings `yaml:",inline"`
		Credentials      struct {
			Name string `json:"name" yaml:"name"`
		} `json:"credentials" yaml:"credentials"`
	}
	var cfg appDatabase
	decoder := yaml.NewDecoder(strings.NewReader("host: localhost\ndbname: orders\nmax_conns: 7\ncredentials: {name: my-secret}\n"))
	decoder.KnownFields(true)
	if err := decoder.Decode(&cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Host != "localhost" || cfg.Database != "orders" || cfg.MaxConns != 7 || cfg.Credentials.Name != "my-secret" {
		t.Fatal("lost shared settings or application credentials")
	}
	for _, document := range []string{`{"password":"secret"}`, `{"credentials":{"password":"secret"}}`} {
		decoder := json.NewDecoder(strings.NewReader(document))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&appDatabase{}); err == nil {
			t.Fatal("accepted inline credentials")
		}
	}
	decoder = yaml.NewDecoder(strings.NewReader("password: secret\n"))
	decoder.KnownFields(true)
	if err := decoder.Decode(&appDatabase{}); err == nil {
		t.Fatal("accepted YAML inline credentials")
	}
}
