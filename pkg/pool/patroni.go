// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package pool

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// PatroniProbe describes one Patroni role-health request.
type PatroniProbe struct {
	IP     string
	Path   string
	Client *http.Client
}

// ProbePatroni performs one Patroni GET request. Patroni responds with 503
// when a server does not match the requested role. systemID is the server's
// database_system_identifier, shared by every member of one Postgres cluster.
// It is empty when the body does not carry it.
func ProbePatroni(probe PatroniProbe) (healthy bool, systemID string, statusCode int, err error) {
	// TODO: Accept the URL and TLS/request policy from the embedder. This keeps
	// the current Datadog Patroni endpoint (https://<ip>:8009) for extraction
	// parity. Changing it would break production health probes.
	response, err := probe.Client.Get(fmt.Sprintf("https://%s:8009%s", probe.IP, probe.Path))
	if response == nil {
		return false, "", 0, err
	}
	defer response.Body.Close()
	var body struct {
		SystemID json.RawMessage `json:"database_system_identifier"`
	}
	_ = json.NewDecoder(io.LimitReader(response.Body, 64<<10)).Decode(&body)
	return response.StatusCode == http.StatusOK, strings.Trim(string(body.SystemID), `"`), response.StatusCode, err
}
