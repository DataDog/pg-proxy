// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package config

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPasswordAuthenticationSelectionField(t *testing.T) {
	jsonConfig, err := ReadJSON(strings.NewReader(`{"proxy":{"passwordAuthEnabled":true}}`))
	require.NoError(t, err)
	yamlConfig, err := ReadYAML(strings.NewReader("proxy:\n  password_auth_enabled: true\n"))
	require.NoError(t, err)
	require.Equal(t, jsonConfig, yamlConfig)
	require.True(t, jsonConfig.Proxy.PasswordAuthEnabled)
	defaultConfig, err := ReadJSON(strings.NewReader(`{}`))
	require.NoError(t, err)
	require.False(t, defaultConfig.Proxy.PasswordAuthEnabled)
}
