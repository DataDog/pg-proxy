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
