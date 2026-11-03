package warden

import (
	"encoding/json"
	"fmt"

	pgproxy "github.com/DataDog/pg-proxy"
)

// Takes any go value and marshals it to JSON for better readability
// This is used to format and log the configuration values for the proxy.
func logConfigs(label string, value any) {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		pgproxy.Logger().Error(fmt.Sprintf("Failed to marshal %s: %v", label, err))
		return
	}
	pgproxy.Logger().Info(fmt.Sprintf("Updated %s:\n%s", label, data))
}
