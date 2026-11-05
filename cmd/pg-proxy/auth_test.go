package main

import (
	"crypto/tls"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestListenerAuthenticationPolicy(t *testing.T) {
	for _, address := range []string{"127.0.0.1:0", "[::1]:15432", "0.0.0.0:15432", "[::]:15432"} {
		for _, authenticated := range []bool{false, true} {
			for _, required := range []bool{false, true} {
				for _, certificate := range []bool{false, true} {
					cfg := config{listenAddress: address, requireTLS: required}
					if certificate {
						cfg.tlsConfig = &tls.Config{}
					}
					err := cfg.validateListener(authenticated)
					allowed := authenticated && required && certificate || !authenticated && (address == "127.0.0.1:0" || address == "[::1]:15432") && (!required || certificate)
					require.Equal(t, allowed, err == nil, "%s authenticated=%t required=%t certificate=%t: %v", address, authenticated, required, certificate, err)
				}
			}
		}
	}
	for _, address := range []string{"localhost:15432", ":15432", "invalid"} {
		require.Error(t, (config{listenAddress: address, requireTLS: true, tlsConfig: &tls.Config{}}).validateListener(true))
	}
}
