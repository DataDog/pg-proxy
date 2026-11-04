package main

import (
	"errors"
	"net/netip"
)

// Only a successfully constructed authentication method may enable remote
// listeners. TLS by itself never enables remote trust.
func (c config) validateListener(authenticated bool) error {
	address, err := netip.ParseAddrPort(c.listenAddress)
	if err != nil {
		if !authenticated {
			return errors.New("proxy.listener_address must use a loopback IP and port in trust mode")
		}
		return errors.New("proxy.listener_address must be an IP address and port")
	}
	if authenticated {
		if !c.requireTLS || c.tlsConfig == nil {
			return errors.New("client authentication requires proxy.tls_required and a TLS certificate/key pair")
		}
	} else if !address.Addr().IsLoopback() {
		return errors.New("proxy.listener_address must use a loopback IP unless client authentication is enabled")
	}
	if c.requireTLS && c.tlsConfig == nil {
		return errors.New("proxy.tls_required requires tls_cert_file and tls_key_file")
	}
	return nil
}
