package main

import (
	"context"
	"errors"
	"net/netip"
	"strings"

	"github.com/DataDog/pg-proxy/pkg/auth"
	pgconfig "github.com/DataDog/pg-proxy/pkg/config"
	"github.com/DataDog/pg-proxy/pkg/listener"
	"github.com/DataDog/pg-proxy/pkg/protocol"
	"github.com/DataDog/pg-proxy/pkg/scram"
	"github.com/DataDog/pg-proxy/pkg/server"
)

type authenticationHook = func(context.Context, listener.StartupRequest, listener.StartupResult[server.ResolvedStartup], protocol.Exchange) (listener.AuthenticationResult, error)

func passwordAuthentication(shared pgconfig.Config) (authenticationHook, error) {
	users := make([]auth.StaticUser, 0, len(shared.Users))
	for _, user := range shared.Users {
		if user.Email != "" || user.Group != "" || len(user.PGRoles) != 0 || user.MaxSessions != 0 {
			return nil, errors.New("password authentication supports only static_credentials and databases in each user")
		}
		credentials := user.StaticCredentials
		if credentials.Username == "" || len(credentials.Username) > 256 || strings.ContainsRune(credentials.Username, 0) || (credentials.Password == "") == (credentials.SCRAMSHA256 == "") {
			return nil, errors.New("each proxy user requires a username and exactly one password or SCRAM verifier")
		}
		for _, database := range user.Databases {
			if _, exists := shared.LogicalDatabases[database]; !exists {
				return nil, errors.New("allowed databases must refer to configured logical databases")
			}
		}
		var verifier *scram.Verifier
		var err error
		if credentials.Password != "" {
			verifier, err = scram.NewVerifier(string(credentials.Password))
		} else {
			verifier, err = scram.ParseVerifier(string(credentials.SCRAMSHA256))
		}
		if err != nil {
			return nil, err
		}
		users = append(users, auth.StaticUser{Username: credentials.Username, Verifier: verifier, Databases: user.Databases})
	}
	static, err := auth.NewStatic(users)
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context, request listener.StartupRequest, resolved listener.StartupResult[server.ResolvedStartup], ex protocol.Exchange) (listener.AuthenticationResult, error) {
		if request.Options.PassthroughUser {
			return listener.AuthenticationResult{}, listener.ErrUnsupportedAuthOption
		}
		if err := static.Authenticate(ctx, request.User, resolved.LogicalDatabase, ex); err != nil {
			return listener.AuthenticationResult{}, err
		}
		return listener.AuthenticationResult{Service: request.User}, nil
	}, nil
}

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
